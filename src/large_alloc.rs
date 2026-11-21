//! Linux only. The *requested size* selects the allocator for the entire
//! lifetime of each allocation. Never fall back to mimalloc for a large
//! request: dealloc only has the Layout, not an in-band tag or address table.
use std::alloc::{GlobalAlloc, Layout};
use std::ptr::{self, null_mut};
use std::sync::atomic::{AtomicPtr, Ordering};

use mimalloc::MiMalloc;

const LARGE: usize = 1 << 20;
const SLOTS: usize = 6;

pub(super) struct Hybrid {
    // At most one *whole, freed* mapping at each of 1, 2, 4, 8, 16 and
    // 32 MiB (63 MiB total, independent of thread count). No extra rounding
    // to fit a bin. No locks, recursion, TLS, or eviction on the alloc path.
    cache: [AtomicPtr<u8>; SLOTS],
}

impl Hybrid {
    pub(super) const fn new() -> Self {
        Self {
            cache: [const { AtomicPtr::new(null_mut()) }; SLOTS],
        }
    }

    fn slot(&self, size: usize) -> Option<&AtomicPtr<u8>> {
        if size >= LARGE && size.is_power_of_two() {
            self.cache
                .get((size.trailing_zeros() - LARGE.trailing_zeros()) as usize)
        } else {
            None
        }
    }

    unsafe fn large(&self, layout: Layout, zeroed: bool) -> *mut u8 {
        let size = layout.size();
        let page = libc::sysconf(libc::_SC_PAGESIZE) as usize;
        if !page.is_power_of_two() {
            return null_mut();
        }
        let p = if layout.align() <= page {
            if let Some(slot) = self.slot(size) {
                let reused = slot.swap(null_mut(), Ordering::Acquire);
                if !reused.is_null() {
                    if zeroed {
                        ptr::write_bytes(reused, 0, size);
                    }
                    return reused;
                }
            }
            map(size)
        } else {
            // The trimmed mapping starts at the returned pointer and covers
            // exactly ceil(size/page) pages, just like an ordinary mmap.
            let rounded = size.next_multiple_of(page); // size <= isize::MAX
            let Some(reserve) = rounded.checked_add(layout.align() - page) else {
                return null_mut();
            };
            if reserve > isize::MAX as usize {
                return null_mut();
            }
            let base = map(reserve);
            if base.is_null() {
                return null_mut();
            }
            let prefix = base.addr().wrapping_neg() & (layout.align() - 1);
            let aligned = base.add(prefix);
            let suffix = reserve - prefix - rounded;
            if prefix != 0 && libc::munmap(base.cast(), prefix) != 0 {
                libc::munmap(base.cast(), reserve);
                return null_mut();
            }
            if suffix != 0 && libc::munmap(aligned.add(rounded).cast(), suffix) != 0 {
                // The prefix is already gone; never free it a second time.
                libc::munmap(aligned.cast(), reserve - prefix);
                return null_mut();
            }
            aligned
        };
        if !p.is_null() {
            // Best effort, only within our own fresh private anonymous map.
            libc::madvise(p.cast(), size, libc::MADV_HUGEPAGE);
        }
        p
    }
}

unsafe fn map(size: usize) -> *mut u8 {
    // NORESERVE is advisory under Linux overcommit policy. Faulted pages
    // still count against the cgroup memory cap; mmap success isn't a RAM guarantee.
    let p = libc::mmap(
        null_mut(),
        size,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
        -1,
        0,
    );
    if p == libc::MAP_FAILED {
        return null_mut();
    }
    if p.is_null() {
        // Null is reserved as GlobalAlloc's failure sentinel.
        libc::munmap(p, size);
    }
    p.cast()
}

unsafe impl GlobalAlloc for Hybrid {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() < LARGE {
            MiMalloc.alloc(layout)
        } else {
            self.large(layout, false)
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() < LARGE {
            MiMalloc.alloc_zeroed(layout)
        } else {
            self.large(layout, true)
        }
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        if layout.size() < LARGE {
            MiMalloc.dealloc(p, layout);
            return;
        }
        if let Some(slot) = self.slot(layout.size()) {
            // Only this transition donates pages, *after* the last owner has
            // relinquished them. Never write to p again on a successful CAS.
            if slot
                .compare_exchange(null_mut(), p, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
        libc::munmap(p.cast(), layout.size());
    }

    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if layout.size() < LARGE && size < LARGE {
            return MiMalloc.realloc(p, layout, size);
        }
        if layout.size() >= LARGE && size >= LARGE {
            // MAYMOVE only promises page alignment. Over-aligned allocations
            // may grow/shrink in place; otherwise use an aligned replacement.
            let page = libc::sysconf(libc::_SC_PAGESIZE) as usize;
            let flags = if page.is_power_of_two() && layout.align() <= page {
                libc::MREMAP_MAYMOVE
            } else {
                0
            };
            let q = libc::mremap(p.cast(), layout.size(), size, flags);
            if q != libc::MAP_FAILED {
                return q.cast();
            }
        }
        // A failed (non-FIXED) mremap leaves p live. This path also handles
        // allocator crossings, so every successful result follows the same
        // size-based dispatch as a fresh allocation.
        let new = Layout::from_size_align_unchecked(size, layout.align());
        let q = self.alloc(new);
        if !q.is_null() {
            ptr::copy_nonoverlapping(p, q, layout.size().min(size));
            self.dealloc(p, layout);
        }
        q
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests use private bins, never the process-global allocator's cache.
    impl Drop for Hybrid {
        fn drop(&mut self) {
            for (n, slot) in self.cache.iter_mut().enumerate() {
                let p = *slot.get_mut();
                if !p.is_null() {
                    unsafe { libc::munmap(p.cast(), LARGE << n) };
                }
            }
        }
    }

    fn layout(size: usize, align: usize) -> Layout {
        Layout::from_size_align(size, align).unwrap()
    }

    #[test]
    fn dirty_donation_is_exclusive_bounded_and_zeroed_on_request() {
        unsafe {
            let alloc = Hybrid::new();
            let l = layout(LARGE, 64);
            let a = alloc.alloc_zeroed(l);
            let b = alloc.alloc(l);
            assert!(!a.is_null() && !b.is_null() && a != b);
            assert!(std::slice::from_raw_parts(a, LARGE).iter().all(|&x| x == 0));
            ptr::write_bytes(a, 0xa7, LARGE);
            ptr::write_bytes(b, 0x5b, LARGE);
            alloc.dealloc(a, l);
            alloc.dealloc(b, l); // bin full, not a second retained mapping
            let c = alloc.alloc_zeroed(l);
            assert_eq!(a, c);
            assert!(std::slice::from_raw_parts(c, LARGE).iter().all(|&x| x == 0));
            assert!(alloc.slot(LARGE).unwrap().load(Ordering::Relaxed).is_null());
            let d = alloc.alloc(l);
            assert_ne!(c, d);
            assert!(!d.is_null());
            alloc.dealloc(c, l);
            alloc.dealloc(d, l);
            assert!(alloc.slot(LARGE + 1).is_none());
            assert!(alloc.slot(64 * LARGE).is_none());
        }
    }

    #[test]
    fn shrink_grow_and_cross_allocator_boundaries_preserve_prefix_and_alignment() {
        unsafe {
            for align in [8, 4096, 2 * LARGE] {
                let alloc = Hybrid::new();
                let mut l = layout(LARGE - 1, align);
                let mut p = alloc.alloc(l);
                for size in [LARGE, LARGE + 31, 4 * LARGE + 7, LARGE + 3, 133] {
                    assert!(!p.is_null());
                    assert_eq!(p.addr() % align, 0);
                    ptr::write_bytes(p, 0x31, l.size());
                    p = alloc.realloc(p, l, size);
                    assert!(!p.is_null());
                    assert_eq!(p.addr() % align, 0);
                    assert!(std::slice::from_raw_parts(p, size.min(l.size()))
                        .iter()
                        .all(|&x| x == 0x31));
                    l = layout(size, align);
                }
                alloc.dealloc(p, l);
            }
        }
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn impossible_resize_leaves_original_owned_and_writable() {
        unsafe {
            let alloc = Hybrid::new();
            let l = layout(LARGE + 1, 8);
            let p = alloc.alloc(l);
            assert!(!p.is_null());
            ptr::write_bytes(p, 0x19, l.size());
            // Valid Layout, but larger than the Linux user address space:
            // neither mremap nor a fresh replacement can succeed.
            let impossible = (isize::MAX as usize) & !4095;
            assert!(alloc.realloc(p, l, impossible).is_null());
            assert!(std::slice::from_raw_parts(p, l.size())
                .iter()
                .all(|&x| x == 0x19));
            *p.add(LARGE) = 0x2a;
            assert_eq!(*p.add(LARGE), 0x2a);
            alloc.dealloc(p, l);
        }
    }
}
