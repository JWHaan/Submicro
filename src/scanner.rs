//! CSV here is not RFC 4180: every comma and every LF is a delimiter, even in
//! a malformed quoted field. Only a matching pair of whole-field wrappers is
//! syntax. Zero bytes, spaces and CR are ordinary data.

/// Tasks start and end on physical line boundaries (or at file end). Hand out
/// slices of the original task, not u32 offsets that could truncate or be
/// mistaken for absolute file offsets. An empty slice means null.
pub(super) fn each_line<'a, const N: usize>(
    file: &'a [u8],
    lo: usize,
    hi: usize,
    header: bool,
    f: impl FnMut(&[&'a [u8]; N]),
) {
    let mut data = &file[lo..hi];
    if header && lo == 0 {
        let Some(end) = memchr::memchr(b'\n', data) else {
            return;
        };
        data = &data[end + 1..];
    }
    // Dispatch once per entire task, not per vector, field, or row.
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("avx2") {
        unsafe { scan_avx2(data, f) };
        return;
    }
    scan(data, f, portable_masks);
}

#[derive(Default, PartialEq, Eq, Debug)]
struct Masks {
    sep: u64, // comma or LF
    lf: u64,
    quote: u64,
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn scan_avx2<'a, const N: usize>(data: &'a [u8], f: impl FnMut(&[&'a [u8]; N])) {
    scan(data, f, |block| unsafe { avx2_masks(block) });
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn avx2_masks(block: &[u8; 64]) -> Masks {
    use std::arch::x86_64::*;
    let p = block.as_ptr();
    // The fixed-size reference covers both unaligned loads, including at EOF.
    let a = unsafe { _mm256_loadu_si256(p.cast()) };
    let b = unsafe { _mm256_loadu_si256(p.add(32).cast()) };
    let mask = |c| {
        let v = _mm256_set1_epi8(c);
        let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32;
        let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32;
        lo as u64 | ((hi as u64) << 32)
    };
    let lf = mask(b'\n' as i8);
    Masks {
        sep: mask(b',' as i8) | lf,
        lf,
        quote: mask(b'"' as i8),
    }
}

/// Eight exact equality bits, packed in input order, without target features.
#[inline(always)]
fn eq8(w: u64, c: u8) -> u8 {
    let x = w ^ u64::from_le_bytes([c; 8]);
    const LOW: u64 = 0x7f7f_7f7f_7f7f_7f7f;
    let high_bits = !(((x & LOW) + LOW) | x | LOW);
    ((high_bits >> 7).wrapping_mul(0x0102_0408_1020_4080) >> 56) as u8
}

#[inline]
fn portable_masks(block: &[u8; 64]) -> Masks {
    let mut m = Masks::default();
    for (i, bytes) in block.chunks_exact(8).enumerate() {
        let w = u64::from_le_bytes(bytes.try_into().unwrap());
        let shift = i * 8;
        m.sep |= (eq8(w, b',') as u64) << shift;
        m.lf |= (eq8(w, b'\n') as u64) << shift;
        m.quote |= (eq8(w, b'"') as u64) << shift;
    }
    m.sep |= m.lf;
    m
}

fn tail_masks(tail: &[u8]) -> Masks {
    let mut m = Masks::default();
    for (i, &c) in tail.iter().enumerate() {
        match c {
            b',' => m.sep |= 1 << i,
            b'\n' => m.lf |= 1 << i,
            b'"' => m.quote |= 1 << i,
            _ => (),
        }
    }
    m.sep |= m.lf;
    m
}

/// Map each mark to the next stop, including a carry from the previous block.
/// Marks never overlap stops. Each run of non-stops consists of ones in
/// `!stops`; adding any marks in that run carries into its terminating zero.
#[inline(always)]
fn next_stops(marks: u64, stops: u64, carry: &mut bool) -> u64 {
    let (x, c1) = (!stops).overflowing_add(marks);
    let (y, c2) = x.overflowing_add(*carry as u64);
    *carry = c1 || c2;
    y & stops
}

struct State<'a, const N: usize> {
    fields: [&'a [u8]; N],
    n: usize,
    start: usize,
    prev_sep: u64,
    prev_nonopening_quote: u64,
    pending_open: bool,
    pending_error: bool,
}

impl<'a, const N: usize> State<'a, N> {
    #[inline(always)]
    fn consume(
        &mut self,
        data: &'a [u8],
        base: usize,
        m: Masks,
        f: &mut impl FnMut(&[&'a [u8]; N]),
    ) {
        let open = m.quote & ((m.sep << 1) | self.prev_sep);
        let other = m.quote ^ open;
        let after_other = (other << 1) | self.prev_nonopening_quote;
        self.prev_sep = m.sep >> 63;
        self.prev_nonopening_quote = other >> 63;

        // Every opening quote requires exactly one other quote, immediately
        // before this field's separator. XOR catches both missing closes and
        // stray quotes. A singleton quote cannot double as its own close.
        let expect_close = next_stops(open, m.sep, &mut self.pending_open);
        let errors = expect_close ^ after_other;
        let bad_lines = if errors != 0 || self.pending_error {
            (errors & m.lf) | next_stops(errors & !m.lf, m.lf, &mut self.pending_error)
        } else {
            0
        };
        let wrapped = expect_close & after_other;

        let mut seps = m.sep;
        while seps != 0 {
            let i = seps.trailing_zeros();
            let bit = seps & seps.wrapping_neg();
            seps &= seps - 1;
            let end = base + i as usize;
            let blank = N == 1 && end == self.start;
            if self.n < N {
                // A bit in `wrapped` requires *distinct* opening and closing
                // bytes, so stripping both is safe even in a rejected row.
                let strip = ((wrapped >> i) & 1) as usize;
                self.fields[self.n] = &data[self.start + strip..end - strip];
            }
            self.n += 1;
            self.start = end + 1;
            if m.lf & bit != 0 {
                if self.n == N && bad_lines & bit == 0 && !blank {
                    f(&self.fields);
                }
                self.n = 0;
            }
        }
    }
}

#[inline(always)]
fn scan<'a, const N: usize>(
    data: &'a [u8],
    mut f: impl FnMut(&[&'a [u8]; N]),
    classify: impl Fn(&[u8; 64]) -> Masks,
) {
    let mut st = State {
        fields: [&[][..]; N],
        n: 0,
        start: 0,
        prev_sep: 1,
        prev_nonopening_quote: 0,
        pending_open: false,
        pending_error: false,
    };
    let blocks = data.chunks_exact(64);
    let tail = blocks.remainder();
    for (i, block) in blocks.enumerate() {
        st.consume(data, i * 64, classify(block.try_into().unwrap()), &mut f);
    }
    let mut m = tail_masks(tail);
    if !data.is_empty() && !data.ends_with(b"\n") {
        // Same finalization as LF, without manufacturing a byte in the slice.
        m.lf |= 1 << tail.len();
        m.sep |= m.lf;
    }
    st.consume(data, data.len() - tail.len(), m, &mut f);
}

#[cfg(test)]
#[path = "scanner_tests.rs"]
mod tests;
