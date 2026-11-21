use super::*;

#[test]
fn inline_and_interned_keys_match_slice_order_and_identity() {
    FB.get_or_init(Fb::new);
    let mut values: Vec<Vec<u8>> = [
        &b""[..],
        b"\0",
        b"\0\0",
        b"a",
        b"A",
        b"a\0",
        b"a ",
        b" a",
        b"a\0x",
        b"a\0\0",
        b"123456789abcdef",
        b"123456789abcdef\0",
        b"123456789abcdef ",
        b"123456789abcdeg",
        b"123456789abcdeg ",
        b"123456789abcdeg!\0",
        "é".as_bytes(),
    ]
    .into_iter()
    .map(<[u8]>::to_vec)
    .collect();
    // All length tags, including the boundary: exercise zero padding and the
    // highest possible byte, not just UTF-8 IDs typical of our inputs.
    for n in 1..=17 {
        for b in [0x00, 0xff] {
            values.push(vec![b; n]);
        }
        // Vary each byte to catch misplaced shifts in overlapping loads.
        values.push((0..n).map(|i| (0x91 + i * 11) as u8).collect());
    }
    let keys: Vec<_> = values.iter().map(|v| key_of(v)).collect();
    for (i, (s, &k)) in values.iter().zip(&keys).enumerate() {
        assert_eq!(std::mem::size_of_val(&k), 16);
        assert_eq!(k == 0, s.is_empty());
        assert_eq!(k as u8, if s.len() <= 15 { s.len() as u8 } else { 0xff });
        assert_eq!(key_bytes(k, &mut Default::default()), s);
        assert_eq!(key_of(s), k);
        for (t, &q) in values.iter().zip(&keys) {
            assert_eq!(k == q, s == t, "equality for {i}: {s:?} vs {t:?}");
            assert_eq!(key_cmp(k, q), s.cmp(t), "order for {i}: {s:?} vs {t:?}");
        }
    }
}

#[test]
fn full_keys_survive_probe_collisions_grow_and_dedup() {
    let mut seen = [None; 64];
    // Find two distinct IDs with the same initial bucket in both stages of
    // the four-row dedup below (64 counters, 32 final slots).
    let (a, b) = (0u32..=64)
        .find_map(|v| {
            let k = key_of(&v.to_be_bytes());
            seen[(key_hash(k) & 63) as usize]
                .replace(k)
                .map(|old| (old, k))
        })
        .unwrap();
    let rows = [(a, 2), (b, 1), (a, 1), (a, 1)].map(|(eid, ingest)| DRec {
        eid,
        ingest,
        gidx: 0,
    });
    let mut winners = dedup_min_d(&rows);
    winners.sort_unstable();
    assert_eq!(winners, [1, 2]); // different ID retained; min ingest, first tie

    let mut m = KeyMap::new(1);
    m.insert(a, b);
    m.insert(b, a);
    for i in 0u16..200 {
        let k = key_of(&i.to_be_bytes());
        m.insert(k, k);
    }
    assert_eq!(m.get(0), None);
    assert_eq!(m.get(a), Some(b));
    assert_eq!(m.get(b), Some(a));
}

#[cfg(unix)]
#[test]
fn no_load_crosses_input_page_boundary() {
    FB.get_or_init(Fb::new);
    unsafe {
        let ps = libc::sysconf(libc::_SC_PAGESIZE);
        assert!(ps >= 17);
        let ps = ps as usize;
        let mapping = libc::mmap(
            std::ptr::null_mut(),
            ps * 3,
            libc::PROT_NONE,
            libc::MAP_ANON | libc::MAP_PRIVATE,
            -1,
            0,
        );
        assert_ne!(mapping, libc::MAP_FAILED);
        let page = mapping.cast::<u8>().add(ps);
        assert_eq!(
            libc::mprotect(page.cast(), ps, libc::PROT_READ | libc::PROT_WRITE),
            0
        );
        let bytes = std::slice::from_raw_parts_mut(page, ps);
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = i as u8;
        }
        for n in 0..=17 {
            for input in [&bytes[..n], &bytes[ps - n..]] {
                let key = key_of(std::hint::black_box(input));
                assert_eq!(key_bytes(key, &mut [0; 16]), input);
            }
        }
        assert_eq!(libc::munmap(mapping, ps * 3), 0);
    }
}

#[test]
fn record_layout() {
    use std::mem::{align_of, size_of};
    eprintln!("size/alignment (bytes):");
    macro_rules! layout {
        ($($t:ty),+) => {$(
            eprintln!("  {} {}/{}", stringify!($t), size_of::<$t>(), align_of::<$t>());
        )+};
    }
    layout!(Key, FEv, DIn, DRec, Ev, RawCl, Cl, UdRow, (Key, u32, u32));
    assert_eq!(size_of::<Key>(), 16);
}
