use super::*;

// Literal split/validate oracle. Save offsets as well as lengths: duplicate
// values and nulls would otherwise hide some chunk-base bugs.
fn spans<const N: usize>(file: &[u8], row: &[&[u8]; N]) -> [(usize, usize); N] {
    row.map(|s| (s.as_ptr() as usize - file.as_ptr() as usize, s.len()))
}

fn oracle<const N: usize>(
    file: &[u8],
    lo: usize,
    hi: usize,
    header: bool,
) -> Vec<[(usize, usize); N]> {
    let mut lines = file[lo..hi].split(|&c| c == b'\n');
    if header && lo == 0 {
        lines.next();
    }
    let mut out = Vec::new();
    'row: for line in lines {
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split(|&c| c == b',');
        let mut result = [&[][..]; N];
        for dst in &mut result {
            let Some(raw) = fields.next() else {
                continue 'row;
            };
            let value = match raw {
                [b'"', inner @ .., b'"'] => inner,
                _ => raw,
            };
            if value.contains(&b'"') {
                continue 'row;
            }
            *dst = value;
        }
        if fields.next().is_none() {
            out.push(spans(file, &result));
        }
    }
    out
}

fn compare<const N: usize>(file: &[u8], lo: usize, hi: usize, header: bool) {
    let want = oracle::<N>(file, lo, hi, header);
    let mut got = Vec::new();
    each_line(file, lo, hi, header, |row| got.push(spans(file, row)));
    assert_eq!(want, got, "task {lo}..{hi}, ncols={N}, header={header}");
    // On x86 also exercise the non-dispatched portable full-block path.
    #[cfg(target_arch = "x86_64")]
    {
        let mut data = &file[lo..hi];
        if header && lo == 0 {
            data = data
                .iter()
                .position(|&c| c == b'\n')
                .map_or(&[][..], |i| &data[i + 1..]);
        }
        got.clear();
        scan(data, |row| got.push(spans(file, row)), portable_masks);
        assert_eq!(want, got);
    }
}

#[test]
fn full_block_masks_all_bytes_all_lanes_and_alignments() {
    let mut buf = [0u8; 128];
    for i in 0..64 {
        // Give each shifted lane neighbours of all structural types.
        for (k, c) in buf.iter_mut().enumerate() {
            *c = b",\n\"\r\0abc"[k % 8];
        }
        for c in 0..=255 {
            buf[2 * i] = c;
            let block: &[u8; 64] = buf[i..i + 64].try_into().unwrap();
            let want = tail_masks(block);
            assert_eq!(portable_masks(block), want);
            #[cfg(target_arch = "x86_64")]
            if std::is_x86_feature_detected!("avx2") {
                assert_eq!(unsafe { avx2_masks(block) }, want);
            }
        }
    }
    // Exercise all lane subsets, including adjacent matches in both halves.
    for bits in 0..=255u8 {
        let bytes = std::array::from_fn(|i| if bits & (1 << i) != 0 { b',' } else { b'-' });
        assert_eq!(eq8(u64::from_le_bytes(bytes), b','), bits);
    }
}

fn enumerate(buf: &mut Vec<u8>, depth: usize, f: &mut impl FnMut(&[u8])) {
    f(buf);
    if depth > 0 {
        for &c in b"x,\"\n\r" {
            buf.push(c);
            enumerate(buf, depth - 1, f);
            buf.pop();
        }
    }
}

#[test]
fn exhaustive_short_inputs_and_eof() {
    // All lengths 0..=7, including sequences ending in comma/quote/LF.
    enumerate(&mut Vec::new(), 7, &mut |b| {
        compare::<1>(b, 0, b.len(), false);
        compare::<2>(b, 0, b.len(), false);
        compare::<3>(b, 0, b.len(), false);
    });
}

#[test]
fn grammar_at_every_block_seam() {
    // For each possible lane, start every <=5-byte word there, inside a
    // 3-column record. Add a known-good row after it to check LF resync.
    for lane in 0..64 {
        let mut b = Vec::new();
        enumerate(&mut Vec::new(), 5, &mut |word| {
            let pad = (lane + 63 - (b.len() % 64)) % 64;
            b.resize(b.len() + pad, b'x');
            b.push(b',');
            assert_eq!(b.len() % 64, lane);
            b.extend_from_slice(word);
            b.extend_from_slice(b",\"\"\n\r,\"ok\",z\n");
        });
        compare::<3>(&b, 0, b.len(), false);
    }
}

#[test]
fn long_fields_empty_fields_and_malformed_recovery() {
    for n in [
        1, 2, 30, 31, 32, 33, 62, 63, 64, 65, 126, 127, 128, 129, 511,
    ] {
        for pad in 0..64 {
            let x = vec![b'x'; n];
            let mut b = vec![b' '; pad];
            b.extend_from_slice(b",\"");
            b.extend_from_slice(&x);
            b.extend_from_slice(b"\",z\n\0,\""); // long but invalid: interior quote
            b.extend_from_slice(&x);
            b.push(b'"');
            b.extend_from_slice(&x);
            b.extend_from_slice(b"\",q\n,,\"unterminated");
            b.extend_from_slice(&x);
            b.extend_from_slice(b"\n,\r,ok\r\n,\"\",");
            compare::<3>(&b, 0, b.len(), false);
        }
    }
}

#[test]
fn headers_subtasks_eof_and_borrowed_offsets() {
    let mut file = b"odd,,header\"\r\n".to_vec();
    for i in 0..130 {
        file.extend_from_slice(b"\"u,"); // malformed, must not poison next LF
        file.extend_from_slice("中".as_bytes());
        file.extend_from_slice(b"\n\"u\",");
        file.resize(file.len() + i, b'v');
        file.extend_from_slice(b"\r\n");
    }
    file.extend_from_slice(b"last,\" u\0 \"");
    let whole = oracle::<2>(&file, 0, file.len(), true);
    // Splitting at *any* physical newline should concatenate to the exact
    // same offsets; `header` is deliberately true even for the later task.
    for split in file
        .iter()
        .enumerate()
        .filter(|(_, c)| **c == b'\n')
        .map(|(i, _)| i + 1)
    {
        let mut got = Vec::new();
        for (lo, hi) in [(0, split), (split, file.len())] {
            each_line(&file, lo, hi, true, |row| got.push(spans(&file, row)));
        }
        assert_eq!(got, whole);
    }
    // Header-only and partial last records, with unrelated bytes on both
    // sides of the task. Ends on every possible quote, separator and CR.
    let row = b"ignore\n\" a \",x\r\n,\"\"\n\"\r\",z\nu,\"a\"\"b\"\nu,\"\"\nu,\nu,\"";
    for lo in [0, 7] {
        for hi in lo..=row.len() {
            for header in [false, true] {
                compare::<2>(row, lo, hi, header);
            }
        }
    }
}

#[cfg(unix)]
#[test]
fn guard_pages_at_both_ends() {
    unsafe {
        let page = libc::sysconf(libc::_SC_PAGESIZE) as usize;
        let p = libc::mmap(
            std::ptr::null_mut(),
            page * 3,
            libc::PROT_NONE,
            libc::MAP_PRIVATE | libc::MAP_ANON,
            -1,
            0,
        );
        assert_ne!(p, libc::MAP_FAILED);
        let mid = p.cast::<u8>().add(page);
        assert_eq!(
            libc::mprotect(mid.cast(), page, libc::PROT_READ | libc::PROT_WRITE),
            0
        );
        let data = std::slice::from_raw_parts_mut(mid, page);
        let pattern = b"foo,\"x\",z\r\nq,,\"\"\n\"\", \"x\",z\n";
        for (i, c) in data.iter_mut().enumerate() {
            *c = pattern[i % pattern.len()];
        }
        for n in 0..=193 {
            for slice in [&data[..n], &data[page - n..]] {
                compare::<3>(slice, 0, slice.len(), false);
            }
        }
        assert_eq!(libc::munmap(p, page * 3), 0);
    }
}
