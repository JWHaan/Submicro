// Shopping Sessions — v2 pipeline.
// Same semantics as the verified v1; packed u64 keys, streaming tokenizer,
// 16-24B records, open-addressing dedup, dense per-person runs.
use std::cmp::{max, min};
use std::env;
use std::fs;
use std::hash::Hasher;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

use memchr::{memchr, memchr3, memmem};
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};

use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const MIN30: i64 = 1_800_000;
const D24: i64 = 86_400_000;
const D7: i64 = 604_800_000;
const T_INVALID: i64 = i64::MIN;
const ORGANIC: &[u8] = b"organic";
const FB_TAG: u64 = 0xF << 60; // fallback key tag (top nibble); packed keys stay below

static FILES: OnceLock<Vec<&'static [u8]>> = OnceLock::new();

#[inline]
fn buf(f: u16) -> &'static [u8] {
    FILES.get().unwrap()[f as usize]
}

// ---------------------------------------------------------------------------
// Packed keys: [0-9A-Za-z_] x <=10 chars -> u64, 6 bits/char, left-aligned.
// Byte order is preserved among packed keys. key==0 is the empty sentinel.
// Longer/other ids -> fallback interner, key = FB_TAG | (local*64 + shard).
// ---------------------------------------------------------------------------
#[inline]
fn packable(s: &[u8]) -> u64 {
    if s.is_empty() || s.len() > 10 {
        return u64::MAX; // signal: not packable
    }
    let mut k = 0u64;
    for &c in s {
        let v = match c {
            b'0'..=b'9' => c - b'0' + 1,
            b'A'..=b'Z' => c - b'A' + 11,
            b'_' => 37,
            b'a'..=b'z' => c - b'a' + 38,
            _ => return u64::MAX,
        };
        k = (k << 6) | v as u64;
    }
    // Left-align in 60 bits: pad with zero digits so integer order == byte order.
    k << (6 * (10 - s.len()))
}

/// Unpack a packed key back into `out` (<=10 bytes); returns the length.
#[inline]
fn unpack(k: u64, out: &mut [u8; 10]) -> usize {
    let mut n = 0usize;
    for i in 0..10 {
        let v = ((k >> (6 * (9 - i))) & 63) as u8;
        if v == 0 {
            break;
        }
        out[i] = match v {
            1..=10 => b'0' + v - 1,
            11..=36 => b'A' + v - 11,
            37 => b'_',
            _ => b'a' + v - 38,
        };
        n += 1;
    }
    n
}

#[inline]
fn fx64(k: u64) -> u64 {
    let h = k.wrapping_mul(0x9E3779B97F4A7C15);
    h ^ (h >> 33)
}

fn fxh(s: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write(s);
    h.finish()
}

/// Flat open-addressing u64->u64 map (key 0 = empty). One cache line per probe.
struct U64Map {
    keys: Vec<u64>,
    vals: Vec<u64>,
    mask: usize,
    len: usize,
}
impl U64Map {
    fn new(min_cap: usize) -> Self {
        let cap = (min_cap.max(16) * 2).next_power_of_two();
        U64Map {
            keys: vec![0; cap],
            vals: vec![0; cap],
            mask: cap - 1,
            len: 0,
        }
    }
    fn grow(&mut self) {
        let ncap = self.keys.len() * 2;
        let (mut nk, mut nv) = (vec![0u64; ncap], vec![0u64; ncap]);
        let nmask = ncap - 1;
        for i in 0..self.keys.len() {
            let k = self.keys[i];
            if k != 0 {
                let mut s = (fx64(k) as usize) & nmask;
                while nk[s] != 0 {
                    s = (s + 1) & nmask;
                }
                nk[s] = k;
                nv[s] = self.vals[i];
            }
        }
        self.keys = nk;
        self.vals = nv;
        self.mask = nmask;
    }
    #[inline]
    fn insert(&mut self, k: u64, v: u64) {
        let mut s = (fx64(k) as usize) & self.mask;
        loop {
            if self.keys[s] == 0 {
                self.keys[s] = k;
                self.vals[s] = v;
                self.len += 1;
                if self.len * 10 > self.keys.len() * 7 {
                    self.grow();
                }
                return;
            }
            if self.keys[s] == k {
                self.vals[s] = v;
                return;
            }
            s = (s + 1) & self.mask;
        }
    }
    #[inline]
    fn get(&self, k: u64) -> Option<u64> {
        let mut s = (fx64(k) as usize) & self.mask;
        loop {
            let kk = unsafe { *self.keys.get_unchecked(s) };
            if kk == 0 {
                return None;
            }
            if kk == k {
                return Some(unsafe { *self.vals.get_unchecked(s) });
            }
            s = (s + 1) & self.mask;
        }
    }
}

// Fallback interner: 64 shards, id = local*64 + shard (mirrors key tag layout).
struct FbShard {
    map: FxHashMap<Box<[u8]>, u32>,
    tab: Vec<Box<[u8]>>,
}
struct Fb {
    shards: Vec<RwLock<FbShard>>,
}
const ISHARD: usize = 64;
const ISHARD_MASK: u64 = (ISHARD - 1) as u64;
impl Fb {
    fn new() -> Self {
        Fb {
            shards: (0..ISHARD)
                .map(|_| {
                    RwLock::new(FbShard {
                        map: FxHashMap::default(),
                        tab: Vec::new(),
                    })
                })
                .collect(),
        }
    }
    #[inline]
    fn intern(&self, s: &[u8]) -> u64 {
        let i = (fxh(s) as usize) & (ISHARD - 1);
        {
            let sh = self.shards[i].read().unwrap();
            if let Some(&v) = sh.map.get(s) {
                return FB_TAG | ((v as u64) << 6) | i as u64;
            }
        }
        let mut sh = self.shards[i].write().unwrap();
        if let Some(&v) = sh.map.get(s) {
            return FB_TAG | ((v as u64) << 6) | i as u64;
        }
        let v = sh.tab.len() as u64;
        let b: Box<[u8]> = s.into();
        sh.tab.push(b.clone());
        sh.map.insert(b, v as u32);
        FB_TAG | (v << 6) | i as u64
    }
    /// Resolve a fallback key back to bytes (reads the live shard).
    #[inline]
    fn get(&self, k: u64) -> &[u8] {
        let sh = self.shards[(k & ISHARD_MASK) as usize].read().unwrap();
        let ptr = sh.tab[(k >> 6) as usize & ((1 << 54) - 1)].as_ptr();
        let len = sh.tab[(k >> 6) as usize & ((1 << 54) - 1)].len();
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

static FB: OnceLock<Fb> = OnceLock::new();

/// Pack or intern `s` into a key. len==0 -> 0.
#[inline]
fn key_of(s: &[u8]) -> u64 {
    if s.is_empty() {
        return 0;
    }
    let k = packable(s);
    if k != u64::MAX {
        return k;
    }
    FB.get().unwrap().intern(s)
}

/// Key -> bytes. Uses `scratch` for packed keys.
#[inline]
fn key_bytes<'a>(k: u64, scratch: &'a mut [u8; 10]) -> &'a [u8] {
    if k == 0 {
        return &[];
    }
    if k < FB_TAG {
        let n = unpack(k, scratch);
        &scratch[..n]
    } else {
        FB.get().unwrap().get(k)
    }
}

/// Byte-order compare of two keys (matches lexicographic comparison of the
/// original ids). Hot path is packed-vs-packed = integer compare.
#[inline]
fn key_cmp(a: u64, b: u64) -> std::cmp::Ordering {
    if a < FB_TAG && b < FB_TAG {
        return a.cmp(&b);
    }
    let (mut sa, mut sb) = ([0u8; 10], [0u8; 10]);
    key_bytes(a, &mut sa).cmp(key_bytes(b, &mut sb))
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------
#[inline]
fn is_digits(s: &[u8]) -> bool {
    !s.is_empty() && s.iter().all(|&c| c.is_ascii_digit())
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z0: i64) -> (i64, i64, i64) {
    let z = z0 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn atoi(s: &[u8]) -> i64 {
    let mut v = 0i64;
    for &c in s {
        v = v * 10 + (c - b'0') as i64;
    }
    v
}

/// Epoch ms, or T_INVALID on any violation. Same strictness as the reference.
fn parse_ts(s: &[u8]) -> i64 {
    if s.is_empty() {
        return T_INVALID;
    }
    if is_digits(s) {
        let mut v: i64 = 0;
        for &c in s {
            v = v.saturating_mul(10).saturating_add((c - b'0') as i64);
        }
        return v;
    }
    let n = s.len();
    let (base, off_ms) = if n == 20 && s[19] == b'Z' {
        (&s[..19], 0i64)
    } else if n == 25
        && (s[19] == b'+' || s[19] == b'-')
        && s[22] == b':'
        && is_digits(&s[20..22])
        && is_digits(&s[23..25])
    {
        let oh = atoi(&s[20..22]);
        let om = atoi(&s[23..25]);
        if oh > 23 || om > 59 {
            return T_INVALID;
        }
        let o = (oh * 60 + om) * 60000;
        (&s[..19], if s[19] == b'+' { o } else { -o })
    } else {
        return T_INVALID;
    };
    if !(base[4] == b'-'
        && base[7] == b'-'
        && base[10] == b'T'
        && base[13] == b':'
        && base[16] == b':')
    {
        return T_INVALID;
    }
    for &i in &[0usize, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18] {
        if !base[i].is_ascii_digit() {
            return T_INVALID;
        }
    }
    let y = atoi(&base[0..4]);
    let mo = atoi(&base[5..7]);
    let d = atoi(&base[8..10]);
    let h = atoi(&base[11..13]);
    let mi = atoi(&base[14..16]);
    let sec = atoi(&base[17..19]);
    if mo < 1 || mo > 12 || h > 23 || mi > 59 || sec > 59 {
        return T_INVALID;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let mdays = [
        31,
        if leap { 29 } else { 28 },
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
    ][(mo - 1) as usize];
    if d < 1 || d > mdays {
        return T_INVALID;
    }
    ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60000 + sec * 1000 - off_ms
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------
#[derive(Clone, Copy)]
struct RawEv {
    eid: u64,   // event_id key (0 impossible — empty eids dropped)
    uid: u64,   // user_id key, 0 = empty
    prod: u64,  // product key, 0 = empty
    ts: i64,
    ingest: i64,
    price: u32,
    etype: u8, // 0 view,1 cart,2 purchase,3 bad
    pflag: u8, // 0 null,1 ok,2 bad
    _p: [u8; 6],
}
const RAWE_E: RawEv = RawEv {
    eid: 0,
    uid: 0,
    prod: 0,
    ts: 0,
    ingest: 0,
    price: 0,
    etype: 0,
    pflag: 0,
    _p: [0; 6],
};

#[derive(Clone, Copy)]
struct Ev {
    person: u64, // canonical person key
    ts: i64,
    cat: u32,
    price: u32,
}
const EV_E: Ev = Ev {
    person: 0,
    ts: 0,
    cat: 0,
    price: 0,
};

#[derive(Clone, Copy)]
struct RawCl {
    cid: u64,
    uid: u64,
    camp: u32, // campaign idx, u32::MAX = unknown
    ts: i64,
    _p: u32,
}
const RAWC_E: RawCl = RawCl {
    cid: 0,
    uid: 0,
    camp: 0,
    ts: 0,
    _p: 0,
};

#[derive(Clone, Copy)]
struct Cl {
    person: u64,
    ts: i64,
    camp: u32,
    cid: u64, // key; byte order needed only for equal-ts ties
    _p: u32,
}
const CL_E: Cl = Cl {
    person: 0,
    ts: 0,
    camp: 0,
    cid: 0,
    _p: 0,
};

/// Bucket `v` into NSHARD shards by key hash, sort each shard in parallel,
/// concatenate. Equal keys stay contiguous; global key order is NOT produced.
fn shard_sort<T: Copy + Send + Sync, const NSHARD: usize>(
    v: Vec<T>,
    key: impl Fn(&T) -> u64 + Sync,
    fill: T,
    cmp: impl Fn(&T, &T) -> std::cmp::Ordering + Sync,
) -> Vec<T> {
    let mut cnt = vec![0u32; NSHARD + 1].into_boxed_slice();
    for e in &v {
        cnt[((fx64(key(e)) as usize) & (NSHARD - 1)) + 1] += 1;
    }
    for i in 0..NSHARD {
        cnt[i + 1] += cnt[i];
    }
    let mut out = vec![fill; v.len()];
    let mut pos = cnt.clone();
    for e in v {
        let s = (fx64(key(&e)) as usize) & (NSHARD - 1);
        out[pos[s] as usize] = e;
        pos[s] += 1;
    }
    let mut rest: &mut [T] = &mut out;
    let mut slices = Vec::with_capacity(NSHARD);
    for s in 0..NSHARD {
        let n = (cnt[s + 1] - cnt[s]) as usize;
        let (a, b) = rest.split_at_mut(n);
        slices.push(a);
        rest = b;
    }
    slices
        .into_par_iter()
        .for_each(|sl| sl.sort_unstable_by(|a, b| cmp(a, b)));
    out
}

// ---------------------------------------------------------------------------
// Union-find over dense ids
// ---------------------------------------------------------------------------
struct Uf {
    p: Vec<u32>,
    r: Vec<u8>,
}
impl Uf {
    fn new(n: usize) -> Self {
        Uf {
            p: (0..n as u32).collect(),
            r: vec![0; n],
        }
    }
    fn find(&mut self, mut x: u32) -> u32 {
        let mut root = x;
        while self.p[root as usize] != root {
            root = self.p[root as usize];
        }
        while self.p[x as usize] != x {
            let nx = self.p[x as usize];
            self.p[x as usize] = root;
            x = nx;
        }
        root
    }
    fn union(&mut self, a: u32, b: u32) {
        let (mut ra, mut rb) = (self.find(a), self.find(b));
        if ra == rb {
            return;
        }
        if self.r[ra as usize] < self.r[rb as usize] {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.p[rb as usize] = ra;
        if self.r[ra as usize] == self.r[rb as usize] {
            self.r[ra as usize] += 1;
        }
    }
}

// ---------------------------------------------------------------------------
fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn mmap_file(p: &Path) -> &'static [u8] {
    match fs::File::open(p) {
        Ok(f) => {
            if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
                return &[];
            }
            let m = unsafe { Mmap::map(&f).unwrap() };
            let m: &'static Mmap = Box::leak(Box::new(m));
            &m[..]
        }
        Err(_) => &[],
    }
}

fn json_str<'a>(j: &'a [u8], key: &str) -> Option<&'a [u8]> {
    let pat = format!("\"{}\"", key);
    let i = memmem::find(j, pat.as_bytes())?;
    let rest = &j[i + pat.len()..];
    let q1 = memchr(b'"', rest)?;
    let q2 = memchr(b'"', &rest[q1 + 1..])?;
    Some(&rest[q1 + 1..q1 + 1 + q2])
}

// ---------------------------------------------------------------------------
// Tokenizer: one pass over a chunk via memchr3(',', '"', '\n'). Emits parsed
// field slices per line. Quotes only legal as whole-field wrappers.
// ---------------------------------------------------------------------------
/// Parse all lines of `b[pos..end)` into `f(out, fields)` — fields are
/// Option<(off,len)> into b. Returns after processing the whole range.
/// `hdr` skips a header line when pos==0.
#[inline]
fn each_line<F: FnMut(&[Option<(u32, u32)>; 8], &[u8])>(
    b: &[u8],
    lo: usize,
    hi: usize,
    hdr: bool,
    ncols: usize,
    mut f: F,
) {
    let mut pos = lo;
    if hdr && pos == 0 {
        match memchr(b'\n', &b[..hi]) {
            Some(p) => pos = p + 1,
            None => return,
        }
    }
    let mut fields: [Option<(u32, u32)>; 8] = [None; 8];
    let mut n = 0usize;
    let mut fstart = pos;
    let mut q = 0u32;
    let mut ffirst = 0u8;
    let mut flast = 0u8;
    let mut ok = true;
    let mut p = pos;
    while p < hi {
        let d = match memchr3(b',', b'"', b'\n', &b[p..hi]) {
            Some(x) => p + x,
            None => hi,
        };
        if d == hi {
            // tail of file without newline: last field runs to hi
            let flen = hi - fstart;
            if n < ncols {
                ok = ok && field_ok(&b[fstart..hi], q, flen);
                if ok {
                    let (o, l) = field_span(&b[fstart..hi], q, flen);
                    fields[n] = if o == u32::MAX {
                        None
                    } else {
                        Some(((fstart - lo) as u32 + o, l))
                    };
                    n += 1;
                }
            } else {
                ok = false;
            }
            if n == ncols && ok {
                f(&fields, b);
            }
            return;
        }
        let c = b[d];
        if c == b'"' {
            if d == fstart {
                ffirst = 1;
            }
            flast = 1;
            q += 1;
            p = d + 1;
            continue;
        }
        if c == b',' {
            if n >= ncols {
                ok = false;
                n += 1;
            } else {
                let flen = d - fstart;
                ok = ok && field_ok(&b[fstart..d], q, flen);
                if ok {
                    let (o, l) = field_span(&b[fstart..d], q, flen);
                    fields[n] = if o == u32::MAX {
                        None
                    } else {
                        Some(((fstart - lo) as u32 + o, l))
                    };
                    n += 1;
                } else {
                    n += 1;
                }
            }
            fstart = d + 1;
            q = 0;
            ffirst = 0;
            flast = 0;
            p = d + 1;
            continue;
        }
        // '\n': finalize last field + emit
        let flen = d - fstart;
        if n < ncols {
            ok = ok && field_ok(&b[fstart..d], q, flen);
            if ok {
                let (o, l) = field_span(&b[fstart..d], q, flen);
                fields[n] = if o == u32::MAX {
                    None
                } else {
                    Some(((fstart - lo) as u32 + o, l))
                };
                n += 1;
            }
        } else {
            ok = false;
        }
        let _ = (ffirst, flast);
        if n == ncols && ok {
            f(&fields, b);
        }
        n = 0;
        ok = true;
        q = 0;
        ffirst = 0;
        flast = 0;
        fstart = d + 1;
        p = d + 1;
    }
    // File ended right after a comma: one pending empty field remains.
    if n > 0 && n < ncols {
        fields[n] = None;
        n += 1;
        if n == ncols && ok {
            f(&fields, b);
        }
    }
}

/// Field validity: unquoted -> no quotes; quoted -> exactly `"..."` with no
/// inner quotes (q==2 and wrapper positions).
#[inline]
fn field_ok(s: &[u8], q: u32, flen: usize) -> bool {
    q == 0 || (q == 2 && flen >= 2 && s[0] == b'"' && s[flen - 1] == b'"')
}

/// Span of the value inside the field; (u32::MAX,0) = empty/null.
#[inline]
fn field_span(s: &[u8], q: u32, flen: usize) -> (u32, u32) {
    if flen == 0 {
        return (u32::MAX, 0);
    }
    if q == 2 {
        let inner = flen - 2;
        if inner == 0 {
            return (u32::MAX, 0);
        }
        let _ = s;
        return (1, inner as u32);
    }
    (0, flen as u32)
}

/// Byte ranges aligned to line starts.
fn line_chunks(b: &[u8], n: usize) -> Vec<(usize, usize)> {
    let mut v = Vec::new();
    if b.is_empty() {
        return v;
    }
    let per = max(1, b.len() / max(1, n));
    let mut pos = 0usize;
    while pos < b.len() {
        let mut end = min(b.len(), pos + per);
        if end < b.len() {
            end += match memchr(b'\n', &b[end..]) {
                Some(p) => p + 1,
                None => b.len() - end,
            };
        }
        v.push((pos, end));
        pos = end;
    }
    v
}

// ---------------------------------------------------------------------------
// Accumulators
// ---------------------------------------------------------------------------
#[derive(Default)]
struct UdCell {
    sessions: u64,
    events: u64,
    rev: i64,
    cats: Vec<(u32, i64)>,
    used: bool,
}

#[derive(Default, Clone, Copy)]
struct CdAcc {
    sessions: u64,
    purch: u64,
    rev: i64,
    assist: u64,
}

#[derive(Default)]
struct Session {
    start: i64,
    n: u64,
    rev: i64,
}

#[derive(Default)]
struct Alive {
    pos: Vec<u32>,
    cnt: Vec<u32>,
    elems: Vec<u32>,
}
impl Alive {
    fn new(n: u32) -> Self {
        Alive {
            pos: vec![u32::MAX; n as usize],
            cnt: vec![0; n as usize],
            elems: Vec::new(),
        }
    }
    fn add(&mut self, c: u32) {
        let c = c as usize;
        if self.cnt[c] == 0 {
            self.pos[c] = self.elems.len() as u32;
            self.elems.push(c as u32);
        }
        self.cnt[c] += 1;
    }
    fn del(&mut self, c: u32) {
        let c = c as usize;
        self.cnt[c] -= 1;
        if self.cnt[c] == 0 {
            let i = self.pos[c] as usize;
            let last = *self.elems.last().unwrap();
            self.elems[i] = last;
            self.pos[last as usize] = i as u32;
            self.elems.pop();
            self.pos[c] = u32::MAX;
        }
    }
    fn reset(&mut self) {
        for &c in self.elems.iter() {
            self.pos[c as usize] = u32::MAX;
            self.cnt[c as usize] = 0;
        }
        self.elems.clear();
    }
}

#[derive(Default)]
struct Acc {
    ud_out: Vec<(u64, i64, u64, u64, i64, Option<u32>)>,
    ud_groups: Vec<(u64, u32, u32)>,
    cd: Vec<CdAcc>,
    cells: Vec<UdCell>,
    touched: Vec<u32>,
    sessions: Vec<(i64, i64, i64)>,
    ats: Vec<i64>,
    acamp: Vec<u32>,
    cts: Vec<i64>,
    ccamp: Vec<u32>,
    cur: Session,
    alive: Alive,
    day_lo: i64,
}
impl Acc {
    fn new(n_days: usize, n_camps: u32, day_lo: i64) -> Self {
        let mut a = Acc::default();
        a.cd = vec![CdAcc::default(); (n_camps as usize + 1) * n_days];
        a.cells = (0..n_days).map(|_| UdCell::default()).collect();
        a.alive = Alive::new(n_camps);
        a.day_lo = day_lo;
        a
    }
    fn merge(&mut self, o: Acc) {
        let base = self.ud_out.len() as u32;
        self.ud_out.extend(o.ud_out);
        self.ud_groups
            .extend(o.ud_groups.iter().map(|&(k, s, l)| (k, s + base, l)));
        if self.cd.len() < o.cd.len() {
            self.cd = o.cd;
            return;
        }
        for (a, b) in self.cd.iter_mut().zip(o.cd.iter()) {
            a.sessions += b.sessions;
            a.purch += b.purch;
            a.rev += b.rev;
            a.assist += b.assist;
        }
    }
    fn flush_session(&mut self) {
        let day = self.cur.start / D24;
        self.sessions.push((self.cur.start, day, self.cur.rev));
        let di = (day - self.day_lo) as usize;
        if !self.cells[di].used {
            self.cells[di].used = true;
            self.touched.push(di as u32);
        }
        let c = &mut self.cells[di];
        c.sessions += 1;
        c.events += self.cur.n;
        c.rev += self.cur.rev;
        self.cur.n = 0;
        self.cur.rev = 0;
    }
}

fn person_runs<T: Sync>(arr: &[T], key: impl Fn(&T) -> u64 + Sync) -> Vec<(u64, u32, u32)> {
    let n = arr.len();
    if n == 0 {
        return Vec::new();
    }
    let nblk = (rayon::current_num_threads() * 4).max(1);
    let blksz = n.div_ceil(nblk).max(1);
    let parts: Vec<Vec<(u64, u32, u32)>> = (0..nblk)
        .into_par_iter()
        .map(|b| {
            let lo = b * blksz;
            let hi = min(n, lo + blksz);
            let mut v = Vec::new();
            if lo >= hi {
                return v;
            }
            let mut i = lo;
            while i < hi {
                let p = key(&arr[i]);
                let mut j = i + 1;
                while j < hi && key(&arr[j]) == p {
                    j += 1;
                }
                v.push((p, i as u32, j as u32));
                i = j;
            }
            v
        })
        .collect();
    let mut out: Vec<(u64, u32, u32)> = Vec::new();
    for blk in parts {
        for (p, l, h) in blk {
            if let Some(last) = out.last_mut() {
                if last.0 == p && last.2 == l {
                    last.2 = h;
                    continue;
                }
            }
            out.push((p, l, h));
        }
    }
    out
}

// Open-addressing u64 key table over a rows slice; keeps the row index with
// min `ingest` (first wins ties). Returns indices of surviving rows.
fn dedup_min(rows: &[RawEv], keep: impl Fn(&RawEv) -> i64) -> Vec<u32> {
    // 2-bit counting sketch: rows whose slot stayed at 1 are provably unique.
    // Only potential duplicates (count>=2, incl. hash collisions) take the
    // exact open-addressing path.
    let slots = (rows.len().max(16) * 4).next_power_of_two();
    let mut cnt = vec![0u64; slots / 32];
    for r in rows {
        let s = (fx64(r.eid) as usize) & (slots - 1);
        let w = s >> 5;
        let sh = (s & 31) * 2;
        if (cnt[w] >> sh) & 3 < 2 {
            cnt[w] += 1u64 << sh;
        }
    }
    let mut out = Vec::with_capacity(rows.len());
    let mut dup_rows: Vec<u32> = Vec::new();
    for (i, r) in rows.iter().enumerate() {
        let s = (fx64(r.eid) as usize) & (slots - 1);
        if (cnt[s >> 5] >> ((s & 31) * 2)) & 3 >= 2 {
            dup_rows.push(i as u32);
        } else {
            out.push(i as u32);
        }
    }
    if dup_rows.is_empty() {
        return out;
    }
    let cap = (dup_rows.len().max(16) * 2).next_power_of_two();
    let mut keys = vec![0u64; cap];
    let mut vals = vec![u32::MAX; cap];
    for &i in &dup_rows {
        let r = &rows[i as usize];
        let mut s = (fx64(r.eid) as usize) & (cap - 1);
        loop {
            if keys[s] == 0 {
                keys[s] = r.eid;
                vals[s] = i;
                break;
            }
            if keys[s] == r.eid {
                if keep(r) < keep(&rows[vals[s] as usize]) {
                    vals[s] = i;
                }
                break;
            }
            s = (s + 1) & (cap - 1);
        }
    }
    for s in 0..cap {
        if keys[s] != 0 {
            out.push(vals[s]);
        }
    }
    out
}

fn main() {
    let data = env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let outdir = env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into());
    let ncpu: usize = env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    rayon::ThreadPoolBuilder::new()
        .num_threads(ncpu)
        .build_global()
        .ok();
    let write_csv = env::var("WRITE_CSV").is_ok();
    let data = Path::new(&data);
    let outdir = Path::new(&outdir);
    FB.set(Fb::new()).ok();

    let manifest = fs::read(data.join("manifest.json")).unwrap_or_default();
    let ws = json_str(&manifest, "window_start").map(parse_ts).unwrap_or(T_INVALID);
    let we = json_str(&manifest, "window_end").map(parse_ts).unwrap_or(T_INVALID);
    assert!(ws != T_INVALID && we != T_INVALID, "bad manifest");
    let click_lo = ws - D7;

    let ev_files = list_csv(&data.join("events"));
    let cl_files = list_csv(&data.join("clicks"));
    let ln_files = list_csv(&data.join("links"));
    let mut file_list: Vec<&'static [u8]> = Vec::new();
    for p in &ev_files {
        file_list.push(mmap_file(p));
    }
    let nev = file_list.len();
    for p in &cl_files {
        file_list.push(mmap_file(p));
    }
    let ncl = file_list.len();
    for p in &ln_files {
        file_list.push(mmap_file(p));
    }
    let nln = file_list.len();
    file_list.push(mmap_file(&data.join("products.csv")));
    file_list.push(mmap_file(&data.join("campaigns.csv")));
    let prod_f = (file_list.len() - 2) as u16;
    let camp_f = (file_list.len() - 1) as u16;
    FILES.set(file_list).unwrap();
    let phase_timing = env::var("PHASE_TIMING").is_ok();
    let mut t_last = std::time::Instant::now();
    macro_rules! pt {
        ($name:expr) => {
            if phase_timing {
                let n = std::time::Instant::now();
                eprintln!("[phase] {}: {:?}", $name, n - t_last);
                t_last = n;
            }
        };
    }

    // products.csv -> prod key -> cat idx
    let mut cat_map: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();
    let mut cat_tab: Vec<Box<[u8]>> = Vec::new();
    let mut intern_cat = |c: &[u8]| -> u32 {
        if let Some(&i) = cat_map.get(c) {
            return i;
        }
        let i = cat_tab.len() as u32;
        cat_tab.push(c.into());
        cat_map.insert(c.into(), i);
        i
    };
    let unknown_cat = intern_cat(b"unknown");
    let mut prod_map = U64Map::new(4096);
    {
        let b = buf(prod_f);
        each_line(b, 0, b.len(), true, 2, |fields, b| {
            if let (Some(po), Some(co)) = (fields[0], fields[1]) {
                let prod = &b[po.0 as usize..(po.0 + po.1) as usize];
                let cat = &b[co.0 as usize..(co.0 + co.1) as usize];
                let ci = intern_cat(cat);
                prod_map.insert(key_of(prod), ci as u64); // last wins
            }
        });
    }

    // campaigns.csv -> camp key -> idx
    let mut camp_names: Vec<Box<[u8]>> = Vec::new();
    let mut camp_chan: Vec<Box<[u8]>> = Vec::new();
    let mut camp_map = U64Map::new(4096);
    {
        let b = buf(camp_f);
        each_line(b, 0, b.len(), true, 2, |fields, b| {
            if let (Some(co), Some(ho)) = (fields[0], fields[1]) {
                let cid = &b[co.0 as usize..(co.0 + co.1) as usize];
                let ch = &b[ho.0 as usize..(ho.0 + ho.1) as usize];
                let k = key_of(cid);
                match camp_map.get(k) {
                    Some(i) => camp_chan[i as usize] = ch.into(),
                    None => {
                        let i = camp_names.len() as u32;
                        camp_names.push(cid.into());
                        camp_chan.push(ch.into());
                        camp_map.insert(k, i as u64);
                    }
                }
            }
        });
    }
    let n_camps = camp_names.len() as u32;
    let organic_camp = n_camps;
    pt!("catalogs");

    // links -> union-find over u64 keys -> canon map
    let canon_map = {
        let link_tasks: Vec<(u16, usize, usize)> = (ncl..nln)
            .flat_map(|fi| {
                line_chunks(buf(fi as u16), ncpu * 2)
                    .into_iter()
                    .map(move |(cs, ce)| (fi as u16, cs, ce))
            })
            .collect();
        let pairs: Vec<(u64, u64)> = link_tasks
            .par_iter()
            .flat_map(|&(f, cs, ce)| {
                let b = buf(f);
                let mut out = Vec::new();
                each_line(b, cs, ce, cs == 0, 2, |fields, b| {
                    if let (Some(ao), Some(bo)) = (fields[0], fields[1]) {
                        if ao.1 == 0 || bo.1 == 0 {
                            return;
                        }
                        let a = &b[cs + ao.0 as usize..cs + (ao.0 + ao.1) as usize];
                        let bb = &b[cs + bo.0 as usize..cs + (bo.0 + bo.1) as usize];
                        out.push((key_of(a), key_of(bb)));
                    }
                });
                out
            })
            .collect();
        // dense ids for UF via open map
        let mut idmap: FxHashMap<u64, u32> = FxHashMap::default();
        let mut keys: Vec<u64> = Vec::new();
        let mut idx_of = |k: u64, idmap: &mut FxHashMap<u64, u32>, keys: &mut Vec<u64>| -> u32 {
            if let Some(&i) = idmap.get(&k) {
                return i;
            }
            let i = keys.len() as u32;
            keys.push(k);
            idmap.insert(k, i);
            i
        };
        let mut pairs2: Vec<(u32, u32)> = Vec::with_capacity(pairs.len());
        for (a, b) in pairs {
            let ia = idx_of(a, &mut idmap, &mut keys);
            let ib = idx_of(b, &mut idmap, &mut keys);
            pairs2.push((ia, ib));
        }
        let n_ids = keys.len();
        let mut uf = Uf::new(n_ids);
        for (a, b) in pairs2 {
            uf.union(a, b);
        }
        // canonical member = byte-min key in component
        let mut comp_min: FxHashMap<u32, u32> = FxHashMap::default();
        for i in 0..n_ids as u32 {
            let r = uf.find(i);
            let e = comp_min.entry(r).or_insert(i);
            if key_cmp(keys[i as usize], keys[*e as usize]) == std::cmp::Ordering::Less {
                *e = i;
            }
        }
        let mut m = U64Map::new(n_ids);
        let bmask = (n_ids * 8).max(1 << 16).next_power_of_two() - 1;
        let mut bloom = vec![0u64; (bmask + 1) / 64];
        for i in 0..n_ids as u32 {
            let k = keys[i as usize];
            let canon = keys[comp_min[&uf.find(i)] as usize];
            m.insert(k, canon);
            let bi = (fx64(k) as usize) & bmask;
            bloom[bi >> 6] |= 1 << (bi & 63);
        }
        (m, bloom, bmask)
    };
    let (canon_map, cbloom, cbmask) = canon_map;
    let canon_of = |k: u64| -> u64 {
        let h = fx64(k) as usize;
        if cbloom[(h & cbmask) >> 6] >> (h & 63) & 1 == 0 {
            return k;
        }
        canon_map.get(k).unwrap_or(k)
    };
    pt!("links");

    // ---------------- events ----------------
    struct Task {
        file: u16,
        cs: usize,
        ce: usize,
    }
    let mut tasks = Vec::new();
    for f in 0..nev {
        let fi = f as u16;
        for (cs, ce) in line_chunks(buf(fi), ncpu * 2) {
            tasks.push(Task { file: fi, cs, ce });
        }
    }
    let task_rows: Vec<Vec<RawEv>> = tasks
        .par_iter()
        .map(|t| {
            let b = buf(t.file);
            let mut rows: Vec<RawEv> = Vec::with_capacity((t.ce - t.cs) / 64);
            let lo = t.cs;
            each_line(b, t.cs, t.ce, t.cs == 0, 7, |fields, b| {
                let get = |i: usize| -> &[u8] {
                    match fields[i] {
                        Some((o, l)) => &b[lo + o as usize..lo + (o + l) as usize],
                        None => &[],
                    }
                };
                let (eid, uid, ts_s, ing_s, et_s, prod, price) = (
                    get(0),
                    get(1),
                    get(2),
                    get(3),
                    get(4),
                    get(5),
                    get(6),
                );
                if eid.is_empty() || fields[0].is_none() {
                    return;
                }
                let ingest = if fields[3].is_some() {
                    parse_ts(ing_s)
                } else {
                    return;
                };
                if ingest == T_INVALID {
                    return;
                }
                let ts = if fields[2].is_some() {
                    parse_ts(ts_s)
                } else {
                    T_INVALID
                };
                let etype = match et_s {
                    b"view" => 0u8,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 3,
                };
                let (pv, pflag) = match fields[6] {
                    None => (0u32, 0u8),
                    Some(_) => {
                        if is_digits(price) {
                            let v = atoi(price);
                            if v >= 1 && v <= 10_000_000 {
                                (v as u32, 1)
                            } else {
                                (0, 2)
                            }
                        } else {
                            (0, 2)
                        }
                    }
                };
                // product key only needed for purchases; other types just
                // need "non-empty" (sentinel 1 — key_of never returns 1)
                let prod_key = if prod.is_empty() {
                    0
                } else if etype == 2 {
                    key_of(prod)
                } else {
                    1
                };
                rows.push(RawEv {
                    eid: key_of(eid),
                    uid: key_of(uid),
                    prod: prod_key,
                    ingest,
                    ts,
                    price: pv,
                    etype,
                    pflag,
                    _p: [0; 6],
                });
            });
            rows
        })
        .collect();

    pt!("ev_parse");

    // single-arena scatter by eid-hash: per-task histograms -> global offsets
    // -> each task writes directly into its disjoint (task, shard) region.
    const NSHARD: usize = 256;
    let shard_of = |eid: u64| (fx64(eid) as usize) & (NSHARD - 1);
    let cnts: Vec<Vec<u32>> = task_rows
        .par_iter()
        .map(|v| {
            let mut c = vec![0u32; NSHARD];
            for r in v {
                c[shard_of(r.eid)] += 1;
            }
            c
        })
        .collect();
    let nt = task_rows.len();
    // prefix[t][s] = rows of shard s contributed by tasks 0..t
    let mut prefix = vec![vec![0u32; NSHARD]; nt + 1];
    for t in 0..nt {
        for s in 0..NSHARD {
            prefix[t + 1][s] = prefix[t][s] + cnts[t][s];
        }
    }
    let mut base = vec![0u32; NSHARD + 1];
    for s in 0..NSHARD {
        base[s + 1] = base[s] + prefix[nt][s];
    }
    let total = base[NSHARD] as usize;
    struct RawPtr<T>(usize, std::marker::PhantomData<T>);
    unsafe impl<T> Send for RawPtr<T> {}
    unsafe impl<T> Sync for RawPtr<T> {}
    let mut arena: Vec<RawEv> = Vec::with_capacity(total);
    let arena_ptr = RawPtr::<RawEv>(arena.as_mut_ptr() as usize, std::marker::PhantomData);
    let task_rows_ref = &task_rows;
    let base_ref = &base;
    let prefix_ref = &prefix;
    (0..nt).into_par_iter().for_each(|t| {
        let mut off: [u32; NSHARD] = [0; NSHARD];
        for s in 0..NSHARD {
            off[s] = base_ref[s] + prefix_ref[t][s];
        }
        let dst = arena_ptr.0 as *mut RawEv;
        for r in &task_rows_ref[t] {
            let s = shard_of(r.eid);
            unsafe { *dst.add(off[s] as usize) = *r };
            off[s] += 1;
        }
    });
    unsafe { arena.set_len(total) };
    drop(task_rows);
    drop(cnts);
    drop(prefix);
    pt!("ev_scatter");

    let prod_map_ref = &prod_map;
    let arena_ref = &arena;
    let keeps: Vec<Vec<u32>> = (0..NSHARD)
        .into_par_iter()
        .map(|s| {
            let rows = &arena_ref[base_ref[s] as usize..base_ref[s + 1] as usize];
            dedup_min(rows, |r| r.ingest)
        })
        .collect();
    pt!("ev_dedup");
    let keeps_ref = &keeps;
    let evs: Vec<Ev> = (0..NSHARD)
        .into_par_iter()
        .flat_map(|s| {
            let rows = &arena_ref[base_ref[s] as usize..base_ref[s + 1] as usize];
            let mut out = Vec::with_capacity(keeps_ref[s].len());
            for &i in &keeps_ref[s] {
                let d = rows[i as usize];
                if d.uid == 0 || d.prod == 0 || d.etype == 3 || d.ts == T_INVALID || d.ts < ws
                    || d.ts >= we
                {
                    continue;
                }
                if d.ingest - d.ts > D24 {
                    continue;
                }
                if d.etype == 2 && d.pflag != 1 {
                    continue;
                }
                let person = canon_of(d.uid);
                let (cat, price) = if d.etype == 2 {
                    (
                        prod_map_ref.get(d.prod).map(|v| v as u32).unwrap_or(unknown_cat),
                        d.price,
                    )
                } else {
                    (0, 0)
                };
                out.push(Ev {
                    person,
                    ts: d.ts,
                    cat,
                    price,
                });
            }
            out
        })
        .collect();
    pt!("ev_filter");
    let evs = shard_sort::<Ev, 256>(evs, |e| e.person, EV_E, |a, b| {
        (a.person, a.ts).cmp(&(b.person, b.ts))
    });
    pt!("ev_sort");

    // ---------------- clicks ----------------
    let mut ctasks = Vec::new();
    for f in nev..ncl {
        let fi = f as u16;
        for (cs, ce) in line_chunks(buf(fi), ncpu * 2) {
            ctasks.push(Task { file: fi, cs, ce });
        }
    }
    let camp_map_ref = &camp_map;
    let ctask_rows: Vec<Vec<RawCl>> = ctasks
        .par_iter()
        .map(|t| {
            let b = buf(t.file);
            let mut rows: Vec<RawCl> = Vec::with_capacity((t.ce - t.cs) / 48);
            let lo = t.cs;
            each_line(b, t.cs, t.ce, t.cs == 0, 4, |fields, b| {
                let get = |i: usize| -> &[u8] {
                    match fields[i] {
                        Some((o, l)) => &b[lo + o as usize..lo + (o + l) as usize],
                        None => &[],
                    }
                };
                let (cid, uid, camp, ts_s) = (get(0), get(1), get(2), get(3));
                if cid.is_empty() || fields[0].is_none() {
                    return;
                }
                rows.push(RawCl {
                    cid: key_of(cid),
                    uid: key_of(uid),
                    camp: match fields[2] {
                        Some(_) => {
                            let ck = key_of(camp);
                            camp_map_ref.get(ck).map(|v| v as u32).unwrap_or(u32::MAX)
                        }
                        None => u32::MAX,
                    },
                    ts: if fields[3].is_some() {
                        parse_ts(ts_s)
                    } else {
                        T_INVALID
                    },
                    _p: 0,
                });
            });
            rows
        })
        .collect();
    pt!("cl_parse");

    let cshard_vecs: Vec<Vec<RawCl>> = {
        let sorted: Vec<(Vec<RawCl>, Vec<u32>)> = ctask_rows
            .par_iter()
            .map(|v| {
                let mut c = vec![0u32; NSHARD];
                for r in v {
                    c[(fx64(r.cid) as usize) & (NSHARD - 1)] += 1;
                }
                let mut offs = Vec::with_capacity(NSHARD + 1);
                let mut acc = 0u32;
                for s in 0..NSHARD {
                    offs.push(acc);
                    acc += c[s];
                }
                offs.push(acc);
                let mut pos = offs.clone();
                let mut flat = vec![RAWC_E; v.len()];
                for r in v {
                    let s = (fx64(r.cid) as usize) & (NSHARD - 1);
                    flat[pos[s] as usize] = *r;
                    pos[s] += 1;
                }
                (flat, offs)
            })
            .collect();
        (0..NSHARD)
            .into_par_iter()
            .map(|s| {
                let n: usize = sorted.iter().map(|(_, o)| (o[s + 1] - o[s]) as usize).sum();
                let mut v = Vec::with_capacity(n);
                for (flat, offs) in &sorted {
                    v.extend_from_slice(&flat[offs[s] as usize..offs[s + 1] as usize]);
                }
                v
            })
            .collect()
    };
    drop(ctask_rows);
    pt!("cl_scatter");

    let clicks: Vec<Cl> = cshard_vecs
        .into_par_iter()
        .flat_map(|rows| {
            // first-copy-wins dedup on cid
            let cap = (rows.len().max(16) * 2).next_power_of_two();
            let mut keys = vec![0u64; cap];
            let mut out = Vec::with_capacity(rows.len());
            for r in &rows {
                let mut s = (fx64(r.cid) as usize) & (cap - 1);
                loop {
                    if keys[s] == 0 {
                        keys[s] = r.cid;
                        if r.uid != 0
                            && r.ts != T_INVALID
                            && r.ts >= click_lo
                            && r.ts < we
                            && r.camp != u32::MAX
                        {
                            out.push(Cl {
                                person: canon_of(r.uid),
                                ts: r.ts,
                                camp: r.camp,
                                cid: r.cid,
                                _p: 0,
                            });
                        }
                        break;
                    }
                    if keys[s] == r.cid {
                        break;
                    }
                    s = (s + 1) & (cap - 1);
                }
            }
            out
        })
        .collect();
    // sort by (person, ts); equal-ts ties broken by click_id BYTES
    pt!("cl_dedup_filter");
    let clicks = shard_sort::<Cl, 256>(clicks, |c| c.person, CL_E, |a, b| {
        (a.person, a.ts)
            .cmp(&(b.person, b.ts))
            .then_with(|| key_cmp(a.cid, b.cid))
    });
    pt!("cl_sort");

    // ---------------- sessionize + attribute ----------------
    let ev_runs = person_runs(&evs, |e| e.person);
    let mut cl_runs = person_runs(&clicks, |c| c.person);
    cl_runs.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let day_lo = ws / D24;
    let n_days = ((we - 1) / D24 - day_lo + 1) as usize;
    let nslots = n_camps as usize + 1;
    let cat_tab_ref = &cat_tab;
    let total = ev_runs
        .par_iter()
        .fold(
            || Acc::new(n_days, n_camps, day_lo),
            |mut acc, &(person, lo, hi)| {
                acc.sessions.clear();
                acc.ats.clear();
                acc.acamp.clear();
                acc.cts.clear();
                acc.ccamp.clear();
                acc.alive.reset();
                if let Ok(ri) = cl_runs.binary_search_by(|r| r.0.cmp(&person)) {
                    let (_, clo, chi) = cl_runs[ri];
                    for c in &clicks[clo as usize..chi as usize] {
                        acc.ats.push(c.ts);
                        acc.acamp.push(c.camp);
                        if acc.cts.last() != Some(&c.ts) {
                            acc.cts.push(c.ts);
                            acc.ccamp.push(c.camp);
                        }
                    }
                }
                let (mut lo_p, mut hi_p) = (0usize, 0usize);
                let mut have = false;
                let mut prev_ts = T_INVALID;
                for e in &evs[lo as usize..hi as usize] {
                    if have && e.ts - prev_ts > MIN30 {
                        acc.flush_session();
                        acc.cur.start = e.ts;
                    }
                    if !have {
                        acc.cur.start = e.ts;
                        have = true;
                    }
                    acc.cur.n += 1;
                    if e.price > 0 {
                        acc.cur.rev += e.price as i64;
                        let di = (acc.cur.start / D24 - acc.day_lo) as usize;
                        let cell = &mut acc.cells[di];
                        match cell.cats.iter_mut().find(|x| x.0 == e.cat) {
                            Some(x) => x.1 += e.price as i64,
                            None => cell.cats.push((e.cat, e.price as i64)),
                        }
                    }
                    prev_ts = e.ts;
                }
                if have {
                    acc.flush_session();
                }
                for &(start, day, rev) in acc.sessions.iter() {
                    while hi_p < acc.ats.len() && acc.ats[hi_p] <= start {
                        acc.alive.add(acc.acamp[hi_p]);
                        hi_p += 1;
                    }
                    while lo_p < acc.ats.len() && acc.ats[lo_p] < start - D7 {
                        acc.alive.del(acc.acamp[lo_p]);
                        lo_p += 1;
                    }
                    let i = acc.cts.partition_point(|&t| t <= start);
                    let ck = if i > 0 && start - acc.cts[i - 1] <= D7 {
                        acc.ccamp[i - 1]
                    } else {
                        organic_camp
                    };
                    let base = (day - day_lo) as usize * nslots;
                    let e = &mut acc.cd[base + ck as usize];
                    e.sessions += 1;
                    e.rev += rev;
                    if rev > 0 {
                        e.purch += 1;
                    }
                    for i2 in 0..acc.alive.elems.len() {
                        let c = acc.alive.elems[i2];
                        acc.cd[base + c as usize].assist += 1;
                    }
                }
                let gstart = acc.ud_out.len() as u32;
                for &di in acc.touched.iter() {
                    let c = &mut acc.cells[di as usize];
                    let top = c
                        .cats
                        .iter()
                        .max_by(|a, b| {
                            a.1.cmp(&b.1).then_with(|| {
                                cat_tab_ref[b.0 as usize].cmp(&cat_tab_ref[a.0 as usize])
                            })
                        })
                        .map(|x| x.0);
                    acc.ud_out.push((
                        person,
                        acc.day_lo + di as i64,
                        c.sessions,
                        c.events,
                        c.rev,
                        top,
                    ));
                    c.sessions = 0;
                    c.events = 0;
                    c.rev = 0;
                    c.cats.clear();
                    c.used = false;
                }
                acc.touched.clear();
                let glen = acc.ud_out.len() as u32 - gstart;
                if glen > 0 {
                    acc.ud_groups.push((person, gstart, glen));
                }
                acc
            },
        )
        .reduce(Acc::default, |mut a, b| {
            a.merge(b);
            a
        });
    pt!("sessionize");

    // ---------------- outputs ----------------
    let mut ud_groups = total.ud_groups;
    ud_groups.par_sort_unstable_by(|a, b| key_cmp(a.0, b.0));
    let mut ud_rows: Vec<(u64, i64, u64, u64, i64, Option<u32>)> =
        Vec::with_capacity(total.ud_out.len());
    for &(_, s, l) in &ud_groups {
        ud_rows.extend_from_slice(&total.ud_out[s as usize..(s + l) as usize]);
    }

    let mut cd_rows: Vec<(u32, i64, u64, u64, i64, u64)> = Vec::new();
    for (i, a) in total.cd.iter().enumerate() {
        if a.sessions == 0 && a.assist == 0 {
            continue;
        }
        cd_rows.push((
            (i % nslots) as u32,
            day_lo + (i / nslots) as i64,
            a.sessions,
            a.purch,
            a.rev,
            a.assist,
        ));
    }
    let cn = |c: u32| -> &[u8] {
        if c == organic_camp {
            ORGANIC
        } else {
            &camp_names[c as usize]
        }
    };
    cd_rows.sort_by(|a, b| cn(a.0).cmp(cn(b.0)).then(a.1.cmp(&b.1)));

    fs::create_dir_all(outdir.join("user_daily")).unwrap();
    fs::create_dir_all(outdir.join("campaign_daily")).unwrap();
    write_user_parquet(&outdir.join("user_daily"), &ud_rows, &cat_tab);
    write_camp_parquet(
        &outdir.join("campaign_daily"),
        &cd_rows,
        &camp_names,
        &camp_chan,
        organic_camp,
    );
    pt!("output_rows");
    if write_csv {
        write_csvs(
            outdir, &ud_rows, &cd_rows, &camp_names, &camp_chan, &cat_tab, organic_camp,
        );
    }
    pt!("write");
    std::process::exit(0);
}

fn str_of(b: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(b) }
}

/// person key -> display bytes (scratch for packed keys)
fn person_bytes<'a>(k: u64, s: &'a mut [u8; 10]) -> &'a [u8] {
    key_bytes(k, s)
}

fn write_user_parquet(
    dir: &Path,
    rows: &[(u64, i64, u64, u64, i64, Option<u32>)],
    cat_tab: &[Box<[u8]>],
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let f = fs::File::create(dir.join("part-0.parquet")).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut w = ArrowWriter::try_new(f, schema.clone(), Some(props)).unwrap();
    const CHUNK: usize = 2_000_000;
    for chunk in rows.chunks(CHUNK) {
        let mut vals: Vec<u8> = Vec::with_capacity(chunk.len() * 8);
        let mut offs: Vec<i32> = Vec::with_capacity(chunk.len() + 1);
        offs.push(0);
        for r in chunk {
            let mut s = [0u8; 10];
            vals.extend_from_slice(person_bytes(r.0, &mut s));
            offs.push(vals.len() as i32);
        }
        let uid =
            StringArray::new(arrow_buffer::OffsetBuffer::new(offs.into()), vals.into(), None);
        let day: Vec<i32> = chunk.iter().map(|r| r.1 as i32).collect();
        let sess: Vec<i64> = chunk.iter().map(|r| r.2 as i64).collect();
        let ev: Vec<i64> = chunk.iter().map(|r| r.3 as i64).collect();
        let rev: Vec<i64> = chunk.iter().map(|r| r.4).collect();
        let top: Vec<Option<&str>> = chunk
            .iter()
            .map(|r| r.5.map(|c| str_of(&cat_tab[c as usize])))
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(uid)),
                Arc::new(Date32Array::from(day)),
                Arc::new(Int64Array::from(sess)),
                Arc::new(Int64Array::from(ev)),
                Arc::new(Int64Array::from(rev)),
                Arc::new(StringArray::from(top)),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
    }
    w.close().unwrap();
}

fn write_camp_parquet(
    dir: &Path,
    rows: &[(u32, i64, u64, u64, i64, u64)],
    camp_names: &[Box<[u8]>],
    camp_chan: &[Box<[u8]>],
    organic_camp: u32,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let f = fs::File::create(dir.join("part-0.parquet")).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut w = ArrowWriter::try_new(f, schema.clone(), Some(props)).unwrap();
    const CHUNK: usize = 2_000_000;
    for chunk in rows.chunks(CHUNK) {
        let cid: Vec<&str> = chunk
            .iter()
            .map(|r| {
                if r.0 == organic_camp {
                    str_of(ORGANIC)
                } else {
                    str_of(&camp_names[r.0 as usize])
                }
            })
            .collect();
        let ch: Vec<&str> = chunk
            .iter()
            .map(|r| {
                if r.0 == organic_camp {
                    "organic"
                } else {
                    str_of(&camp_chan[r.0 as usize])
                }
            })
            .collect();
        let day: Vec<i32> = chunk.iter().map(|r| r.1 as i32).collect();
        let sess: Vec<i64> = chunk.iter().map(|r| r.2 as i64).collect();
        let purch: Vec<i64> = chunk.iter().map(|r| r.3 as i64).collect();
        let rev: Vec<i64> = chunk.iter().map(|r| r.4 as i64).collect();
        let assist: Vec<i64> = chunk.iter().map(|r| r.5 as i64).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(cid)),
                Arc::new(StringArray::from(ch)),
                Arc::new(Date32Array::from(day)),
                Arc::new(Int64Array::from(sess)),
                Arc::new(Int64Array::from(purch)),
                Arc::new(Int64Array::from(rev)),
                Arc::new(Int64Array::from(assist)),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
    }
    w.close().unwrap();
}

fn write_csvs(
    outdir: &Path,
    ud_rows: &[(u64, i64, u64, u64, i64, Option<u32>)],
    cd_rows: &[(u32, i64, u64, u64, i64, u64)],
    camp_names: &[Box<[u8]>],
    camp_chan: &[Box<[u8]>],
    cat_tab: &[Box<[u8]>],
    organic_camp: u32,
) {
    let fmt = |days: i64| {
        let (y, m, d) = civil_from_days(days);
        format!("{:04}-{:02}-{:02}", y, m, d)
    };
    let mut f = BufWriter::new(fs::File::create(outdir.join("user_daily.csv")).unwrap());
    f.write_all(b"user_id,day,sessions,events,revenue_cents,top_category\n")
        .unwrap();
    for r in ud_rows {
        let top = r.5.map(|c| str_of(&cat_tab[c as usize])).unwrap_or("");
        let mut s = [0u8; 10];
        writeln!(
            f,
            "{},{},{},{},{},{}",
            str_of(person_bytes(r.0, &mut s)),
            fmt(r.1),
            r.2,
            r.3,
            r.4,
            top
        )
        .unwrap();
    }
    let mut f = BufWriter::new(
        fs::File::create(outdir.join("campaign_daily.csv")).unwrap(),
    );
    f.write_all(b"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n")
        .unwrap();
    for r in cd_rows {
        let (nm, ch) = if r.0 == organic_camp {
            (str_of(ORGANIC), "organic")
        } else {
            (str_of(&camp_names[r.0 as usize]), str_of(&camp_chan[r.0 as usize]))
        };
        writeln!(f, "{},{},{},{},{},{},{}", nm, ch, fmt(r.1), r.2, r.3, r.4, r.5).unwrap();
    }
}
