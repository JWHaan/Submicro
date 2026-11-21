// Shopping Sessions — v2 pipeline.
// Same semantics as the verified v1; packed u64 keys, streaming tokenizer,
// 16-24B records, open-addressing dedup, dense per-person runs.
use std::cell::Cell;
use std::cmp::{max, min};
use std::env;
use std::fs;
use std::hash::Hasher;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use memchr::{memchr, memmem};
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};

use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

mod plain_writer;
mod scanner;
use scanner::each_line;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const MIN30: i64 = 1_800_000;
const D24: i64 = 86_400_000;
const D7: i64 = 604_800_000;
const T_INVALID: i64 = i64::MIN;
const ORGANIC: &[u8] = b"organic";
const FB_TAG: u64 = 0xF << 60; // fallback key tag (top nibble); packed keys stay below

static FILES: OnceLock<Vec<&'static [u8]>> = OnceLock::new();
thread_local! {
    static T_LAST: std::cell::Cell<std::time::Instant> = std::cell::Cell::new(std::time::Instant::now());
}

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
const fn build_pack_tab() -> [u8; 256] {
    let mut t = [0xFFu8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = i as u8 + 1;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[b'A' as usize + i] = i as u8 + 11;
        i += 1;
    }
    t[b'_' as usize] = 37;
    let mut i = 0;
    while i < 26 {
        t[b'a' as usize + i] = i as u8 + 38;
        i += 1;
    }
    t
}
static PACK_TAB: [u8; 256] = build_pack_tab();

#[inline(always)]
fn packable(s: &[u8]) -> u64 {
    if s.is_empty() || s.len() > 10 {
        return u64::MAX; // signal: not packable
    }
    let mut k = 0u64;
    for &c in s {
        let v = PACK_TAB[c as usize];
        if v == 0xFF {
            return u64::MAX;
        }
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
        v = v.saturating_mul(10).saturating_add((c - b'0') as i64);
    }
    v
}

/// All 8 lanes are ASCII digits ('0'..='9'). Carry-in can only narrow a
/// lane's pass range, and carries originate only at lanes that already
/// fail the high-nibble test, so no false positives/negatives.
#[inline(always)]
fn swar_digits(w: u64) -> bool {
    const HI: u64 = 0xF0F0F0F0F0F0F0F0;
    const D3: u64 = 0x3030303030303030;
    (w & HI) == D3 && (w.wrapping_add(0x0606060606060606) & HI) == D3
}

thread_local! {
    /// (s[0..8] as u64, s[8..10] as u16) -> day epoch ms. Last seen "YYYY-MM-DD".
    static TS_CACHE: Cell<(u64, u16, i64)> = const { Cell::new((0, 0, T_INVALID)) };
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
    let off_ms = if n == 20 && s[19] == b'Z' {
        0i64
    } else if n == 25
        && (s[19] == b'+' || s[19] == b'-')
        && s[22] == b':'
        && is_digits(&s[20..22])
        && is_digits(&s[23..25])
    {
        let oh = (s[20] - b'0') as i64 * 10 + (s[21] - b'0') as i64;
        let om = (s[23] - b'0') as i64 * 10 + (s[24] - b'0') as i64;
        if oh > 23 || om > 59 {
            return T_INVALID;
        }
        let o = (oh * 60 + om) * 60000;
        if s[19] == b'+' {
            o
        } else {
            -o
        }
    } else {
        return T_INVALID;
    };
    if !(s[4] == b'-' && s[7] == b'-' && s[10] == b'T' && s[13] == b':' && s[16] == b':') {
        return T_INVALID;
    }
    // SWAR digit check over the 14 digit positions; separator lanes are
    // masked to '0' (already verified above), byte 19 masked (checked as
    // 'Z'/'+''-' by the length test).
    // Date-prefix cache: input spans few distinct days; a byte-identical
    // "YYYY-MM-DD" prefix has already passed its digit/range checks, so a
    // hit reuses the day->ms decode and skips the w0 SWAR check, the
    // month/leap/day validation, and days_from_civil.
    let k0 = u64::from_le_bytes(s[0..8].try_into().unwrap());
    let k1 = u16::from_le_bytes(s[8..10].try_into().unwrap());
    let day_ms = match TS_CACHE.with(|c| c.get()) {
        (ck0, ck1, ms) if ck0 == k0 && ck1 == k1 => ms,
        _ => {
            let w0 = u64::from_le_bytes(s[0..8].try_into().unwrap());
            if !swar_digits((w0 & 0x00FFFF00FFFFFFFF) | 0x3000003000000000) {
                return T_INVALID;
            }
            let dg = |i: usize| (s[i] - b'0') as i64;
            let y = dg(0) * 1000 + dg(1) * 100 + dg(2) * 10 + dg(3);
            let mo = dg(5) * 10 + dg(6);
            let d = dg(8) * 10 + dg(9);
            if mo < 1 || mo > 12 {
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
            let ms = days_from_civil(y, mo, d) * 86400000;
            TS_CACHE.with(|c| c.set((k0, k1, ms)));
            ms
        }
    };
    let w1 = u64::from_le_bytes(s[8..16].try_into().unwrap());
    let x2 = (u32::from_le_bytes(s[16..20].try_into().unwrap()) & 0x00FFFF00) | 0x30000030;
    if !swar_digits((w1 & 0xFFFF00FFFF00FFFF) | 0x0000300000300000)
        || (x2 & 0xF0F0F0F0 != 0x30303030)
        || (x2.wrapping_add(0x06060606) & 0xF0F0F0F0 != 0x30303030)
    {
        return T_INVALID;
    }
    let dg = |i: usize| (s[i] - b'0') as i64;
    let h = dg(11) * 10 + dg(12);
    let mi = dg(14) * 10 + dg(15);
    let sec = dg(17) * 10 + dg(18);
    if h > 23 || mi > 59 || sec > 59 {
        return T_INVALID;
    }
    day_ms + (h * 3600 + mi * 60 + sec) * 1000 - off_ms
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------
#[derive(Clone, Copy)]
struct FEv {
    uid: u64,  // user_id key, 0 = empty
    prod: u64, // product key, 0 = empty
    ts: i64,
    price: u32,
    etype: u8, // 0 view,1 cart,2 purchase,3 bad
    pflag: u8, // 0 null,1 ok,2 bad
    stale: u8, // wrapping(ingest - ts) > D24; only read for valid in-window ts
    _p: u8,
}
// Dedup scatter source: eid+ingest only. gidx is implicit (task base + index).
#[derive(Clone, Copy)]
struct DIn {
    eid: u64,
    ingest: i64,
}

#[derive(Clone, Copy)]
struct Ev {
    person: u64, // canonical person key
    ts: i64,
    cat: u32,
    price: u32,
}


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

#[derive(Clone, Copy)]
#[repr(C)]
struct DRec {
    eid: u64,
    ingest: i64,
    gidx: u32,
}

// Open-addressing u64 key table over a dedup-record slice; keeps the row
// index with min `ingest` (first wins ties). Returns surviving indices.
fn dedup_min_d(rows: &[DRec]) -> Vec<u32> {
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
                if r.ingest < rows[vals[s] as usize].ingest {
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
    #[cfg(unix)]
    {
        let files = FILES.get().unwrap().clone();
        std::thread::spawn(move || {
            for b in files {
                unsafe {
                    libc::madvise(b.as_ptr() as *mut libc::c_void, b.len(), libc::MADV_WILLNEED);
                }
            }
        });
    }
    let phase_timing = env::var("PHASE_TIMING").is_ok();
    macro_rules! pt {
        ($name:expr) => {
            if phase_timing {
                let n = std::time::Instant::now();
                T_LAST.with(|t| {
                    eprintln!("[phase] {}: {:?}", $name, n - t.get());
                    t.set(n);
                });
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
        each_line(b, 0, b.len(), true, |&[prod, cat]| {
            if !prod.is_empty() && !cat.is_empty() {
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
        each_line(b, 0, b.len(), true, |&[cid, ch]| {
            if !cid.is_empty() && !ch.is_empty() {
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
                each_line(b, cs, ce, cs == 0, |&[a, bb]| {
                    if !a.is_empty() && !bb.is_empty() {
                        out.push((key_of(a), key_of(bb)));
                    }
                });
                out
            })
            .collect();
        // dense ids for UF via open map
        let mut idmap: FxHashMap<u64, u32> = FxHashMap::default();
        let mut keys: Vec<u64> = Vec::new();
        let idx_of = |k: u64, idmap: &mut FxHashMap<u64, u32>, keys: &mut Vec<u64>| -> u32 {
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
    const NSHARD: usize = 256;
    let mut tasks = Vec::new();
    for f in 0..nev {
        let fi = f as u16;
        for (cs, ce) in line_chunks(buf(fi), ncpu * 2) {
            tasks.push(Task { file: fi, cs, ce });
        }
    }
    let shard_of = |eid: u64| (fx64(eid) as usize) & (NSHARD - 1);
    let parsed: Vec<(Vec<FEv>, Vec<DIn>, Vec<u32>)> = tasks
        .par_iter()
        .map(|t| {
            let b = buf(t.file);
            let mut rows: Vec<FEv> = Vec::with_capacity((t.ce - t.cs) / 64);
            let mut din: Vec<DIn> = Vec::with_capacity((t.ce - t.cs) / 64);
            let mut cnt = vec![0u32; NSHARD];
            each_line(b, t.cs, t.ce, t.cs == 0, |&[eid, uid, ts_s, ing_s, et_s, prod, price]| {
                if eid.is_empty() {
                    return;
                }
                let ingest = parse_ts(ing_s);
                if ingest == T_INVALID {
                    return;
                }
                let ts = parse_ts(ts_s);
                let etype = match et_s {
                    b"view" => 0u8,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 3,
                };
                let (pv, pflag) = match price {
                    [] => (0u32, 0u8),
                    _ => {
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
                let ek = key_of(eid);
                cnt[shard_of(ek)] += 1;
                rows.push(FEv {
                    uid: key_of(uid),
                    prod: prod_key,
                    ts,
                    price: pv,
                    etype,
                    pflag,
                    stale: (ingest.wrapping_sub(ts) > D24) as u8,
                    _p: 0,
                });
                din.push(DIn { eid: ek, ingest });
            });
            (rows, din, cnt)
        })
        .collect();
    let mut task_rows = Vec::with_capacity(parsed.len());
    let mut dins = Vec::with_capacity(parsed.len());
    let mut cnts = Vec::with_capacity(parsed.len());
    for (r, d, c) in parsed {
        task_rows.push(r);
        dins.push(d);
        cnts.push(c);
    }

    pt!("ev_parse");

    // single-arena scatter by eid-hash over compact dedup records:
    // per-task histograms -> global offsets -> each task writes directly
    // into its disjoint (task, shard) region. task_rows stays in place;
    // survivors are marked in `kept` and the filter scans rows in situ.
    let nt = task_rows.len();
    let mut tbase = vec![0u32; nt + 1];
    for t in 0..nt {
        tbase[t + 1] = tbase[t] + task_rows[t].len() as u32;
    }
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
    let mut arena: Vec<DRec> = Vec::with_capacity(total);
    let arena_ptr = RawPtr::<DRec>(arena.as_mut_ptr() as usize, std::marker::PhantomData);
    let task_rows_ref = &task_rows;
    let dins_ref = &dins;
    let base_ref = &base;
    let prefix_ref = &prefix;
    let tbase_ref = &tbase;
    (0..nt).into_par_iter().for_each(|t| {
        let mut off: [u32; NSHARD] = [0; NSHARD];
        for s in 0..NSHARD {
            off[s] = base_ref[s] + prefix_ref[t][s];
        }
        let g0 = tbase_ref[t];
        let dst = arena_ptr.0 as *mut DRec;
        for (j, r) in dins_ref[t].iter().enumerate() {
            let s = shard_of(r.eid);
            unsafe {
                *dst.add(off[s] as usize) = DRec {
                    eid: r.eid,
                    ingest: r.ingest,
                    gidx: g0 + j as u32,
                }
            };
            off[s] += 1;
        }
    });
    unsafe { arena.set_len(total) };
    drop(cnts);
    drop(prefix);
    drop(dins);
    pt!("ev_scatter");

    // per-eid-shard dedup -> mark surviving flat indices
    let arena_ref = &arena;
    let kept: Vec<AtomicU8> = (0..total).map(|_| AtomicU8::new(0)).collect();
    let kept_ptr = RawPtr::<AtomicU8>(kept.as_ptr() as usize, std::marker::PhantomData);
    (0..NSHARD).into_par_iter().for_each(|s| {
        let rows = &arena_ref[base_ref[s] as usize..base_ref[s + 1] as usize];
        let dst = kept_ptr.0 as *const AtomicU8;
        for i in dedup_min_d(rows) {
            unsafe {
                (*dst.add(rows[i as usize].gidx as usize)).store(1, Ordering::Relaxed)
            };
        }
    });
    drop(arena);
    drop(base);
    pt!("ev_dedup");

    let prod_map_ref = &prod_map;
    let kept_ref = &kept;
    let pshard_of = |person: u64| (fx64(person) as usize) & (NSHARD - 1);
    let ev_parts: Vec<(Vec<Ev>, Vec<u32>)> = (0..nt)
        .into_par_iter()
        .map(|t| {
            let rows = &task_rows_ref[t];
            let g0 = tbase_ref[t] as usize;
            let mut out = Vec::with_capacity(rows.len());
            let mut cnt = vec![0u32; NSHARD];
            for (j, d) in rows.iter().enumerate() {
                if kept_ref[g0 + j].load(Ordering::Relaxed) == 0 {
                    continue;
                }
                if d.uid == 0 || d.prod == 0 || d.etype == 3 || d.ts == T_INVALID || d.ts < ws
                    || d.ts >= we
                {
                    continue;
                }
                if d.stale != 0 {
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
                cnt[pshard_of(person)] += 1;
                out.push(Ev {
                    person,
                    ts: d.ts,
                    cat,
                    price,
                });
            }
            (out, cnt)
        })
        .collect();
    drop(task_rows);
    drop(kept);
    drop(tbase);
    pt!("ev_filter");
    // parallel scatter into person-shard-grouped arena, then per-shard sort
    let nt = ev_parts.len();
    let mut prefix = vec![vec![0u32; NSHARD]; nt + 1];
    for t in 0..nt {
        for s in 0..NSHARD {
            prefix[t + 1][s] = prefix[t][s] + ev_parts[t].1[s];
        }
    }
    let mut pbase = vec![0u32; NSHARD + 1];
    for s in 0..NSHARD {
        pbase[s + 1] = pbase[s] + prefix[nt][s];
    }
    let total = pbase[NSHARD] as usize;
    let mut ev_arena: Vec<Ev> = Vec::with_capacity(total);
    let ev_ptr = RawPtr::<Ev>(ev_arena.as_mut_ptr() as usize, std::marker::PhantomData);
    let parts_ref = &ev_parts;
    let pbase_ref = &pbase;
    let prefix_ref = &prefix;
    (0..nt).into_par_iter().for_each(|t| {
        let mut off: [u32; NSHARD] = [0; NSHARD];
        for s in 0..NSHARD {
            off[s] = pbase_ref[s] + prefix_ref[t][s];
        }
        let dst = ev_ptr.0 as *mut Ev;
        for r in &parts_ref[t].0 {
            let s = pshard_of(r.person);
            unsafe { *dst.add(off[s] as usize) = *r };
            off[s] += 1;
        }
    });
    unsafe { ev_arena.set_len(total) };
    drop(ev_parts);
    drop(prefix);
    let mut rest: &mut [Ev] = &mut ev_arena;
    let mut slices = Vec::with_capacity(NSHARD);
    for s in 0..NSHARD {
        let n = (pbase[s + 1] - pbase[s]) as usize;
        let (a, b) = rest.split_at_mut(n);
        slices.push(a);
        rest = b;
    }
    slices.into_par_iter().for_each(|sl| {
        sl.sort_unstable_by(|a, b| (a.person, a.ts).cmp(&(b.person, b.ts)))
    });
    let evs = ev_arena;
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
            each_line(b, t.cs, t.ce, t.cs == 0, |&[cid, uid, camp, ts_s]| {
                if cid.is_empty() {
                    return;
                }
                rows.push(RawCl {
                    cid: key_of(cid),
                    uid: key_of(uid),
                    camp: match camp {
                        [] => u32::MAX,
                        _ => {
                            let ck = key_of(camp);
                            camp_map_ref.get(ck).map(|v| v as u32).unwrap_or(u32::MAX)
                        }
                    },
                    ts: parse_ts(ts_s),
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
    let cl_runs = person_runs(&clicks, |c| c.person);
    // both arenas are grouped by fx64(person)&255 shards; per-shard merge join
    // maps each event-run to its click-run without any binary search
    let mut ev_bounds = [0u32; NSHARD + 1];
    for r in &ev_runs {
        ev_bounds[((fx64(r.0) as usize) & (NSHARD - 1)) + 1] += 1;
    }
    for i in 0..NSHARD {
        ev_bounds[i + 1] += ev_bounds[i];
    }
    let mut cl_bounds = [0u32; NSHARD + 1];
    for r in &cl_runs {
        cl_bounds[((fx64(r.0) as usize) & (NSHARD - 1)) + 1] += 1;
    }
    for i in 0..NSHARD {
        cl_bounds[i + 1] += cl_bounds[i];
    }
    let cl_idx: Vec<u32> = {
        let mut v = vec![u32::MAX; ev_runs.len()];
        let p = RawPtr::<u32>(v.as_mut_ptr() as usize, std::marker::PhantomData);
        let (evr, clr) = (&ev_runs, &cl_runs);
        (0..NSHARD).into_par_iter().for_each(|s| {
            let (mut i, mut j) = (ev_bounds[s] as usize, cl_bounds[s] as usize);
            let (ie, je) = (ev_bounds[s + 1] as usize, cl_bounds[s + 1] as usize);
            while i < ie && j < je {
                let (ep, cp) = (evr[i].0, clr[j].0);
                if ep < cp {
                    i += 1;
                } else if cp < ep {
                    j += 1;
                } else {
                    unsafe { *(p.0 as *mut u32).add(i) = j as u32 };
                    i += 1;
                    j += 1;
                }
            }
        });
        v
    };

    let day_lo = ws / D24;
    let n_days = ((we - 1) / D24 - day_lo + 1) as usize;
    let nslots = n_camps as usize + 1;
    let cat_tab_ref = &cat_tab;
    let total = ev_runs
        .par_iter()
        .enumerate()
        .with_min_len(1 << 16)
        .fold(
            || Acc::new(n_days, n_camps, day_lo),
            |mut acc, (ri, &(person, lo, hi))| {
                acc.sessions.clear();
                acc.ats.clear();
                acc.acamp.clear();
                acc.cts.clear();
                acc.ccamp.clear();
                acc.alive.reset();
                let ci = cl_idx[ri];
                if ci != u32::MAX {
                    let (_, clo, chi) = cl_runs[ci as usize];
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
    let codec = plain_writer::selected_codec();
    let mut ud_groups = total.ud_groups;
    // The Parquet tables are unordered. The direct writer can consume the
    // accumulator's dense rows in place; only the reference/CSV use sorted IDs.
    if codec.is_none() || write_csv {
        ud_groups.par_sort_unstable_by(|a, b| key_cmp(a.0, b.0));
    }
    let mut goff = Vec::new();
    if codec.is_none() {
        goff.reserve(ud_groups.len() + 1);
        goff.push(0);
        for &(_, _, l) in &ud_groups {
            goff.push(goff.last().unwrap() + l);
        }
    }
    let ud_rows: Vec<(u64, i64, u64, u64, i64, Option<u32>)> = if write_csv {
        let mut ud_rows = Vec::with_capacity(total.ud_out.len());
        for &(_, s, l) in &ud_groups {
            ud_rows.extend_from_slice(&total.ud_out[s as usize..(s + l) as usize]);
        }
        ud_rows
    } else {
        Vec::new()
    };

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
    if codec.is_none() || write_csv {
        cd_rows.sort_by(|a, b| cn(a.0).cmp(cn(b.0)).then(a.1.cmp(&b.1)));
    }

    fs::create_dir_all(outdir.join("user_daily")).unwrap();
    fs::create_dir_all(outdir.join("campaign_daily")).unwrap();
    let ud = outdir.join("user_daily");
    let cd = outdir.join("campaign_daily");
    if let Some(codec) = codec {
        rayon::join(
            || plain_writer::parts(&ud, &total.ud_out, |out, rows| {
                plain_writer::users(out, rows, &cat_tab, codec)
            }),
            || plain_writer::parts(&cd, &cd_rows, |out, rows| {
                plain_writer::campaigns(out, rows, &camp_names, &camp_chan, organic_camp, codec)
            }),
        );
    } else {
        rayon::join(
            || write_user_parquet(&ud, &total.ud_out, &ud_groups, &goff, &cat_tab),
            || write_camp_parquet(&cd, &cd_rows, &camp_names, &camp_chan, organic_camp),
        );
    }
    pt!("output_rows");
    if write_csv {
        write_csvs(
            outdir, &ud_rows, &cd_rows, &camp_names, &camp_chan, &cat_tab, organic_camp,
        );
    }
    pt!("write");
    if std::env::var_os("NO_TEARDOWN").is_none() {
        let mut regions: Vec<(usize, usize)> = Vec::new();
        reg_buf(&mut regions, &evs);
        reg_buf(&mut regions, &clicks);
        reg_buf(&mut regions, &total.ud_out);
        reg_buf(&mut regions, &total.cd);
        reg_buf(&mut regions, &ud_rows);
        reg_buf(&mut regions, &cd_rows);
        reg_buf(&mut regions, &ud_groups);
        parallel_teardown(&regions);
    }
    std::process::exit(0);
}

fn reg_buf<T>(out: &mut Vec<(usize, usize)>, v: &Vec<T>) {
    let n = v.capacity().saturating_mul(std::mem::size_of::<T>());
    if n >= (1 << 20) {
        out.push((v.as_ptr() as usize, n));
    }
}

/// Discard interior pages of our own dead buffers across all threads so the
/// kernel's serial VMA teardown at exit has nothing left to zap. Only pages
/// fully inside each buffer are touched: malloc chunk headers/footers and
/// neighboring allocations stay intact. Set NO_TEARDOWN to skip.
#[cfg(unix)]
fn parallel_teardown(regions: &[(usize, usize)]) {
    let mut spans: Vec<(usize, usize)> = Vec::new();
    for &(s, n) in regions {
        let s0 = (s + 4095) & !4095usize;
        let e0 = (s + n) & !4095usize;
        let mut p = s0;
        while p < e0 {
            let q = std::cmp::min(p + (64 << 20), e0);
            spans.push((p, q));
            p = q;
        }
    }
    spans.par_iter().for_each(|&(s, e)| unsafe {
        libc::madvise(s as *mut _, e - s, libc::MADV_DONTNEED);
    });
}

#[cfg(not(unix))]
fn parallel_teardown(_regions: &[(usize, usize)]) {}

fn str_of(b: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(b) }
}

/// person key -> display bytes (scratch for packed keys)
fn person_bytes<'a>(k: u64, s: &'a mut [u8; 10]) -> &'a [u8] {
    key_bytes(k, s)
}

type UdRow = (u64, i64, u64, u64, i64, Option<u32>);

fn write_user_parquet(
    dir: &Path,
    ud_out: &[UdRow],
    groups: &[(u64, u32, u32)],
    goff: &[u32],
    cat_tab: &[Box<[u8]>],
) {
    let nrows = *goff.last().unwrap() as usize;
    let nth = rayon::current_num_threads().min(nrows.max(1));
    let chunk = nrows.div_ceil(nth);
    (0..nth).into_par_iter().for_each(|i| {
        let lo = i * chunk;
        let hi = (lo + chunk).min(nrows);
        if lo >= hi && !(i == 0 && nrows == 0) {
            return;
        }
        write_user_part(dir, i, ud_out, groups, goff, lo, hi, cat_tab);
    });
}

fn write_user_part(
    dir: &Path,
    part: usize,
    ud_out: &[UdRow],
    groups: &[(u64, u32, u32)],
    goff: &[u32],
    lo: usize,
    hi: usize,
    cat_tab: &[Box<[u8]>],
) {
    // groups whose output ranges [goff[g], goff[g]+len) intersect [lo, hi)
    let g_lo = goff[1..].partition_point(|&e| (e as usize) <= lo);
    let g_hi = goff.partition_point(|&e| (e as usize) < hi).min(groups.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let f = fs::File::create(dir.join(format!("part-{}.parquet", part))).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let mut w = ArrowWriter::try_new(f, schema.clone(), Some(props)).unwrap();
    const CHUNK: usize = 2_000_000;
    let mut vals: Vec<u8> = Vec::with_capacity(CHUNK.min(hi - lo) * 8);
    let mut offs: Vec<i32> = Vec::with_capacity(CHUNK.min(hi - lo) + 1);
    let mut day: Vec<i32> = Vec::with_capacity(CHUNK.min(hi - lo));
    let mut sess: Vec<i64> = Vec::with_capacity(CHUNK.min(hi - lo));
    let mut ev: Vec<i64> = Vec::with_capacity(CHUNK.min(hi - lo));
    let mut rev: Vec<i64> = Vec::with_capacity(CHUNK.min(hi - lo));
    let mut top: Vec<Option<&str>> = Vec::with_capacity(CHUNK.min(hi - lo));
    offs.push(0);
    let mut cur = 0usize;
    for g in g_lo..g_hi {
        let &(_, s, l) = &groups[g];
        let gstart = goff[g] as usize;
        let mut a = lo.max(gstart) - gstart;
        let b = hi.min(gstart + l as usize) - gstart;
        while a < b {
            let take = (b - a).min(CHUNK - cur);
            for r in &ud_out[s as usize + a..s as usize + a + take] {
                let mut sc = [0u8; 10];
                vals.extend_from_slice(person_bytes(r.0, &mut sc));
                offs.push(vals.len() as i32);
                day.push(r.1 as i32);
                sess.push(r.2 as i64);
                ev.push(r.3 as i64);
                rev.push(r.4);
                top.push(r.5.map(|c| str_of(&cat_tab[c as usize])));
            }
            a += take;
            cur += take;
            if cur == CHUNK {
                flush_user_batch(&mut w, &schema, &mut vals, &mut offs, &mut day, &mut sess, &mut ev, &mut rev, &mut top);
                cur = 0;
            }
        }
    }
    if cur > 0 {
        flush_user_batch(&mut w, &schema, &mut vals, &mut offs, &mut day, &mut sess, &mut ev, &mut rev, &mut top);
    }
    w.close().unwrap();
}

fn flush_user_batch(
    w: &mut ArrowWriter<fs::File>,
    schema: &SchemaRef,
    vals: &mut Vec<u8>,
    offs: &mut Vec<i32>,
    day: &mut Vec<i32>,
    sess: &mut Vec<i64>,
    ev: &mut Vec<i64>,
    rev: &mut Vec<i64>,
    top: &mut Vec<Option<&str>>,
) {
    let uid = StringArray::new(
        arrow_buffer::OffsetBuffer::new(std::mem::take(offs).into()),
        std::mem::take(vals).into(),
        None,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(uid)),
            Arc::new(Date32Array::from(std::mem::take(day))),
            Arc::new(Int64Array::from(std::mem::take(sess))),
            Arc::new(Int64Array::from(std::mem::take(ev))),
            Arc::new(Int64Array::from(std::mem::take(rev))),
            Arc::new(StringArray::from(std::mem::take(top))),
        ],
    )
    .unwrap();
    w.write(&batch).unwrap();
    offs.push(0);
}

fn write_camp_parquet(
    dir: &Path,
    rows: &[(u32, i64, u64, u64, i64, u64)],
    camp_names: &[Box<[u8]>],
    camp_chan: &[Box<[u8]>],
    organic_camp: u32,
) {
    let nth = rayon::current_num_threads().min(rows.len().max(1));
    let chunk = rows.len().div_ceil(nth);
    (0..nth).into_par_iter().for_each(|i| {
        let lo = i * chunk;
        let hi = (lo + chunk).min(rows.len());
        if lo >= hi && !(i == 0 && rows.is_empty()) {
            return;
        }
        write_camp_part(dir, i, &rows[lo..hi], camp_names, camp_chan, organic_camp);
    });
}

fn write_camp_part(
    dir: &Path,
    part: usize,
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
    let f = fs::File::create(dir.join(format!("part-{}.parquet", part))).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
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
