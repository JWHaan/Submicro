// Shopping Sessions — single-binary pipeline.
// Mirrors reference/ref.py semantics exactly; optimized for throughput.
use std::cmp::{max, min};
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

use memchr::{memchr, memmem};
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
const NSHARD: usize = 256;
const T_INVALID: i64 = i64::MIN;
const ORGANIC: &[u8] = b"organic";

// Every input file is mmap'd; buffers live for 'static in FILES.
static FILES: OnceLock<Vec<&'static [u8]>> = OnceLock::new();
static PERSONS: OnceLock<Persons> = OnceLock::new();

#[inline]
fn buf(f: u16) -> &'static [u8] {
    FILES.get().unwrap()[f as usize]
}

fn fxh(s: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write(s);
    h.finish()
}

/// A string slice addressed by (file, offset, len); hashes/compares by content.
/// len==0 is the null marker (empty field).
#[derive(Clone, Copy)]
struct BKey {
    f: u16,
    off: u32,
    len: u32,
}
impl BKey {
    #[inline]
    fn get(&self) -> &'static [u8] {
        &buf(self.f)[self.off as usize..(self.off + self.len) as usize]
    }
    #[inline]
    fn null(f: u16) -> BKey {
        BKey { f, off: 0, len: 0 }
    }
}
impl PartialEq for BKey {
    fn eq(&self, o: &BKey) -> bool {
        self.get() == o.get()
    }
}
impl Eq for BKey {}
impl Hash for BKey {
    fn hash<H: Hasher>(&self, st: &mut H) {
        st.write(self.get());
    }
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
// Custom CSV dialect
// ---------------------------------------------------------------------------
#[inline]
fn field<'a>(s: &'a [u8]) -> Result<Option<&'a [u8]>, ()> {
    if s.is_empty() {
        return Ok(None);
    }
    if s[0] != b'"' {
        if memchr(b'"', s).is_some() {
            return Err(());
        }
        return Ok(Some(s));
    }
    if s.len() >= 2 && s[s.len() - 1] == b'"' && memchr(b'"', &s[1..s.len() - 1]).is_none() {
        let inner = &s[1..s.len() - 1];
        return Ok(if inner.is_empty() { None } else { Some(inner) });
    }
    Err(())
}

/// Split a physical line on commas; require exactly ncols and all fields valid.
fn split_line<'a>(line: &'a [u8], ncols: usize, out: &mut [Option<&'a [u8]>]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut n = 0;
    let mut start = 0usize;
    loop {
        let end = match memchr(b',', &line[start..]) {
            Some(p) => start + p,
            None => line.len(),
        };
        if n >= ncols {
            return false;
        }
        match field(&line[start..end]) {
            Err(()) => return false,
            Ok(v) => {
                out[n] = v;
                n += 1;
            }
        }
        if end == line.len() {
            break;
        }
        start = end + 1;
    }
    n == ncols
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
// Sharded concurrent interner.
// ID space is partitioned per shard: id = local_idx * ISHARD + shard.
// Each shard owns its map+table under a single lock; there is no global lock.
// ---------------------------------------------------------------------------
const ISHARD: usize = 64;
const ISHARD_MASK: u32 = (ISHARD - 1) as u32;

struct InternerShard {
    map: FxHashMap<Box<[u8]>, u32>,
    tab: Vec<Box<[u8]>>,
}
struct Interner {
    shards: Vec<RwLock<InternerShard>>,
}
impl Interner {
    fn new() -> Self {
        Interner {
            shards: (0..ISHARD)
                .map(|_| {
                    RwLock::new(InternerShard {
                        map: FxHashMap::default(),
                        tab: Vec::new(),
                    })
                })
                .collect(),
        }
    }
    #[inline]
    fn intern(&self, s: &[u8]) -> u32 {
        let i = (fxh(s) as usize) & (ISHARD - 1);
        {
            let sh = self.shards[i].read().unwrap();
            if let Some(&v) = sh.map.get(s) {
                return v;
            }
        }
        let mut sh = self.shards[i].write().unwrap();
        if let Some(&v) = sh.map.get(s) {
            return v;
        }
        let v = (sh.tab.len() as u32) * (ISHARD as u32) + i as u32;
        let b: Box<[u8]> = s.into();
        sh.tab.push(b.clone());
        sh.map.insert(b, v);
        v
    }
    /// Per-shard table lengths (used to bound the link-phase ID space).
    fn shard_lens(&self) -> Vec<u32> {
        self.shards
            .iter()
            .map(|s| s.read().unwrap().tab.len() as u32)
            .collect()
    }
    /// Consume the interner into lock-free per-shard tables for lookups by id.
    fn freeze(self) -> Tab {
        Tab {
            tabs: self
                .shards
                .into_iter()
                .map(|s| s.into_inner().unwrap().tab)
                .collect(),
        }
    }
}

/// Frozen interner tables: `get(id)` resolves an interned id back to bytes.
struct Tab {
    tabs: Vec<Vec<Box<[u8]>>>,
}
impl Tab {
    #[inline]
    fn get(&self, id: u32) -> &[u8] {
        &self.tabs[(id & ISHARD_MASK) as usize][(id >> 6) as usize]
    }
}

/// Identity resolution result built after the links phase.
/// `link_len[s]` = interned-id locals in shard s that came from links/.
/// `person[id]` = canonical (lex-min) interned id of the id's component.
/// Interned ids with local >= link_len are event/click-only uids (their own person).
struct Persons {
    link_len: Vec<u32>,
    person: Vec<u32>,
}
impl Persons {
    #[inline]
    fn lookup(&self, id: u32) -> u32 {
        let s = (id & ISHARD_MASK) as usize;
        let l = (id >> 6) as usize;
        if l < self.link_len[s] as usize {
            self.person[id as usize]
        } else {
            id
        }
    }
}

/// Flatten per-task row vecs into N per-shard vecs. Each task is locally
/// counting-sorted (flat + offsets), then shards gather slices in parallel —
/// O(N) total, contiguous output, no per-bucket allocation churn.
fn scatter<T: Copy + Send + Sync, const N: usize>(
    tasks: &[Vec<T>],
    fill: T,
    shard: impl Fn(&T) -> usize + Sync,
) -> Vec<Vec<T>> {
    let sorted: Vec<(Vec<T>, Vec<u32>)> = tasks
        .par_iter()
        .map(|v| {
            let mut c = vec![0u32; N];
            for r in v {
                c[shard(r)] += 1;
            }
            let mut offs = Vec::with_capacity(N + 1);
            let mut acc = 0u32;
            for s in 0..N {
                offs.push(acc);
                acc += c[s];
            }
            offs.push(acc);
            let mut pos = offs.clone();
            let mut flat = vec![fill; v.len()];
            for r in v {
                let s = shard(r);
                flat[pos[s] as usize] = *r;
                pos[s] += 1;
            }
            (flat, offs)
        })
        .collect();
    (0..N)
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
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------
#[derive(Clone, Copy)]
struct RawEv {
    eid_h: u64, // fxh(eid) — shard partitioning + dedup probing
    eid: BKey,
    uid: u32, // interned id, u32::MAX = empty (dropped at validate, after dedup)
    prod: BKey,
    ingest: i64,
    ts: i64,
    price: u32,
    etype: u8, // 0 view,1 cart,2 purchase,3 bad
    pflag: u8, // 0 null,1 ok,2 bad
}
impl RawEv {
    #[inline]
    fn empty() -> RawEv {
        RawEv {
            eid_h: 0,
            eid: BKey::null(0),
            uid: u32::MAX,
            prod: BKey::null(0),
            ingest: 0,
            ts: 0,
            price: 0,
            etype: 0,
            pflag: 0,
        }
    }
}

#[derive(Clone, Copy)]
struct Ev {
    person: u32,
    ts: i64,
    cat: u32,  // cat index; only meaningful when price>0
    price: u32,
}

#[derive(Clone, Copy)]
struct RawCl {
    cid_h: u64,
    cid: BKey,
    uid: u32,  // interned id, u32::MAX = empty
    camp: u32, // campaign table index, u32::MAX = unknown
    ts: i64,
}
impl RawCl {
    #[inline]
    fn empty() -> RawCl {
        RawCl {
            cid_h: 0,
            cid: BKey::null(0),
            uid: u32::MAX,
            camp: u32::MAX,
            ts: 0,
        }
    }
}

#[derive(Clone, Copy)]
struct Cl {
    person: u32,
    ts: i64,
    camp: u32,
    cid: BKey,
}

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
#[derive(Default)]
struct UdAcc {
    sessions: u64,
    events: u64,
    rev: i64,
    cats: FxHashMap<u32, i64>,
}

#[derive(Default)]
struct CdAcc {
    sessions: u64,
    purch: u64,
    rev: i64,
    assist: u64,
}

struct Session {
    start: i64,
    n: u64,
    rev: i64,
    cats: FxHashMap<u32, i64>,
}
impl Default for Session {
    fn default() -> Self {
        Session {
            start: 0,
            n: 0,
            rev: 0,
            cats: FxHashMap::default(),
        }
    }
}

/// Indexed multiset of live campaign indices for the assist two-pointer.
/// A campaign stays live until every one of its clicks leaves the window.
#[derive(Default)]
struct Alive {
    pos: Vec<u32>, // elems-index of c, or u32::MAX when not live
    cnt: Vec<u32>, // live click count per campaign
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
    ud: FxHashMap<u64, UdAcc>,
    cd: FxHashMap<u64, CdAcc>,
    // scratch buffers reused across persons on this thread
    sessions: Vec<(i64, i64, i64)>, // (start, day, rev)
    ats: Vec<i64>,
    acamp: Vec<u32>,
    cts: Vec<i64>,
    ccamp: Vec<u32>,
    cur: Session,
    alive: Alive,
}

impl Acc {
    fn merge(&mut self, o: Acc) {
        for (k, a) in o.ud {
            let e = self.ud.entry(k).or_default();
            e.sessions += a.sessions;
            e.events += a.events;
            e.rev += a.rev;
            for (c, v) in a.cats {
                *e.cats.entry(c).or_insert(0) += v;
            }
        }
        for (k, a) in o.cd {
            let e = self.cd.entry(k).or_default();
            e.sessions += a.sessions;
            e.purch += a.purch;
            e.rev += a.rev;
            e.assist += a.assist;
        }
    }

    /// Close the in-progress session: record it and fold its counts into ud.
    fn flush_session(&mut self, person: u32) {
        let day = self.cur.start / D24;
        self.sessions.push((self.cur.start, day, self.cur.rev));
        let ud = self.ud.entry(udkey(person, day)).or_default();
        ud.sessions += 1;
        ud.events += self.cur.n;
        ud.rev += self.cur.rev;
        for (c, v) in self.cur.cats.drain() {
            *ud.cats.entry(c).or_insert(0) += v;
        }
        self.cur.n = 0;
        self.cur.rev = 0;
    }
}

#[inline]
fn udkey(person: u32, day: i64) -> u64 {
    ((person as u64) << 32) | (day as u64 & 0xffff_ffff)
}
#[inline]
fn cdkey(camp: u32, day: i64) -> u64 {
    ((camp as u64) << 32) | (day as u64 & 0xffff_ffff)
}

fn person_runs<T: Sync>(arr: &[T], key: impl Fn(&T) -> u32 + Sync) -> Vec<(u32, u32, u32)> {
    let n = arr.len();
    if n == 0 {
        return Vec::new();
    }
    let nblk = (rayon::current_num_threads() * 4).max(1);
    let blksz = n.div_ceil(nblk).max(1);
    let parts: Vec<Vec<(u32, u32, u32)>> = (0..nblk)
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
    let mut out: Vec<(u32, u32, u32)> = Vec::new();
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

    // manifest
    let manifest = fs::read(data.join("manifest.json")).unwrap_or_default();
    let ws = json_str(&manifest, "window_start").map(parse_ts).unwrap_or(T_INVALID);
    let we = json_str(&manifest, "window_end").map(parse_ts).unwrap_or(T_INVALID);
    assert!(ws != T_INVALID && we != T_INVALID, "bad manifest");
    let click_lo = ws - D7;

    // enumerate + map files
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

    // products.csv -> prod -> cat_idx
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
    let mut prod_map: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();
    {
        let b = buf(prod_f);
        let mut pos = match memchr(b'\n', b) {
            Some(p) => p + 1,
            None => b.len(),
        };
        let mut fields = [None; 8];
        while pos < b.len() {
            let e = memchr(b'\n', &b[pos..]).map(|p| pos + p).unwrap_or(b.len());
            if split_line(&b[pos..e], 2, &mut fields) {
                if let (Some(prod), Some(cat)) = (fields[0], fields[1]) {
                    let ci = intern_cat(cat);
                    prod_map.insert(prod.into(), ci); // last wins, as reference
                }
            }
            pos = e + 1;
        }
    }

    // campaigns.csv -> camp -> (camp_idx, channel); last wins
    let mut camp_names: Vec<Box<[u8]>> = Vec::new();
    let mut camp_chan: Vec<Box<[u8]>> = Vec::new();
    let mut camp_map: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();
    {
        let b = buf(camp_f);
        let mut pos = match memchr(b'\n', b) {
            Some(p) => p + 1,
            None => b.len(),
        };
        let mut fields = [None; 8];
        while pos < b.len() {
            let e = memchr(b'\n', &b[pos..]).map(|p| pos + p).unwrap_or(b.len());
            if split_line(&b[pos..e], 2, &mut fields) {
                if let (Some(cid), Some(ch)) = (fields[0], fields[1]) {
                    match camp_map.get(cid) {
                        Some(&i) => camp_chan[i as usize] = ch.into(),
                        None => {
                            let i = camp_names.len() as u32;
                            camp_names.push(cid.into());
                            camp_chan.push(ch.into());
                            camp_map.insert(cid.into(), i);
                        }
                    }
                }
            }
            pos = e + 1;
        }
    }
    let n_camps = camp_names.len() as u32;
    let organic_camp = n_camps;

    // links -> union-find -> person_of
    let interner = Interner::new();
    {
        let link_tasks: Vec<(u16, usize, usize)> = (ncl..nln)
            .flat_map(|fi| {
                line_chunks(buf(fi as u16), ncpu * 2)
                    .into_iter()
                    .map(move |(cs, ce)| (fi as u16, cs, ce))
            })
            .collect();
        let pairs: Vec<(u32, u32)> = link_tasks
            .par_iter()
            .flat_map(|&(f, cs, ce)| {
                let b = buf(f);
                let mut out = Vec::new();
                let mut pos = cs;
                let mut fields = [None; 8];
                if cs == 0 {
                    match memchr(b'\n', &b[..ce]) {
                        Some(p) => pos = p + 1,
                        None => pos = ce,
                    }
                }
                while pos < ce {
                    let e = memchr(b'\n', &b[pos..ce]).map(|p| pos + p).unwrap_or(ce);
                    if split_line(&b[pos..e], 2, &mut fields) {
                        if let (Some(a), Some(bb)) = (fields[0], fields[1]) {
                            out.push((a, bb));
                        }
                    }
                    pos = e + 1;
                }
                out
            })
            .map(|(a, b)| (interner.intern(a), interner.intern(b)))
            .collect();
        // Partitioned id space: id = local*ISHARD + shard. UF is sized to
        // cover every link-phase id; non-link interned ids are their own person.
        let link_len = interner.shard_lens();
        let n_ids = (link_len.iter().max().copied().unwrap_or(0) as usize + 1) * ISHARD;
        let mut uf = Uf::new(n_ids);
        for (a, b) in pairs {
            uf.union(a, b);
        }
        // comp_min: lexicographically smallest interned id per component.
        let guards: Vec<_> = interner.shards.iter().map(|s| s.read().unwrap()).collect();
        let name = |id: u32| -> &[u8] {
            &guards[(id & ISHARD_MASK) as usize].tab[(id >> 6) as usize]
        };
        let mut comp_min: FxHashMap<u32, u32> = FxHashMap::default();
        for s in 0..ISHARD {
            for l in 0..link_len[s] {
                let id = l * ISHARD as u32 + s as u32;
                let r = uf.find(id);
                let e = comp_min.entry(r).or_insert(id);
                if name(id) < name(*e) {
                    *e = id;
                }
            }
        }
        let mut person = vec![0u32; n_ids];
        for s in 0..ISHARD {
            for l in 0..link_len[s] {
                let id = l * ISHARD as u32 + s as u32;
                person[id as usize] = comp_min[&uf.find(id)];
            }
        }
        drop(guards);
        PERSONS.set(Persons { link_len, person }).ok();
    }

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
            let mut pos = t.cs;
            let mut fields = [None; 8];
            if t.cs == 0 {
                match memchr(b'\n', &b[..t.ce]) {
                    Some(p) => pos = p + 1,
                    None => pos = t.ce,
                }
            }
            let base = b.as_ptr() as usize;
            let mk = |s: Option<&[u8]>| -> BKey {
                match s {
                    Some(x) => BKey {
                        f: t.file,
                        off: (x.as_ptr() as usize - base) as u32,
                        len: x.len() as u32,
                    },
                    None => BKey::null(t.file),
                }
            };
            while pos < t.ce {
                let e = memchr(b'\n', &b[pos..t.ce]).map(|p| pos + p).unwrap_or(t.ce);
                let line = &b[pos..e];
                pos = e + 1;
                if !split_line(line, 7, &mut fields) {
                    continue;
                }
                let (eid, uid, ts_s, ing_s, et_s, prod, price) =
                    (fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]);
                let eid = match eid {
                    Some(x) => x,
                    None => continue, // empty event_id dropped at parse
                };
                let ingest = match ing_s {
                    Some(x) => parse_ts(x),
                    None => continue,
                };
                if ingest == T_INVALID {
                    continue;
                }
                let ts = ts_s.map(parse_ts).unwrap_or(T_INVALID);
                let etype = match et_s {
                    Some(b"view") => 0u8,
                    Some(b"cart") => 1,
                    Some(b"purchase") => 2,
                    _ => 3,
                };
                let (pv, pflag) = match price {
                    None => (0u32, 0u8),
                    Some(p) => {
                        if is_digits(p) {
                            let v = atoi(p);
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
                let uid_slot = match uid {
                    Some(u) => interner.intern(u),
                    None => u32::MAX,
                };
                rows.push(RawEv {
                    eid_h: fxh(eid),
                    eid: mk(Some(eid)),
                    uid: uid_slot,
                    prod: mk(prod),
                    ingest,
                    ts,
                    price: pv,
                    etype,
                    pflag,
                });
            }
            rows
        })
        .collect();

    // counting-sort flatten into NSHARD shard vectors (contiguous, one alloc each)
    let shard_vecs: Vec<Vec<RawEv>> = scatter::<RawEv, NSHARD>(&task_rows, RawEv::empty(), |r| {
        (r.eid_h as usize) & (NSHARD - 1)
    });
    drop(task_rows);

    let persons: &'static Persons = PERSONS.get().unwrap();
    let prod_map_ref = &prod_map;
    let evs: Vec<Ev> = shard_vecs
        .into_par_iter()
        .flat_map(|rows| {
            // open-addressing dedup on event_id; keeps min-ingest copy (first wins ties)
            let cap = (rows.len().max(16) * 2).next_power_of_two();
            let mut state = vec![0u8; cap];
            let mut keys = vec![BKey::null(0); cap];
            let mut vals = vec![RawEv::empty(); cap];
            for r in &rows {
                let mut i = (r.eid_h as usize) & (cap - 1);
                loop {
                    if state[i] == 0 {
                        state[i] = 1;
                        keys[i] = r.eid;
                        vals[i] = *r;
                        break;
                    }
                    if keys[i] == r.eid {
                        if r.ingest < vals[i].ingest {
                            vals[i] = *r;
                        }
                        break;
                    }
                    i = (i + 1) & (cap - 1);
                }
            }
            let mut out = Vec::new();
            for i in 0..cap {
                if state[i] == 0 {
                    continue;
                }
                let d = vals[i];
                if d.uid == u32::MAX
                    || d.prod.len == 0
                    || d.etype == 3
                    || d.ts == T_INVALID
                    || d.ts < ws
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
                let person = persons.lookup(d.uid);
                let (cat, price) = if d.etype == 2 {
                    (
                        prod_map_ref.get(d.prod.get()).copied().unwrap_or(unknown_cat),
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

    let mut evs = evs;
    evs.par_sort_unstable_by(|a, b| (a.person, a.ts).cmp(&(b.person, b.ts)));

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
            let mut pos = t.cs;
            let mut fields = [None; 8];
            if t.cs == 0 {
                match memchr(b'\n', &b[..t.ce]) {
                    Some(p) => pos = p + 1,
                    None => pos = t.ce,
                }
            }
            let base = b.as_ptr() as usize;
            let mk = |s: Option<&[u8]>| -> BKey {
                match s {
                    Some(x) => BKey {
                        f: t.file,
                        off: (x.as_ptr() as usize - base) as u32,
                        len: x.len() as u32,
                    },
                    None => BKey::null(t.file),
                }
            };
            while pos < t.ce {
                let e = memchr(b'\n', &b[pos..t.ce]).map(|p| pos + p).unwrap_or(t.ce);
                let line = &b[pos..e];
                pos = e + 1;
                if !split_line(line, 4, &mut fields) {
                    continue;
                }
                let (cid, uid, camp, ts_s) = (fields[0], fields[1], fields[2], fields[3]);
                let cid = match cid {
                    Some(x) => x,
                    None => continue, // empty click_id dropped at parse
                };
                rows.push(RawCl {
                    cid_h: fxh(cid),
                    cid: mk(Some(cid)),
                    uid: match uid {
                        Some(u) => interner.intern(u),
                        None => u32::MAX,
                    },
                    camp: match camp {
                        Some(c) => camp_map_ref.get(c).copied().unwrap_or(u32::MAX),
                        None => u32::MAX,
                    },
                    ts: ts_s.map(parse_ts).unwrap_or(T_INVALID),
                });
            }
            rows
        })
        .collect();
    let cshard_vecs: Vec<Vec<RawCl>> = scatter::<RawCl, NSHARD>(&ctask_rows, RawCl::empty(), |r| {
        (r.cid_h as usize) & (NSHARD - 1)
    });
    drop(ctask_rows);

    let clicks: Vec<Cl> = cshard_vecs
        .into_par_iter()
        .flat_map(|rows| {
            // open-addressing dedup on click_id; first copy wins
            let cap = (rows.len().max(16) * 2).next_power_of_two();
            let mut state = vec![0u8; cap];
            let mut keys = vec![BKey::null(0); cap];
            let mut vals = vec![RawCl::empty(); cap];
            for r in &rows {
                let mut i = (r.cid_h as usize) & (cap - 1);
                loop {
                    if state[i] == 0 {
                        state[i] = 1;
                        keys[i] = r.cid;
                        vals[i] = *r;
                        break;
                    }
                    if keys[i] == r.cid {
                        break;
                    }
                    i = (i + 1) & (cap - 1);
                }
            }
            let mut out = Vec::new();
            for i in 0..cap {
                if state[i] == 0 {
                    continue;
                }
                let r = vals[i];
                if r.uid == u32::MAX || r.ts == T_INVALID || r.ts < click_lo || r.ts >= we {
                    continue;
                }
                if r.camp == u32::MAX {
                    continue;
                }
                out.push(Cl {
                    person: persons.lookup(r.uid),
                    ts: r.ts,
                    camp: r.camp,
                    cid: r.cid,
                });
            }
            out
        })
        .collect();
    let mut clicks = clicks;
    clicks.par_sort_unstable_by(|a, b| {
        (a.person, a.ts)
            .cmp(&(b.person, b.ts))
            .then_with(|| a.cid.get().cmp(b.cid.get()))
    });

    // ---------------- sessionize + attribute ----------------
    let ev_runs = person_runs(&evs, |e| e.person);
    let cl_runs = person_runs(&clicks, |c| c.person);

    let total = ev_runs
        .par_iter()
        .fold(
            || {
                let mut a = Acc::default();
                a.alive = Alive::new(n_camps);
                a
            },
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
                        acc.flush_session(person);
                        acc.cur.start = e.ts;
                    }
                    if !have {
                        acc.cur.start = e.ts;
                        have = true;
                    }
                    acc.cur.n += 1;
                    if e.price > 0 {
                        acc.cur.rev += e.price as i64;
                        *acc.cur.cats.entry(e.cat).or_insert(0) += e.price as i64;
                    }
                    prev_ts = e.ts;
                }
                if have {
                    acc.flush_session(person);
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
                    let e = acc.cd.entry(cdkey(ck, day)).or_default();
                    e.sessions += 1;
                    e.rev += rev;
                    if rev > 0 {
                        e.purch += 1;
                    }
                    for i2 in 0..acc.alive.elems.len() {
                        let c = acc.alive.elems[i2];
                        acc.cd.entry(cdkey(c, day)).or_default().assist += 1;
                    }
                }
                acc
            },
        )
        .reduce(Acc::default, |mut a, b| {
            a.merge(b);
            a
        });

    // ---------------- outputs ----------------
    let tab = interner.freeze();
    let mut ud_rows: Vec<(u32, i64, u64, u64, i64, Option<u32>)> = Vec::new();
    for (k, a) in &total.ud {
        let person = (k >> 32) as u32;
        let day = (k & 0xffff_ffff) as u32 as i64;
        let top = if a.cats.is_empty() {
            None
        } else {
            a.cats
                .iter()
                .max_by(|(c1, r1), (c2, r2)| {
                    r1.cmp(r2).then_with(|| cat_tab[**c2 as usize].cmp(&cat_tab[**c1 as usize]))
                })
                .map(|(c, _)| *c)
        };
        ud_rows.push((person, day, a.sessions, a.events, a.rev, top));
    }
    ud_rows.sort_by(|a, b| tab.get(a.0).cmp(tab.get(b.0)).then(a.1.cmp(&b.1)));

    let mut cd_rows: Vec<(u32, i64, u64, u64, i64, u64)> = Vec::new();
    for (k, a) in &total.cd {
        let camp = (k >> 32) as u32;
        let day = (k & 0xffff_ffff) as u32 as i64;
        cd_rows.push((camp, day, a.sessions, a.purch, a.rev, a.assist));
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
    write_user_parquet(&outdir.join("user_daily"), &ud_rows, &tab, &cat_tab);
    write_camp_parquet(
        &outdir.join("campaign_daily"),
        &cd_rows,
        &camp_names,
        &camp_chan,
        organic_camp,
    );
    if write_csv {
        write_csvs(
            outdir, &ud_rows, &cd_rows, &tab, &camp_names, &camp_chan, &cat_tab, organic_camp,
        );
    }
}

// ---------------------------------------------------------------------------
fn str_of(b: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(b) }
}

fn write_user_parquet(
    dir: &Path,
    rows: &[(u32, i64, u64, u64, i64, Option<u32>)],
    tab: &Tab,
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
        let uid: Vec<&str> = chunk.iter().map(|r| str_of(tab.get(r.0))).collect();
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
        let rev: Vec<i64> = chunk.iter().map(|r| r.4).collect();
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
    ud_rows: &[(u32, i64, u64, u64, i64, Option<u32>)],
    cd_rows: &[(u32, i64, u64, u64, i64, u64)],
    tab: &Tab,
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
        writeln!(
            f,
            "{},{},{},{},{},{}",
            str_of(tab.get(r.0)),
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
