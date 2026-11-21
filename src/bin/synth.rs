// Deterministic synthetic dataset generator for CI benchmarking.
// Usage: synth --events N --clicks M --users U --seed S --output DIR
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};

struct Rng(u64);
impl Rng {
    #[inline]
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    #[inline]
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const ALPHA: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";

fn id(r: &mut Rng, prefix: &str, len: usize, out: &mut String) {
    out.push_str(prefix);
    for _ in prefix.len()..len {
        out.push(ALPHA[r.below(36) as usize] as char);
    }
}

const BASE_MS: i64 = 1_814_976_000_000; // 2027-07-08T00:00Z

fn ts(r: &mut Rng, from: i64, span: i64, out: &mut String) -> i64 {
    let t = from + (r.next() % span as u64) as i64;
    let off = t - BASE_MS; // ms since window start; day 8..22 of July 2027
    let (d, h, m, s) = (
        off / 86_400_000,
        off / 3_600_000 % 24,
        off / 60_000 % 60,
        off / 1_000 % 60,
    );
    out.push_str(&format!("2027-07-{:02}T{h:02}:{m:02}:{s:02}Z", 8 + d));
    t
}

fn main() {
    let (mut n_ev, mut n_cl, mut n_users, mut seed) = (20_000_000u64, 5_000_000u64, 2_000_000u64, 9u64);
    let mut out = String::from("data");
    let a: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < a.len() {
        match a[i].as_str() {
            "--events" => n_ev = a[i + 1].parse().unwrap(),
            "--clicks" => n_cl = a[i + 1].parse().unwrap(),
            "--users" => n_users = a[i + 1].parse().unwrap(),
            "--seed" => seed = a[i + 1].parse().unwrap(),
            "--output" => out = a[i + 1].clone(),
            _ => {}
        }
        i += 2;
    }
    let base_ms: i64 = 1_814_976_000_000; // 2027-07-08T00:00Z
    let span_ms: i64 = 14 * 86_400_000; // 14 days

    fs::create_dir_all(format!("{out}/events")).unwrap();
    fs::create_dir_all(format!("{out}/clicks")).unwrap();
    fs::create_dir_all(format!("{out}/links")).unwrap();

    let cats = [
        "garden", "apparel", "tech", "home", "sports", "beauty", "toys", "books",
    ];
    let chans = ["email", "social", "search", "display", "affiliate"];

    // products.csv / campaigns.csv
    {
        let mut w = BufWriter::new(File::create(format!("{out}/products.csv")).unwrap());
        w.write_all(b"product_id,category\n").unwrap();
        for p in 1..=20000u32 {
            writeln!(w, "p{p},{}", cats[(p as usize) % 8]).unwrap();
        }
        let mut w = BufWriter::new(File::create(format!("{out}/campaigns.csv")).unwrap());
        w.write_all(b"campaign_id,channel\n").unwrap();
        for c in 1..=2000u32 {
            writeln!(w, "c_{c},{}", chans[(c as usize) % 5]).unwrap();
        }
        fs::write(
            format!("{out}/manifest.json"),
            "{\n  \"window_start\": \"2027-07-08T00:00:00Z\",\n  \"window_end\": \"2027-07-22T00:00:00Z\"\n}\n",
        )
        .unwrap();
    }

    let mut r = Rng(seed | 1);
    let n_parts = 4u64;
    for part in 0..n_parts {
        let mut w = BufWriter::new(
            File::create(format!("{out}/events/part-{part:05}.csv")).unwrap(),
        );
        w.write_all(b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n")
            .unwrap();
        let (mut s, mut u, mut t1, mut t2, mut p) =
            (String::new(), String::new(), String::new(), String::new(), String::new());
        for _ in 0..n_ev / n_parts {
            s.clear();
            u.clear();
            t1.clear();
            t2.clear();
            p.clear();
            let e_len = 9 + r.below(2) as usize;
            id(&mut r, "e", e_len, &mut s);
            let uid_n = r.below(n_users);
            u.push_str(&format!("u{uid_n}"));
            let t_ev = ts(&mut r, base_ms, span_ms, &mut t1);
            let _ = ts(&mut r, t_ev, 600_000, &mut t2);
            let (kind, has_prod) = match r.below(100) {
                0..=9 => ("purchase", true),
                10..=39 => ("add_to_cart", true),
                _ => ("view", r.below(100) < 95),
            };
            if has_prod {
                p.push_str(&format!("p{}", 1 + r.below(20000)));
            }
            w.write_all(s.as_bytes()).unwrap();
            w.write_all(b",").unwrap();
            w.write_all(u.as_bytes()).unwrap();
            w.write_all(b",").unwrap();
            w.write_all(t1.as_bytes()).unwrap();
            w.write_all(b",").unwrap();
            w.write_all(t2.as_bytes()).unwrap();
            w.write_all(b",").unwrap();
            w.write_all(kind.as_bytes()).unwrap();
            w.write_all(b",").unwrap();
            w.write_all(p.as_bytes()).unwrap();
            w.write_all(b",").unwrap();
            if kind == "purchase" {
                w.write_all(format!("{}", 1 + r.below(50000)).as_bytes()).unwrap();
            }
            w.write_all(b"\n").unwrap();
        }
    }
    for part in 0..n_parts {
        let mut w = BufWriter::new(
            File::create(format!("{out}/clicks/part-{part:05}.csv")).unwrap(),
        );
        w.write_all(b"click_id,user_id,campaign_id,ts\n").unwrap();
        let (mut s, mut u, mut t1) = (String::new(), String::new(), String::new());
        for _ in 0..n_cl / n_parts {
            s.clear();
            u.clear();
            t1.clear();
            let k_len = 9 + r.below(2) as usize;
            id(&mut r, "k", k_len, &mut s);
            u.push_str(&format!("u{}", r.below(n_users)));
            let _ = ts(&mut r, base_ms, span_ms, &mut t1);
            writeln!(w, "{s},{u},c_{},{t1}", 1 + r.below(2000)).unwrap();
        }
    }
    {
        let mut w = BufWriter::new(
            File::create(format!("{out}/links/part-00000.csv")).unwrap(),
        );
        w.write_all(b"user_id_a,user_id_b\n").unwrap();
        for _ in 0..n_users / 50 {
            writeln!(w, "u{},u{}", r.below(n_users), r.below(n_users)).unwrap();
        }
    }
    eprintln!("synth done: {out}");
}
