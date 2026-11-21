// Semantic comparison of two parquet output dirs (or single files).
// Usage: pqcmp <dirA|fileA> <dirB|fileB>
// Canonical form: multiset of "colname=value" tuples per row, sorted. Ignores
// column order, row order, file partitioning, encoding, and compression.
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs;
use std::path::{Path, PathBuf};

fn files(p: &Path) -> Vec<PathBuf> {
    if p.is_file() {
        return vec![p.to_path_buf()];
    }
    let mut v: Vec<PathBuf> = fs::read_dir(p)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|path| {
            let n = path.file_name().unwrap().to_string_lossy();
            n.ends_with(".parquet") && !n.starts_with('.') && !n.starts_with('_')
        })
        .collect();
    v.sort();
    v
}

fn rows_of(p: &Path) -> Vec<String> {
    let mut out = Vec::new();
    for f in files(p) {
        let rdr = SerializedFileReader::new(fs::File::open(&f).unwrap()).unwrap();
        let mut it = rdr.get_row_iter(None).unwrap();
        while let Some(Ok(row)) = it.next() {
            let mut cols: Vec<String> = row
                .get_column_iter()
                .map(|(name, field)| format!("{}={:?}", name, field))
                .collect();
            cols.sort();
            out.push(cols.join(","));
        }
    }
    out
}

fn main() {
    let a = std::env::args().nth(1).unwrap();
    let b = std::env::args().nth(2).unwrap();
    let mut ra = rows_of(Path::new(&a));
    let mut rb = rows_of(Path::new(&b));
    ra.sort();
    rb.sort();
    if ra == rb {
        println!("MATCH {} rows", ra.len());
    } else {
        eprintln!("MISMATCH: {} vs {} rows", ra.len(), rb.len());
        for (x, y) in ra.iter().zip(rb.iter()) {
            if x != y {
                eprintln!("first diff:\n  A: {}\n  B: {}", x, y);
                break;
            }
        }
        std::process::exit(1);
    }
}
