//! Flat Parquet v1 for just the two output tables. One PLAIN data page
//! per column per row group; no dictionary, stats, indexes or Arrow buffers.
use super::{person_bytes, UdRow, ORGANIC};
use parquet::format as pf;
use parquet::thrift::{TCompactOutputProtocol, TSerializable};
use rayon::prelude::*;
use snap::raw::{max_compress_len, Encoder};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

#[cfg(test)]
#[path = "plain_writer_tests.rs"]
mod tests;

pub(super) type Codec = pf::CompressionCodec;
type CampRow = (u32, i64, u64, u64, i64, u64);

// PARQUET_WRITER=arrow runs the original ArrowWriter without modifying its
// properties. One binary suffices for interleaved runs of all three modes.
pub(super) fn selected_codec() -> Option<Codec> {
    match std::env::var("PARQUET_WRITER").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("uncompressed") => Some(Codec::UNCOMPRESSED),
        Ok("snappy") => Some(Codec::SNAPPY),
        Ok("arrow") => None,
        _ => panic!("expected PARQUET_WRITER=snappy|uncompressed|arrow"),
    }
}

pub(super) fn parts<T: Sync>(dir: &Path, rows: &[T], write: impl Fn(BufWriter<File>, &[T]) + Sync) {
    let width = rows.len().div_ceil(rayon::current_num_threads()).max(1);
    // Even an empty table needs a file carrying its schema.
    (0..rows.len().div_ceil(width).max(1))
        .into_par_iter()
        .for_each(|i| {
            let file = File::create(dir.join(format!("part-{i}.parquet"))).unwrap();
            write(
                BufWriter::new(file),
                &rows[i * width..((i + 1) * width).min(rows.len())],
            );
        });
}

#[derive(Clone, Copy)]
enum Kind {
    Text,
    Date,
    Count,
    NullableText,
}
use Kind::*;

impl Kind {
    fn physical(self) -> pf::Type {
        match self {
            Text | NullableText => pf::Type::BYTE_ARRAY,
            Date => pf::Type::INT32,
            Count => pf::Type::INT64,
        }
    }
}

type Fields = &'static [(&'static str, Kind)];
const USER: Fields = &[
    ("user_id", Text),
    ("day", Date),
    ("sessions", Count),
    ("events", Count),
    ("revenue_cents", Count),
    ("top_category", NullableText),
];
const CAMPAIGN: Fields = &[
    ("campaign_id", Text),
    ("channel", Text),
    ("day", Date),
    ("sessions", Count),
    ("purchasing_sessions", Count),
    ("revenue_cents", Count),
    ("assisted_sessions", Count),
];

fn schema(fields: Fields) -> Vec<pf::SchemaElement> {
    let root = pf::SchemaElement::new(
        None,
        None,
        None,
        "schema".into(),
        fields.len() as i32,
        None,
        None,
        None,
        None,
        None,
    );
    std::iter::once(root)
        .chain(fields.iter().map(|&(name, kind)| {
            let (converted, logical) = match kind {
                Text | NullableText => (
                    Some(pf::ConvertedType::UTF8),
                    Some(pf::LogicalType::STRING(pf::StringType::new())),
                ),
                Date => (
                    Some(pf::ConvertedType::DATE),
                    Some(pf::LogicalType::DATE(pf::DateType::new())),
                ),
                Count => (None, None),
            };
            let rep = if matches!(kind, NullableText) {
                pf::FieldRepetitionType::OPTIONAL
            } else {
                pf::FieldRepetitionType::REQUIRED
            };
            pf::SchemaElement::new(
                kind.physical(),
                None,
                rep,
                name.into(),
                None,
                converted,
                None,
                None,
                None,
                logical,
            )
        }))
        .collect()
}

// Largest fixed-width column is INT64. The two variable-width columns are
// budgeted by their actual wire sizes BEFORE adding a row (including levels).
const PAGE_BYTES: usize = 256 * 1024;
const ROW_LIMIT: usize = PAGE_BYTES / 8;

fn fits(n: usize, bytes1: usize, bytes2: usize) -> bool {
    let size = bytes1.max(bytes2);
    if size <= PAGE_BYTES {
        return true;
    }
    if n > 0 {
        return false;
    }
    // A string cannot be split across pages. An over-target singleton is OK,
    // but a page size that can't be represented by the spec is not.
    assert!(
        size <= i32::MAX as usize,
        "single row exceeds Parquet v1 page size"
    );
    true
}

fn string(buf: &mut Vec<u8>, value: &[u8]) {
    buf.extend_from_slice(&i32::try_from(value.len()).unwrap().to_le_bytes());
    buf.extend_from_slice(value);
}

fn varint(dst: &mut Vec<u8>, mut v: u32) {
    while v >= 128 {
        dst.push(v as u8 | 128);
        v >>= 7;
    }
    dst.push(v as u8);
}

fn def_size(n: usize) -> usize {
    let blocks = n.div_ceil(8);
    let header = (blocks as u32) * 2 + 1;
    4 + ((32 - header.leading_zeros()) as usize).div_ceil(7) + blocks
}

/// V1 flat OPTIONAL has no repetition levels. Definitions form one hybrid
/// bit-packed run of width 1, padded only at the end, LSB = first row.
fn definitions(dst: &mut Vec<u8>, rows: &[UdRow]) {
    let blocks = rows.len().div_ceil(8);
    dst.extend_from_slice(&((def_size(rows.len()) - 4) as u32).to_le_bytes());
    varint(dst, (blocks as u32) * 2 + 1);
    for block in rows.chunks(8) {
        let mut bits = 0u8;
        for (i, row) in block.iter().enumerate() {
            bits |= u8::from(row.5.is_some()) << i;
        }
        dst.push(bits);
    }
}

fn compact<T: TSerializable>(value: &T, buf: &mut Vec<u8>) {
    buf.clear();
    value
        .write_to_out_protocol(&mut TCompactOutputProtocol::new(buf))
        .unwrap();
}

struct FlatFile<W> {
    out: W,
    fields: Fields,
    codec: Codec,
    offset: i64,
    groups: Vec<pf::RowGroup>,
    columns: Vec<pf::ColumnChunk>,
    header: Vec<u8>,
    encoder: Encoder,
    compressed: Vec<u8>,
}

impl<W: Write> FlatFile<W> {
    fn new(mut out: W, fields: Fields, codec: Codec) -> Self {
        out.write_all(b"PAR1").unwrap();
        Self {
            out,
            fields,
            codec,
            offset: 4,
            groups: vec![],
            columns: vec![],
            header: vec![],
            encoder: Encoder::new(),
            compressed: vec![],
        }
    }

    fn page(&mut self, n: usize, body: &[u8]) {
        let (name, kind) = self.fields[self.columns.len()];
        let raw_len = i32::try_from(body.len()).expect("Parquet page size");
        // For a near-2 GiB singleton, even legal input can exceed INT32_MAX
        // after Snappy. Codec is per column chunk: store that chunk verbatim.
        let (codec, data) =
            if self.codec == Codec::SNAPPY && max_compress_len(body.len()) <= i32::MAX as usize {
                let bound = max_compress_len(body.len());
                if self.compressed.len() < bound {
                    self.compressed.resize(bound, 0);
                }
                let size = self.encoder.compress(body, &mut self.compressed).unwrap();
                (Codec::SNAPPY, &self.compressed[..size])
            } else {
                (Codec::UNCOMPRESSED, body)
            };
        let compressed_len = i32::try_from(data.len()).unwrap();
        let header = pf::PageHeader::new(
            pf::PageType::DATA_PAGE,
            raw_len,
            compressed_len,
            None,
            pf::DataPageHeader::new(
                i32::try_from(n).unwrap(),
                pf::Encoding::PLAIN,
                pf::Encoding::RLE,
                pf::Encoding::RLE,
                None,
            ),
            None,
            None,
            None,
        );
        compact(&header, &mut self.header);
        self.out.write_all(&self.header).unwrap();
        self.out.write_all(data).unwrap();
        let header_len = self.header.len() as i64;
        let meta = pf::ColumnMetaData {
            type_: kind.physical(),
            encodings: if matches!(kind, NullableText) {
                vec![pf::Encoding::PLAIN, pf::Encoding::RLE]
            } else {
                vec![pf::Encoding::PLAIN]
            },
            path_in_schema: vec![name.into()],
            codec,
            num_values: n as i64,
            total_uncompressed_size: header_len + i64::from(raw_len),
            total_compressed_size: header_len + i64::from(compressed_len),
            data_page_offset: self.offset,
            key_value_metadata: None,
            index_page_offset: None,
            dictionary_page_offset: None,
            statistics: None,
            encoding_stats: None,
            bloom_filter_offset: None,
            bloom_filter_length: None,
            size_statistics: None,
            geospatial_statistics: None,
        };
        self.offset = self.offset.checked_add(meta.total_compressed_size).unwrap();
        self.columns.push(pf::ColumnChunk::new(
            None, 0, meta, None, None, None, None, None, None,
        ));
    }

    fn fixed<T, const N: usize>(
        &mut self,
        body: &mut Vec<u8>,
        rows: &[T],
        f: impl Fn(&T) -> [u8; N],
    ) {
        body.clear();
        body.reserve(rows.len() * N);
        for row in rows {
            body.extend_from_slice(&f(row));
        }
        self.page(rows.len(), body);
    }

    fn end_group(&mut self, n: usize) {
        assert_eq!(self.fields.len(), self.columns.len());
        let columns = std::mem::take(&mut self.columns);
        let start = columns[0].meta_data.as_ref().unwrap().data_page_offset;
        let bytes = columns
            .iter()
            .map(|c| c.meta_data.as_ref().unwrap().total_uncompressed_size)
            .sum();
        self.groups.push(pf::RowGroup::new(
            columns,
            bytes,
            n as i64,
            None,
            start,
            self.offset - start,
            None,
        ));
    }

    fn close(mut self) {
        let rows = self.groups.iter().map(|g| g.num_rows).sum();
        assert!(
            self.groups.len() <= i32::MAX as usize,
            "too many Parquet row groups"
        );
        let footer = pf::FileMetaData::new(
            1,
            schema(self.fields),
            rows,
            self.groups,
            None,
            Some("shopping-sessions version 0.1.0".into()),
            None,
            None,
            None,
        );
        compact(&footer, &mut self.header);
        let len = u32::try_from(self.header.len()).expect("Parquet footer length");
        self.out.write_all(&self.header).unwrap();
        self.out.write_all(&len.to_le_bytes()).unwrap();
        self.out.write_all(b"PAR1").unwrap();
        self.out.flush().unwrap();
    }
}

pub(super) fn users(out: impl Write, mut rows: &[UdRow], cats: &[Box<[u8]>], codec: Codec) {
    let mut file = FlatFile::new(out, USER, codec);
    let mut ids = Vec::new();
    let mut body = Vec::new();
    while !rows.is_empty() {
        ids.clear();
        let mut n = 0;
        let mut top_bytes = 0;
        for row in rows.iter().take(ROW_LIMIT) {
            let mut scratch = [0u8; 10];
            let id = person_bytes(row.0, &mut scratch);
            let top_next = top_bytes + row.5.map_or(0, |c| 4 + cats[c as usize].len());
            if !fits(n, ids.len() + 4 + id.len(), def_size(n + 1) + top_next) {
                break;
            }
            string(&mut ids, id);
            top_bytes = top_next;
            n += 1;
        }
        let group = &rows[..n];
        file.page(n, &ids);
        file.fixed(&mut body, group, |r| (r.1 as i32).to_le_bytes());
        file.fixed(&mut body, group, |r| (r.2 as i64).to_le_bytes());
        file.fixed(&mut body, group, |r| (r.3 as i64).to_le_bytes());
        file.fixed(&mut body, group, |r| r.4.to_le_bytes());
        body.clear();
        body.reserve(def_size(n) + top_bytes);
        definitions(&mut body, group);
        for row in group {
            if let Some(c) = row.5 {
                string(&mut body, &cats[c as usize]);
            }
        }
        file.page(n, &body);
        file.end_group(n);
        rows = &rows[n..];
    }
    file.close();
}

pub(super) fn campaigns(
    out: impl Write,
    mut rows: &[CampRow],
    names: &[Box<[u8]>],
    channels: &[Box<[u8]>],
    organic: u32,
    codec: Codec,
) {
    let mut file = FlatFile::new(out, CAMPAIGN, codec);
    let (mut ids, mut chans, mut body) = (Vec::new(), Vec::new(), Vec::new());
    while !rows.is_empty() {
        ids.clear();
        chans.clear();
        let mut n = 0;
        for row in rows.iter().take(ROW_LIMIT) {
            let (name, channel): (&[u8], &[u8]) = if row.0 == organic {
                (ORGANIC, ORGANIC)
            } else {
                (&names[row.0 as usize], &channels[row.0 as usize])
            };
            if !fits(
                n,
                ids.len() + 4 + name.len(),
                chans.len() + 4 + channel.len(),
            ) {
                break;
            }
            string(&mut ids, name);
            string(&mut chans, channel);
            n += 1;
        }
        let group = &rows[..n];
        file.page(n, &ids);
        file.page(n, &chans);
        file.fixed(&mut body, group, |r| (r.1 as i32).to_le_bytes());
        file.fixed(&mut body, group, |r| (r.2 as i64).to_le_bytes());
        file.fixed(&mut body, group, |r| (r.3 as i64).to_le_bytes());
        file.fixed(&mut body, group, |r| r.4.to_le_bytes());
        file.fixed(&mut body, group, |r| (r.5 as i64).to_le_bytes());
        file.end_group(n);
        rows = &rows[n..];
    }
    file.close();
}
