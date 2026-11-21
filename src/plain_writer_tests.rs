use super::*;
use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType::{Date32, Int64, Utf8};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, Encoding};
use parquet::column::page::Page;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::sync::Arc;

fn expected_schema(fields: &[(&str, DataType, bool)]) -> Arc<Schema> {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, ty, nullable)| Field::new(*name, ty.clone(), *nullable))
            .collect::<Vec<_>>(),
    ))
}

fn user_schema() -> Arc<Schema> {
    expected_schema(&[
        ("user_id", Utf8, false),
        ("day", Date32, false),
        ("sessions", Int64, false),
        ("events", Int64, false),
        ("revenue_cents", Int64, false),
        ("top_category", Utf8, true),
    ])
}

fn camp_schema() -> Arc<Schema> {
    expected_schema(&[
        ("campaign_id", Utf8, false),
        ("channel", Utf8, false),
        ("day", Date32, false),
        ("sessions", Int64, false),
        ("purchasing_sessions", Int64, false),
        ("revenue_cents", Int64, false),
        ("assisted_sessions", Int64, false),
    ])
}

// Inspect actual pages, not just our in-memory header/metadata structures.
// Return row-group boundaries so the caller can verify the byte/row cutoffs.
fn roundtrip(buf: Vec<u8>, expected: RecordBatch, codec: Codec) -> Vec<usize> {
    let bytes = Bytes::from(buf);
    let reader = SerializedFileReader::new(bytes.clone()).unwrap();
    let mut offset = 4;
    let mut counts = vec![];
    let footer_len =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
    for rg in 0..reader.num_row_groups() {
        let group = reader.get_row_group(rg).unwrap();
        let n = group.metadata().num_rows();
        counts.push(n as usize);
        assert!(n > 0 && n <= ROW_LIMIT as i64);
        let mut raw = 0;
        for j in 0..group.num_columns() {
            let col = group.metadata().column(j);
            assert_eq!(col.data_page_offset(), offset);
            assert_eq!(col.dictionary_page_offset(), None);
            assert_eq!(
                col.compression(),
                if codec == Codec::SNAPPY {
                    Compression::SNAPPY
                } else {
                    Compression::UNCOMPRESSED
                }
            );
            let mut pages = group.get_column_page_reader(j).unwrap();
            let page = pages.get_next_page().unwrap().unwrap();
            assert!(matches!(page, Page::DataPage { .. }));
            assert_eq!(page.num_values() as i64, n);
            assert_eq!(page.encoding(), Encoding::PLAIN);
            assert!(page.buffer().len() <= PAGE_BYTES || n == 1);
            assert!(pages.get_next_page().unwrap().is_none());
            offset += col.compressed_size();
            raw += col.uncompressed_size();
        }
        assert_eq!(group.metadata().total_byte_size(), raw);
    }
    assert_eq!(offset as usize, bytes.len() - 8 - footer_len as usize);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    assert_eq!(builder.schema(), &expected.schema());
    let mut pos = 0;
    for batch in builder.with_batch_size(499).build().unwrap() {
        let batch = batch.unwrap();
        assert_eq!(batch, expected.slice(pos, batch.num_rows()));
        pos += batch.num_rows();
    }
    assert_eq!(pos, expected.num_rows());
    counts
}

const CODECS: [Codec; 2] = [Codec::SNAPPY, Codec::UNCOMPRESSED];

#[test]
fn zero_rows_keep_both_schemas() {
    for codec in CODECS {
        let mut buf = Vec::new();
        users(&mut buf, &[], &[], codec);
        assert!(roundtrip(buf, RecordBatch::new_empty(user_schema()), codec).is_empty());
        let mut buf = Vec::new();
        campaigns(&mut buf, &[], &[], &[], 0, codec);
        assert!(roundtrip(buf, RecordBatch::new_empty(camp_schema()), codec).is_empty());
    }
}

#[test]
fn user_rows_mixed_all_null_and_present_empty() {
    crate::FB.get_or_init(crate::Fb::new);
    let id = " ué\0long fallback ";
    let key = crate::key_of(id.as_bytes());
    let cats: Vec<Box<[u8]>> = vec!["雪\r".as_bytes().into(), b"".as_slice().into()];
    // Cross the one-byte hybrid-run header boundary & leave a partial group of 8.
    for all_null in [true, false] {
        let rows: Vec<UdRow> = (0..777)
            .map(|i| {
                let top = if all_null || i % 5 == 0 {
                    None
                } else {
                    Some((i % 2) as u32)
                };
                (key, i - 1000, (1u64 << 33) + i as u64, 5, i64::MAX - i, top)
            })
            .collect();
        let expected = RecordBatch::try_new(
            user_schema(),
            vec![
                Arc::new(StringArray::from(vec![id; rows.len()])),
                Arc::new(Date32Array::from_iter_values(
                    rows.iter().map(|r| r.1 as i32),
                )),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|r| r.2 as i64),
                )),
                Arc::new(Int64Array::from(vec![5; rows.len()])),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.4))),
                Arc::new(StringArray::from_iter(
                    rows.iter()
                        .map(|r| r.5.map(|c| if c == 0 { "雪\r" } else { "" })),
                )),
            ],
        )
        .unwrap();
        for codec in CODECS {
            let mut buf = vec![];
            users(&mut buf, &rows, &cats, codec);
            assert_eq!(roundtrip(buf, expected.clone(), codec), [rows.len()]);
        }
    }
}

#[test]
fn category_bytes_including_definitions_split_before_soft_limit() {
    let cats: Vec<Box<[u8]>> = vec![vec![b'x'; PAGE_BYTES - 10].into(), b"".as_slice().into()];
    let rows: Vec<UdRow> = [Some(0), None, None, Some(1), Some(0)]
        .iter()
        .map(|&top| (crate::packable(b"a"), 0, 1, 2, 3, top))
        .collect();
    let expected = RecordBatch::try_new(
        user_schema(),
        vec![
            Arc::new(StringArray::from(vec!["a"; 5])),
            Arc::new(Date32Array::from(vec![0; 5])),
            Arc::new(Int64Array::from(vec![1; 5])),
            Arc::new(Int64Array::from(vec![2; 5])),
            Arc::new(Int64Array::from(vec![3; 5])),
            Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                r.5.map(|c| std::str::from_utf8(&cats[c as usize]).unwrap())
            }))),
        ],
    )
    .unwrap();
    for codec in CODECS {
        let mut buf = vec![];
        users(&mut buf, &rows, &cats, codec);
        // 6-byte levels + 4-byte length + string == PAGE_BYTES. Two
        // additional nulls fit the same packed group; a present "" does not.
        assert_eq!(roundtrip(buf, expected.clone(), codec), [3, 1, 1]);
    }
}

#[test]
fn oversized_singleton_and_fixed_width_boundary() {
    let channels: Vec<Box<[u8]>> = vec![vec![b'z'; PAGE_BYTES + 9].into()];
    let names: Vec<Box<[u8]>> = vec![" 雪 ".as_bytes().into()];
    // Short non-organic names also exercise the independent fixed-width cap.
    let small_names: Vec<Box<[u8]>> = vec![b"c".as_slice().into()];
    let small_chans: Vec<Box<[u8]>> = vec![b"t".as_slice().into()];
    let short: Vec<CampRow> = vec![(0, -4, 1, 2, 3, 4); ROW_LIMIT + 1];
    let long = vec![
        (1, -3, 0, 0, 0, 1u64 << 33),
        (0, 5, 6, 7, 8, 9),
        (1, 10, 11, 12, 13, 14),
    ];
    for (rows, ids, chans, counts) in [
        (&long, &names, &channels, vec![1, 1, 1]),
        (&short, &small_names, &small_chans, vec![ROW_LIMIT, 1]),
    ] {
        let strs = |tab: &[Box<[u8]>]| {
            StringArray::from_iter_values(rows.iter().map(|r| {
                if r.0 == 1 {
                    "organic"
                } else {
                    std::str::from_utf8(&tab[r.0 as usize]).unwrap()
                }
            }))
        };
        let expected = RecordBatch::try_new(
            camp_schema(),
            vec![
                Arc::new(strs(ids)),
                Arc::new(strs(chans)),
                Arc::new(Date32Array::from_iter_values(
                    rows.iter().map(|r| r.1 as i32),
                )),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|r| r.2 as i64),
                )),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|r| r.3 as i64),
                )),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.4))),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|r| r.5 as i64),
                )),
            ],
        )
        .unwrap();
        for codec in CODECS {
            let mut buf = vec![];
            campaigns(&mut buf, rows, ids, chans, 1, codec);
            assert_eq!(roundtrip(buf, expected.clone(), codec), counts);
        }
    }
}

#[test]
#[should_panic(expected = "single row exceeds Parquet v1 page size")]
fn impossible_page_fails_before_copy_or_narrowing() {
    fits(0, 4, i32::MAX as usize + 1);
}
