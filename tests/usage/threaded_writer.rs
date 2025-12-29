use std::{fs::File, io::Write, thread};

use clickhouse_rowbinary::{
    Row, RowBinaryFormat, RowBinaryValueReader, RowBinaryValueWriter, Schema, Value,
};
use zstd::stream::{Decoder, Encoder};

use crate::common::unique_table;

// Demonstrates combining per-thread RowBinary chunks into a single compressed
// RowBinaryWithNamesAndTypes file by writing one header and then appending
// raw row bytes.
#[test]
fn combine_threaded_rowbinary_chunks_into_one_file() {
    let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")]).unwrap();

    let rows_a: Vec<Row> = vec![
        vec![Value::UInt8(1), Value::String(b"a".to_vec())],
        vec![Value::UInt8(2), Value::String(b"b".to_vec())],
    ];
    let rows_b: Vec<Row> = vec![
        vec![Value::UInt8(3), Value::String(b"c".to_vec())],
        vec![Value::UInt8(4), Value::String(b"d".to_vec())],
    ];

    // Each worker encodes rows as plain RowBinary (no header).
    let schema_a = schema.clone();
    let handle_a = thread::spawn(move || {
        let mut writer =
            RowBinaryValueWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema_a);
        writer.write_header().unwrap();
        writer.write_rows(&rows_a).unwrap();
        writer.into_inner()
    });

    let schema_b = schema.clone();
    let handle_b = thread::spawn(move || {
        let mut writer =
            RowBinaryValueWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema_b);
        writer.write_header().unwrap();
        writer.write_rows(&rows_b).unwrap();
        writer.into_inner()
    });

    let chunk_a = handle_a.join().unwrap();
    let chunk_b = handle_b.join().unwrap();

    // Aggregator writes header once, then appends the raw row bytes.
    let file_id = unique_table("");
    let file_path = std::env::temp_dir().join(format!("rowbinary_merge_{file_id}.zst"));
    let file = File::create(&file_path).unwrap();
    let mut encoder = Encoder::new(file, 0).unwrap();

    let mut header_writer = RowBinaryValueWriter::new(
        Vec::new(),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        schema.clone(),
    );
    header_writer.write_header().unwrap();
    encoder.write_all(&header_writer.into_inner()).unwrap();
    encoder.write_all(&chunk_a).unwrap();
    encoder.write_all(&chunk_b).unwrap();
    encoder.finish().unwrap();

    // Read the merged file back and verify row order.
    let file = File::open(&file_path).unwrap();
    let zstd_decoder = Decoder::new(file).unwrap();
    let mut reader = RowBinaryValueReader::with_schema(
        zstd_decoder,
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        schema,
    )
    .unwrap();
    let mut rows = Vec::new();
    while let Some(row) = reader.read_row().unwrap() {
        rows.push(row);
    }

    let expected: Vec<Row> = vec![
        vec![Value::UInt8(1), Value::String(b"a".to_vec())],
        vec![Value::UInt8(2), Value::String(b"b".to_vec())],
        vec![Value::UInt8(3), Value::String(b"c".to_vec())],
        vec![Value::UInt8(4), Value::String(b"d".to_vec())],
    ];
    assert_eq!(rows, expected);
}
