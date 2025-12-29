use std::path::PathBuf;

use clickhouse_rowbinary::{
    RowBinaryFormat, RowBinaryReader, RowBinaryValueReader, RowBinaryWriter, Schema, Value,
    io::write_uvarint,
};
use rand::{Rng, distr::Alphanumeric, rng};
use std::{
    fs::File,
    io::{BufReader, BufWriter},
};
use zeekstd::seek_table::Format;

use crate::common::row_bytes;
fn temp_path(prefix: &str) -> PathBuf {
    let suffix: String = rng()
        .sample_iter(Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    std::env::temp_dir().join(format!("{prefix}_{suffix}.zst"))
}

#[test]
fn seekable_reader_can_seek_and_decode_rows() {
    let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")]).unwrap();
    let rows = vec![
        vec![Value::UInt8(1), Value::String(b"alpha".to_vec())],
        vec![Value::UInt8(2), Value::String(b"beta".to_vec())],
        vec![Value::UInt8(3), Value::String(b"gamma".to_vec())],
    ];

    let path = temp_path("seekable_roundtrip");
    let file = File::create(&path).unwrap();
    let mut writer = RowBinaryWriter::new(
        BufWriter::new(file),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
    )
    .unwrap();
    writer.write_header(&schema).unwrap();
    for row in &rows {
        let bytes = row_bytes(&schema, row);
        writer.write_row_bytes(&bytes).unwrap();
    }
    writer.finish().unwrap();

    let file = File::open(&path).unwrap();
    let mut reader = RowBinaryReader::new(
        BufReader::new(file),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        None,
    )
    .unwrap();
    let header = reader.header().unwrap();
    assert_eq!(header.names, vec!["id".to_string(), "name".to_string()]);

    let first = reader.current_row().unwrap().unwrap();
    let mut decoded = RowBinaryValueReader::with_schema(
        std::io::Cursor::new(first),
        RowBinaryFormat::RowBinary,
        schema.clone(),
    )
    .unwrap();
    assert_eq!(decoded.read_row().unwrap().unwrap()[0], Value::UInt8(1));

    reader.seek_relative(1).unwrap();
    let second_bytes = reader.current_row().unwrap().unwrap();
    assert!(!second_bytes.is_empty());

    reader.seek_relative(-1).unwrap();
    let prev = reader.current_row().unwrap().unwrap();
    let mut decoded = RowBinaryValueReader::with_schema(
        std::io::Cursor::new(prev),
        RowBinaryFormat::RowBinary,
        schema.clone(),
    )
    .unwrap();
    assert_eq!(decoded.read_row().unwrap().unwrap()[0], Value::UInt8(1));

    reader.seek_row(2).unwrap();
    let third = reader.current_row().unwrap().unwrap();
    let mut decoded = RowBinaryValueReader::with_schema(
        std::io::Cursor::new(third),
        RowBinaryFormat::RowBinary,
        schema.clone(),
    )
    .unwrap();
    assert_eq!(decoded.read_row().unwrap().unwrap()[0], Value::UInt8(3));

    std::fs::remove_file(path).unwrap();
}

#[test]
fn seekable_reader_rejects_missing_header_without_schema() {
    let schema = Schema::from_type_strings(&[("id", "UInt8")]).unwrap();
    let row = vec![Value::UInt8(1)];
    let payload = row_bytes(&schema, &row);

    let path = temp_path("seekable_missing_header");
    let file = File::create(&path).unwrap();
    let mut writer = RowBinaryWriter::new(
        BufWriter::new(file),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
    )
    .unwrap();
    writer.write_row_bytes(&payload).unwrap();
    writer.finish().unwrap();

    let file = File::open(&path).unwrap();
    let err = RowBinaryReader::new(
        BufReader::new(file),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        None,
    )
    .err()
    .unwrap();
    assert!(matches!(err, clickhouse_rowbinary::Error::InvalidValue(_)));

    std::fs::remove_file(path).unwrap();
}

#[test]
fn seekable_reader_requires_schema_for_rowbinary() {
    let schema = Schema::from_type_strings(&[("id", "UInt8")]).unwrap();
    let row = vec![Value::UInt8(1)];
    let payload = row_bytes(&schema, &row);

    let path = temp_path("seekable_rowbinary_no_schema");
    let file = File::create(&path).unwrap();
    let mut writer =
        RowBinaryWriter::new(BufWriter::new(file), RowBinaryFormat::RowBinary).unwrap();
    writer.write_row_bytes(&payload).unwrap();
    writer.finish().unwrap();

    let file = File::open(&path).unwrap();
    let err = RowBinaryReader::new(BufReader::new(file), RowBinaryFormat::RowBinary, None)
        .err()
        .unwrap();
    assert!(matches!(err, clickhouse_rowbinary::Error::InvalidValue(_)));

    std::fs::remove_file(path).unwrap();
}

#[test]
fn seekable_reader_rejects_header_schema_mismatch() {
    let schema = Schema::from_type_strings(&[("id", "UInt8")]).unwrap();
    let row = vec![Value::UInt8(1)];
    let payload = row_bytes(&schema, &row);

    let path = temp_path("seekable_header_mismatch");
    let file = File::create(&path).unwrap();
    let mut writer =
        RowBinaryWriter::new(BufWriter::new(file), RowBinaryFormat::RowBinaryWithNames).unwrap();
    writer.write_header(&schema).unwrap();
    writer.write_row_bytes(&payload).unwrap();
    writer.finish().unwrap();

    let file = File::open(&path).unwrap();
    let wrong_schema = Schema::from_type_strings(&[("other", "UInt8")]).unwrap();
    let err = RowBinaryReader::new(
        BufReader::new(file),
        RowBinaryFormat::RowBinaryWithNames,
        Some(wrong_schema),
    )
    .err()
    .unwrap();
    assert!(matches!(err, clickhouse_rowbinary::Error::InvalidValue(_)));

    std::fs::remove_file(path).unwrap();
}

#[test]
fn seekable_reader_reports_row_out_of_range() {
    let schema = Schema::from_type_strings(&[("id", "UInt8")]).unwrap();
    let row = vec![Value::UInt8(1)];
    let payload = row_bytes(&schema, &row);

    let path = temp_path("seekable_row_oob");
    let file = File::create(&path).unwrap();
    let mut writer =
        RowBinaryWriter::new(BufWriter::new(file), RowBinaryFormat::RowBinaryWithNames).unwrap();
    writer.write_header(&schema).unwrap();
    writer.write_row_bytes(&payload).unwrap();
    writer.finish().unwrap();

    let file = File::open(&path).unwrap();
    let mut reader = RowBinaryReader::new(
        BufReader::new(file),
        RowBinaryFormat::RowBinaryWithNames,
        Some(schema),
    )
    .unwrap();
    let err = reader.seek_row(2).unwrap_err();
    assert!(matches!(err, clickhouse_rowbinary::Error::InvalidValue(_)));

    std::fs::remove_file(path).unwrap();
}

#[test]
fn seekable_reader_with_custom_stride_can_seek() {
    let schema = Schema::from_type_strings(&[("id", "UInt8")]).unwrap();
    let rows = vec![
        vec![Value::UInt8(1)],
        vec![Value::UInt8(2)],
        vec![Value::UInt8(3)],
        vec![Value::UInt8(4)],
    ];

    let path = temp_path("seekable_stride");
    let file = File::create(&path).unwrap();
    let mut writer =
        RowBinaryWriter::new(BufWriter::new(file), RowBinaryFormat::RowBinaryWithNames).unwrap();
    writer.write_header(&schema).unwrap();
    for row in &rows {
        let bytes = row_bytes(&schema, row);
        writer.write_row_bytes(&bytes).unwrap();
    }
    writer.finish().unwrap();

    let file = File::open(&path).unwrap();
    let mut reader = RowBinaryReader::new_with_stride(
        BufReader::new(file),
        RowBinaryFormat::RowBinaryWithNames,
        Some(schema.clone()),
        2,
    )
    .unwrap();
    reader.seek_row(3).unwrap();
    let bytes = reader.current_row().unwrap().unwrap();
    let mut decoded = RowBinaryValueReader::with_schema(
        std::io::Cursor::new(bytes),
        RowBinaryFormat::RowBinary,
        schema,
    )
    .unwrap();
    assert_eq!(decoded.read_row().unwrap().unwrap()[0], Value::UInt8(4));

    std::fs::remove_file(path).unwrap();
}

#[test]
fn seekable_reader_does_not_advance_on_failed_seek() {
    let schema = Schema::from_type_strings(&[("id", "UInt8")]).unwrap();
    let rows = vec![vec![Value::UInt8(1)]];

    let path = temp_path("seekable_failed_seek");
    let file = File::create(&path).unwrap();
    let mut writer =
        RowBinaryWriter::new(BufWriter::new(file), RowBinaryFormat::RowBinaryWithNames).unwrap();
    writer.write_header(&schema).unwrap();
    for row in &rows {
        let bytes = row_bytes(&schema, row);
        writer.write_row_bytes(&bytes).unwrap();
    }
    writer.finish().unwrap();

    let file = File::open(&path).unwrap();
    let mut reader = RowBinaryReader::new(
        BufReader::new(file),
        RowBinaryFormat::RowBinaryWithNames,
        Some(schema.clone()),
    )
    .unwrap();

    let err = reader.seek_row(5).unwrap_err();
    assert!(matches!(err, clickhouse_rowbinary::Error::InvalidValue(_)));

    let bytes = reader.current_row().unwrap().unwrap();
    let mut decoded = RowBinaryValueReader::with_schema(
        std::io::Cursor::new(bytes),
        RowBinaryFormat::RowBinary,
        schema,
    )
    .unwrap();
    assert_eq!(decoded.read_row().unwrap().unwrap()[0], Value::UInt8(1));

    std::fs::remove_file(path).unwrap();
}

#[test]
fn seekable_reader_stride_one_records_next_offset() {
    let schema = Schema::from_type_strings(&[("id", "UInt8")]).unwrap();
    let rows = vec![vec![Value::UInt8(1)], vec![Value::UInt8(2)]];

    let path = temp_path("seekable_stride_one");
    let file = File::create(&path).unwrap();
    let mut writer =
        RowBinaryWriter::new(BufWriter::new(file), RowBinaryFormat::RowBinaryWithNames).unwrap();
    writer.write_header(&schema).unwrap();
    for row in &rows {
        let bytes = row_bytes(&schema, row);
        writer.write_row_bytes(&bytes).unwrap();
    }
    writer.finish().unwrap();

    let file = File::open(&path).unwrap();
    let mut reader = RowBinaryReader::new_with_stride(
        BufReader::new(file),
        RowBinaryFormat::RowBinaryWithNames,
        Some(schema.clone()),
        1,
    )
    .unwrap();
    reader.seek_row(1).unwrap();
    let bytes = reader.current_row().unwrap().unwrap();
    let mut decoded = RowBinaryValueReader::with_schema(
        std::io::Cursor::new(bytes),
        RowBinaryFormat::RowBinary,
        schema,
    )
    .unwrap();
    assert_eq!(decoded.read_row().unwrap().unwrap()[0], Value::UInt8(2));

    std::fs::remove_file(path).unwrap();
}

#[test]
fn seekable_writer_rejects_header_after_data() {
    let schema = Schema::from_type_strings(&[("id", "UInt8")]).unwrap();
    let row = vec![Value::UInt8(1)];
    let payload = row_bytes(&schema, &row);

    let path = temp_path("seekable_header_after_data");
    let file = File::create(&path).unwrap();
    let mut writer = RowBinaryWriter::new(
        BufWriter::new(file),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
    )
    .unwrap();
    writer.write_row_bytes(&payload).unwrap();
    let err = writer.write_header(&schema).unwrap_err();
    assert!(matches!(err, clickhouse_rowbinary::Error::InvalidValue(_)));

    std::fs::remove_file(path).unwrap();
}

#[test]
fn seekable_reader_requires_schema_for_with_names_even_with_header() {
    let schema = Schema::from_type_strings(&[("id", "UInt8")]).unwrap();
    let row = vec![Value::UInt8(1)];
    let payload = row_bytes(&schema, &row);

    let path = temp_path("seekable_with_names_requires_schema");
    let file = File::create(&path).unwrap();
    let mut writer =
        RowBinaryWriter::new(BufWriter::new(file), RowBinaryFormat::RowBinaryWithNames).unwrap();
    writer.write_header(&schema).unwrap();
    writer.write_row_bytes(&payload).unwrap();
    writer.finish().unwrap();

    let file = File::open(&path).unwrap();
    let err = RowBinaryReader::new(
        BufReader::new(file),
        RowBinaryFormat::RowBinaryWithNames,
        None,
    )
    .err()
    .unwrap();
    assert!(matches!(err, clickhouse_rowbinary::Error::InvalidValue(_)));

    std::fs::remove_file(path).unwrap();
}

#[test]
fn seekable_reader_handles_header_only_payload() {
    let schema = Schema::from_type_strings(&[("id", "UInt8")]).unwrap();

    let path = temp_path("seekable_header_only");
    let file = File::create(&path).unwrap();
    let mut writer =
        RowBinaryWriter::new(BufWriter::new(file), RowBinaryFormat::RowBinaryWithNames).unwrap();
    writer.write_header(&schema).unwrap();
    writer.finish().unwrap();

    let file = File::open(&path).unwrap();
    let mut reader = RowBinaryReader::new(
        BufReader::new(file),
        RowBinaryFormat::RowBinaryWithNames,
        Some(schema),
    )
    .unwrap();
    assert!(reader.current_row().unwrap().is_none());

    std::fs::remove_file(path).unwrap();
}

#[test]
fn seekable_reader_rejects_zero_column_header() {
    let path = temp_path("seekable_zero_columns");
    let file = File::create(&path).unwrap();
    let mut encoder = zeekstd::Encoder::new(BufWriter::new(file)).unwrap();
    write_uvarint(0, &mut encoder).unwrap();
    encoder.finish_format(Format::Foot).unwrap();

    let schema = Schema::from_type_strings(&[("id", "UInt8")]).unwrap();
    let file = File::open(&path).unwrap();
    let err = RowBinaryReader::new(
        BufReader::new(file),
        RowBinaryFormat::RowBinaryWithNames,
        Some(schema),
    )
    .err()
    .unwrap();
    assert!(matches!(err, clickhouse_rowbinary::Error::InvalidValue(_)));

    std::fs::remove_file(path).unwrap();
}
