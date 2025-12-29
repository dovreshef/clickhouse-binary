use clickhouse_rowbinary::{
    Row, RowBinaryFormat, RowBinaryValueReader, RowBinaryValueWriter, Schema, Value,
};

use crate::common::decode_rows;

#[test]
fn read_row_into_reuses_buffer() {
    let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")]).unwrap();
    let rows: Vec<Row> = vec![
        vec![Value::UInt8(1), Value::String(b"alpha".to_vec())],
        vec![Value::UInt8(2), Value::String(b"beta".to_vec())],
        vec![Value::UInt8(3), Value::String(b"gamma".to_vec())],
    ];

    let mut writer = RowBinaryValueWriter::new(
        Vec::new(),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        schema.clone(),
    );
    writer.write_header().unwrap();
    writer.write_rows(&rows).unwrap();
    let payload = writer.into_inner();

    let mut reader = RowBinaryValueReader::new(
        payload.as_slice(),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut decoded = Vec::new();
    while reader.read_row_into(&mut buf).unwrap() {
        decoded.push(buf.clone());
    }

    assert_eq!(decoded, rows);
}

#[test]
fn rows_iterator_reads_all_rows() {
    let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")]).unwrap();
    let rows: Vec<Row> = vec![
        vec![Value::UInt8(1), Value::String(b"alpha".to_vec())],
        vec![Value::UInt8(2), Value::String(b"beta".to_vec())],
    ];

    let mut writer = RowBinaryValueWriter::new(
        Vec::new(),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        schema,
    );
    writer.write_header().unwrap();
    writer.write_rows(&rows).unwrap();
    let payload = writer.into_inner();

    let reader = RowBinaryValueReader::new(
        payload.as_slice(),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
    )
    .unwrap();
    let decoded: Vec<Row> = reader.rows().collect::<Result<_, _>>().unwrap();

    assert_eq!(decoded, rows);
}

#[test]
fn take_inner_and_reset_reuse_buffer() {
    let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")]).unwrap();
    let rows: Vec<Row> = vec![
        vec![Value::UInt8(1), Value::String(b"alpha".to_vec())],
        vec![Value::UInt8(2), Value::String(b"beta".to_vec())],
    ];

    let mut writer = RowBinaryValueWriter::new(
        Vec::new(),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        schema.clone(),
    );
    writer.write_header().unwrap();
    writer.write_row(&rows[0]).unwrap();
    let mut payload = writer.take_inner();
    let decoded = decode_rows(
        &payload,
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        &schema,
    );
    assert_eq!(decoded, vec![rows[0].clone()]);

    payload.clear();
    writer.reset(payload);
    writer.write_header().unwrap();
    writer.write_row(&rows[1]).unwrap();
    let payload = writer.into_inner();
    let decoded = decode_rows(
        &payload,
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        &schema,
    );
    assert_eq!(decoded, vec![rows[1].clone()]);
}
