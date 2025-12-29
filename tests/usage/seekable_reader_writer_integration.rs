use std::path::PathBuf;

use clickhouse_rowbinary::{
    RowBinaryFormat, RowBinaryReader, RowBinaryValueWriter, RowBinaryWriter, Schema, Value,
};
use rand::{Rng, distr::Alphanumeric, rng};
use std::{
    fs::File,
    io::{BufReader, BufWriter},
};

use crate::common::{ClickhouseServer, row_bytes, unique_table};

fn temp_path(prefix: &str) -> PathBuf {
    let suffix: String = rng()
        .sample_iter(Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    std::env::temp_dir().join(format!("{prefix}_{suffix}.zst"))
}

#[test]
fn seekable_reader_writer_roundtrip_clickhouse() {
    let server = ClickhouseServer::connect();
    let table = unique_table("seekable_src");
    let dest = unique_table("seekable_dest");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!("DROP TABLE IF EXISTS {dest}"));

    server.exec(&format!(
        "CREATE TABLE {table} (id UInt8, name String) ENGINE=Memory"
    ));
    server.exec(&format!(
        "CREATE TABLE {dest} (id UInt8, name String) ENGINE=Memory"
    ));

    let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")]).unwrap();
    let rows = vec![
        vec![Value::UInt8(1), Value::String(b"alpha".to_vec())],
        vec![Value::UInt8(2), Value::String(b"beta".to_vec())],
        vec![Value::UInt8(3), Value::String(b"gamma".to_vec())],
    ];

    server.insert_rowbinary(
        &format!("INSERT INTO {table} FORMAT RowBinary"),
        RowBinaryFormat::RowBinary,
        &schema,
        &rows,
    );

    let path = temp_path("seekable_clickhouse");
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
    let mut out = RowBinaryValueWriter::new(
        Vec::new(),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        schema.clone(),
    );
    out.write_header().unwrap();
    loop {
        let Some(row) = reader.current_row().unwrap() else {
            break;
        };
        out.write_row_bytes(row).unwrap();
        if reader.seek_relative(1).is_err() {
            break;
        }
    }
    let payload = out.into_inner();
    server.insert_payload(
        &format!("INSERT INTO {dest} FORMAT RowBinaryWithNamesAndTypes"),
        &payload,
    );

    let result = server.fetch_json(&format!("SELECT * FROM {dest} ORDER BY id"));
    assert_eq!(result.len(), 3);

    std::fs::remove_file(path).unwrap();
}
