use std::{fs::File, io::Write};

use clickhouse_binary::{Row, RowBinaryFormat, RowBinaryReader, RowBinaryWriter, Schema, Value};
use serde_json::json;
use zstd::stream::{Decoder, Encoder};

use crate::common::{ClickhouseServer, unique_table};

// Demonstrates reading a compressed RowBinaryWithNamesAndTypes file and
// re-encoding batches with the header included per batch.
#[test]
fn read_compressed_rowbinary_with_names_and_types_in_batches() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (id UInt8, name String) ENGINE=Memory"
    ));

    let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")]).unwrap();
    let rows: Vec<Row> = vec![
        vec![Value::UInt8(1), Value::String(b"alpha".to_vec())],
        vec![Value::UInt8(2), Value::String(b"beta".to_vec())],
        vec![Value::UInt8(3), Value::String(b"gamma".to_vec())],
    ];

    // Write a compressed file once (header only at the start).
    let file_path = std::env::temp_dir().join(format!("rowbinary_{table}.zst"));
    let file = File::create(&file_path).unwrap();
    let mut encoder = Encoder::new(file, 0).unwrap();
    let mut writer = RowBinaryWriter::new(
        Vec::new(),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        schema.clone(),
    );
    writer.write_rows(&rows).unwrap();
    encoder.write_all(&writer.into_inner()).unwrap();
    encoder.finish().unwrap();

    // Stream-decode and stream-encode into batch payloads. Each batch needs
    // its own header for RowBinaryWithNamesAndTypes.
    let file = File::open(&file_path).unwrap();
    let decoder = Decoder::new(file).unwrap();
    let mut reader = RowBinaryReader::new(decoder, RowBinaryFormat::RowBinaryWithNamesAndTypes);
    reader.read_header().unwrap();

    let mut out = RowBinaryWriter::new(
        Vec::new(),
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        schema.clone(),
    );
    let mut count = 0usize;
    let mut sent = 0usize;
    while let Some(row) = reader.read_row().unwrap() {
        out.write_row(&row).unwrap();
        count += 1;
        if count == 2 {
            let payload = out.into_inner();
            let insert_sql = format!(
                "INSERT INTO {table} FORMAT {}",
                RowBinaryFormat::RowBinaryWithNamesAndTypes
            );
            server.insert_payload(&insert_sql, &payload);
            sent += count;
            out = RowBinaryWriter::new(
                Vec::new(),
                RowBinaryFormat::RowBinaryWithNamesAndTypes,
                schema.clone(),
            );
            count = 0;
        }
    }
    if count > 0 {
        let payload = out.into_inner();
        let insert_sql = format!(
            "INSERT INTO {table} FORMAT {}",
            RowBinaryFormat::RowBinaryWithNamesAndTypes
        );
        server.insert_payload(&insert_sql, &payload);
        sent += count;
    }

    let json_rows = server.fetch_json(&format!("SELECT id, name FROM {table} ORDER BY id"));
    assert_eq!(
        json_rows,
        vec![
            json!({"id": 1, "name": "alpha"}),
            json!({"id": 2, "name": "beta"}),
            json!({"id": 3, "name": "gamma"}),
        ]
    );
    assert_eq!(sent, rows.len());
}
