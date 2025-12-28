use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn array_of_maps_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Map(String, UInt8))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([map('a',1,'b',2),map('c',3)])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Map(String, UInt8))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Map(vec![
                    (Value::String(b"a".to_vec()), Value::UInt8(1)),
                    (Value::String(b"b".to_vec()), Value::UInt8(2)),
                ]),
                Value::Map(vec![(Value::String(b"c".to_vec()), Value::UInt8(3))]),
            ])]]
        );
    }
}

#[test]
fn array_of_maps_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Map(String, UInt8))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Map(String, UInt8))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Map(vec![
                    (Value::String(b"a".to_vec()), Value::UInt8(1)),
                    (Value::String(b"b".to_vec()), Value::UInt8(2)),
                ]),
                Value::Map(vec![(Value::String(b"c".to_vec()), Value::UInt8(3))]),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({
                "value": [
                    {"a": 1, "b": 2},
                    {"c": 3}
                ]
            })]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn map_of_arrays_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Map(String, Array(UInt8))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (map('a',[1,2],'b',[3]))"
    ));
    let schema = Schema::from_type_strings(&[("value", "Map(String, Array(UInt8))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Map(vec![
                (
                    Value::String(b"a".to_vec()),
                    Value::Array(vec![Value::UInt8(1), Value::UInt8(2)]),
                ),
                (
                    Value::String(b"b".to_vec()),
                    Value::Array(vec![Value::UInt8(3)]),
                ),
            ])]]
        );
    }
}

#[test]
fn map_of_arrays_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Map(String, Array(UInt8))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Map(String, Array(UInt8))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Map(vec![
                (
                    Value::String(b"a".to_vec()),
                    Value::Array(vec![Value::UInt8(1), Value::UInt8(2)]),
                ),
                (
                    Value::String(b"b".to_vec()),
                    Value::Array(vec![Value::UInt8(3)]),
                ),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": {"a": [1, 2], "b": [3]}})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn array_of_arrays_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Array(UInt8))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([[1,2],[],[3]])"));
    let schema = Schema::from_type_strings(&[("value", "Array(Array(UInt8))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Array(vec![Value::UInt8(1), Value::UInt8(2)]),
                Value::Array(Vec::new()),
                Value::Array(vec![Value::UInt8(3)]),
            ])]]
        );
    }
}

#[test]
fn array_of_arrays_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Array(UInt8))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Array(UInt8))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Array(vec![Value::UInt8(1), Value::UInt8(2)]),
                Value::Array(Vec::new()),
                Value::Array(vec![Value::UInt8(3)]),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [[1, 2], [], [3]]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
