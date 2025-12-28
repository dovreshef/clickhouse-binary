use clickhouse_binary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn map_string_uint8_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Map(String, UInt8)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (map('a',1,'b',2))"));
    let schema = Schema::from_type_strings(&[("value", "Map(String, UInt8)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Map(vec![
                (Value::String(b"a".to_vec()), Value::UInt8(1)),
                (Value::String(b"b".to_vec()), Value::UInt8(2)),
            ])]]
        );
    }
}

#[test]
fn map_string_uint8_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Map(String, UInt8)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (map('a',1)),(map())"));
    let schema = Schema::from_type_strings(&[("value", "Map(String, UInt8)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Map(vec![(
                    Value::String(b"a".to_vec()),
                    Value::UInt8(1),
                )])],
                vec![Value::Map(Vec::new())],
            ]
        );
    }
}

#[test]
fn map_string_uint8_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Map(String, UInt8)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Map(String, UInt8)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Map(vec![
                (Value::String(b"a".to_vec()), Value::UInt8(1)),
                (Value::String(b"b".to_vec()), Value::UInt8(2)),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": {"a": 1, "b": 2}})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn map_string_uint8_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Map(String, UInt8)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Map(String, UInt8)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Map(vec![(
                    Value::String(b"a".to_vec()),
                    Value::UInt8(1),
                )])],
                vec![Value::Map(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": {"a": 1}}), json!({"value": {}})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn map_rejects_nullable_key_schema() {
    // ClickHouse forbids Nullable keys in Map types; reject during schema parsing.
    let err = Schema::from_type_strings(&[("value", "Map(Nullable(String), UInt8)")]);
    assert!(err.is_err());
}

#[test]
fn map_rejects_low_cardinality_nullable_key_schema() {
    // LowCardinality(Nullable(...)) should also be rejected for Map keys.
    let err =
        Schema::from_type_strings(&[("value", "Map(LowCardinality(Nullable(String)), UInt8)")]);
    assert!(err.is_err());
}

#[test]
fn map_allows_low_cardinality_key_schema() {
    // LowCardinality(String) is a valid Map key.
    let schema = Schema::from_type_strings(&[("value", "Map(LowCardinality(String), UInt8)")]);
    assert!(schema.is_ok());
}
