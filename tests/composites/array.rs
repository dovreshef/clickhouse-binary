use clickhouse_binary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn array_uint8_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(UInt8)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([1,2,3])"));
    let schema = Schema::from_type_strings(&[("value", "Array(UInt8)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::UInt8(1),
                Value::UInt8(2),
                Value::UInt8(3),
            ])]]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn array_uint8_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(UInt8)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([1,2,3]),([])"));
    let schema = Schema::from_type_strings(&[("value", "Array(UInt8)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![
                    Value::UInt8(1),
                    Value::UInt8(2),
                    Value::UInt8(3),
                ])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn array_uint8_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(UInt8)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(UInt8)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::UInt8(1),
                Value::UInt8(2),
                Value::UInt8(3),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [1, 2, 3]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn array_uint8_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(UInt8)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(UInt8)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![
                    Value::UInt8(1),
                    Value::UInt8(2),
                    Value::UInt8(3),
                ])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [1, 2, 3]}), json!({"value": []})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn array_string_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(String)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (['a','b'])"));
    let schema = Schema::from_type_strings(&[("value", "Array(String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::String(b"a".to_vec()),
                Value::String(b"b".to_vec()),
            ])]]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn array_string_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::String(b"a".to_vec()),
                Value::String(b"b".to_vec()),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": ["a", "b"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}
