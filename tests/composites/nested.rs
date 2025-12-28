use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn nested_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (n Nested(a UInt8, b String)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} (n.a, n.b) VALUES ([7, 9], ['alpha', 'beta'])"
    ));
    let schema = Schema::from_type_strings(&[("n", "Nested(a UInt8, b String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT n FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Tuple(vec![Value::UInt8(7), Value::String(b"alpha".to_vec()),]),
                Value::Tuple(vec![Value::UInt8(9), Value::String(b"beta".to_vec()),]),
            ])]]
        );
    }
}

#[test]
fn nested_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (n Nested(a UInt8, b String)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} (n.a, n.b) VALUES ([7], ['alpha']), ([9, 10], ['beta', 'gamma'])"
    ));
    let schema = Schema::from_type_strings(&[("n", "Nested(a UInt8, b String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT n FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![Value::Tuple(vec![
                    Value::UInt8(7),
                    Value::String(b"alpha".to_vec()),
                ])])],
                vec![Value::Array(vec![
                    Value::Tuple(vec![Value::UInt8(9), Value::String(b"beta".to_vec()),]),
                    Value::Tuple(vec![Value::UInt8(10), Value::String(b"gamma".to_vec()),]),
                ])],
            ]
        );
    }
}

#[test]
fn nested_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (n Nested(a UInt8, b String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("n", "Nested(a UInt8, b String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Tuple(vec![Value::UInt8(7), Value::String(b"alpha".to_vec())]),
                Value::Tuple(vec![Value::UInt8(9), Value::String(b"beta".to_vec())]),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT n.a, n.b FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"n.a": [7, 9], "n.b": ["alpha", "beta"]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn nested_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (n Nested(a UInt8, b String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("n", "Nested(a UInt8, b String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![Value::Tuple(vec![
                    Value::UInt8(7),
                    Value::String(b"alpha".to_vec()),
                ])])],
                vec![Value::Array(vec![
                    Value::Tuple(vec![Value::UInt8(9), Value::String(b"beta".to_vec())]),
                    Value::Tuple(vec![Value::UInt8(10), Value::String(b"gamma".to_vec())]),
                ])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT n.a, n.b FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"n.a": [7], "n.b": ["alpha"]}),
                json!({"n.a": [9, 10], "n.b": ["beta", "gamma"]})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
