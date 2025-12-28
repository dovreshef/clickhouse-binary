use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn tuple_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Tuple(UInt8, String)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ((7, 'alpha'))"));
    let schema = Schema::from_type_strings(&[("value", "Tuple(UInt8, String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Tuple(vec![
                Value::UInt8(7),
                Value::String(b"alpha".to_vec()),
            ])]]
        );
    }
}

#[test]
fn tuple_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Tuple(UInt8, String)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ((7, 'alpha')),((9, 'beta'))"
    ));
    let schema = Schema::from_type_strings(&[("value", "Tuple(UInt8, String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Tuple(vec![
                    Value::UInt8(7),
                    Value::String(b"alpha".to_vec()),
                ])],
                vec![Value::Tuple(vec![
                    Value::UInt8(9),
                    Value::String(b"beta".to_vec()),
                ])],
            ]
        );
    }
}

#[test]
fn tuple_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Tuple(UInt8, String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Tuple(UInt8, String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Tuple(vec![
                Value::UInt8(7),
                Value::String(b"alpha".to_vec()),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [7, "alpha"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn tuple_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Tuple(UInt8, String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Tuple(UInt8, String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Tuple(vec![
                    Value::UInt8(7),
                    Value::String(b"alpha".to_vec()),
                ])],
                vec![Value::Tuple(vec![
                    Value::UInt8(9),
                    Value::String(b"beta".to_vec()),
                ])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": [7, "alpha"]}),
                json!({"value": [9, "beta"]})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn tuple_nullable_elements_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Tuple(Nullable(UInt8), String)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ((NULL, 'alpha')),((7, 'beta'))"
    ));
    let schema = Schema::from_type_strings(&[("value", "Tuple(Nullable(UInt8), String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Tuple(vec![
                    Value::Nullable(None),
                    Value::String(b"alpha".to_vec()),
                ])],
                vec![Value::Tuple(vec![
                    Value::Nullable(Some(Box::new(Value::UInt8(7)))),
                    Value::String(b"beta".to_vec()),
                ])],
            ]
        );
    }
}

#[test]
fn tuple_nullable_elements_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Tuple(Nullable(UInt8), String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Tuple(Nullable(UInt8), String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Tuple(vec![
                    Value::Nullable(None),
                    Value::String(b"alpha".to_vec()),
                ])],
                vec![Value::Tuple(vec![
                    Value::Nullable(Some(Box::new(Value::UInt8(7)))),
                    Value::String(b"beta".to_vec()),
                ])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": [null, "alpha"]}),
                json!({"value": [7, "beta"]})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn array_tuple_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Tuple(UInt8, String))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([(7, 'alpha'), (9, 'beta')])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Tuple(UInt8, String))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
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
fn array_tuple_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Tuple(UInt8, String))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Tuple(UInt8, String))")]).unwrap();

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
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [[7, "alpha"], [9, "beta"]]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn nested_tuple_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Tuple(UInt8, Tuple(String, UInt8))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ((7, ('alpha', 9)))"));
    let schema =
        Schema::from_type_strings(&[("value", "Tuple(UInt8, Tuple(String, UInt8))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Tuple(vec![
                Value::UInt8(7),
                Value::Tuple(vec![Value::String(b"alpha".to_vec()), Value::UInt8(9),]),
            ])]]
        );
    }
}

#[test]
fn nested_tuple_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Tuple(UInt8, Tuple(String, UInt8))) ENGINE=Memory"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Tuple(UInt8, Tuple(String, UInt8))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Tuple(vec![
                Value::UInt8(7),
                Value::Tuple(vec![Value::String(b"alpha".to_vec()), Value::UInt8(9)]),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [7, ["alpha", 9]]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

// Nullable(Tuple(...)) is rejected by ClickHouse, so it is intentionally
// unsupported by the parser.
