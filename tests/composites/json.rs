use clickhouse_binary::{RowBinaryFormat, Schema, TypeDesc, Value};

use crate::common::{ClickhouseServer, decode_rows, normalize_json_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn json_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!("CREATE TABLE {table} (value JSON) ENGINE=Memory"));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('{{\"a\":1,\"b\":\"x\"}}')"
    ));
    let schema = Schema::from_type_strings(&[("value", "JSON")]).unwrap();

    let mut expected = vec![vec![Value::JsonObject(vec![
        (
            "a".to_string(),
            Value::Dynamic {
                ty: Box::new(TypeDesc::Int64),
                value: Box::new(Value::Int64(1)),
            },
        ),
        (
            "b".to_string(),
            Value::Dynamic {
                ty: Box::new(TypeDesc::String),
                value: Box::new(Value::String(b"x".to_vec())),
            },
        ),
    ])]];
    normalize_json_rows(&mut expected, 0);

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        normalize_json_rows(&mut decoded, 0);
        assert_eq!(decoded, expected);
    }
}

#[test]
fn json_typed_paths_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value JSON(a UInt8, b String)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('{{\"a\":1,\"b\":\"x\",\"c\":2}}')"
    ));
    let schema = Schema::from_type_strings(&[("value", "JSON(a UInt8, b String)")]).unwrap();

    let mut expected = vec![vec![Value::JsonObject(vec![
        ("a".to_string(), Value::UInt8(1)),
        ("b".to_string(), Value::String(b"x".to_vec())),
        (
            "c".to_string(),
            Value::Dynamic {
                ty: Box::new(TypeDesc::Int64),
                value: Box::new(Value::Int64(2)),
            },
        ),
    ])]];
    normalize_json_rows(&mut expected, 0);

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        normalize_json_rows(&mut decoded, 0);
        assert_eq!(decoded, expected);
    }
}

#[test]
fn json_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!("CREATE TABLE {table} (value JSON) ENGINE=Memory"));
    let schema = Schema::from_type_strings(&[("value", "JSON")]).unwrap();

    let row = vec![Value::JsonObject(vec![
        (
            "a".to_string(),
            Value::Dynamic {
                ty: Box::new(TypeDesc::Int64),
                value: Box::new(Value::Int64(1)),
            },
        ),
        (
            "b".to_string(),
            Value::Dynamic {
                ty: Box::new(TypeDesc::String),
                value: Box::new(Value::String(b"x".to_vec())),
            },
        ),
    ])];

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, std::slice::from_ref(&row));
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        normalize_json_rows(&mut decoded, 0);
        let mut expected = vec![row.clone()];
        normalize_json_rows(&mut expected, 0);
        assert_eq!(decoded, expected);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn json_typed_paths_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value JSON(a UInt8, b String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "JSON(a UInt8, b String)")]).unwrap();

    let row = vec![Value::JsonObject(vec![
        ("a".to_string(), Value::UInt8(1)),
        ("b".to_string(), Value::String(b"x".to_vec())),
        (
            "c".to_string(),
            Value::Dynamic {
                ty: Box::new(TypeDesc::Int64),
                value: Box::new(Value::Int64(2)),
            },
        ),
    ])];

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, std::slice::from_ref(&row));
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        normalize_json_rows(&mut decoded, 0);
        let mut expected = vec![row.clone()];
        normalize_json_rows(&mut expected, 0);
        assert_eq!(decoded, expected);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
