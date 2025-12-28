use clickhouse_binary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn string_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value String) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('alpha')"));
    let schema = Schema::from_type_strings(&[("value", "String")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::String(b"alpha".to_vec())]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn string_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value String) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('alpha'),('beta')"));
    let schema = Schema::from_type_strings(&[("value", "String")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::String(b"alpha".to_vec())],
                vec![Value::String(b"beta".to_vec())]
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn string_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value String) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "String")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::String(b"alpha".to_vec())]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "alpha"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn string_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value String) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "String")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::String(b"alpha".to_vec())],
                vec![Value::String(b"beta".to_vec())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": "alpha"}), json!({"value": "beta"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn string_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(String)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn string_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(String)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL),('alpha')"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::String(
                    b"alpha".to_vec()
                ))))],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn string_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Nullable(None)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": null})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn string_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::String(
                    b"alpha".to_vec(),
                ))))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": null}), json!({"value": "alpha"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn string_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(String)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('alpha')"));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::String(b"alpha".to_vec())]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn string_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(String)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('alpha'),('beta'),('alpha')"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::String(b"alpha".to_vec())],
                vec![Value::String(b"beta".to_vec())],
                vec![Value::String(b"alpha".to_vec())],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn string_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::String(b"alpha".to_vec())]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "alpha"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn string_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::String(b"alpha".to_vec())],
                vec![Value::String(b"beta".to_vec())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": "alpha"}), json!({"value": "beta"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}
