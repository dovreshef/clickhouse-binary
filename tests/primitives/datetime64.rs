use clickhouse_binary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn datetime64_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value DateTime64(3)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('1970-01-01 00:00:01.234')"
    ));
    let schema = Schema::from_type_strings(&[("value", "DateTime64(3)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::DateTime64(1234)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn datetime64_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value DateTime64(3)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('1970-01-01 00:00:01.234'),('1970-01-01 00:00:02.345')"
    ));
    let schema = Schema::from_type_strings(&[("value", "DateTime64(3)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::DateTime64(1234)], vec![Value::DateTime64(2345)]]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn datetime64_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value DateTime64(3)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "DateTime64(3)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::DateTime64(1234)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "1970-01-01 00:00:01.234"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn datetime64_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value DateTime64(3)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "DateTime64(3)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::DateTime64(1234)], vec![Value::DateTime64(2345)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": "1970-01-01 00:00:01.234"}),
                json!({"value": "1970-01-01 00:00:02.345"})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn datetime64_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(DateTime64(3))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(DateTime64(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn datetime64_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(DateTime64(3))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (NULL),('1970-01-01 00:00:01.234')"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(DateTime64(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::DateTime64(1234))))],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn datetime64_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(DateTime64(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(DateTime64(3))")]).unwrap();

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
fn datetime64_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(DateTime64(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(DateTime64(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::DateTime64(1234))))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": null}),
                json!({"value": "1970-01-01 00:00:01.234"})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn datetime64_low_cardinality_is_rejected() {
    let err = Schema::from_type_strings(&[("value", "LowCardinality(DateTime64(3))")])
        .expect_err("expected schema parsing to reject LowCardinality(DateTime64)");
    assert!(matches!(
        err,
        clickhouse_binary::Error::UnsupportedCombination(_)
    ));
}
