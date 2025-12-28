use clickhouse_binary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn uint8_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value UInt8) ENGINE=Memory"));
    server.exec(&format!("INSERT INTO {table} VALUES (7)"));
    let schema = Schema::from_type_strings(&[("value", "UInt8")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::UInt8(7)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uint8_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value UInt8) ENGINE=Memory"));
    server.exec(&format!("INSERT INTO {table} VALUES (7),(9)"));
    let schema = Schema::from_type_strings(&[("value", "UInt8")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::UInt8(7)], vec![Value::UInt8(9)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uint8_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value UInt8) ENGINE=Memory"));
    let schema = Schema::from_type_strings(&[("value", "UInt8")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::UInt8(7)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uint8_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value UInt8) ENGINE=Memory"));
    let schema = Schema::from_type_strings(&[("value", "UInt8")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::UInt8(7)], vec![Value::UInt8(9)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7}), json!({"value": 9})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uint8_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UInt8)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UInt8)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uint8_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UInt8)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL),(7)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UInt8)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::UInt8(7))))],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uint8_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UInt8)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UInt8)")]).unwrap();

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
fn uint8_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UInt8)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UInt8)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::UInt8(7))))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": null}), json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uint8_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(UInt8)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!("INSERT INTO {table} VALUES (7)"));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UInt8)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::UInt8(7)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uint8_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(UInt8)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!("INSERT INTO {table} VALUES (7),(9),(7)"));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UInt8)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::UInt8(7)],
                vec![Value::UInt8(9)],
                vec![Value::UInt8(7)],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uint8_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(UInt8)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UInt8)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::UInt8(7)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uint8_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(UInt8)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UInt8)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::UInt8(7)], vec![Value::UInt8(9)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7}), json!({"value": 9})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}
