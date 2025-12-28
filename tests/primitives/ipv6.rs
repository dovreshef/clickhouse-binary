use std::net::Ipv6Addr;

use clickhouse_binary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn ipv6_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value IPv6) ENGINE=Memory"));
    server.exec(&format!("INSERT INTO {table} VALUES ('::1')"));
    let schema = Schema::from_type_strings(&[("value", "IPv6")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Ipv6(Ipv6Addr::LOCALHOST)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn ipv6_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value IPv6) ENGINE=Memory"));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('::1'),('2607:f8b0:4005:805::200e')"
    ));
    let schema = Schema::from_type_strings(&[("value", "IPv6")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Ipv6(Ipv6Addr::LOCALHOST)],
                vec![Value::Ipv6(Ipv6Addr::new(
                    0x2607, 0xf8b0, 0x4005, 0x0805, 0, 0, 0, 0x200e
                ))],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn ipv6_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value IPv6) ENGINE=Memory"));
    let schema = Schema::from_type_strings(&[("value", "IPv6")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Ipv6(Ipv6Addr::LOCALHOST)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "::1"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn ipv6_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value IPv6) ENGINE=Memory"));
    let schema = Schema::from_type_strings(&[("value", "IPv6")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Ipv6(Ipv6Addr::LOCALHOST)],
                vec![Value::Ipv6(Ipv6Addr::new(
                    0x2607, 0xf8b0, 0x4005, 0x0805, 0, 0, 0, 0x200e,
                ))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": "::1"}),
                json!({"value": "2607:f8b0:4005:805::200e"})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn ipv6_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(IPv6)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(IPv6)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn ipv6_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(IPv6)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL),('::1')"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(IPv6)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::Ipv6(
                    Ipv6Addr::LOCALHOST,
                ))))],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn ipv6_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(IPv6)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(IPv6)")]).unwrap();

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
fn ipv6_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(IPv6)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(IPv6)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::Ipv6(
                    Ipv6Addr::LOCALHOST,
                ))))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": null}), json!({"value": "::1"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn ipv6_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(IPv6)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!("INSERT INTO {table} VALUES ('::1')"));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(IPv6)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Ipv6(Ipv6Addr::LOCALHOST)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn ipv6_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(IPv6)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('::1'),('2607:f8b0:4005:805::200e'),('::1')"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(IPv6)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Ipv6(Ipv6Addr::LOCALHOST)],
                vec![Value::Ipv6(Ipv6Addr::new(
                    0x2607, 0xf8b0, 0x4005, 0x0805, 0, 0, 0, 0x200e
                ))],
                vec![Value::Ipv6(Ipv6Addr::LOCALHOST)],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn ipv6_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(IPv6)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(IPv6)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Ipv6(Ipv6Addr::LOCALHOST)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "::1"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn ipv6_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(IPv6)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(IPv6)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Ipv6(Ipv6Addr::LOCALHOST)],
                vec![Value::Ipv6(Ipv6Addr::new(
                    0x2607, 0xf8b0, 0x4005, 0x0805, 0, 0, 0, 0x200e,
                ))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": "::1"}),
                json!({"value": "2607:f8b0:4005:805::200e"})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}
