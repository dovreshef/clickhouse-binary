use clickhouse_binary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn date_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value Date) ENGINE=Memory"));
    server.exec(&format!("INSERT INTO {table} VALUES ('1970-01-01')"));
    let schema = Schema::from_type_strings(&[("value", "Date")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Date(0)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn date_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value Date) ENGINE=Memory"));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('1970-01-01'),('1970-01-02')"
    ));
    let schema = Schema::from_type_strings(&[("value", "Date")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Date(0)], vec![Value::Date(1)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn date_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value Date) ENGINE=Memory"));
    let schema = Schema::from_type_strings(&[("value", "Date")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Date(0)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "1970-01-01"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn date_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value Date) ENGINE=Memory"));
    let schema = Schema::from_type_strings(&[("value", "Date")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Date(0)], vec![Value::Date(1)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": "1970-01-01"}),
                json!({"value": "1970-01-02"})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn date_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Date)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Date)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn date_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Date)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL),('1970-01-01')"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Date)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::Date(0))))],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn date_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Date)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Date)")]).unwrap();

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
fn date_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Date)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Date)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::Date(0))))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": null}), json!({"value": "1970-01-01"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn date_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Date)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!(
        "INSERT INTO {table} VALUES (toDate('1970-01-01'))"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Date)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Date(0)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn date_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Date)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!(
        "INSERT INTO {table} VALUES (toDate('1970-01-01')),(toDate('1970-01-02')),(toDate('1970-01-01'))"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Date)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Date(0)],
                vec![Value::Date(1)],
                vec![Value::Date(0)]
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn date_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Date)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Date)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Date(0)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "1970-01-01"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn date_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Date)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Date)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Date(0)], vec![Value::Date(1)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": "1970-01-01"}),
                json!({"value": "1970-01-02"})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}
