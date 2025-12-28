use clickhouse_binary::{RowBinaryFormat, Schema, Value};
use serde_json::json;
use uuid::Uuid;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

fn uuid_values() -> (Uuid, Uuid) {
    (
        Uuid::parse_str("e4eaaaf2-d142-11e1-b3e4-080027620cdd").unwrap(),
        Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap(),
    )
}

#[test]
fn uuid_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value UUID) ENGINE=Memory"));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('e4eaaaf2-d142-11e1-b3e4-080027620cdd')"
    ));
    let schema = Schema::from_type_strings(&[("value", "UUID")]).unwrap();
    let (first, _) = uuid_values();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Uuid(first)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uuid_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value UUID) ENGINE=Memory"));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('e4eaaaf2-d142-11e1-b3e4-080027620cdd'),('f47ac10b-58cc-4372-a567-0e02b2c3d479')"
    ));
    let schema = Schema::from_type_strings(&[("value", "UUID")]).unwrap();
    let (first, second) = uuid_values();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Uuid(first)], vec![Value::Uuid(second)]]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uuid_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value UUID) ENGINE=Memory"));
    let schema = Schema::from_type_strings(&[("value", "UUID")]).unwrap();
    let (first, _) = uuid_values();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Uuid(first)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": "e4eaaaf2-d142-11e1-b3e4-080027620cdd"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uuid_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("CREATE TABLE {table} (value UUID) ENGINE=Memory"));
    let schema = Schema::from_type_strings(&[("value", "UUID")]).unwrap();
    let (first, second) = uuid_values();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Uuid(first)], vec![Value::Uuid(second)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": "e4eaaaf2-d142-11e1-b3e4-080027620cdd"}),
                json!({"value": "f47ac10b-58cc-4372-a567-0e02b2c3d479"})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uuid_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UUID)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UUID)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uuid_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UUID)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (NULL),('e4eaaaf2-d142-11e1-b3e4-080027620cdd')"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UUID)")]).unwrap();
    let (first, _) = uuid_values();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::Uuid(first))))],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uuid_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UUID)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UUID)")]).unwrap();

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
fn uuid_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UUID)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UUID)")]).unwrap();
    let (first, _) = uuid_values();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::Uuid(first))))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": null}),
                json!({"value": "e4eaaaf2-d142-11e1-b3e4-080027620cdd"})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uuid_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(UUID)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('e4eaaaf2-d142-11e1-b3e4-080027620cdd')"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UUID)")]).unwrap();
    let (first, _) = uuid_values();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Uuid(first)]]);
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uuid_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(UUID)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('e4eaaaf2-d142-11e1-b3e4-080027620cdd'),('f47ac10b-58cc-4372-a567-0e02b2c3d479'),('e4eaaaf2-d142-11e1-b3e4-080027620cdd')"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UUID)")]).unwrap();
    let (first, second) = uuid_values();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Uuid(first)],
                vec![Value::Uuid(second)],
                vec![Value::Uuid(first)],
            ]
        );
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uuid_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(UUID)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UUID)")]).unwrap();
    let (first, _) = uuid_values();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Uuid(first)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": "e4eaaaf2-d142-11e1-b3e4-080027620cdd"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}

#[test]
fn uuid_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(UUID)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UUID)")]).unwrap();
    let (first, second) = uuid_values();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Uuid(first)], vec![Value::Uuid(second)]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": "e4eaaaf2-d142-11e1-b3e4-080027620cdd"}),
                json!({"value": "f47ac10b-58cc-4372-a567-0e02b2c3d479"})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }

    server.exec(&format!("DROP TABLE {table}"));
}
