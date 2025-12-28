use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, uint256_from_u128, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

fn uint256_a() -> Value {
    Value::UInt256(uint256_from_u128(7))
}

fn uint256_b() -> Value {
    Value::UInt256(uint256_from_u128(9))
}

#[test]
fn uint256_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value UInt256) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (CAST(7 AS UInt256))"));
    let schema = Schema::from_type_strings(&[("value", "UInt256")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![uint256_a()]]);
    }
}

#[test]
fn uint256_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value UInt256) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (CAST(7 AS UInt256)),(CAST(9 AS UInt256))"
    ));
    let schema = Schema::from_type_strings(&[("value", "UInt256")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![uint256_a()], vec![uint256_b()]]);
    }
}

#[test]
fn uint256_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value UInt256) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "UInt256")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![uint256_a()]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn uint256_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value UInt256) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "UInt256")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![uint256_a()], vec![uint256_b()]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7}), json!({"value": 9})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn uint256_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UInt256)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UInt256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }
}

#[test]
fn uint256_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UInt256)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (NULL),(CAST(7 AS UInt256))"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UInt256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(uint256_a())))]
            ]
        );
    }
}

#[test]
fn uint256_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UInt256)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UInt256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Nullable(Some(Box::new(uint256_a())))]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn uint256_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(UInt256)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(UInt256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(uint256_a())))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": null}), json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn uint256_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(UInt256)) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES (CAST(7 AS UInt256))"));
    if Schema::from_type_strings(&[("value", "LowCardinality(UInt256)")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UInt256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![uint256_a()]]);
    }
}

#[test]
fn uint256_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(UInt256)) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES (CAST(7 AS UInt256)),(CAST(9 AS UInt256)),(CAST(7 AS UInt256))"
    ));
    if Schema::from_type_strings(&[("value", "LowCardinality(UInt256)")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UInt256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![uint256_a()], vec![uint256_b()], vec![uint256_a()]]
        );
    }
}

#[test]
fn uint256_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(UInt256)) ENGINE=Memory"
    )) {
        return;
    }
    if Schema::from_type_strings(&[("value", "LowCardinality(UInt256)")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UInt256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![uint256_a()]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn uint256_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(UInt256)) ENGINE=Memory"
    )) {
        return;
    }
    if Schema::from_type_strings(&[("value", "LowCardinality(UInt256)")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(UInt256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![uint256_a()], vec![uint256_b()]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7}), json!({"value": 9})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn uint256_array_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(UInt256)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([CAST(7 AS UInt256), CAST(9 AS UInt256)])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(UInt256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![uint256_a(), uint256_b()])]]
        );
    }
}

#[test]
fn uint256_array_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(UInt256)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([CAST(7 AS UInt256)]),([CAST(9 AS UInt256), CAST(7 AS UInt256)])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(UInt256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![uint256_a()])],
                vec![Value::Array(vec![uint256_b(), uint256_a()])]
            ]
        );
    }
}

#[test]
fn uint256_array_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(UInt256)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(UInt256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![uint256_a(), uint256_b()])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [7, 9]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn uint256_array_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(UInt256)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(UInt256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![uint256_a()])],
                vec![Value::Array(vec![uint256_b(), uint256_a()])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [7]}), json!({"value": [9, 7]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn uint256_array_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(UInt256))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([NULL, CAST(7 AS UInt256)])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(UInt256))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(uint256_a()))),
            ])]]
        );
    }
}

#[test]
fn uint256_array_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(UInt256))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([NULL]),([CAST(7 AS UInt256), NULL])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(UInt256))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(vec![
                    Value::Nullable(Some(Box::new(uint256_a()))),
                    Value::Nullable(None)
                ])]
            ]
        );
    }
}

#[test]
fn uint256_array_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(UInt256))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(UInt256))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(uint256_a()))),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [null, 7]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn uint256_array_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(UInt256))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(UInt256))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(vec![
                    Value::Nullable(Some(Box::new(uint256_a()))),
                    Value::Nullable(None),
                ])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [null]}), json!({"value": [7, null]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn uint256_array_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(LowCardinality(UInt256))) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([CAST(7 AS UInt256), CAST(9 AS UInt256)])"
    ));
    if Schema::from_type_strings(&[("value", "Array(LowCardinality(UInt256))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(UInt256))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![uint256_a(), uint256_b()])]]
        );
    }
}

#[test]
fn uint256_array_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(LowCardinality(UInt256))) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([CAST(7 AS UInt256)]),([CAST(9 AS UInt256), CAST(7 AS UInt256)])"
    ));
    if Schema::from_type_strings(&[("value", "Array(LowCardinality(UInt256))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(UInt256))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![uint256_a()])],
                vec![Value::Array(vec![uint256_b(), uint256_a()])]
            ]
        );
    }
}

#[test]
fn uint256_array_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(LowCardinality(UInt256))) ENGINE=Memory"
    )) {
        return;
    }
    if Schema::from_type_strings(&[("value", "Array(LowCardinality(UInt256))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(UInt256))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![uint256_a(), uint256_b()])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [7, 9]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn uint256_array_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(LowCardinality(UInt256))) ENGINE=Memory"
    )) {
        return;
    }
    if Schema::from_type_strings(&[("value", "Array(LowCardinality(UInt256))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(UInt256))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![uint256_a()])],
                vec![Value::Array(vec![uint256_b(), uint256_a()])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [7]}), json!({"value": [9, 7]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
