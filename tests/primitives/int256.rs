use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decimal256_from_i128, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

fn int256_a() -> Value {
    Value::Int256(decimal256_from_i128(7))
}

fn int256_b() -> Value {
    Value::Int256(decimal256_from_i128(-9))
}

#[test]
fn int256_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Int256) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (CAST(7 AS Int256))"));
    let schema = Schema::from_type_strings(&[("value", "Int256")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![int256_a()]]);
    }
}

#[test]
fn int256_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Int256) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (CAST(7 AS Int256)),(CAST(-9 AS Int256))"
    ));
    let schema = Schema::from_type_strings(&[("value", "Int256")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![int256_a()], vec![int256_b()]]);
    }
}

#[test]
fn int256_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Int256) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Int256")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![int256_a()]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn int256_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Int256) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Int256")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![int256_a()], vec![int256_b()]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7}), json!({"value": -9})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn int256_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Int256)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Int256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }
}

#[test]
fn int256_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Int256)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (NULL),(CAST(7 AS Int256))"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Int256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(int256_a())))]
            ]
        );
    }
}

#[test]
fn int256_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Int256)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Int256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Nullable(Some(Box::new(int256_a())))]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn int256_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Int256)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Int256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(int256_a())))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": null}), json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn int256_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(Int256)) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES (CAST(7 AS Int256))"));
    if Schema::from_type_strings(&[("value", "LowCardinality(Int256)")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Int256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![int256_a()]]);
    }
}

#[test]
fn int256_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(Int256)) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES (CAST(7 AS Int256)),(CAST(-9 AS Int256)),(CAST(7 AS Int256))"
    ));
    if Schema::from_type_strings(&[("value", "LowCardinality(Int256)")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Int256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![int256_a()], vec![int256_b()], vec![int256_a()]]
        );
    }
}

#[test]
fn int256_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(Int256)) ENGINE=Memory"
    )) {
        return;
    }
    if Schema::from_type_strings(&[("value", "LowCardinality(Int256)")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Int256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![int256_a()]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn int256_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(Int256)) ENGINE=Memory"
    )) {
        return;
    }
    if Schema::from_type_strings(&[("value", "LowCardinality(Int256)")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Int256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![int256_a()], vec![int256_b()]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7}), json!({"value": -9})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn int256_array_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Int256)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([CAST(7 AS Int256), CAST(-9 AS Int256)])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Int256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![int256_a(), int256_b()])]]
        );
    }
}

#[test]
fn int256_array_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Int256)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([CAST(7 AS Int256)]),([CAST(-9 AS Int256), CAST(7 AS Int256)])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Int256)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![int256_a()])],
                vec![Value::Array(vec![int256_b(), int256_a()])]
            ]
        );
    }
}

#[test]
fn int256_array_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Int256)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Int256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![int256_a(), int256_b()])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [7, -9]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn int256_array_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Int256)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Int256)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![int256_a()])],
                vec![Value::Array(vec![int256_b(), int256_a()])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [7]}), json!({"value": [-9, 7]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn int256_array_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Int256))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([NULL, CAST(7 AS Int256)])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Int256))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(int256_a()))),
            ])]]
        );
    }
}

#[test]
fn int256_array_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Int256))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([NULL]),([CAST(7 AS Int256), NULL])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Int256))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(vec![
                    Value::Nullable(Some(Box::new(int256_a()))),
                    Value::Nullable(None)
                ])]
            ]
        );
    }
}

#[test]
fn int256_array_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Int256))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Int256))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(int256_a()))),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [null, 7]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn int256_array_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Int256))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Int256))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(vec![
                    Value::Nullable(Some(Box::new(int256_a()))),
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
fn int256_array_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(LowCardinality(Int256))) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([CAST(7 AS Int256), CAST(-9 AS Int256)])"
    ));
    if Schema::from_type_strings(&[("value", "Array(LowCardinality(Int256))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(Int256))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![int256_a(), int256_b()])]]
        );
    }
}

#[test]
fn int256_array_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(LowCardinality(Int256))) ENGINE=Memory"
    )) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([CAST(7 AS Int256)]),([CAST(-9 AS Int256), CAST(7 AS Int256)])"
    ));
    if Schema::from_type_strings(&[("value", "Array(LowCardinality(Int256))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(Int256))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![int256_a()])],
                vec![Value::Array(vec![int256_b(), int256_a()])]
            ]
        );
    }
}

#[test]
fn int256_array_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(LowCardinality(Int256))) ENGINE=Memory"
    )) {
        return;
    }
    if Schema::from_type_strings(&[("value", "Array(LowCardinality(Int256))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(Int256))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![int256_a(), int256_b()])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [7, -9]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn int256_array_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value Array(LowCardinality(Int256))) ENGINE=Memory"
    )) {
        return;
    }
    if Schema::from_type_strings(&[("value", "Array(LowCardinality(Int256))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(Int256))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![int256_a()])],
                vec![Value::Array(vec![int256_b(), int256_a()])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [7]}), json!({"value": [-9, 7]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
