use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{
    ClickhouseServer, decimal_json, decimal256_from_i128, decode_rows, unique_table,
};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

const SCALE: u32 = 4;

fn decimal256_a() -> Value {
    Value::Decimal256(decimal256_from_i128(12_345_678))
}

fn decimal256_b() -> Value {
    Value::Decimal256(decimal256_from_i128(-90_123_456))
}

#[test]
fn decimal256_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Decimal256(4)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (1234.5678)"));
    let schema = Schema::from_type_strings(&[("value", "Decimal256(4)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![decimal256_a()]]);
    }
}

#[test]
fn decimal256_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Decimal256(4)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (1234.5678),(-9012.3456)"
    ));
    let schema = Schema::from_type_strings(&[("value", "Decimal256(4)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![decimal256_a()], vec![decimal256_b()]]);
    }
}

#[test]
fn decimal256_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Decimal256(4)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Decimal256(4)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![decimal256_a()]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": decimal_json(12_345_678, SCALE)})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal256_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Decimal256(4)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Decimal256(4)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![decimal256_a()], vec![decimal256_b()]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": decimal_json(12_345_678, SCALE)}),
                json!({"value": decimal_json(-90_123_456, SCALE)}),
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal256_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Decimal256(4))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }
}

#[test]
fn decimal256_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Decimal256(4))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL),(1234.5678)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(decimal256_a())))],
            ]
        );
    }
}

#[test]
fn decimal256_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Decimal256(4))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Nullable(None)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": null})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal256_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Decimal256(4))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(decimal256_a())))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": null}),
                json!({"value": decimal_json(12_345_678, SCALE)})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal256_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Decimal256(4))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES (1234.5678)"));
    if Schema::from_type_strings(&[("value", "LowCardinality(Decimal256(4))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![decimal256_a()]]);
    }
}

#[test]
fn decimal256_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Decimal256(4))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES (1234.5678),(-9012.3456),(1234.5678)"
    ));
    if Schema::from_type_strings(&[("value", "LowCardinality(Decimal256(4))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![decimal256_a()],
                vec![decimal256_b()],
                vec![decimal256_a()]
            ]
        );
    }
}

#[test]
fn decimal256_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Decimal256(4))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    if Schema::from_type_strings(&[("value", "LowCardinality(Decimal256(4))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![decimal256_a()]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": decimal_json(12_345_678, SCALE)})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal256_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Decimal256(4))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    if Schema::from_type_strings(&[("value", "LowCardinality(Decimal256(4))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![decimal256_a()], vec![decimal256_b()]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": decimal_json(12_345_678, SCALE)}),
                json!({"value": decimal_json(-90_123_456, SCALE)}),
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal256_array_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Decimal256(4))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([1234.5678, -9012.3456])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![decimal256_a(), decimal256_b()])]]
        );
    }
}

#[test]
fn decimal256_array_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Decimal256(4))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([1234.5678]),([-9012.3456, 1234.5678])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![decimal256_a()])],
                vec![Value::Array(vec![decimal256_b(), decimal256_a()])],
            ]
        );
    }
}

#[test]
fn decimal256_array_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Decimal256(4))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![decimal256_a(), decimal256_b()])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({
                "value": [decimal_json(12_345_678, SCALE), decimal_json(-90_123_456, SCALE)]
            })]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal256_array_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Decimal256(4))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Decimal256(4))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![decimal256_a()])],
                vec![Value::Array(vec![decimal256_b(), decimal256_a()])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": [decimal_json(12_345_678, SCALE)]}),
                json!({
                    "value": [decimal_json(-90_123_456, SCALE), decimal_json(12_345_678, SCALE)]
                }),
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal256_array_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Decimal256(4)))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([NULL, 1234.5678])"));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Decimal256(4)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(decimal256_a()))),
            ])]]
        );
    }
}

#[test]
fn decimal256_array_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Decimal256(4)))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([NULL]),([1234.5678, NULL])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Decimal256(4)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(vec![
                    Value::Nullable(Some(Box::new(decimal256_a()))),
                    Value::Nullable(None),
                ])],
            ]
        );
    }
}

#[test]
fn decimal256_array_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Decimal256(4)))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Decimal256(4)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(decimal256_a()))),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [null, decimal_json(12_345_678, SCALE)]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal256_array_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Decimal256(4)))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Decimal256(4)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(vec![
                    Value::Nullable(Some(Box::new(decimal256_a()))),
                    Value::Nullable(None),
                ])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": [null]}),
                json!({"value": [decimal_json(12_345_678, SCALE), null]}),
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal256_array_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Decimal256(4)))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES ([1234.5678])"));
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(Decimal256(4)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Array(vec![decimal256_a()])]]);
    }
}

#[test]
fn decimal256_array_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Decimal256(4)))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([1234.5678]),([-9012.3456, 1234.5678])"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(Decimal256(4)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![decimal256_a()])],
                vec![Value::Array(vec![decimal256_b(), decimal256_a()])],
            ]
        );
    }
}

#[test]
fn decimal256_array_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Decimal256(4)))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(Decimal256(4)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![decimal256_a()])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [decimal_json(12_345_678, SCALE)]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal256_array_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Decimal256(4)))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(Decimal256(4)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![decimal256_a()])],
                vec![Value::Array(vec![decimal256_b(), decimal256_a()])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": [decimal_json(12_345_678, SCALE)]}),
                json!({
                    "value": [decimal_json(-90_123_456, SCALE), decimal_json(12_345_678, SCALE)]
                }),
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
