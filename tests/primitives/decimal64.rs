use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decimal_json, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

const SCALE: u32 = 3;

fn decimal64_a() -> Value {
    Value::Decimal64(12345)
}

fn decimal64_b() -> Value {
    Value::Decimal64(-567_890)
}

#[test]
fn decimal64_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Decimal64(3)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (12.345)"));
    let schema = Schema::from_type_strings(&[("value", "Decimal64(3)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![decimal64_a()]]);
    }
}

#[test]
fn decimal64_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Decimal64(3)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (12.345),(-567.890)"));
    let schema = Schema::from_type_strings(&[("value", "Decimal64(3)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![decimal64_a()], vec![decimal64_b()]]);
    }
}

#[test]
fn decimal64_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Decimal64(3)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Decimal64(3)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![decimal64_a()]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": decimal_json(12_345, SCALE)})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal64_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Decimal64(3)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Decimal64(3)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![decimal64_a()], vec![decimal64_b()]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": decimal_json(12_345, SCALE)}),
                json!({"value": decimal_json(-567_890, SCALE)}),
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal64_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Decimal64(3))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }
}

#[test]
fn decimal64_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Decimal64(3))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL),(12.345)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(decimal64_a())))],
            ]
        );
    }
}

#[test]
fn decimal64_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Decimal64(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Nullable(None)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": null})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal64_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Decimal64(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(decimal64_a())))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": null}),
                json!({"value": decimal_json(12_345, SCALE)})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal64_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Decimal64(3))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES (12.345)"));
    if Schema::from_type_strings(&[("value", "LowCardinality(Decimal64(3))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![decimal64_a()]]);
    }
}

#[test]
fn decimal64_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Decimal64(3))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES (12.345),(-567.890),(12.345)"
    ));
    if Schema::from_type_strings(&[("value", "LowCardinality(Decimal64(3))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![decimal64_a()],
                vec![decimal64_b()],
                vec![decimal64_a()]
            ]
        );
    }
}

#[test]
fn decimal64_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Decimal64(3))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    if Schema::from_type_strings(&[("value", "LowCardinality(Decimal64(3))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![decimal64_a()]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": decimal_json(12_345, SCALE)})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal64_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Decimal64(3))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    if Schema::from_type_strings(&[("value", "LowCardinality(Decimal64(3))")]).is_err() {
        return;
    }
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![decimal64_a()], vec![decimal64_b()]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": decimal_json(12_345, SCALE)}),
                json!({"value": decimal_json(-567_890, SCALE)}),
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal64_array_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Decimal64(3))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([12.345, -567.890])"));
    let schema = Schema::from_type_strings(&[("value", "Array(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![decimal64_a(), decimal64_b()])]]
        );
    }
}

#[test]
fn decimal64_array_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Decimal64(3))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([12.345]),([-567.890, 12.345])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![decimal64_a()])],
                vec![Value::Array(vec![decimal64_b(), decimal64_a()])],
            ]
        );
    }
}

#[test]
fn decimal64_array_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Decimal64(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![decimal64_a(), decimal64_b()])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({
                "value": [decimal_json(12_345, SCALE), decimal_json(-567_890, SCALE)]
            })]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal64_array_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Decimal64(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Decimal64(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![decimal64_a()])],
                vec![Value::Array(vec![decimal64_b(), decimal64_a()])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": [decimal_json(12_345, SCALE)]}),
                json!({
                    "value": [decimal_json(-567_890, SCALE), decimal_json(12_345, SCALE)]
                }),
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal64_array_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Decimal64(3)))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([NULL, 12.345])"));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Decimal64(3)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(decimal64_a()))),
            ])]]
        );
    }
}

#[test]
fn decimal64_array_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Decimal64(3)))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([NULL]),([12.345, NULL])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Decimal64(3)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(vec![
                    Value::Nullable(Some(Box::new(decimal64_a()))),
                    Value::Nullable(None),
                ])],
            ]
        );
    }
}

#[test]
fn decimal64_array_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Decimal64(3)))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Decimal64(3)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(decimal64_a()))),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [null, decimal_json(12_345, SCALE)]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal64_array_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Decimal64(3)))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Decimal64(3)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(vec![
                    Value::Nullable(Some(Box::new(decimal64_a()))),
                    Value::Nullable(None),
                ])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": [null]}),
                json!({"value": [decimal_json(12_345, SCALE), null]}),
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal64_array_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Decimal64(3)))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES ([12.345])"));
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(Decimal64(3)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Array(vec![decimal64_a()])]]);
    }
}

#[test]
fn decimal64_array_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Decimal64(3)))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([12.345]),([-567.890, 12.345])"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(Decimal64(3)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![decimal64_a()])],
                vec![Value::Array(vec![decimal64_b(), decimal64_a()])],
            ]
        );
    }
}

#[test]
fn decimal64_array_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Decimal64(3)))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(Decimal64(3)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![decimal64_a()])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [decimal_json(12_345, SCALE)]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn decimal64_array_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Decimal64(3)))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(Decimal64(3)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![decimal64_a()])],
                vec![Value::Array(vec![decimal64_b(), decimal64_a()])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": [decimal_json(12_345, SCALE)]}),
                json!({
                    "value": [decimal_json(-567_890, SCALE), decimal_json(12_345, SCALE)]
                }),
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
