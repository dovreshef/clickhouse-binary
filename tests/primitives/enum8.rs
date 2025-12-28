use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

fn enum8_a() -> Value {
    Value::Enum8(1)
}

fn enum8_b() -> Value {
    Value::Enum8(2)
}

#[test]
fn enum8_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Enum8('alpha' = 1, 'beta' = 2)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('alpha')"));
    let schema = Schema::from_type_strings(&[("value", "Enum8('alpha' = 1, 'beta' = 2)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![enum8_a()]]);
    }
}

#[test]
fn enum8_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Enum8('alpha' = 1, 'beta' = 2)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('alpha'),('beta')"));
    let schema = Schema::from_type_strings(&[("value", "Enum8('alpha' = 1, 'beta' = 2)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![enum8_a()], vec![enum8_b()]]);
    }
}

#[test]
fn enum8_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Enum8('alpha' = 1, 'beta' = 2)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Enum8('alpha' = 1, 'beta' = 2)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![enum8_a()]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "alpha"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn enum8_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Enum8('alpha' = 1, 'beta' = 2)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Enum8('alpha' = 1, 'beta' = 2)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![enum8_a()], vec![enum8_b()]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": "alpha"}), json!({"value": "beta"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn enum8_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema =
        Schema::from_type_strings(&[("value", "Nullable(Enum8('alpha' = 1, 'beta' = 2))")])
            .unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }
}

#[test]
fn enum8_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL),('alpha')"));
    let schema =
        Schema::from_type_strings(&[("value", "Nullable(Enum8('alpha' = 1, 'beta' = 2))")])
            .unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(enum8_a())))],
            ]
        );
    }
}

#[test]
fn enum8_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Nullable(Enum8('alpha' = 1, 'beta' = 2))")])
            .unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Nullable(None)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": null})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn enum8_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Nullable(Enum8('alpha' = 1, 'beta' = 2))")])
            .unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(enum8_a())))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": null}), json!({"value": "alpha"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn enum8_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value LowCardinality(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES ('alpha')"));
    if Schema::from_type_strings(&[("value", "LowCardinality(Enum8('alpha' = 1, 'beta' = 2))")])
        .is_err()
    {
        return;
    }
    let schema =
        Schema::from_type_strings(&[("value", "LowCardinality(Enum8('alpha' = 1, 'beta' = 2))")])
            .unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![enum8_a()]]);
    }
}

#[test]
fn enum8_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value LowCardinality(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('alpha'),('beta'),('alpha')"
    ));
    if Schema::from_type_strings(&[("value", "LowCardinality(Enum8('alpha' = 1, 'beta' = 2))")])
        .is_err()
    {
        return;
    }
    let schema =
        Schema::from_type_strings(&[("value", "LowCardinality(Enum8('alpha' = 1, 'beta' = 2))")])
            .unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![enum8_a()], vec![enum8_b()], vec![enum8_a()]]
        );
    }
}

#[test]
fn enum8_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value LowCardinality(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    if Schema::from_type_strings(&[("value", "LowCardinality(Enum8('alpha' = 1, 'beta' = 2))")])
        .is_err()
    {
        return;
    }
    let schema =
        Schema::from_type_strings(&[("value", "LowCardinality(Enum8('alpha' = 1, 'beta' = 2))")])
            .unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![enum8_a()]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "alpha"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn enum8_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value LowCardinality(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    if Schema::from_type_strings(&[("value", "LowCardinality(Enum8('alpha' = 1, 'beta' = 2))")])
        .is_err()
    {
        return;
    }
    let schema =
        Schema::from_type_strings(&[("value", "LowCardinality(Enum8('alpha' = 1, 'beta' = 2))")])
            .unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![enum8_a()], vec![enum8_b()]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": "alpha"}), json!({"value": "beta"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn enum8_array_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (['alpha', 'beta'])"));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Enum8('alpha' = 1, 'beta' = 2))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![enum8_a(), enum8_b()])]]
        );
    }
}

#[test]
fn enum8_array_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (['alpha']),(['beta', 'alpha'])"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Enum8('alpha' = 1, 'beta' = 2))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![enum8_a()])],
                vec![Value::Array(vec![enum8_b(), enum8_a()])],
            ]
        );
    }
}

#[test]
fn enum8_array_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Enum8('alpha' = 1, 'beta' = 2))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![enum8_a(), enum8_b()])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": ["alpha", "beta"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn enum8_array_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Enum8('alpha' = 1, 'beta' = 2))) ENGINE=Memory"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Enum8('alpha' = 1, 'beta' = 2))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![enum8_a()])],
                vec![Value::Array(vec![enum8_b(), enum8_a()])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": ["alpha"]}),
                json!({"value": ["beta", "alpha"]})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn enum8_array_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Enum8('alpha' = 1, 'beta' = 2)))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([NULL, 'alpha'])"));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Nullable(Enum8('alpha' = 1, 'beta' = 2)))")])
            .unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(enum8_a()))),
            ])]]
        );
    }
}

#[test]
fn enum8_array_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Enum8('alpha' = 1, 'beta' = 2)))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([NULL]),(['alpha', NULL])"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Nullable(Enum8('alpha' = 1, 'beta' = 2)))")])
            .unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(vec![
                    Value::Nullable(Some(Box::new(enum8_a()))),
                    Value::Nullable(None),
                ])],
            ]
        );
    }
}

#[test]
fn enum8_array_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Enum8('alpha' = 1, 'beta' = 2)))) ENGINE=Memory"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Nullable(Enum8('alpha' = 1, 'beta' = 2)))")])
            .unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(enum8_a()))),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [null, "alpha"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn enum8_array_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Enum8('alpha' = 1, 'beta' = 2)))) ENGINE=Memory"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Nullable(Enum8('alpha' = 1, 'beta' = 2)))")])
            .unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![Value::Nullable(None)])],
                vec![Value::Array(vec![
                    Value::Nullable(Some(Box::new(enum8_a()))),
                    Value::Nullable(None),
                ])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [null]}), json!({"value": ["alpha", null]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn enum8_array_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value Array(LowCardinality(Enum8('alpha' = 1, 'beta' = 2)))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!("INSERT INTO {table} VALUES (['alpha'])"));
    let Ok(schema) = Schema::from_type_strings(&[(
        "value",
        "Array(LowCardinality(Enum8('alpha' = 1, 'beta' = 2)))",
    )]) else {
        return;
    };

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Array(vec![enum8_a()])]]);
    }
}

#[test]
fn enum8_array_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value Array(LowCardinality(Enum8('alpha' = 1, 'beta' = 2)))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    server.exec(&format!(
        "INSERT INTO {table} VALUES (['alpha']),(['beta', 'alpha'])"
    ));
    let Ok(schema) = Schema::from_type_strings(&[(
        "value",
        "Array(LowCardinality(Enum8('alpha' = 1, 'beta' = 2)))",
    )]) else {
        return;
    };

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![enum8_a()])],
                vec![Value::Array(vec![enum8_b(), enum8_a()])],
            ]
        );
    }
}

#[test]
fn enum8_array_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value Array(LowCardinality(Enum8('alpha' = 1, 'beta' = 2)))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    let Ok(schema) = Schema::from_type_strings(&[(
        "value",
        "Array(LowCardinality(Enum8('alpha' = 1, 'beta' = 2)))",
    )]) else {
        return;
    };

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![enum8_a()])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": ["alpha"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn enum8_array_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value Array(LowCardinality(Enum8('alpha' = 1, 'beta' = 2)))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    ) {
        return;
    }
    let Ok(schema) = Schema::from_type_strings(&[(
        "value",
        "Array(LowCardinality(Enum8('alpha' = 1, 'beta' = 2)))",
    )]) else {
        return;
    };

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![enum8_a()])],
                vec![Value::Array(vec![enum8_b(), enum8_a()])],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": ["alpha"]}),
                json!({"value": ["beta", "alpha"]})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
