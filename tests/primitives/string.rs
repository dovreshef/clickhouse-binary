use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn string_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value String) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('alpha')"));
    let schema = Schema::from_type_strings(&[("value", "String")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::String(b"alpha".to_vec())]]);
    }
}

#[test]
fn string_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value String) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('alpha'),('beta')"));
    let schema = Schema::from_type_strings(&[("value", "String")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::String(b"alpha".to_vec())],
                vec![Value::String(b"beta".to_vec())]
            ]
        );
    }
}

#[test]
fn string_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value String) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "String")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::String(b"alpha".to_vec())]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "alpha"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn string_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value String) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "String")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::String(b"alpha".to_vec())],
                vec![Value::String(b"beta".to_vec())],
            ],
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
fn string_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(String)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }
}

#[test]
fn string_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(String)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL),('alpha')"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::String(
                    b"alpha".to_vec()
                ))))],
            ]
        );
    }
}

#[test]
fn string_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Nullable(None)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": null})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn string_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::String(
                    b"alpha".to_vec(),
                ))))],
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
fn string_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(String)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('alpha')"));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::String(b"alpha".to_vec())]]);
    }
}

#[test]
fn string_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(String)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('alpha'),('beta'),('alpha')"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::String(b"alpha".to_vec())],
                vec![Value::String(b"beta".to_vec())],
                vec![Value::String(b"alpha".to_vec())],
            ]
        );
    }
}

#[test]
fn string_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::String(b"alpha".to_vec())]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "alpha"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn string_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::String(b"alpha".to_vec())],
                vec![Value::String(b"beta".to_vec())],
            ],
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
fn string_array_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(String)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (['alpha','beta'])"));
    let schema = Schema::from_type_strings(&[("value", "Array(String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::String(b"alpha".to_vec()),
                Value::String(b"beta".to_vec()),
            ])]]
        );
    }
}

#[test]
fn string_array_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(String)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (['alpha','beta']),([])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(String)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![
                    Value::String(b"alpha".to_vec()),
                    Value::String(b"beta".to_vec()),
                ])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn string_array_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::String(b"alpha".to_vec()),
                Value::String(b"beta".to_vec()),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": ["alpha", "beta"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn string_array_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(String)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(String)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![
                    Value::String(b"alpha".to_vec()),
                    Value::String(b"beta".to_vec()),
                ])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": ["alpha", "beta"]}), json!({"value": []})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn string_codec_zstd_reading_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value String CODEC(ZSTD)) ENGINE=Memory"
    )) {
        return;
    }
    // CODEC is storage-only; RowBinary headers still use plain `String`.
    let schema = Schema::from_type_strings(&[("value", "String")]).unwrap();

    for format in FORMATS {
        server.exec(&format!("INSERT INTO {table} VALUES ('alpha')"));
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::String(b"alpha".to_vec())]]);
        server.exec(&format!("TRUNCATE TABLE {table}"));

        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::String(b"alpha".to_vec())]],
        );
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::String(b"alpha".to_vec())]]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn string_low_cardinality_codec_zstd_reading_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(String) CODEC(ZSTD)) ENGINE=Memory"
    )) {
        return;
    }
    // CODEC does not change RowBinary types for LowCardinality(String).
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(String)")]).unwrap();

    for format in FORMATS {
        server.exec(&format!("INSERT INTO {table} VALUES ('alpha')"));
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::String(b"alpha".to_vec())]]);
        server.exec(&format!("TRUNCATE TABLE {table}"));

        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::String(b"alpha".to_vec())]],
        );
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::String(b"alpha".to_vec())]]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn string_array_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(String))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([NULL,'alpha'])"));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(String))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(Value::String(b"alpha".to_vec())))),
            ])]]
        );
    }
}

#[test]
fn string_array_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(String))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([NULL,'alpha']),([])"));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(String))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![
                    Value::Nullable(None),
                    Value::Nullable(Some(Box::new(Value::String(b"alpha".to_vec())))),
                ])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn string_array_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(String))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(String))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(Value::String(b"alpha".to_vec())))),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [null, "alpha"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn string_array_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(String))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(String))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![
                    Value::Nullable(None),
                    Value::Nullable(Some(Box::new(Value::String(b"alpha".to_vec())))),
                ])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [null, "alpha" ]}), json!({"value": []})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn string_array_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(String))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!("INSERT INTO {table} VALUES (['alpha','beta'])"));
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(String))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::String(b"alpha".to_vec()),
                Value::String(b"beta".to_vec())
            ])]]
        );
    }
}

#[test]
fn string_array_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(String))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!(
        "INSERT INTO {table} VALUES (['alpha','beta']),([])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(String))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![
                    Value::String(b"alpha".to_vec()),
                    Value::String(b"beta".to_vec())
                ])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn string_array_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(String))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(String))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::String(b"alpha".to_vec()),
                Value::String(b"beta".to_vec()),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": ["alpha", "beta"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn string_array_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(String))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(String))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![
                    Value::String(b"alpha".to_vec()),
                    Value::String(b"beta".to_vec()),
                ])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": ["alpha", "beta"]}), json!({"value": []})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
