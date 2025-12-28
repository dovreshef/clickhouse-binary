use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn fixed_string_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value FixedString(3)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('abc')"));
    let schema = Schema::from_type_strings(&[("value", "FixedString(3)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::FixedString(b"abc".to_vec())]]);
    }
}

#[test]
fn fixed_string_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value FixedString(3)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('abc'),('xyz')"));
    let schema = Schema::from_type_strings(&[("value", "FixedString(3)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::FixedString(b"abc".to_vec())],
                vec![Value::FixedString(b"xyz".to_vec())]
            ]
        );
    }
}

#[test]
fn fixed_string_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value FixedString(3)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "FixedString(3)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::FixedString(b"abc".to_vec())]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "abc"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value FixedString(3)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "FixedString(3)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::FixedString(b"abc".to_vec())],
                vec![Value::FixedString(b"xyz".to_vec())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": "abc"}), json!({"value": "xyz"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_codec_zstd_reading_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    if !server.try_exec(&format!(
        "CREATE TABLE {table} (value FixedString(3) CODEC(ZSTD)) ENGINE=Memory"
    )) {
        return;
    }
    // CODEC is storage-only; RowBinary headers keep `FixedString(3)`.
    let schema = Schema::from_type_strings(&[("value", "FixedString(3)")]).unwrap();

    for format in FORMATS {
        server.exec(&format!("INSERT INTO {table} VALUES ('abc')"));
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::FixedString(b"abc".to_vec())]]);
        server.exec(&format!("TRUNCATE TABLE {table}"));

        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::FixedString(b"abc".to_vec())]],
        );
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::FixedString(b"abc".to_vec())]]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(FixedString(3))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }
}

#[test]
fn fixed_string_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(FixedString(3))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL),('abc')"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::FixedString(
                    b"abc".to_vec()
                ))))],
            ]
        );
    }
}

#[test]
fn fixed_string_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(FixedString(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Nullable(None)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": null})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(FixedString(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::FixedString(
                    b"abc".to_vec(),
                ))))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": null}), json!({"value": "abc"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(FixedString(3))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('abc')"));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::FixedString(b"abc".to_vec())]]);
    }
}

#[test]
fn fixed_string_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(FixedString(3))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('abc'),('xyz'),('abc')"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::FixedString(b"abc".to_vec())],
                vec![Value::FixedString(b"xyz".to_vec())],
                vec![Value::FixedString(b"abc".to_vec())],
            ]
        );
    }
}

#[test]
fn fixed_string_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(FixedString(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::FixedString(b"abc".to_vec())]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "abc"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value LowCardinality(FixedString(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::FixedString(b"abc".to_vec())],
                vec![Value::FixedString(b"xyz".to_vec())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": "abc"}), json!({"value": "xyz"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_array_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(FixedString(3))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (['abc','xyz'])"));
    let schema = Schema::from_type_strings(&[("value", "Array(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::FixedString(b"abc".to_vec()),
                Value::FixedString(b"xyz".to_vec()),
            ])]]
        );
    }
}

#[test]
fn fixed_string_array_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(FixedString(3))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (['abc','xyz']),([])"));
    let schema = Schema::from_type_strings(&[("value", "Array(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![
                    Value::FixedString(b"abc".to_vec()),
                    Value::FixedString(b"xyz".to_vec()),
                ])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn fixed_string_array_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(FixedString(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::FixedString(b"abc".to_vec()),
                Value::FixedString(b"xyz".to_vec()),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": ["abc", "xyz"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_array_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(FixedString(3))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(FixedString(3))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![
                    Value::FixedString(b"abc".to_vec()),
                    Value::FixedString(b"xyz".to_vec()),
                ])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": ["abc", "xyz"]}), json!({"value": []})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_array_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(FixedString(3)))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([NULL,'abc'])"));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Nullable(FixedString(3)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(Value::FixedString(b"abc".to_vec())))),
            ])]]
        );
    }
}

#[test]
fn fixed_string_array_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(FixedString(3)))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([NULL,'abc']),([])"));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Nullable(FixedString(3)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![
                    Value::Nullable(None),
                    Value::Nullable(Some(Box::new(Value::FixedString(b"abc".to_vec())))),
                ])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn fixed_string_array_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(FixedString(3)))) ENGINE=Memory"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Nullable(FixedString(3)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(Value::FixedString(b"abc".to_vec())))),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [null, "abc"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_array_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(FixedString(3)))) ENGINE=Memory"
    ));
    let schema =
        Schema::from_type_strings(&[("value", "Array(Nullable(FixedString(3)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![
                    Value::Nullable(None),
                    Value::Nullable(Some(Box::new(Value::FixedString(b"abc".to_vec())))),
                ])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": [null, "abc" ]}), json!({"value": []})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_array_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value Array(LowCardinality(FixedString(3)))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!("INSERT INTO {table} VALUES (['abc','xyz'])"));
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(FixedString(3)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::FixedString(b"abc".to_vec()),
                Value::FixedString(b"xyz".to_vec())
            ])]]
        );
    }
}

#[test]
fn fixed_string_array_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value Array(LowCardinality(FixedString(3)))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!("INSERT INTO {table} VALUES (['abc','xyz']),([])"));
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(FixedString(3)))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![
                    Value::FixedString(b"abc".to_vec()),
                    Value::FixedString(b"xyz".to_vec())
                ])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn fixed_string_array_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value Array(LowCardinality(FixedString(3)))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(FixedString(3)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::FixedString(b"abc".to_vec()),
                Value::FixedString(b"xyz".to_vec()),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": ["abc", "xyz"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn fixed_string_array_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!(
            "CREATE TABLE {table} (value Array(LowCardinality(FixedString(3)))) ENGINE=Memory"
        ),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema =
        Schema::from_type_strings(&[("value", "Array(LowCardinality(FixedString(3)))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![
                    Value::FixedString(b"abc".to_vec()),
                    Value::FixedString(b"xyz".to_vec()),
                ])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": ["abc", "xyz"]}), json!({"value": []})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
