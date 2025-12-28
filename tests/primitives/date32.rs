use clickhouse_rowbinary::{RowBinaryFormat, Schema, Value};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn date32_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Date32) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ('1970-01-01')"));
    let schema = Schema::from_type_strings(&[("value", "Date32")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Date32(0)]]);
    }
}

#[test]
fn date32_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Date32) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('1970-01-01'),('1970-01-02')"
    ));
    let schema = Schema::from_type_strings(&[("value", "Date32")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Date32(0)], vec![Value::Date32(1)]]
        );
    }
}

#[test]
fn date32_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Date32) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Date32")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Date32(0)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "1970-01-01"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn date32_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Date32) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Date32")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Date32(0)], vec![Value::Date32(1)]],
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
}

#[test]
fn date32_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Date32)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Date32)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Nullable(None)]]);
    }
}

#[test]
fn date32_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Date32)) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL),('1970-01-01')"));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Date32)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::Date32(0))))],
            ]
        );
    }
}

#[test]
fn date32_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Date32)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Date32)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Nullable(None)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": null})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn date32_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Nullable(Date32)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Nullable(Date32)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Nullable(None)],
                vec![Value::Nullable(Some(Box::new(Value::Date32(0))))],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": null}), json!({"value": "1970-01-01"})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn date32_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Date32)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!("INSERT INTO {table} VALUES ('1970-01-01')"));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Date32)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::Date32(0)]]);
    }
}

#[test]
fn date32_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Date32)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!(
        "INSERT INTO {table} VALUES ('1970-01-01'),('1970-01-02'),('1970-01-01')"
    ));
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Date32)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Date32(0)],
                vec![Value::Date32(1)],
                vec![Value::Date32(0)],
            ]
        );
    }
}

#[test]
fn date32_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Date32)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Date32)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &[vec![Value::Date32(0)]]);
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": "1970-01-01"})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn date32_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value LowCardinality(Date32)) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "LowCardinality(Date32)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Date32(0)], vec![Value::Date32(1)]],
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
}

#[test]
fn date32_array_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Date32)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (['1970-01-01','1970-01-02'])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Date32)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![Value::Date32(0), Value::Date32(1)])]]
        );
    }
}

#[test]
fn date32_array_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Date32)) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES (['1970-01-01','1970-01-02']),([])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Date32)")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![Value::Date32(0), Value::Date32(1)])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn date32_array_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Date32)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Date32)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![Value::Date32(0), Value::Date32(1)])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": ["1970-01-01", "1970-01-02"]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn date32_array_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Date32)) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Date32)")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![Value::Date32(0), Value::Date32(1)])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": ["1970-01-01", "1970-01-02"]}),
                json!({"value": []})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn date32_array_nullable_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Date32))) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES ([NULL,'1970-01-01'])"));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Date32))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(Value::Date32(0)))),
            ])]]
        );
    }
}

#[test]
fn date32_array_nullable_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Date32))) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES ([NULL,'1970-01-01']),([])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Date32))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![
                    Value::Nullable(None),
                    Value::Nullable(Some(Box::new(Value::Date32(0)))),
                ])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn date32_array_nullable_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Date32))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Date32))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![
                Value::Nullable(None),
                Value::Nullable(Some(Box::new(Value::Date32(0)))),
            ])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": [null, "1970-01-01"]})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn date32_array_nullable_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Array(Nullable(Date32))) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(Nullable(Date32))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![
                    Value::Nullable(None),
                    Value::Nullable(Some(Box::new(Value::Date32(0)))),
                ])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": [null, "1970-01-01" ]}),
                json!({"value": []})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn date32_array_low_cardinality_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Date32))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!(
        "INSERT INTO {table} VALUES (['1970-01-01','1970-01-02'])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(Date32))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Array(vec![Value::Date32(0), Value::Date32(1)])]]
        );
    }
}

#[test]
fn date32_array_low_cardinality_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Date32))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    server.exec(&format!(
        "INSERT INTO {table} VALUES (['1970-01-01','1970-01-02']),([])"
    ));
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(Date32))")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Array(vec![Value::Date32(0), Value::Date32(1)])],
                vec![Value::Array(Vec::new())],
            ]
        );
    }
}

#[test]
fn date32_array_low_cardinality_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Date32))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(Date32))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Array(vec![Value::Date32(0), Value::Date32(1)])]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![json!({"value": ["1970-01-01", "1970-01-02"]})]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn date32_array_low_cardinality_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec_with_settings(
        &format!("CREATE TABLE {table} (value Array(LowCardinality(Date32))) ENGINE=Memory"),
        "allow_suspicious_low_cardinality_types=1",
    );
    let schema = Schema::from_type_strings(&[("value", "Array(LowCardinality(Date32))")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Array(vec![Value::Date32(0), Value::Date32(1)])],
                vec![Value::Array(Vec::new())],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": ["1970-01-01", "1970-01-02"]}),
                json!({"value": []})
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
