use clickhouse_rowbinary::{Row, RowBinaryFormat, Schema, Value};

use crate::common::{ClickhouseServer, decode_rows, normalize_json_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

// ClickHouse normalizes typed JSON paths in the RowBinaryWithNamesAndTypes
// header. Keep them in the server's canonical order to avoid header mismatches.
fn json_stress_schema() -> Schema {
    Schema::from_type_strings(&[(
        "value",
        "JSON(\
        a UInt32,\
        attrs Map(String, UInt16),\
        items Array(Tuple(UInt8, String)),\
        map_of_arrays Map(String, Array(UInt8)),\
        maps Array(Map(String, UInt8)),\
        pair Tuple(UInt8, String),\
        tags Array(String)\
        )",
    )])
    .unwrap()
}

fn json_stress_rows() -> (Row, Row) {
    let first = vec![Value::JsonObject(vec![
        (
            "tags".to_string(),
            Value::Array(vec![
                Value::String(b"red".to_vec()),
                Value::String(b"blue".to_vec()),
            ]),
        ),
        ("a".to_string(), Value::UInt32(7)),
        (
            "pair".to_string(),
            Value::Tuple(vec![Value::UInt8(9), Value::String(b"ok".to_vec())]),
        ),
        (
            "attrs".to_string(),
            Value::Map(vec![(Value::String(b"x".to_vec()), Value::UInt16(1))]),
        ),
        (
            "items".to_string(),
            Value::Array(vec![Value::Tuple(vec![
                Value::UInt8(1),
                Value::String(b"alpha".to_vec()),
            ])]),
        ),
        (
            "maps".to_string(),
            Value::Array(vec![Value::Map(vec![(
                Value::String(b"k".to_vec()),
                Value::UInt8(3),
            )])]),
        ),
        (
            "map_of_arrays".to_string(),
            Value::Map(vec![(
                Value::String(b"m".to_vec()),
                Value::Array(vec![Value::UInt8(1), Value::UInt8(2)]),
            )]),
        ),
        // Untyped JSON paths are encoded using Dynamic.
        (
            "note".to_string(),
            Value::Dynamic {
                ty: Box::new(clickhouse_rowbinary::TypeDesc::String),
                value: Box::new(Value::String(b"hi".to_vec())),
            },
        ),
    ])];
    let second = vec![Value::JsonObject(vec![
        ("a".to_string(), Value::UInt32(42)),
        ("tags".to_string(), Value::Array(Vec::new())),
        ("attrs".to_string(), Value::Map(Vec::new())),
        ("items".to_string(), Value::Array(Vec::new())),
        ("maps".to_string(), Value::Array(Vec::new())),
        ("map_of_arrays".to_string(), Value::Map(Vec::new())),
        (
            "pair".to_string(),
            Value::Tuple(vec![Value::UInt8(1), Value::String(b"yo".to_vec())]),
        ),
    ])];
    (first, second)
}

fn create_json_stress_table(server: &ClickhouseServer, table: &str) {
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (\
         value JSON(a UInt32, attrs Map(String, UInt16), items Array(Tuple(UInt8, String)), map_of_arrays Map(String, Array(UInt8)), maps Array(Map(String, UInt8)), pair Tuple(UInt8, String), tags Array(String))\
         ) ENGINE=Memory"
    ));
}

#[test]
fn json_stress_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_json_stress_table(&server, &table);
    let schema = json_stress_schema();
    let (first, _) = json_stress_rows();
    let insert_sql = format!(
        "INSERT INTO {table} FORMAT {}",
        RowBinaryFormat::RowBinaryWithNamesAndTypes
    );
    server.insert_rowbinary(
        &insert_sql,
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        &schema,
        std::slice::from_ref(&first),
    );

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        let mut expected = vec![first.clone()];
        // ClickHouse may reorder JSON paths in output.
        normalize_json_rows(&mut decoded, 0);
        normalize_json_rows(&mut expected, 0);
        assert_eq!(decoded, expected);
    }
}

#[test]
fn json_stress_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_json_stress_table(&server, &table);
    let schema = json_stress_schema();
    let (first, second) = json_stress_rows();
    let insert_sql = format!(
        "INSERT INTO {table} FORMAT {}",
        RowBinaryFormat::RowBinaryWithNamesAndTypes
    );
    server.insert_rowbinary(
        &insert_sql,
        RowBinaryFormat::RowBinaryWithNamesAndTypes,
        &schema,
        &[first.clone(), second.clone()],
    );

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        let mut expected = vec![first.clone(), second.clone()];
        normalize_json_rows(&mut decoded, 0);
        normalize_json_rows(&mut expected, 0);
        assert_eq!(decoded, expected);
    }
}

#[test]
fn json_stress_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_json_stress_table(&server, &table);
    let schema = json_stress_schema();
    let (first, _) = json_stress_rows();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, std::slice::from_ref(&first));
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        let mut expected = vec![first.clone()];
        normalize_json_rows(&mut decoded, 0);
        normalize_json_rows(&mut expected, 0);
        assert_eq!(decoded, expected);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn json_stress_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_json_stress_table(&server, &table);
    let schema = json_stress_schema();
    let (first, second) = json_stress_rows();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[first.clone(), second.clone()],
        );
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        let mut expected = vec![first.clone(), second.clone()];
        normalize_json_rows(&mut decoded, 0);
        normalize_json_rows(&mut expected, 0);
        assert_eq!(decoded, expected);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
