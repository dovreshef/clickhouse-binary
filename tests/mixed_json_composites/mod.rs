use clickhouse_binary::{Row, RowBinaryFormat, Schema, TypeDesc, Value};

use crate::common::{ClickhouseServer, decode_rows, normalize_json_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

// Nested columns expand to separate columns on SELECT *; use explicit
// selection.
const MIXED_SELECT: &str = "SELECT id, payload, nested, arr_maps, map_arrs, pair FROM";

fn mixed_json_composite_schema() -> Schema {
    Schema::from_type_strings(&[
        ("id", "UInt64"),
        (
            "payload",
            "JSON(\
            a UInt32,\
            attrs Map(String, UInt16),\
            items Array(Tuple(UInt8, String)),\
            map_of_arrays Map(String, Array(UInt8)),\
            maps Array(Map(String, UInt8)),\
            pair Tuple(UInt8, String),\
            tags Array(String)\
            )",
        ),
        ("nested", "Nested(a UInt8, b String)"),
        ("arr_maps", "Array(Map(String, UInt8))"),
        ("map_arrs", "Map(String, Array(UInt16))"),
        ("pair", "Tuple(UInt8, Array(UInt16))"),
    ])
    .unwrap()
}

fn mixed_json_composite_rows() -> (Row, Row) {
    let first = vec![
        Value::UInt64(1),
        Value::JsonObject(vec![
            ("a".to_string(), Value::UInt32(7)),
            (
                "tags".to_string(),
                Value::Array(vec![
                    Value::String(b"red".to_vec()),
                    Value::String(b"blue".to_vec()),
                ]),
            ),
            (
                "attrs".to_string(),
                Value::Map(vec![(Value::String(b"x".to_vec()), Value::UInt16(1))]),
            ),
            (
                "pair".to_string(),
                Value::Tuple(vec![Value::UInt8(9), Value::String(b"ok".to_vec())]),
            ),
            (
                "items".to_string(),
                Value::Array(vec![
                    Value::Tuple(vec![Value::UInt8(1), Value::String(b"a".to_vec())]),
                    Value::Tuple(vec![Value::UInt8(2), Value::String(b"b".to_vec())]),
                ]),
            ),
            (
                "maps".to_string(),
                Value::Array(vec![
                    Value::Map(vec![(Value::String(b"k".to_vec()), Value::UInt8(1))]),
                    Value::Map(vec![(Value::String(b"z".to_vec()), Value::UInt8(2))]),
                ]),
            ),
            (
                "map_of_arrays".to_string(),
                Value::Map(vec![(
                    Value::String(b"m".to_vec()),
                    Value::Array(vec![Value::UInt8(1), Value::UInt8(2)]),
                )]),
            ),
            (
                "extra".to_string(),
                Value::Dynamic {
                    ty: Box::new(TypeDesc::String),
                    value: Box::new(Value::String(b"note".to_vec())),
                },
            ),
        ]),
        Value::Array(vec![
            Value::Tuple(vec![Value::UInt8(1), Value::String(b"n1".to_vec())]),
            Value::Tuple(vec![Value::UInt8(2), Value::String(b"n2".to_vec())]),
        ]),
        Value::Array(vec![Value::Map(vec![(
            Value::String(b"a".to_vec()),
            Value::UInt8(7),
        )])]),
        Value::Map(vec![(
            Value::String(b"left".to_vec()),
            Value::Array(vec![Value::UInt16(10), Value::UInt16(11)]),
        )]),
        Value::Tuple(vec![Value::UInt8(3), Value::Array(vec![Value::UInt16(4)])]),
    ];
    let second = vec![
        Value::UInt64(2),
        Value::JsonObject(vec![
            ("a".to_string(), Value::UInt32(42)),
            ("tags".to_string(), Value::Array(Vec::new())),
            (
                "attrs".to_string(),
                Value::Map(vec![(Value::String(b"y".to_vec()), Value::UInt16(2))]),
            ),
            (
                "pair".to_string(),
                Value::Tuple(vec![Value::UInt8(1), Value::String(b"yo".to_vec())]),
            ),
            ("items".to_string(), Value::Array(Vec::new())),
            ("maps".to_string(), Value::Array(Vec::new())),
            ("map_of_arrays".to_string(), Value::Map(Vec::new())),
        ]),
        Value::Array(Vec::new()),
        Value::Array(Vec::new()),
        Value::Map(Vec::new()),
        Value::Tuple(vec![Value::UInt8(0), Value::Array(Vec::new())]),
    ];
    (first, second)
}

fn create_mixed_json_composite_table(server: &ClickhouseServer, table: &str) {
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (\
         id UInt64,\
         payload JSON(a UInt32, attrs Map(String, UInt16), items Array(Tuple(UInt8, String)), map_of_arrays Map(String, Array(UInt8)), maps Array(Map(String, UInt8)), pair Tuple(UInt8, String), tags Array(String)),\
         nested Nested(a UInt8, b String),\
         arr_maps Array(Map(String, UInt8)),\
         map_arrs Map(String, Array(UInt16)),\
         pair Tuple(UInt8, Array(UInt16))\
         ) ENGINE=Memory"
    ));
}

#[test]
fn mixed_json_composite_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_mixed_json_composite_table(&server, &table);
    let schema = mixed_json_composite_schema();
    let (first, _) = mixed_json_composite_rows();
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
        let payload = server.fetch_rowbinary(&format!("{MIXED_SELECT} {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        let mut expected = vec![first.clone()];
        normalize_json_rows(&mut decoded, 1);
        normalize_json_rows(&mut expected, 1);
        assert_eq!(decoded, expected);
    }
}

#[test]
fn mixed_json_composite_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_mixed_json_composite_table(&server, &table);
    let schema = mixed_json_composite_schema();
    let (first, second) = mixed_json_composite_rows();
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
        let payload = server.fetch_rowbinary(&format!("{MIXED_SELECT} {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        let mut expected = vec![first.clone(), second.clone()];
        normalize_json_rows(&mut decoded, 1);
        normalize_json_rows(&mut expected, 1);
        assert_eq!(decoded, expected);
    }
}

#[test]
fn mixed_json_composite_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_mixed_json_composite_table(&server, &table);
    let schema = mixed_json_composite_schema();
    let (first, _) = mixed_json_composite_rows();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, std::slice::from_ref(&first));
        let payload = server.fetch_rowbinary(&format!("{MIXED_SELECT} {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        let mut expected = vec![first.clone()];
        normalize_json_rows(&mut decoded, 1);
        normalize_json_rows(&mut expected, 1);
        assert_eq!(decoded, expected);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn mixed_json_composite_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_mixed_json_composite_table(&server, &table);
    let schema = mixed_json_composite_schema();
    let (first, second) = mixed_json_composite_rows();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[first.clone(), second.clone()],
        );
        let payload = server.fetch_rowbinary(&format!("{MIXED_SELECT} {table}"), format);
        let mut decoded = decode_rows(&payload, format, &schema);
        let mut expected = vec![first.clone(), second.clone()];
        normalize_json_rows(&mut decoded, 1);
        normalize_json_rows(&mut expected, 1);
        assert_eq!(decoded, expected);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
