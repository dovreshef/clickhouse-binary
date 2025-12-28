use clickhouse_binary::{
    Error, RowBinaryFormat, RowBinaryReader, Schema, TypeDesc, Value, types::TupleItem,
};
use serde_json::json;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

#[test]
fn dynamic_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Dynamic) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (CAST(7 AS UInt8))"));
    let schema = Schema::from_type_strings(&[("value", "Dynamic")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![vec![Value::Dynamic {
                ty: TypeDesc::UInt8,
                value: Box::new(Value::UInt8(7)),
            }]]
        );
    }
}

#[test]
fn dynamic_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Dynamic) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES \
        (CAST(7 AS UInt8)),\
        (CAST('alpha' AS String)),\
        (CAST([1,2,3] AS Array(UInt8)))"
    ));
    let schema = Schema::from_type_strings(&[("value", "Dynamic")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(
            decoded,
            vec![
                vec![Value::Dynamic {
                    ty: TypeDesc::UInt8,
                    value: Box::new(Value::UInt8(7)),
                }],
                vec![Value::Dynamic {
                    ty: TypeDesc::String,
                    value: Box::new(Value::String(b"alpha".to_vec())),
                }],
                vec![Value::Dynamic {
                    ty: TypeDesc::Array(Box::new(TypeDesc::UInt8)),
                    value: Box::new(Value::Array(vec![
                        Value::UInt8(1),
                        Value::UInt8(2),
                        Value::UInt8(3),
                    ])),
                }],
            ]
        );
    }
}

#[test]
fn dynamic_null_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Dynamic) ENGINE=Memory"
    ));
    server.exec(&format!("INSERT INTO {table} VALUES (NULL)"));
    let schema = Schema::from_type_strings(&[("value", "Dynamic")]).unwrap();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![vec![Value::DynamicNull]]);
    }
}

#[test]
fn dynamic_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Dynamic) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Dynamic")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[vec![Value::Dynamic {
                ty: TypeDesc::UInt8,
                value: Box::new(Value::UInt8(7)),
            }]],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(json_rows, vec![json!({"value": 7})]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn dynamic_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Dynamic) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Dynamic")]).unwrap();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[
                vec![Value::Dynamic {
                    ty: TypeDesc::UInt8,
                    value: Box::new(Value::UInt8(7)),
                }],
                vec![Value::Dynamic {
                    ty: TypeDesc::String,
                    value: Box::new(Value::String(b"alpha".to_vec())),
                }],
                vec![Value::DynamicNull],
                vec![Value::Dynamic {
                    ty: TypeDesc::Array(Box::new(TypeDesc::UInt8)),
                    value: Box::new(Value::Array(vec![Value::UInt8(1), Value::UInt8(2)])),
                }],
            ],
        );
        let json_rows = server.fetch_json(&format!("SELECT value FROM {table}"));
        assert_eq!(
            json_rows,
            vec![
                json!({"value": 7}),
                json!({"value": "alpha"}),
                json!({"value": null}),
                json!({"value": [1, 2]}),
            ]
        );
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn dynamic_rejects_unsupported_type_encoding() {
    let schema = Schema::from_type_strings(&[("value", "Dynamic")]).unwrap();
    let payload = [0x30_u8];
    let mut reader = RowBinaryReader::with_schema(&payload[..], RowBinaryFormat::RowBinary, schema);
    let err = reader.read_row().unwrap_err();
    assert!(matches!(err, Error::UnsupportedType(_)));
}

#[test]
fn dynamic_composite_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Dynamic) ENGINE=Memory"
    ));
    server.exec(&format!(
        "INSERT INTO {table} VALUES \
        (CAST((7, 'alpha') AS Tuple(UInt8, String))),\
        (CAST(map('a',1,'b',2) AS Map(String, UInt8))),\
        (CAST([(1,'x'),(2,'y')] AS Nested(a UInt8, b String)))"
    ));
    let schema = Schema::from_type_strings(&[("value", "Dynamic")]).unwrap();
    let expected = vec![
        vec![Value::Dynamic {
            ty: TypeDesc::Tuple(vec![
                TupleItem {
                    name: None,
                    ty: TypeDesc::UInt8,
                },
                TupleItem {
                    name: None,
                    ty: TypeDesc::String,
                },
            ]),
            value: Box::new(Value::Tuple(vec![
                Value::UInt8(7),
                Value::String(b"alpha".to_vec()),
            ])),
        }],
        vec![Value::Dynamic {
            ty: TypeDesc::Map {
                key: Box::new(TypeDesc::String),
                value: Box::new(TypeDesc::UInt8),
            },
            value: Box::new(Value::Map(vec![
                (Value::String(b"a".to_vec()), Value::UInt8(1)),
                (Value::String(b"b".to_vec()), Value::UInt8(2)),
            ])),
        }],
        vec![Value::Dynamic {
            ty: TypeDesc::Array(Box::new(TypeDesc::Tuple(vec![
                TupleItem {
                    name: None,
                    ty: TypeDesc::UInt8,
                },
                TupleItem {
                    name: None,
                    ty: TypeDesc::String,
                },
            ]))),
            value: Box::new(Value::Array(vec![
                Value::Tuple(vec![Value::UInt8(1), Value::String(b"x".to_vec())]),
                Value::Tuple(vec![Value::UInt8(2), Value::String(b"y".to_vec())]),
            ])),
        }],
    ];

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, expected);
    }
}

#[test]
fn dynamic_composite_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (value Dynamic) ENGINE=Memory"
    ));
    let schema = Schema::from_type_strings(&[("value", "Dynamic")]).unwrap();
    let rows = vec![
        vec![Value::Dynamic {
            ty: TypeDesc::Tuple(vec![
                TupleItem {
                    name: None,
                    ty: TypeDesc::UInt8,
                },
                TupleItem {
                    name: None,
                    ty: TypeDesc::String,
                },
            ]),
            value: Box::new(Value::Tuple(vec![
                Value::UInt8(7),
                Value::String(b"alpha".to_vec()),
            ])),
        }],
        vec![Value::Dynamic {
            ty: TypeDesc::Map {
                key: Box::new(TypeDesc::String),
                value: Box::new(TypeDesc::UInt8),
            },
            value: Box::new(Value::Map(vec![
                (Value::String(b"a".to_vec()), Value::UInt8(1)),
                (Value::String(b"b".to_vec()), Value::UInt8(2)),
            ])),
        }],
        vec![Value::Dynamic {
            ty: TypeDesc::Nested(vec![
                TupleItem {
                    name: Some("a".to_string()),
                    ty: TypeDesc::UInt8,
                },
                TupleItem {
                    name: Some("b".to_string()),
                    ty: TypeDesc::String,
                },
            ]),
            value: Box::new(Value::Array(vec![
                Value::Tuple(vec![Value::UInt8(1), Value::String(b"x".to_vec())]),
                Value::Tuple(vec![Value::UInt8(2), Value::String(b"y".to_vec())]),
            ])),
        }],
    ];

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, &rows);
        let payload = server.fetch_rowbinary(&format!("SELECT value FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, rows);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
