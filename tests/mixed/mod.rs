use std::net::{Ipv4Addr, Ipv6Addr};

use clickhouse_binary::{Row, RowBinaryFormat, Schema, Value};
use uuid::Uuid;

use crate::common::{ClickhouseServer, decode_rows, unique_table};

const FORMATS: [RowBinaryFormat; 3] = [
    RowBinaryFormat::RowBinary,
    RowBinaryFormat::RowBinaryWithNames,
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
];

fn mixed_schema() -> Schema {
    Schema::from_type_strings(&[
        ("u8", "UInt8"),
        ("i16", "Int16"),
        ("f32", "Float32"),
        ("s", "String"),
        ("fixed", "FixedString(3)"),
        ("d", "Date"),
        ("dt", "DateTime"),
        ("dt64", "DateTime64(3)"),
        ("u", "UUID"),
        ("ip4", "IPv4"),
        ("ip6", "IPv6"),
        ("n", "Nullable(Int32)"),
        ("arr", "Array(UInt16)"),
        ("map", "Map(String, UInt8)"),
        ("lc", "LowCardinality(String)"),
    ])
    .unwrap()
}

fn mixed_rows() -> (Row, Row) {
    let first = vec![
        Value::UInt8(7),
        Value::Int16(-5),
        Value::Float32(1.5_f32),
        Value::String(b"alpha".to_vec()),
        Value::FixedString(b"abc".to_vec()),
        Value::Date(0),
        Value::DateTime(1),
        Value::DateTime64(1234),
        Value::Uuid(Uuid::parse_str("e4eaaaf2-d142-11e1-b3e4-080027620cdd").unwrap()),
        Value::Ipv4(Ipv4Addr::LOCALHOST),
        Value::Ipv6(Ipv6Addr::LOCALHOST),
        Value::Nullable(None),
        Value::Array(vec![Value::UInt16(7), Value::UInt16(9)]),
        Value::Map(vec![
            (Value::String(b"a".to_vec()), Value::UInt8(1)),
            (Value::String(b"b".to_vec()), Value::UInt8(2)),
        ]),
        Value::String(b"low".to_vec()),
    ];
    let second = vec![
        Value::UInt8(9),
        Value::Int16(42),
        Value::Float32(-2.25_f32),
        Value::String(b"beta".to_vec()),
        Value::FixedString(b"xyz".to_vec()),
        Value::Date(1),
        Value::DateTime(2),
        Value::DateTime64(2345),
        Value::Uuid(Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap()),
        Value::Ipv4(Ipv4Addr::new(10, 0, 0, 1)),
        Value::Ipv6(Ipv6Addr::new(
            0x2607, 0xf8b0, 0x4005, 0x0805, 0, 0, 0, 0x200e,
        )),
        Value::Nullable(Some(Box::new(Value::Int32(-5)))),
        Value::Array(Vec::new()),
        Value::Map(Vec::new()),
        Value::String(b"high".to_vec()),
    ];
    (first, second)
}

fn create_mixed_table(server: &ClickhouseServer, table: &str) {
    server.exec(&format!("DROP TABLE IF EXISTS {table}"));
    server.exec(&format!(
        "CREATE TABLE {table} (\
         u8 UInt8,\
         i16 Int16,\
         f32 Float32,\
         s String,\
         fixed FixedString(3),\
         d Date,\
         dt DateTime,\
         dt64 DateTime64(3),\
         u UUID,\
         ip4 IPv4,\
         ip6 IPv6,\
         n Nullable(Int32),\
         arr Array(UInt16),\
         map Map(String, UInt8),\
         lc LowCardinality(String)\
         ) ENGINE=Memory"
    ));
}

#[test]
fn mixed_schema_single_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_mixed_table(&server, &table);
    server.exec(&format!(
        "INSERT INTO {table} VALUES \
        (7,-5,1.5,'alpha','abc','1970-01-01','1970-01-01 00:00:01','1970-01-01 00:00:01.234','e4eaaaf2-d142-11e1-b3e4-080027620cdd','127.0.0.1','::1',NULL,[7,9],map('a',1,'b',2),'low')"
    ));
    let schema = mixed_schema();
    let (first, _) = mixed_rows();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT * FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![first.clone()]);
    }
}

#[test]
fn mixed_schema_multi_row_reading() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_mixed_table(&server, &table);
    server.exec(&format!(
        "INSERT INTO {table} VALUES \
        (7,-5,1.5,'alpha','abc','1970-01-01','1970-01-01 00:00:01','1970-01-01 00:00:01.234','e4eaaaf2-d142-11e1-b3e4-080027620cdd','127.0.0.1','::1',NULL,[7,9],map('a',1,'b',2),'low'),\
        (9,42,-2.25,'beta','xyz','1970-01-02','1970-01-01 00:00:02','1970-01-01 00:00:02.345','f47ac10b-58cc-4372-a567-0e02b2c3d479','10.0.0.1','2607:f8b0:4005:805::200e',-5,[],map(),'high')"
    ));
    let schema = mixed_schema();
    let (first, second) = mixed_rows();

    for format in FORMATS {
        let payload = server.fetch_rowbinary(&format!("SELECT * FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![first.clone(), second.clone()]);
    }
}

#[test]
fn mixed_schema_single_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_mixed_table(&server, &table);
    let schema = mixed_schema();
    let (first, _) = mixed_rows();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(&insert_sql, format, &schema, std::slice::from_ref(&first));
        let payload = server.fetch_rowbinary(&format!("SELECT * FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![first.clone()]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}

#[test]
fn mixed_schema_multi_row_writing() {
    let server = ClickhouseServer::connect();
    let table = unique_table("");
    create_mixed_table(&server, &table);
    let schema = mixed_schema();
    let (first, second) = mixed_rows();

    for format in FORMATS {
        let insert_sql = format!("INSERT INTO {table} FORMAT {format}");
        server.insert_rowbinary(
            &insert_sql,
            format,
            &schema,
            &[first.clone(), second.clone()],
        );
        let payload = server.fetch_rowbinary(&format!("SELECT * FROM {table}"), format);
        let decoded = decode_rows(&payload, format, &schema);
        assert_eq!(decoded, vec![first.clone(), second.clone()]);
        server.exec(&format!("TRUNCATE TABLE {table}"));
    }
}
