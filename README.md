# ClickHouse RowBinary Toolkit (Rust)

`clickhouse-binary` is a small Rust library for reading and writing ClickHouse
RowBinary formats: `RowBinary`, `RowBinaryWithNames`, and
`RowBinaryWithNamesAndTypes`. It is designed for streaming ingestion and
validation against a live ClickHouse server.

## Features

- Read/write `RowBinary`, `RowBinaryWithNames`, `RowBinaryWithNamesAndTypes`.
- Streaming row APIs for incremental reads/writes.
- Strict schema validation with structured errors (no panics in library code).
- Integration tests for read/write across all formats, single/multi row cases.

## Usage

```rust
use clickhouse_binary::{RowBinaryFormat, RowBinaryWriter, Schema, Value};

let schema = Schema::from_type_strings(&[
    ("id", "UInt32"),
    ("name", "String"),
])?;
let mut writer = RowBinaryWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema);
writer.write_row(&[Value::UInt32(1), Value::String(b"alpha".to_vec())])?;
let payload = writer.into_inner();
```

For `RowBinaryWithNames` and `RowBinaryWithNamesAndTypes`, the header is written
automatically from the schema.

See `usage.md` for end-to-end streaming examples (compressed files, batching,
and multi-threaded producers).

## Supported Types

Currently supported:
- Integers (including 128/256-bit), `Bool`, floats, `String`, `FixedString`.
- `Decimal`, `Decimal32`, `Decimal64`, `Decimal128`, `Decimal256`.
- `Enum8`, `Enum16`.
- `Date`, `Date32`, `DateTime`, `DateTime64`.
- `UUID`, `IPv4`, `IPv6`.
- `Nullable(T)`, `Array(T)`, `Map(K, V)`, `LowCardinality(T)`.
- `Tuple(...)`, `Nested(...)`, `Dynamic`.

Validation mirrors ClickHouse rules where applicable:
- `LowCardinality` is only allowed for types ClickHouse accepts (numbers, string
  types, `Date`, `Date32`, `DateTime`, `UUID`, `IPv4`, `IPv6`, and `Nullable` of
  those). `LowCardinality(DateTime64)` and nested low-cardinality are rejected.
- `Map` keys cannot be `Nullable` or `LowCardinality(Nullable(...))`.

JSON v3 RowBinary encodings are planned but not implemented yet.

Dynamic values embed the concrete type inline using ClickHouse binary type
encoding; use `Value::Dynamic`/`Value::DynamicNull` when writing.

## Codec Notes

RowBinary encodes values independent of storage codecs. A `String CODEC(ZSTD)`
column still uses the same RowBinary encoding; an integration test verifies
roundtrip when the server supports the codec.

## Decimal Notes

- `RowBinaryWithNamesAndTypes` headers use canonical `Decimal(precision, scale)`
  names, even if the schema was declared with `Decimal32/64/128/256`.
- When validating inserts via `FORMAT JSONEachRow`, ClickHouse emits decimals
  as JSON numbers (not strings) and may trim trailing zeroes (for example,
  `-56.00` becomes `-56`).

## Running Tests

```bash
./validate.sh
```

The script runs format, build, clippy, unit tests, and integration tests against
a ClickHouse instance configured via `CLICKHOUSE_DSN`. Integration tests use
`DROP TABLE IF EXISTS` at the start to make debugging easier.
