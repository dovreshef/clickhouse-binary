# Usage Examples

This file shows end-to-end patterns for streaming RowBinary payloads.

## Read a ZSTD-compressed RowBinaryWithNamesAndTypes file and post in batches

The `RowBinaryWithNamesAndTypes` header is required for each INSERT. For best
performance, stream rows directly into the batch writer and only rebuild the
payload when you hit the batch size.

```rust
use std::fs::File;
use std::io::{Read, Write};

use clickhouse_binary::{
    RowBinaryFormat, RowBinaryReader, RowBinaryWriter, Schema, Value,
};
use zstd::stream::{Decoder, Encoder};

// 1) Produce a compressed file (header written once at the start).
let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")])?;
let rows = vec![
    vec![Value::UInt8(1), Value::String(b"alpha".to_vec())],
    vec![Value::UInt8(2), Value::String(b"beta".to_vec())],
];
let file = File::create("data.rowbinary.zst")?;
let mut encoder = Encoder::new(file, 0)?;
let mut writer = RowBinaryWriter::new(
    Vec::new(),
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
    schema.clone(),
);
writer.write_rows(&rows)?;
encoder.write_all(&writer.into_inner())?;
encoder.finish()?;

// 2) Read and post batches to ClickHouse (stream rows; no Vec<Row> buffering).
let file = File::open("data.rowbinary.zst")?;
let decoder = Decoder::new(file)?;
let mut reader = RowBinaryReader::new(decoder, RowBinaryFormat::RowBinaryWithNamesAndTypes);
reader.read_header()?;

let mut out = RowBinaryWriter::new(
    Vec::new(),
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
    schema.clone(),
);
let mut count = 0usize;
while let Some(row) = reader.read_row()? {
    out.write_row(&row)?;
    count += 1;
    if count == 100_000 {
        let payload = out.into_inner();
        // POST: INSERT INTO table FORMAT RowBinaryWithNamesAndTypes
        out = RowBinaryWriter::new(
            Vec::new(),
            RowBinaryFormat::RowBinaryWithNamesAndTypes,
            schema.clone(),
        );
        count = 0;
    }
}

if count > 0 {
    let payload = out.into_inner();
    // POST: INSERT INTO table FORMAT RowBinaryWithNamesAndTypes
}
```

For tighter control over allocations, keep a reusable `Vec<u8>` per batch,
`clear()` it after sending, and pass it into each new `RowBinaryWriter`.

## Writing Nested columns

ClickHouse expands `Nested` columns into separate `Array(T)` columns on write
(`n.a`, `n.b`, ...). The writer handles this for you: supply a `Nested` schema
and pass `Value::Array(Vec<Value::Tuple>)` for the column value. The writer
will emit `n.a`, `n.b` payloads in the expected order.

```rust
use clickhouse_binary::{RowBinaryFormat, RowBinaryWriter, Schema, Value};

let schema = Schema::from_type_strings(&[("n", "Nested(a UInt8, b String)")])?;
let mut writer = RowBinaryWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema);
writer.write_row(&[Value::Array(vec![
    Value::Tuple(vec![Value::UInt8(7), Value::String(b"alpha".to_vec())]),
    Value::Tuple(vec![Value::UInt8(9), Value::String(b"beta".to_vec())]),
])])?;
let payload = writer.into_inner();
// INSERT INTO table FORMAT RowBinary
```

## Combine per-thread RowBinary chunks into one ZSTD file

Workers can emit **plain RowBinary** (no header) and a single aggregator writes
one `RowBinaryWithNamesAndTypes` header before appending worker chunks.

```rust
use std::fs::File;
use std::io::Write;
use std::thread;

use clickhouse_binary::{RowBinaryFormat, RowBinaryWriter, Schema, Value};
use zstd::stream::Encoder;

let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")])?;

let handle = |rows: Vec<Vec<Value>>| {
    thread::spawn(move || {
        let mut writer = RowBinaryWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema);
        writer.write_rows(&rows)?;
        Ok::<_, clickhouse_binary::Error>(writer.into_inner())
    })
};

let h1 = handle(vec![vec![Value::UInt8(1), Value::String(b"a".to_vec())]]);
let h2 = handle(vec![vec![Value::UInt8(2), Value::String(b"b".to_vec())]]);
let chunk1 = h1.join().unwrap()?;
let chunk2 = h2.join().unwrap()?;

let file = File::create("combined.rowbinary.zst")?;
let mut encoder = Encoder::new(file, 0)?;

// Write one header, then append raw row bytes.
let mut header_writer = RowBinaryWriter::new(
    Vec::new(),
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
    schema,
);
header_writer.write_header()?;
encoder.write_all(&header_writer.into_inner())?;
encoder.write_all(&chunk1)?;
encoder.write_all(&chunk2)?;
encoder.finish()?;
```
