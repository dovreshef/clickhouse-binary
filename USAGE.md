# Usage Examples

This file shows end-to-end patterns for streaming RowBinary payloads.

For convenience, `RowBinaryFileReader` and `RowBinaryFileWriter` are type
aliases for buffered file-backed readers/writers (`BufReader<File>` and
`BufWriter<File>`). `RowBinaryValueWriter::new_buffered` wraps any writer in
`BufWriter` when you want buffering for small row payloads.

## Write a seekable Zstd RowBinaryWithNamesAndTypes file

```rust
use std::fs::File;
use std::io::BufWriter;

use clickhouse_rowbinary::{
    RowBinaryFormat, RowBinaryWriter, RowBinaryValueWriter, Schema, Value,
};

let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")])?;
let rows = vec![
    vec![Value::UInt8(1), Value::String(b"alpha".to_vec())],
    vec![Value::UInt8(2), Value::String(b"beta".to_vec())],
];

let file = File::create("data.rowbinary.zst")?;
let mut writer =
    RowBinaryWriter::new(BufWriter::new(file), RowBinaryFormat::RowBinaryWithNamesAndTypes)?;
writer.write_header(&schema)?;
for row in &rows {
    let mut row_writer =
        RowBinaryValueWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema.clone());
    row_writer.write_header()?;
    row_writer.write_row(row)?;
    writer.write_row_bytes(&row_writer.into_inner())?;
}
writer.finish()?;
```

## Read a seekable Zstd RowBinaryWithNamesAndTypes file and post in batches

The `RowBinaryWithNamesAndTypes` header is required for each INSERT. For best
performance, stream row bytes directly into the batch writer and only rebuild
the payload when you hit the batch size.

```rust
use std::fs::File;
use std::io::BufReader;

use clickhouse_rowbinary::{
    RowBinaryFormat, RowBinaryReader, RowBinaryValueWriter, Schema,
};

let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")])?;
let file = File::open("data.rowbinary.zst")?;
let mut reader = RowBinaryReader::new(
    BufReader::new(file),
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
    None,
)?;
// The constructor parses the header and initializes the schema.
// For large files, consider RowBinaryReader::new_with_stride(..., 1024)
// to keep the in-memory row index sparse.

let mut out = RowBinaryValueWriter::new(
    Vec::new(),
    RowBinaryFormat::RowBinaryWithNamesAndTypes,
    schema.clone(),
);
out.write_header()?;

let mut count = 0usize;
loop {
    let Some(row) = reader.current_row()? else {
        break;
    };
    out.write_row_bytes(row)?;
    count += 1;
    if count == 100_000 {
        let mut payload = out.take_inner();
        // POST: INSERT INTO table FORMAT RowBinaryWithNamesAndTypes
        payload.clear();
        out.reset(payload);
        count = 0;
    }
    if reader.seek_relative(1).is_err() {
        break;
    }
}

if count > 0 {
    let payload = out.into_inner();
    // POST: INSERT INTO table FORMAT RowBinaryWithNamesAndTypes
}

// Seek to a specific row if you need to retry from a prior position.
reader.seek_row(50_000)?;
```

For tighter control over allocations, keep a reusable `Vec<u8>` per batch,
`clear()` it after sending, and pass it into each new `RowBinaryValueWriter`.
If your HTTP client supports streaming request bodies, you can write directly
into the request stream instead of buffering the entire batch.

## Dynamic values

`Dynamic` values encode the concrete type before each value using ClickHouse's
binary type encoding. Use `Value::Dynamic` with an explicit `TypeDesc`, or
`Value::DynamicNull` to emit `Nothing`.

Note: when Dynamic values are produced by ClickHouse SQL casts, `Nested` is
encoded as `Array(Tuple(...))`, so the decoded `TypeDesc` will be that array
form rather than `Nested`.

```rust
use clickhouse_rowbinary::{RowBinaryFormat, RowBinaryValueWriter, Schema, TypeDesc, Value};

let schema = Schema::from_type_strings(&[("value", "Dynamic")])?;
let mut writer = RowBinaryValueWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema);
writer.write_header()?;
writer.write_rows(&[
    vec![Value::Dynamic {
        ty: Box::new(TypeDesc::UInt8),
        value: Box::new(Value::UInt8(7)),
    }],
    vec![Value::Dynamic {
        ty: Box::new(TypeDesc::String),
        value: Box::new(Value::String(b"alpha".to_vec())),
    }],
    vec![Value::DynamicNull],
])?;
let payload = writer.into_inner();
// INSERT INTO table FORMAT RowBinary
```

## Writing Nested columns

ClickHouse expands `Nested` columns into separate `Array(T)` columns on write
(`n.a`, `n.b`, ...). The writer handles this for you: supply a `Nested` schema
and pass `Value::Array(Vec<Value::Tuple>)` for the column value. The writer
will emit `n.a`, `n.b` payloads in the expected order. This conversion buffers
the nested values to transpose rows into per-field arrays.

```rust
use clickhouse_rowbinary::{RowBinaryFormat, RowBinaryValueWriter, Schema, Value};

let schema = Schema::from_type_strings(&[("n", "Nested(a UInt8, b String)")])?;
let mut writer = RowBinaryValueWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema);
writer.write_header()?;
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

use clickhouse_rowbinary::{RowBinaryFormat, RowBinaryValueWriter, Schema, Value};
use zstd::stream::Encoder;

let schema = Schema::from_type_strings(&[("id", "UInt8"), ("name", "String")])?;

let handle = |rows: Vec<Vec<Value>>| {
    thread::spawn(move || {
        let mut writer = RowBinaryValueWriter::new(Vec::new(), RowBinaryFormat::RowBinary, schema);
        writer.write_header()?;
        writer.write_rows(&rows)?;
        Ok::<_, clickhouse_rowbinary::Error>(writer.into_inner())
    })
};

let h1 = handle(vec![vec![Value::UInt8(1), Value::String(b"a".to_vec())]]);
let h2 = handle(vec![vec![Value::UInt8(2), Value::String(b"b".to_vec())]]);
let chunk1 = h1.join().unwrap()?;
let chunk2 = h2.join().unwrap()?;

let file = File::create("combined.rowbinary.zst")?;
let mut encoder = Encoder::new(file, 0)?;

// Write one header, then append raw row bytes.
let mut header_writer = RowBinaryValueWriter::new(
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
