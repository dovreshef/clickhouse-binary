# clickhouse-rowbinary

Fast, streaming encoder/decoder for ClickHouse [RowBinary](https://clickhouse.com/docs/en/interfaces/formats#rowbinary) format. Built with Rust for maximum performance.

## Installation

```bash
pip install clickhouse-rowbinary
```

## Quick Start

```python
from clickhouse_rowbinary import Schema, RowBinaryWriter, RowBinaryReader

# Define schema matching your ClickHouse table
schema = Schema.from_clickhouse([
    ("id", "UInt32"),
    ("name", "String"),
    ("score", "Nullable(Float64)"),
])

# Write rows to RowBinary format
writer = RowBinaryWriter(schema)
writer.write_row({"id": 1, "name": b"Alice", "score": 95.5})
writer.write_row({"id": 2, "name": b"Bob", "score": None})
data = writer.take()

# Read rows back
reader = RowBinaryReader(data, schema)
for row in reader:
    print(f"ID: {row['id']}, Name: {row['name'].decode()}, Score: {row['score']}")
```

## Features

- **All ClickHouse types**: Integers (8-256 bit), floats, strings, dates, UUIDs, IPs, decimals, enums, and composite types
- **Three format variants**: `RowBinary`, `RowBinaryWithNames`, `RowBinaryWithNamesAndTypes`
- **Streaming I/O**: Process large datasets without loading everything into memory
- **Compressed files**: Read and write Zstd-compressed files with random access via `SeekableReader` and `SeekableWriter`
- **File support**: Read directly from files with `RowBinaryReader.from_file()`
- **Type safety**: Full type stubs for IDE autocomplete and type checking
- **Fast**: Rust-powered encoding/decoding with GIL release for multi-threaded workloads

## Supported Types

```python
from clickhouse_rowbinary import SUPPORTED_TYPES

# View all supported types and their Python equivalents
for ch_type, py_type in SUPPORTED_TYPES.items():
    print(f"{ch_type}: {py_type}")
```

| ClickHouse Type | Python Type |
|-----------------|-------------|
| `UInt8`..`UInt256`, `Int8`..`Int256` | `int` |
| `Float32`, `Float64` | `float` |
| `Bool` | `bool` |
| `String` | `bytes` (default) or `str` |
| `FixedString(N)` | `bytes` |
| `Date`, `Date32` | `datetime.date` |
| `DateTime`, `DateTime64` | `datetime.datetime` |
| `UUID` | `uuid.UUID` |
| `IPv4`, `IPv6` | `ipaddress.IPv4Address`, `ipaddress.IPv6Address` |
| `Decimal32/64/128/256(S)` | `decimal.Decimal` |
| `Enum8`, `Enum16` | `str` (variant name) |
| `Nullable(T)` | `T \| None` |
| `Array(T)` | `list` |
| `Map(K, V)` | `dict` |
| `Tuple(T1, T2, ...)` | `tuple` |
| `LowCardinality(T)` | same as `T` |

## Writing Data

```python
from clickhouse_rowbinary import Schema, RowBinaryWriter

schema = Schema.from_clickhouse([
    ("id", "UInt32"),
    ("tags", "Array(String)"),
    ("metadata", "Map(String, Int32)"),
])

writer = RowBinaryWriter(schema)

# Write with dict (most readable)
writer.write_row({
    "id": 1,
    "tags": [b"python", b"rust"],
    "metadata": {b"views": 100, b"likes": 42},
})

# Write with tuple (faster, schema order)
writer.write_row((2, [b"database"], {b"views": 50}))

# Write multiple rows from iterator
writer.write_rows(generate_rows())

# Get the encoded bytes
data = writer.take()
```

## Reading Data

```python
from clickhouse_rowbinary import Schema, RowBinaryReader

schema = Schema.from_clickhouse([("id", "UInt32"), ("name", "String")])

# From bytes
reader = RowBinaryReader(data, schema)
for row in reader:
    print(row["id"], row["name"])

# From file
reader = RowBinaryReader.from_file("data.bin", schema)

# Read all at once (releases GIL for parallel workloads)
rows = reader.read_all()

# String mode: decode strings as UTF-8 automatically
reader = RowBinaryReader(data, schema, string_mode="str")
for row in reader:
    print(row["name"])  # Returns str instead of bytes
```

## Row Access Patterns

```python
# Dict-style access
value = row["column_name"]
value = row.get("column_name", default_value)

# Index access
value = row[0]  # First column

# Attribute access
value = row.column_name

# Convert to standard types
d = row.as_dict()    # {"id": 1, "name": b"Alice"}
t = row.as_tuple()   # (1, b"Alice")

# Handle UTF-8 with error control
text = row.get_str("name", errors="replace")  # Replace invalid bytes
```

## Format Variants

```python
from clickhouse_rowbinary import Format, RowBinaryWriter

# Standard format (most compact, requires schema on read)
writer = RowBinaryWriter(schema, format=Format.RowBinary)

# With column names (useful for validation)
writer = RowBinaryWriter(schema, format=Format.RowBinaryWithNames)
writer.write_header()  # Required before writing rows

# Self-describing (includes names and types)
writer = RowBinaryWriter(schema, format=Format.RowBinaryWithNamesAndTypes)
writer.write_header()
```

## Error Handling

```python
from clickhouse_rowbinary import (
    ClickHouseRowBinaryError,  # Base class
    SchemaError,      # Invalid type string
    ValidationError,  # Data doesn't match schema
    EncodingError,    # Failed to encode value
    DecodingError,    # Failed to decode data
)

try:
    schema = Schema.from_clickhouse([("col", "InvalidType")])
except SchemaError as e:
    print(f"Bad type: {e}")

try:
    writer.write_row({"id": "not_an_int"})
except ValidationError as e:
    print(f"Wrong type: {e}")
```

## Working with ClickHouse

```python
import httpx
from clickhouse_rowbinary import Schema, RowBinaryWriter, Format

schema = Schema.from_clickhouse([
    ("id", "UInt32"),
    ("name", "String"),
])

writer = RowBinaryWriter(schema, format=Format.RowBinaryWithNamesAndTypes)
writer.write_header()
writer.write_rows(rows)
data = writer.take()

# Insert via HTTP interface
response = httpx.post(
    "http://localhost:8123/",
    params={"query": "INSERT INTO my_table FORMAT RowBinaryWithNamesAndTypes"},
    content=data,
)
```

## Compressed Files (Zstd)

For large datasets, use `SeekableWriter` and `SeekableReader` to work with Zstd-compressed files that support random access.

### Writing Compressed Files

```python
from clickhouse_rowbinary import Schema, SeekableWriter

schema = Schema.from_clickhouse([("id", "UInt64"), ("name", "String")])

with SeekableWriter.create("data.rowbinary.zst", schema) as writer:
    writer.write_header()
    for i in range(1_000_000):
        writer.write_row({"id": i, "name": f"user{i}".encode()})
# Seek table is written automatically on context exit
```

### Reading Compressed Files with Random Access

```python
from clickhouse_rowbinary import Schema, SeekableReader

schema = Schema.from_clickhouse([("id", "UInt64"), ("name", "String")])

with SeekableReader.open("data.rowbinary.zst", schema=schema) as reader:
    # Sequential iteration
    for row in reader:
        print(row["id"])

    # Random access - seek to any row instantly
    reader.seek(500_000)
    row = reader.read_current()
    print(f"Row 500k: {row['id']}")

    # Batch reading
    reader.seek(0)
    batch = reader.read_rows(1000)
```

### High-Performance Batch Processing

For maximum throughput, work with raw bytes to avoid decoding overhead:

```python
from clickhouse_rowbinary import (
    Schema, SeekableReader, SeekableWriter, RowBinaryWriter, Format
)
import httpx

schema = Schema.from_clickhouse([("id", "UInt64"), ("name", "String")])
BATCH_SIZE = 100_000

# Read compressed file and insert to ClickHouse in batches
with SeekableReader.open("huge_file.rowbinary.zst", schema=schema) as reader:
    client = httpx.Client()

    while True:
        # Create batch with header
        batch = RowBinaryWriter(schema, format=Format.RowBinaryWithNamesAndTypes)
        batch.write_header()

        # Collect raw bytes (no decode/re-encode overhead)
        count = 0
        for _ in range(BATCH_SIZE):
            row_bytes = reader.current_row_bytes()
            if row_bytes is None:
                break
            batch.write_row_bytes(row_bytes)
            count += 1
            try:
                reader.seek_relative(1)
            except Exception:
                break  # End of file

        if count == 0:
            break

        # Send batch to ClickHouse
        client.post(
            "http://localhost:8123/",
            params={"query": "INSERT INTO table FORMAT RowBinaryWithNamesAndTypes"},
            content=batch.take(),
        )
        print(f"Inserted {count} rows")
```

### Copy Between Compressed Files

```python
# Copy rows between compressed files without decoding
with SeekableReader.open("input.zst", schema=schema) as reader:
    with SeekableWriter.create("output.zst", schema) as writer:
        writer.write_header()
        while (row_bytes := reader.current_row_bytes()) is not None:
            writer.write_row_bytes(row_bytes)
            try:
                reader.seek_relative(1)
            except Exception:
                break
```

## API Reference

### Classes

- `Schema` - Defines column names and types
- `Column` - Single column definition
- `RowBinaryWriter` - Encodes rows to RowBinary format (in-memory)
- `RowBinaryReader` - Decodes rows from RowBinary format (in-memory)
- `SeekableWriter` - Writes Zstd-compressed RowBinary files with seek tables
- `SeekableReader` - Reads Zstd-compressed RowBinary files with random access
- `Row` - Decoded row with dict-like access
- `Format` - Enum of format variants

### Exceptions

- `ClickHouseRowBinaryError` - Base exception
- `SchemaError` - Invalid type definition
- `ValidationError` - Data/schema mismatch
- `EncodingError` - Encoding failure
- `DecodingError` - Decoding failure

## License

MIT
