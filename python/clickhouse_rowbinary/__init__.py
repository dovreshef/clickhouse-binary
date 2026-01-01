"""ClickHouse RowBinary format encoder/decoder.

This library provides fast encoding and decoding of ClickHouse RowBinary
format data, supporting all ClickHouse data types.

Quick Start
-----------
    >>> from clickhouse_rowbinary import Schema, RowBinaryWriter, RowBinaryReader
    >>>
    >>> # Define a schema
    >>> schema = Schema.from_clickhouse([
    ...     ("id", "UInt32"),
    ...     ("name", "String"),
    ...     ("active", "Bool"),
    ... ])
    >>>
    >>> # Write some rows
    >>> writer = RowBinaryWriter(schema)
    >>> writer.write_row({"id": 1, "name": b"Alice", "active": True})
    >>> writer.write_row([2, b"Bob", False])
    >>> data = writer.take()
    >>>
    >>> # Read them back
    >>> reader = RowBinaryReader(data, schema)
    >>> for row in reader:
    ...     print(row["id"], row["name"])

Supported Types
---------------
The following ClickHouse types are supported with their Python equivalents:

**Integers:**
    - UInt8, UInt16, UInt32, UInt64, UInt128, UInt256 -> int
    - Int8, Int16, Int32, Int64, Int128, Int256 -> int

**Floating Point:**
    - Float32, Float64 -> float
    - Float16, BFloat16 -> float (half-precision, stored as float)

**Boolean:**
    - Bool -> bool

**Strings:**
    - String -> bytes (default) or str (with string_mode="str")
    - FixedString(N) -> bytes (padded to N bytes)

**Date/Time:**
    - Date, Date32 -> datetime.date
    - DateTime, DateTime('timezone') -> datetime.datetime
    - DateTime64(precision), DateTime64(precision, 'timezone') -> datetime.datetime

**Other Scalar Types:**
    - UUID -> uuid.UUID
    - IPv4 -> ipaddress.IPv4Address
    - IPv6 -> ipaddress.IPv6Address
    - Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S) -> decimal.Decimal
    - Decimal(P, S) -> decimal.Decimal
    - Enum8, Enum16 -> str (enum variant name)

**Composite Types:**
    - Nullable(T) -> T | None
    - Array(T) -> list
    - Map(K, V) -> dict
    - Tuple(T1, T2, ...) -> tuple
    - LowCardinality(T) -> same as T (transparent wrapper)

String Handling
---------------
By default, String and FixedString values are returned as ``bytes``. This is
the safest option since ClickHouse strings can contain arbitrary binary data.

To decode strings as UTF-8, use ``string_mode="str"``:

    >>> reader = RowBinaryReader(data, schema, string_mode="str")
    >>> row = reader.read_row()
    >>> row["name"]  # Returns str instead of bytes
    'Alice'

For fine-grained control over individual fields, use ``Row.get_str()``:

    >>> row.get_str("name", errors="replace")  # Replace invalid UTF-8
    >>> row.get_str("name", errors="ignore")   # Skip invalid bytes

Working with Nullable Types
---------------------------
Nullable columns can contain None values:

    >>> schema = Schema.from_clickhouse([("value", "Nullable(Int32)")])
    >>> writer = RowBinaryWriter(schema)
    >>> writer.write_row({"value": 42})
    >>> writer.write_row({"value": None})

Format Variants
---------------
Three RowBinary format variants are supported:

- ``Format.RowBinary`` - Raw row data only (default)
- ``Format.RowBinaryWithNames`` - Includes column names header
- ``Format.RowBinaryWithNamesAndTypes`` - Includes column names and types

For formats with headers, call ``write_header()`` before writing rows:

    >>> writer = RowBinaryWriter(schema, format=Format.RowBinaryWithNames)
    >>> writer.write_header()
    >>> writer.write_rows(rows)

Error Handling
--------------
The library raises specific exceptions for different error conditions:

- ``SchemaError`` - Invalid type string or schema definition
- ``ValidationError`` - Data doesn't match schema (wrong type, missing column)
- ``EncodingError`` - Failed to encode value to RowBinary
- ``DecodingError`` - Failed to decode RowBinary data

All exceptions inherit from ``ClickHouseRowBinaryError``.

Classes
-------
- ``Schema`` - Defines the structure of RowBinary data
- ``Column`` - A single column definition (name + type)
- ``RowBinaryWriter`` - Encodes rows to RowBinary format (in-memory)
- ``RowBinaryReader`` - Decodes rows from RowBinary format (in-memory)
- ``SeekableWriter`` - Writes Zstd-compressed RowBinary files with seek tables
- ``SeekableReader`` - Reads Zstd-compressed RowBinary files with random access
- ``Row`` - A decoded row with dict-like access
- ``Format`` - Enum of RowBinary format variants

Compressed Files
----------------
For working with large datasets, use ``SeekableWriter`` and ``SeekableReader``
to create and read Zstd-compressed files with efficient random access:

    >>> from clickhouse_rowbinary import Schema, SeekableWriter, SeekableReader
    >>>
    >>> # Write compressed file
    >>> with SeekableWriter.create("data.rowbinary.zst", schema) as writer:
    ...     writer.write_header()
    ...     writer.write_rows(rows)
    ...
    >>> # Read with random access
    >>> with SeekableReader.open("data.rowbinary.zst", schema=schema) as reader:
    ...     reader.seek(1000)  # Jump to row 1000
    ...     row = reader.read_current()
"""

from clickhouse_rowbinary._core import (
    # Exceptions
    ClickHouseRowBinaryError,
    Column,
    DecodingError,
    EncodingError,
    Format,
    Row,
    RowBinaryReader,
    RowBinaryWriter,
    # Main classes
    Schema,
    SchemaError,
    # Seekable classes for compressed files
    SeekableReader,
    SeekableWriter,
    ValidationError,
)

__all__ = [
    # Main classes
    "Schema",
    "Column",
    "Row",
    "RowBinaryWriter",
    "RowBinaryReader",
    "Format",
    # Seekable classes for compressed files
    "SeekableWriter",
    "SeekableReader",
    # Exceptions
    "ClickHouseRowBinaryError",
    "SchemaError",
    "ValidationError",
    "EncodingError",
    "DecodingError",
    # Constants
    "SUPPORTED_TYPES",
]

__version__ = "0.3.0"

# Type mapping for discoverability
SUPPORTED_TYPES: dict[str, str] = {
    # Integers
    "UInt8": "int (0 to 255)",
    "UInt16": "int (0 to 65535)",
    "UInt32": "int (0 to 4294967295)",
    "UInt64": "int (0 to 18446744073709551615)",
    "UInt128": "int (0 to 2^128-1)",
    "UInt256": "int (0 to 2^256-1)",
    "Int8": "int (-128 to 127)",
    "Int16": "int (-32768 to 32767)",
    "Int32": "int (-2147483648 to 2147483647)",
    "Int64": "int (-2^63 to 2^63-1)",
    "Int128": "int (-2^127 to 2^127-1)",
    "Int256": "int (-2^255 to 2^255-1)",
    # Floating point
    "Float32": "float (32-bit IEEE 754)",
    "Float64": "float (64-bit IEEE 754)",
    "Float16": "float (16-bit half-precision)",
    "BFloat16": "float (brain floating point)",
    # Boolean
    "Bool": "bool",
    # Strings
    "String": "bytes (default) or str (with string_mode='str')",
    "FixedString(N)": "bytes (padded/truncated to N bytes)",
    # Date/Time
    "Date": "datetime.date",
    "Date32": "datetime.date (extended range)",
    "DateTime": "datetime.datetime (UTC)",
    "DateTime('tz')": "datetime.datetime (timezone-aware)",
    "DateTime64(p)": "datetime.datetime (sub-second precision)",
    "DateTime64(p, 'tz')": "datetime.datetime (precision + timezone)",
    # UUID
    "UUID": "uuid.UUID",
    # IP addresses
    "IPv4": "ipaddress.IPv4Address",
    "IPv6": "ipaddress.IPv6Address",
    # Decimals
    "Decimal32(S)": "decimal.Decimal (9 digits, S decimal places)",
    "Decimal64(S)": "decimal.Decimal (18 digits, S decimal places)",
    "Decimal128(S)": "decimal.Decimal (38 digits, S decimal places)",
    "Decimal256(S)": "decimal.Decimal (76 digits, S decimal places)",
    "Decimal(P, S)": "decimal.Decimal (P digits, S decimal places)",
    # Enums
    "Enum8": "str (variant name)",
    "Enum16": "str (variant name)",
    # Composite types
    "Nullable(T)": "T | None",
    "Array(T)": "list[T]",
    "Map(K, V)": "dict[K, V]",
    "Tuple(T1, T2, ...)": "tuple[T1, T2, ...]",
    "LowCardinality(T)": "same as T (transparent wrapper)",
    # Special types
    "Nothing": "None",
    "JSON": "dict (parsed JSON object)",
}
