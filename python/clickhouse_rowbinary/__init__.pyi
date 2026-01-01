"""Type stubs for clickhouse_rowbinary."""

from collections.abc import Iterable, Iterator
from enum import Enum
from os import PathLike
from typing import Any, Literal, overload

__version__: str

SUPPORTED_TYPES: dict[str, str]
"""Mapping of ClickHouse types to their Python equivalents.

Example:
    >>> from clickhouse_rowbinary import SUPPORTED_TYPES
    >>> print(SUPPORTED_TYPES["UInt32"])
    'int (0 to 4294967295)'
    >>> print(SUPPORTED_TYPES["Nullable(T)"])
    'T | None'
"""

# Exceptions

class ClickHouseRowBinaryError(Exception):
    """Base exception for clickhouse_rowbinary errors.

    All library-specific exceptions inherit from this class, making it
    useful for catching any error from this library:

    Example:
        >>> try:
        ...     reader = RowBinaryReader(data, schema)
        ...     for row in reader:
        ...         process(row)
        ... except ClickHouseRowBinaryError as e:
        ...     print(f"ClickHouse error: {e}")
    """

    ...

class SchemaError(ClickHouseRowBinaryError):
    """Error related to schema definition or parsing.

    Raised when a type string is invalid or malformed.

    Example:
        >>> try:
        ...     schema = Schema.from_clickhouse([("col", "InvalidType")])
        ... except SchemaError as e:
        ...     print(f"Invalid type: {e}")
    """

    ...

class ValidationError(ClickHouseRowBinaryError):
    """Error validating data against schema.

    Raised when row data doesn't match the expected schema (wrong type,
    missing column, wrong number of values).

    Example:
        >>> schema = Schema.from_clickhouse([("id", "UInt32"), ("name", "String")])
        >>> writer = RowBinaryWriter(schema)
        >>> try:
        ...     writer.write_row({"id": "not_an_int", "name": b"test"})
        ... except ValidationError as e:
        ...     print(f"Invalid data: {e}")
    """

    ...

class EncodingError(ClickHouseRowBinaryError):
    """Error encoding data to RowBinary format.

    Raised when a value cannot be encoded (e.g., integer overflow,
    invalid string encoding).

    Example:
        >>> schema = Schema.from_clickhouse([("value", "UInt8")])
        >>> writer = RowBinaryWriter(schema)
        >>> try:
        ...     writer.write_row({"value": 256})  # UInt8 max is 255
        ... except EncodingError as e:
        ...     print(f"Encoding failed: {e}")
    """

    ...

class DecodingError(ClickHouseRowBinaryError):
    """Error decoding data from RowBinary format.

    Raised when binary data is corrupted or doesn't match the schema.

    Example:
        >>> try:
        ...     reader = RowBinaryReader(b"corrupted", schema)
        ...     row = reader.read_row()
        ... except DecodingError as e:
        ...     print(f"Decoding failed: {e}")
    """

    ...

# Format enum

class Format(Enum):
    """RowBinary format variants.

    Example:
        >>> # Using format with headers for self-describing data
        >>> writer = RowBinaryWriter(schema, format=Format.RowBinaryWithNamesAndTypes)
        >>> writer.write_header()
        >>> writer.write_rows(rows)
        >>> data = writer.take()
        >>>
        >>> # Reader can infer schema from header
        >>> reader = RowBinaryReader(data, format=Format.RowBinaryWithNamesAndTypes)
    """

    RowBinary = ...
    """Standard RowBinary format (no header). Most compact but requires schema."""

    RowBinaryWithNames = ...
    """RowBinary with column names in header. Useful for validation."""

    RowBinaryWithNamesAndTypes = ...
    """RowBinary with column names and types in header. Self-describing format."""

# Column class

class Column:
    """A single column definition in a schema."""

    def __init__(self, name: str, type_str: str) -> None:
        """Create a new Column.

        Args:
            name: The column name.
            type_str: The ClickHouse type string (e.g., "UInt32", "Nullable(String)").

        Raises:
            SchemaError: If the type string is invalid.
        """
        ...

    @property
    def name(self) -> str:
        """The column name."""
        ...

    @property
    def type_str(self) -> str:
        """The ClickHouse type as a string."""
        ...

    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...

# Schema class

class Schema:
    """A schema defining the structure of RowBinary data."""

    def __init__(self, columns: list[Column]) -> None:
        """Create a schema from Column objects.

        Args:
            columns: A list of Column objects.
        """
        ...

    @staticmethod
    def from_clickhouse(columns: list[tuple[str, str]]) -> Schema:
        """Create a schema from a list of (name, type) tuples.

        This is the recommended way to create a schema from ClickHouse
        column definitions.

        Args:
            columns: A list of (name, type_str) tuples defining the columns.

        Returns:
            A new schema.

        Raises:
            SchemaError: If any type string is invalid.

        Example:
            >>> schema = Schema.from_clickhouse([
            ...     ("id", "UInt32"),
            ...     ("name", "String"),
            ...     ("age", "Nullable(UInt8)")
            ... ])
        """
        ...

    @property
    def columns(self) -> list[Column]:
        """The column definitions."""
        ...

    @property
    def names(self) -> list[str]:
        """The column names in order."""
        ...

    @overload
    def __getitem__(self, key: int) -> Column: ...
    @overload
    def __getitem__(self, key: str) -> Column: ...
    def __getitem__(self, key: int | str) -> Column:
        """Get a column by index or name.

        Args:
            key: Either an integer index or a string column name.

        Returns:
            The column at the given index or with the given name.

        Raises:
            IndexError: If the index is out of range.
            KeyError: If the column name is not found.
        """
        ...

    def __len__(self) -> int:
        """Return the number of columns."""
        ...

    def __bool__(self) -> bool:
        """Return True if the schema has columns."""
        ...

    def __contains__(self, name: str) -> bool:
        """Check if a column name exists in the schema."""
        ...

    def __iter__(self) -> Iterator[Column]:
        """Iterate over the columns."""
        ...

    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...

# Row class

class Row:
    """A single row of RowBinary data.

    Provides dict-like and attribute access to column values.
    Column values are lazily converted to Python objects on access.
    """

    @overload
    def __getitem__(self, key: int) -> Any: ...
    @overload
    def __getitem__(self, key: str) -> Any: ...
    def __getitem__(self, key: int | str) -> Any:
        """Get a value by column name or index.

        Args:
            key: Either a string column name or integer index.

        Returns:
            The value at the given column.

        Raises:
            KeyError: If the column name is not found.
            IndexError: If the index is out of range.
        """
        ...

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value by column name, returning default if not found.

        Args:
            key: The column name.
            default: Value to return if key is not found (default: None).

        Returns:
            The value or the default.

        Example:
            >>> row.get("optional_field", "N/A")
            'N/A'
        """
        ...

    def get_str(
        self, key: str, errors: Literal["strict", "replace", "ignore"] = "strict"
    ) -> str:
        """Get a string value with explicit UTF-8 decoding.

        This method allows retrieving string fields with control over
        error handling, regardless of the reader's string_mode setting.
        Useful when data might contain invalid UTF-8 bytes.

        Args:
            key: The column name.
            errors: Error handling mode:
                - "strict": Raise UnicodeDecodeError on invalid bytes
                - "replace": Replace invalid bytes with U+FFFD
                - "ignore": Skip invalid bytes

        Returns:
            The decoded string value.

        Raises:
            KeyError: If the column is not found.
            ValidationError: If the value is not a string type.
            UnicodeDecodeError: If decoding fails with errors="strict".

        Example:
            >>> # Safe handling of potentially invalid UTF-8
            >>> name = row.get_str("name", errors="replace")
            >>>
            >>> # With reader in bytes mode, decode specific fields as str
            >>> reader = RowBinaryReader(data, schema, string_mode="bytes")
            >>> for row in reader:
            ...     text = row.get_str("message", errors="strict")
        """
        ...

    def __getattr__(self, name: str) -> Any:
        """Get a value by column name as an attribute.

        Raises:
            AttributeError: If the column is not found.
        """
        ...

    def as_dict(self) -> dict[str, Any]:
        """Return the row as a dictionary mapping column names to values.

        Example:
            >>> row.as_dict()
            {'id': 1, 'name': b'Alice', 'active': True}
        """
        ...

    def as_tuple(self) -> tuple[Any, ...]:
        """Return the row as a tuple of values in column order.

        Example:
            >>> row.as_tuple()
            (1, b'Alice', True)
            >>> id, name, active = row.as_tuple()  # Unpack
        """
        ...

    def keys(self) -> list[str]:
        """Return the column names."""
        ...

    def values(self) -> list[Any]:
        """Return the column values."""
        ...

    def items(self) -> list[tuple[str, Any]]:
        """Return (name, value) pairs."""
        ...

    def __len__(self) -> int:
        """Return the number of columns."""
        ...

    def __contains__(self, name: str) -> bool:
        """Check if a column name exists."""
        ...

    def __iter__(self) -> Iterator[str]:
        """Iterate over column names."""
        ...

    def __repr__(self) -> str: ...

# Writer class

class RowBinaryWriter:
    """A writer for encoding rows into RowBinary format.

    The writer encodes rows into an in-memory buffer. Use `take()` to
    retrieve the encoded bytes.

    Example:
        >>> schema = Schema.from_clickhouse([("id", "UInt32"), ("name", "String")])
        >>> writer = RowBinaryWriter(schema)
        >>> writer.write_row({"id": 1, "name": b"Alice"})
        >>> writer.write_row([2, b"Bob"])
        >>> data = writer.take()
    """

    def __init__(self, schema: Schema, format: Format = Format.RowBinary) -> None:
        """Create a new RowBinary writer.

        Args:
            schema: The schema defining the columns to write.
            format: The RowBinary format variant (default: RowBinary).
        """
        ...

    def write_header(self) -> None:
        """Write the format header (if required by the format).

        Call this before writing any rows. For RowBinaryWithNames and
        RowBinaryWithNamesAndTypes formats, this writes the column
        names and/or types.

        Raises:
            EncodingError: If writing the header fails.
        """
        ...

    def write_row(self, row: dict[str, Any] | list[Any] | tuple[Any, ...]) -> None:
        """Write a single row.

        Args:
            row: The row data as a dict, list, or tuple.
                - dict: Maps column names to values
                - list/tuple: Values in schema column order

        Raises:
            ValidationError: If the row data doesn't match the schema.
            EncodingError: If encoding fails.

        Example:
            >>> # Using dict (most readable)
            >>> writer.write_row({"id": 1, "name": b"Alice"})
            >>>
            >>> # Using tuple (faster, schema order)
            >>> writer.write_row((2, b"Bob"))
            >>>
            >>> # Nullable fields can be None
            >>> writer.write_row({"id": 3, "name": None})
        """
        ...

    def write_rows(
        self, rows: Iterable[dict[str, Any] | list[Any] | tuple[Any, ...]]
    ) -> None:
        """Write multiple rows.

        Args:
            rows: An iterable of row data (dicts, lists, or tuples).

        Raises:
            ValidationError: If any row data doesn't match the schema.
            EncodingError: If encoding fails.

        Example:
            >>> # Write from a list
            >>> writer.write_rows([
            ...     {"id": 1, "name": b"Alice"},
            ...     {"id": 2, "name": b"Bob"},
            ... ])
            >>>
            >>> # Write from a generator (memory-efficient)
            >>> def generate_rows():
            ...     for i in range(1000):
            ...         yield {"id": i, "name": f"user_{i}".encode()}
            >>> writer.write_rows(generate_rows())
        """
        ...

    def write_row_bytes(self, data: bytes) -> None:
        """Write pre-encoded row bytes directly.

        This is useful for high-performance batching where rows are already
        encoded (e.g., from SeekableReader.current_row_bytes()).

        Args:
            data: The raw RowBinary-encoded row bytes.

        Raises:
            EncodingError: If writing fails.

        Example:
            >>> # Pass through rows from a compressed file without decoding
            >>> with SeekableReader.open("input.zst", schema=schema) as reader:
            ...     writer = RowBinaryWriter(schema)
            ...     writer.write_header()
            ...     while (row_bytes := reader.current_row_bytes()) is not None:
            ...         writer.write_row_bytes(row_bytes)
            ...         reader.seek_relative(1)
        """
        ...

    @property
    def rows_written(self) -> int:
        """The number of rows written."""
        ...

    def take(self) -> bytes:
        """Take the encoded bytes, resetting the writer.

        Returns:
            The encoded RowBinary data.
        """
        ...

    def finish(self) -> bytes:
        """Finish writing and return the encoded bytes.

        This is an alias for `take()` that makes the intent clearer
        when you're done writing.

        Returns:
            The encoded RowBinary data.
        """
        ...

    def __enter__(self) -> RowBinaryWriter: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...
    def __repr__(self) -> str: ...

# Reader class

class RowBinaryReader:
    """A reader for decoding RowBinary data.

    The reader decodes rows from bytes or a file. It supports both
    iteration and batch reading.

    Example:
        >>> schema = Schema.from_clickhouse([("id", "UInt32"), ("name", "String")])
        >>> reader = RowBinaryReader(data, schema)
        >>> for row in reader:
        ...     print(row["id"], row["name"])
    """

    def __init__(
        self,
        data: bytes,
        schema: Schema | None = None,
        format: Format = Format.RowBinary,
        string_mode: Literal["bytes", "str"] = "bytes",
    ) -> None:
        """Create a new RowBinary reader from bytes.

        Args:
            data: The RowBinary data as bytes.
            schema: The schema (required for RowBinary format, optional for
                RowBinaryWithNamesAndTypes which includes the schema).
            format: The RowBinary format variant (default: RowBinary).
            string_mode: How to handle string fields: "bytes" (default) or "str".

        Raises:
            SchemaError: If schema is required but not provided.
            DecodingError: If the data header is invalid.
        """
        ...

    @staticmethod
    def from_file(
        path: str | PathLike[str],
        schema: Schema | None = None,
        format: Format = Format.RowBinary,
        string_mode: Literal["bytes", "str"] = "bytes",
    ) -> RowBinaryReader:
        """Create a reader from a file path.

        Uses buffered I/O for efficient streaming of large files.

        Args:
            path: Path to the RowBinary file.
            schema: The schema (required for RowBinary format).
            format: The RowBinary format variant (default: RowBinary).
            string_mode: How to handle string fields: "bytes" (default) or "str".

        Returns:
            A new reader instance.

        Raises:
            IOError: If the file cannot be opened.
            SchemaError: If schema is required but not provided.

        Example:
            >>> from pathlib import Path
            >>> reader = RowBinaryReader.from_file(
            ...     Path("data.bin"),
            ...     schema=schema,
            ...     string_mode="str"
            ... )
            >>> for row in reader:
            ...     process(row)
        """
        ...

    @property
    def schema(self) -> Schema:
        """The schema used by this reader."""
        ...

    def read_row(self) -> Row | None:
        """Read a single row.

        Returns:
            The next row, or None if no more rows.

        Raises:
            DecodingError: If decoding fails.
        """
        ...

    def read_all(self) -> list[Row]:
        """Read all remaining rows.

        This method releases the GIL during decoding, allowing other
        Python threads to run while processing large datasets.

        Returns:
            All remaining rows.

        Raises:
            DecodingError: If decoding fails.

        Example:
            >>> reader = RowBinaryReader(data, schema)
            >>> rows = reader.read_all()
            >>> print(f"Loaded {len(rows)} rows")
            >>>
            >>> # Convert all to dicts for pandas/polars
            >>> dicts = [row.as_dict() for row in rows]
        """
        ...

    def __iter__(self) -> Iterator[Row]:
        """Iterate over rows."""
        ...

    def __next__(self) -> Row:
        """Return the next row.

        Raises:
            StopIteration: If no more rows.
            DecodingError: If decoding fails.
        """
        ...

    def __repr__(self) -> str: ...

# Seekable Writer class

class SeekableWriter:
    """A writer for creating Zstd-compressed RowBinary files with seek tables.

    This writer produces seekable Zstd files that can be efficiently read
    with random access using `SeekableReader`.

    Example:
        >>> schema = Schema.from_clickhouse([("id", "UInt64"), ("name", "String")])
        >>> with SeekableWriter.create("data.rowbinary.zst", schema) as writer:
        ...     writer.write_header()
        ...     writer.write_row({"id": 1, "name": b"Alice"})
        ...     writer.write_row({"id": 2, "name": b"Bob"})
        ... # finish() called automatically
    """

    @staticmethod
    def create(
        path: str | PathLike[str],
        schema: Schema,
        format: Format = Format.RowBinaryWithNamesAndTypes,
    ) -> SeekableWriter:
        """Create a new Zstd-compressed RowBinary file.

        Args:
            path: Path to the output file.
            schema: The schema defining the columns to write.
            format: The RowBinary format variant (default: RowBinaryWithNamesAndTypes).

        Returns:
            A new writer instance.

        Raises:
            IOError: If the file cannot be created.
            EncodingError: If the writer cannot be initialized.
        """
        ...

    def write_header(self) -> None:
        """Write the format header (if required by the format).

        Call this before writing any rows. For RowBinaryWithNames and
        RowBinaryWithNamesAndTypes formats, this writes the column
        names and/or types.

        Raises:
            RuntimeError: If the writer has already been finished.
            EncodingError: If writing the header fails.
        """
        ...

    def write_row(self, row: dict[str, Any] | list[Any] | tuple[Any, ...]) -> None:
        """Write a single row.

        Args:
            row: The row data as a dict, list, or tuple.
                - dict: Maps column names to values
                - list/tuple: Values in schema column order

        Raises:
            RuntimeError: If the writer has already been finished.
            ValidationError: If the row data doesn't match the schema.
            EncodingError: If encoding fails.
        """
        ...

    def write_rows(
        self, rows: Iterable[dict[str, Any] | list[Any] | tuple[Any, ...]]
    ) -> None:
        """Write multiple rows.

        Args:
            rows: An iterable of row data (dicts, lists, or tuples).

        Raises:
            RuntimeError: If the writer has already been finished.
            ValidationError: If any row data doesn't match the schema.
            EncodingError: If encoding fails.
        """
        ...

    def write_row_bytes(self, data: bytes) -> None:
        """Write pre-encoded row bytes directly.

        This is useful for high-performance batching where rows are already
        encoded (e.g., from SeekableReader.current_row_bytes()).

        Args:
            data: The raw RowBinary-encoded row bytes.

        Raises:
            RuntimeError: If the writer has already been finished.
            EncodingError: If writing fails.
        """
        ...

    def write_rows_bytes(self, data_list: list[bytes]) -> None:
        """Write multiple pre-encoded row bytes.

        Args:
            data_list: A list of raw RowBinary-encoded row bytes.

        Raises:
            RuntimeError: If the writer has already been finished.
            EncodingError: If writing fails.
        """
        ...

    @property
    def rows_written(self) -> int:
        """The number of rows written."""
        ...

    @property
    def compressed_bytes(self) -> int:
        """The number of compressed bytes written so far.

        Note: Call flush() first for an accurate count.
        """
        ...

    def flush(self) -> None:
        """Flush the underlying writer.

        Raises:
            RuntimeError: If the writer has already been finished.
            EncodingError: If flushing fails.
        """
        ...

    def finish(self) -> int:
        """Finalize the file and write the seek table.

        This MUST be called when done writing. The context manager
        calls this automatically.

        Returns:
            The total number of compressed bytes written.

        Raises:
            RuntimeError: If the writer has already been finished.
            EncodingError: If finalization fails.
        """
        ...

    def __enter__(self) -> SeekableWriter: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...
    def __repr__(self) -> str: ...

# Seekable Reader class

class SeekableReader:
    """A reader for Zstd-compressed RowBinary files with random access.

    This reader supports efficient seeking to any row in a compressed file.
    Files must be created with `SeekableWriter` to include the seek table.

    Important: `read_current()` does NOT advance the position. Use `seek_relative(1)`
    to move to the next row after reading.

    Example:
        >>> schema = Schema.from_clickhouse([("id", "UInt64"), ("name", "String")])
        >>> with SeekableReader.open("data.rowbinary.zst", schema=schema) as reader:
        ...     # Random access
        ...     reader.seek(1000)
        ...     row = reader.read_current()
        ...
        ...     # Sequential iteration
        ...     reader.seek(0)
        ...     for row in reader:
        ...         print(row["id"])
    """

    @staticmethod
    def open(
        path: str | PathLike[str],
        schema: Schema | None = None,
        format: Format = Format.RowBinaryWithNamesAndTypes,
        stride: int = 1024,
        string_mode: Literal["bytes", "str"] = "bytes",
    ) -> SeekableReader:
        """Open a Zstd-compressed RowBinary file.

        Args:
            path: Path to the compressed file.
            schema: The schema (required for RowBinary format, optional for
                RowBinaryWithNamesAndTypes which includes the schema in the header).
            format: The RowBinary format variant (default: RowBinaryWithNamesAndTypes).
            stride: Row index stride for seeking (default: 1024). Smaller values
                use more memory but make backward seeks faster.
            string_mode: How to handle string fields: "bytes" (default) or "str".

        Returns:
            A new reader instance.

        Raises:
            IOError: If the file cannot be opened.
            DecodingError: If the file is not valid seekable Zstd or header is invalid.
        """
        ...

    @property
    def schema(self) -> Schema:
        """The schema used by this reader."""
        ...

    @property
    def current_index(self) -> int:
        """The current row index."""
        ...

    def seek(self, index: int) -> None:
        """Seek to an absolute row index.

        Args:
            index: The row index to seek to (0-based).

        Raises:
            DecodingError: If the index is out of range.
        """
        ...

    def seek_relative(self, delta: int) -> None:
        """Seek relative to the current position.

        Args:
            delta: The number of rows to move (positive = forward, negative = backward).

        Raises:
            DecodingError: If the resulting index is out of range.
        """
        ...

    def seek_to_start(self) -> None:
        """Seek to the first row."""
        ...

    def current_row_bytes(self) -> bytes | None:
        """Return the raw bytes of the current row without decoding.

        This is useful for high-performance batching where you want to
        pass row bytes directly to another writer without decoding.

        Note: This does NOT advance the position.

        Returns:
            The raw row bytes, or None if at end of file.
        """
        ...

    def read_current(self) -> Row | None:
        """Read and decode the current row.

        Note: This does NOT advance the position. Use `seek_relative(1)` to
        move to the next row.

        Returns:
            The decoded row, or None if at end of file.

        Raises:
            DecodingError: If decoding fails.
        """
        ...

    def read_rows(self, count: int) -> list[Row]:
        """Read the next n rows as decoded Row objects.

        This is a convenience method that seeks forward after each read.

        Args:
            count: Maximum number of rows to read.

        Returns:
            The decoded rows (may be fewer than count if EOF reached).

        Raises:
            DecodingError: If decoding fails.
        """
        ...

    def __iter__(self) -> Iterator[Row]:
        """Iterate over rows."""
        ...

    def __next__(self) -> Row:
        """Return the next row and advance position.

        Raises:
            StopIteration: If no more rows.
            DecodingError: If decoding fails.
        """
        ...

    def __enter__(self) -> SeekableReader: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...
    def __repr__(self) -> str: ...
