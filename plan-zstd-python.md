# Plan: Zstd Compressed RowBinary Support for Python (Revised)

## Architecture Insight

The Rust crate has a **two-tier architecture** by design:

| Layer | Rust Type | Purpose | Returns |
|-------|-----------|---------|---------|
| **Value Layer** | `RowBinaryValueReader/Writer` | Encode/decode `Value` objects | `Row` (decoded) |
| **Byte Layer** | `RowBinaryReader/Writer` | Seekable compressed I/O | `&[u8]` (raw bytes) |

**Current Python bindings** only expose the Value Layer. We need to expose the Byte Layer for:
- Reading Zstd-compressed files with seeking
- Writing Zstd-compressed files with seek tables
- High-performance batch processing (pass raw bytes without re-encoding)

---

## Proposed Python API

### Naming Strategy

To avoid confusion with existing classes:

| Python Class | Wraps Rust Type | Purpose |
|--------------|-----------------|---------|
| `RowBinaryReader` (existing) | `RowBinaryValueReader` | Decode values from plain bytes/files |
| `RowBinaryWriter` (existing) | `RowBinaryValueWriter` | Encode values to plain bytes |
| `SeekableReader` (NEW) | `RowBinaryReader<S>` | Read compressed files with seeking |
| `SeekableWriter` (NEW) | `RowBinaryWriter<W>` | Write compressed files with seek tables |

### New Class: `SeekableReader`

For reading Zstd-compressed RowBinary files with random access:

```python
from clickhouse_rowbinary import SeekableReader, Schema, Format

# Open compressed file
reader = SeekableReader.open(
    "data.rowbinary.zst",
    schema=schema,                    # Optional if format has types
    format=Format.RowBinaryWithNamesAndTypes,
    stride=1024,                      # Row index stride (default: 1024)
)

# Properties
reader.schema        # Schema (from file or provided)
reader.row_count     # Total rows (None if unknown yet)
reader.current_index # Current row position

# Seeking
reader.seek(1000)           # Seek to absolute row index
reader.seek_relative(100)   # Seek forward/backward
reader.seek_to_start()      # Seek to first row

# Reading raw bytes (for batching/forwarding)
row_bytes: bytes = reader.current_row_bytes()

# Reading decoded row (convenience - combines with value decoding)
row: Row = reader.read_current()

# Iteration (sequential access)
for row in reader:
    print(row["id"])

# Batch reading with seeking
reader.seek(0)
batch = reader.read_rows(1000)  # Read next 1000 rows as list[Row]

# Context manager
with SeekableReader.open("data.zst", schema=schema) as reader:
    ...
```

### New Class: `SeekableWriter`

For writing Zstd-compressed RowBinary files with seek tables:

```python
from clickhouse_rowbinary import SeekableWriter, Schema, Format

# Create compressed file
writer = SeekableWriter.create(
    "output.rowbinary.zst",
    schema=schema,
    format=Format.RowBinaryWithNamesAndTypes,
    compression_level=3,              # Zstd level 1-22 (default: 3)
)

# Write header (required for WithNames/WithNamesAndTypes)
writer.write_header()

# Write pre-encoded row bytes (high-performance batching)
writer.write_row_bytes(row_bytes)
writer.write_rows_bytes([bytes1, bytes2, bytes3])

# Write values directly (convenience - encodes internally)
writer.write_row({"id": 1, "name": b"test"})
writer.write_rows([row1, row2, row3])

# Properties
writer.rows_written          # Count of rows written
writer.compressed_bytes      # Bytes written so far

# Finalize (REQUIRED - writes seek table)
writer.finish()

# Context manager (calls finish automatically)
with SeekableWriter.create("out.zst", schema=schema) as writer:
    writer.write_header()
    writer.write_rows(rows)
# finish() called automatically
```

### Bridging: Raw Bytes ↔ Decoded Values

For high-performance pipelines, users can work with raw bytes:

```python
# Read raw bytes from compressed file
with SeekableReader.open("input.zst", schema=schema) as reader:
    # Get raw bytes without decoding (fast)
    row_bytes = reader.current_row_bytes()

    # Decode only when needed
    row = RowBinaryReader(row_bytes, schema).read_row()

# Write raw bytes to compressed file
with SeekableWriter.create("output.zst", schema=schema) as writer:
    writer.write_header()

    # Write pre-encoded bytes (fast - no re-encoding)
    for row_bytes in raw_bytes_iterator:
        writer.write_row_bytes(row_bytes)
```

### Batch Processing Pattern

The killer use case - reading compressed, batching for HTTP insert:

```python
from clickhouse_rowbinary import (
    SeekableReader, RowBinaryWriter, Schema, Format
)
import httpx

schema = Schema.from_clickhouse([("id", "UInt64"), ("name", "String")])
BATCH_SIZE = 100_000

with SeekableReader.open("huge_file.rowbinary.zst", schema=schema) as reader:
    client = httpx.Client()

    while True:
        # Create batch with header
        batch_writer = RowBinaryWriter(schema, format=Format.RowBinaryWithNamesAndTypes)
        batch_writer.write_header()

        # Collect raw bytes (no decode/re-encode)
        count = 0
        for _ in range(BATCH_SIZE):
            row_bytes = reader.current_row_bytes()
            if row_bytes is None:
                break
            batch_writer.write_row_bytes(row_bytes)  # NEW method on existing writer
            count += 1
            reader.seek_relative(1)

        if count == 0:
            break

        # Send batch
        client.post(
            "http://localhost:8123/",
            params={"query": "INSERT INTO table FORMAT RowBinaryWithNamesAndTypes"},
            content=batch_writer.take(),
        )
        print(f"Inserted {count} rows")
```

---

## Implementation Tasks

### Task 1: Add `write_row_bytes` to existing `RowBinaryWriter`

**File:** `crates/clickhouse_rowbinary_py/src/writer.rs`

Add method to pass through raw bytes without encoding:

```rust
/// Write pre-encoded row bytes directly.
///
/// This is useful for high-performance batching where rows are already
/// encoded (e.g., from SeekableReader.current_row_bytes()).
#[pyo3(text_signature = "(self, data)")]
fn write_row_bytes(&mut self, data: &[u8]) -> PyResult<()> {
    self.inner.write_all(data).map_err(to_py_err)
}
```

**Estimated effort:** Small - just add one method

### Task 2: Implement `SeekableReader` PyClass

**File:** `crates/clickhouse_rowbinary_py/src/seekable_reader.rs` (new)

```rust
use pyo3::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use clickhouse_rowbinary::{
    RowBinaryReader as RustSeekableReader,
    RowBinaryFormat as RustFormat,
    Schema as RustSchema,
};

#[pyclass]
pub struct SeekableReader {
    reader: RustSeekableReader<BufReader<File>>,
    schema: Arc<RustSchema>,
    string_mode: StringMode,
}

#[pymethods]
impl SeekableReader {
    /// Open a compressed RowBinary file.
    #[staticmethod]
    #[pyo3(signature = (path, schema=None, format=Format::RowBinaryWithNamesAndTypes, stride=1024, string_mode="bytes"))]
    fn open(
        path: PathBuf,
        schema: Option<&Schema>,
        format: Format,
        stride: usize,
        string_mode: &str,
    ) -> PyResult<Self> { ... }

    /// Seek to absolute row index.
    fn seek(&mut self, index: usize) -> PyResult<()> {
        py.allow_threads(|| self.reader.seek_row(index))
            .map_err(to_py_err)
    }

    /// Seek relative to current position.
    fn seek_relative(&mut self, delta: i64) -> PyResult<()> {
        py.allow_threads(|| self.reader.seek_relative(delta))
            .map_err(to_py_err)
    }

    /// Get raw bytes of current row (no decoding).
    fn current_row_bytes<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match self.reader.current_row() {
            Ok(Some(bytes)) => Ok(Some(PyBytes::new(py, bytes))),
            Ok(None) => Ok(None),
            Err(e) => Err(to_py_err(e)),
        }
    }

    /// Read and decode current row.
    fn read_current(&mut self, py: Python<'_>) -> PyResult<Option<Row>> { ... }

    /// Read next n rows as decoded Row objects.
    fn read_rows(&mut self, py: Python<'_>, count: usize) -> PyResult<Vec<Row>> { ... }

    // Properties
    #[getter]
    fn schema(&self) -> Schema { ... }

    #[getter]
    fn current_index(&self) -> usize { ... }

    #[getter]
    fn row_count(&self) -> Option<usize> { ... }

    // Iterator protocol
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }
    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Row>> { ... }

    // Context manager
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }
    fn __exit__(&mut self, ...) -> bool { false }
}
```

**Performance considerations:**
- `py.allow_threads()` during seek operations
- Return `PyBytes` directly without copying where possible
- Lazy row count (only known after full scan)

**Estimated effort:** Medium - new PyClass with ~200 lines

### Task 3: Implement `SeekableWriter` PyClass

**File:** `crates/clickhouse_rowbinary_py/src/seekable_writer.rs` (new)

```rust
#[pyclass]
pub struct SeekableWriter {
    writer: Option<RustSeekableWriter<BufWriter<File>>>,
    schema: Arc<RustSchema>,
    path: PathBuf,
}

#[pymethods]
impl SeekableWriter {
    /// Create a new compressed RowBinary file.
    #[staticmethod]
    #[pyo3(signature = (path, schema, format=Format::RowBinaryWithNamesAndTypes, compression_level=3))]
    fn create(
        path: PathBuf,
        schema: &Schema,
        format: Format,
        compression_level: i32,
    ) -> PyResult<Self> { ... }

    /// Write format header.
    fn write_header(&mut self) -> PyResult<()> { ... }

    /// Write pre-encoded row bytes.
    fn write_row_bytes(&mut self, data: &[u8]) -> PyResult<()> { ... }

    /// Write multiple pre-encoded row bytes.
    fn write_rows_bytes(&mut self, data: Vec<&[u8]>) -> PyResult<()> { ... }

    /// Encode and write a row (convenience method).
    fn write_row(&mut self, py: Python<'_>, row: PyObject) -> PyResult<()> { ... }

    /// Finalize file (write seek table). REQUIRED.
    fn finish(&mut self) -> PyResult<()> {
        let writer = self.writer.take()
            .ok_or_else(|| PyRuntimeError::new_err("Writer already finished"))?;
        py.allow_threads(|| writer.finish())
            .map_err(to_py_err)
    }

    // Properties
    #[getter]
    fn rows_written(&self) -> usize { ... }

    #[getter]
    fn compressed_bytes(&self) -> u64 { ... }

    // Context manager (calls finish on exit)
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }
    fn __exit__(&mut self, ...) -> PyResult<bool> {
        if self.writer.is_some() {
            self.finish()?;
        }
        Ok(false)
    }
}
```

**Performance considerations:**
- `BufWriter` for buffered I/O
- `py.allow_threads()` during `finish()` (writes seek table)
- `Option<Writer>` pattern to enforce single `finish()` call

**Estimated effort:** Medium - new PyClass with ~150 lines

### Task 4: Register New Classes in Module

**File:** `crates/clickhouse_rowbinary_py/src/lib.rs`

```rust
mod seekable_reader;
mod seekable_writer;

use seekable_reader::SeekableReader;
use seekable_writer::SeekableWriter;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Existing
    m.add_class::<Schema>()?;
    m.add_class::<Column>()?;
    m.add_class::<Row>()?;
    m.add_class::<RowBinaryWriter>()?;
    m.add_class::<RowBinaryReader>()?;
    m.add_class::<Format>()?;

    // New
    m.add_class::<SeekableReader>()?;
    m.add_class::<SeekableWriter>()?;

    // Exceptions...
}
```

### Task 5: Update Python `__init__.py`

**File:** `python/clickhouse_rowbinary/__init__.py`

```python
from ._core import (
    # Existing
    Schema, Column, Row, RowBinaryWriter, RowBinaryReader, Format,
    ClickHouseRowBinaryError, SchemaError, ValidationError, EncodingError, DecodingError,
    # New
    SeekableReader,
    SeekableWriter,
)

__all__ = [
    # Existing...
    # New
    "SeekableReader",
    "SeekableWriter",
]
```

### Task 6: Update Type Stubs

**File:** `python/clickhouse_rowbinary/__init__.pyi`

Add full type definitions for new classes with docstrings and examples.

### Task 7: Documentation

**File:** `python/README.md`

Add new section:

```markdown
## Compressed Files (Zstd)

### Reading Compressed Files

```python
from clickhouse_rowbinary import SeekableReader, Schema

schema = Schema.from_clickhouse([("id", "UInt64"), ("name", "String")])

with SeekableReader.open("data.rowbinary.zst", schema=schema) as reader:
    # Sequential reading
    for row in reader:
        print(row["id"])

    # Random access
    reader.seek(1000)
    row = reader.read_current()

    # Batch reading
    reader.seek(0)
    batch = reader.read_rows(10000)
```

### Writing Compressed Files

```python
from clickhouse_rowbinary import SeekableWriter, Schema, Format

schema = Schema.from_clickhouse([("id", "UInt64"), ("name", "String")])

with SeekableWriter.create("out.rowbinary.zst", schema=schema) as writer:
    writer.write_header()
    writer.write_row({"id": 1, "name": b"Alice"})
    writer.write_rows([{"id": 2, "name": b"Bob"}, {"id": 3, "name": b"Charlie"}])
# finish() called automatically

### High-Performance Batch Processing

For maximum throughput, work with raw bytes:

```python
# Copy rows between compressed files without decode/re-encode
with SeekableReader.open("input.zst", schema=schema) as reader:
    with SeekableWriter.create("output.zst", schema=schema) as writer:
        writer.write_header()
        while (row_bytes := reader.current_row_bytes()) is not None:
            writer.write_row_bytes(row_bytes)
            reader.seek_relative(1)
```
```

**File:** `USAGE.md`

Add Python examples in the Python section mirroring the Rust examples.

### Task 8: Tests

**File:** `tests/python/test_seekable.py`

```python
import tempfile
from pathlib import Path
import pytest

from clickhouse_rowbinary import (
    Schema, SeekableReader, SeekableWriter, Format,
    RowBinaryWriter, RowBinaryReader,
)

class TestSeekableWriter:
    def test_create_and_write(self):
        """Write rows to compressed file."""

    def test_context_manager_calls_finish(self):
        """Context manager finalizes file."""

    def test_compression_levels(self):
        """Different compression levels work."""

    def test_write_row_bytes(self):
        """Can write pre-encoded bytes."""

class TestSeekableReader:
    def test_open_and_read(self):
        """Read rows from compressed file."""

    def test_seek_absolute(self):
        """Seek to specific row index."""

    def test_seek_relative(self):
        """Seek forward and backward."""

    def test_current_row_bytes(self):
        """Get raw bytes without decoding."""

    def test_iteration(self):
        """Iterator protocol works."""

    def test_read_rows_batch(self):
        """Batch reading works."""

class TestRoundtrip:
    def test_write_read_roundtrip(self):
        """Write then read produces same data."""

    def test_large_file(self):
        """Handle 100k+ rows efficiently."""

    def test_all_types(self):
        """All supported types roundtrip correctly."""

class TestBatchProcessing:
    def test_raw_bytes_passthrough(self):
        """Raw bytes can pass through without decode/re-encode."""

    def test_rebatch_pattern(self):
        """Read compressed, rebatch to plain format."""
```

### Task 9: Benchmarks

**File:** `tests/python/bench_seekable.py`

```python
import pytest
import tempfile
from clickhouse_rowbinary import Schema, SeekableWriter, SeekableReader, RowBinaryWriter

@pytest.fixture
def large_compressed_file():
    """Create 1M row compressed file for benchmarks."""
    ...

def test_write_throughput(benchmark, tmp_path):
    """Benchmark: rows/sec for compressed writing."""

def test_read_sequential_throughput(benchmark, large_compressed_file):
    """Benchmark: rows/sec for sequential reading."""

def test_seek_latency(benchmark, large_compressed_file):
    """Benchmark: time to seek to random row."""

def test_raw_bytes_vs_decoded(benchmark, large_compressed_file):
    """Compare: current_row_bytes() vs read_current()."""
```

---

## Implementation Order

1. **Task 1:** Add `write_row_bytes` to existing `RowBinaryWriter` (enables batching pattern)
2. **Task 3:** Implement `SeekableWriter` (simpler, no seeking logic)
3. **Task 2:** Implement `SeekableReader` (more complex, seeking logic)
4. **Task 4:** Register in module
5. **Task 5:** Update `__init__.py`
6. **Task 6:** Type stubs
7. **Task 8:** Tests
8. **Task 7:** Documentation
9. **Task 9:** Benchmarks

---

## Success Criteria

| Metric | Target |
|--------|--------|
| Write 1M rows to Zstd | < 5 seconds |
| Read 1M rows from Zstd | < 3 seconds |
| Seek to random row | < 10ms |
| Memory for 10M row file | < 100MB |
| Raw bytes passthrough | No decode/re-encode overhead |
| Existing tests | All pass (no regressions) |

---

## Backwards Compatibility

- Existing `RowBinaryReader`/`RowBinaryWriter` unchanged
- New classes are additive
- Only addition to existing class: `write_row_bytes()` method

---

## Open Questions

1. **Should `SeekableReader.read_current()` advance position?**
   - Option A: No, explicit `seek_relative(1)` required (matches Rust)
   - Option B: Yes, like standard file read (more Pythonic)
   - **Recommendation:** Option A for consistency with Rust API

2. **Should we support reading plain (non-compressed) files with `SeekableReader`?**
   - The Rust `RowBinaryReader` requires `Seekable` which implies zeekstd
   - Could add a `compression="auto"|"zstd"|"none"` parameter
   - **Recommendation:** Start with Zstd-only, add plain later if needed

3. **Naming: `SeekableReader` vs `CompressedReader` vs `ZstdReader`?**
   - `SeekableReader` emphasizes the seeking capability
   - `CompressedReader` emphasizes the compression
   - **Recommendation:** `SeekableReader` - the seeking is the key differentiator
