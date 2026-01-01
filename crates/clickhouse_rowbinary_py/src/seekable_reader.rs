//! Seekable Zstd reader for RowBinary data.

use std::{fs::File, path::PathBuf, sync::Arc};

use pyo3::{prelude::*, types::PyBytes};

use clickhouse_rowbinary::{
    RowBinaryFormat as RustFormat, RowBinaryReader as RustReader,
    RowBinaryValueReader as RustValueReader, Schema as RustSchema,
};

use crate::{convert::StringMode, errors::to_py_err, format::Format, row::Row, schema::Schema};

/// A reader for Zstd-compressed RowBinary files with random access.
///
/// This reader supports efficient seeking to any row in a compressed file.
/// Files must be created with `SeekableWriter` to include the seek table.
///
/// Important: `read_current()` does NOT advance the position. Use
/// `seek_relative(1)` to move to the next row after reading.
///
/// Example:
///     >>> schema = Schema.from_clickhouse([("id", "UInt64"), ("name",
/// "String")])     >>> with SeekableReader.open("data.rowbinary.zst",
/// schema=schema) as reader:     ...     # Random access
///     ...     reader.seek(1000)
///     ...     row = reader.read_current()
///     ...
///     ...     # Sequential iteration
///     ...     reader.seek(0)
///     ...     for row in reader:
///     ...         print(row["id"])
#[pyclass]
pub struct SeekableReader {
    reader: RustReader<File>,
    schema: Arc<RustSchema>,
    string_mode: StringMode,
}

#[pymethods]
impl SeekableReader {
    /// Opens a Zstd-compressed RowBinary file.
    ///
    /// Args:
    ///     path: Path to the compressed file.
    ///     schema: The schema (required for RowBinary format, optional for
    ///         RowBinaryWithNamesAndTypes which includes the schema in the
    /// header).     format: The RowBinary format variant (default:
    /// RowBinaryWithNamesAndTypes).     stride: Row index stride for
    /// seeking (default: 1024). Smaller values         use more memory but
    /// make backward seeks faster.     string_mode: How to handle string
    /// fields: "bytes" (default) or "str".
    ///
    /// Returns:
    ///     SeekableReader: A new reader instance.
    ///
    /// Raises:
    ///     IOError: If the file cannot be opened.
    ///     DecodingError: If the file is not valid seekable Zstd or header is
    /// invalid.
    #[staticmethod]
    #[pyo3(signature = (path, schema = None, format = Format::RowBinaryWithNamesAndTypes, stride = 1024, string_mode = "bytes"))]
    fn open(
        path: PathBuf,
        schema: Option<&Schema>,
        format: Format,
        stride: usize,
        string_mode: &str,
    ) -> PyResult<Self> {
        let string_mode = StringMode::from_str(string_mode)?;
        let rust_format: RustFormat = format.into();

        let file = File::open(&path)?;
        let rust_schema = schema.map(|s| (*s.inner).clone());

        let reader = RustReader::new_with_stride(file, rust_format, rust_schema, stride)
            .map_err(to_py_err)?;

        // Get schema from reader header if not provided
        let schema_inner = if let Some(s) = schema {
            Arc::clone(&s.inner)
        } else if let Some(header) = reader.header() {
            if let Some(types) = &header.types {
                let fields: Vec<_> = header
                    .names
                    .iter()
                    .zip(types.iter())
                    .map(|(name, ty)| clickhouse_rowbinary::Field {
                        name: name.clone(),
                        ty: ty.clone(),
                    })
                    .collect();
                Arc::new(RustSchema::new(fields))
            } else {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Schema required: format does not include types in header",
                ));
            }
        } else {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Schema required for this format",
            ));
        };

        Ok(Self {
            reader,
            schema: schema_inner,
            string_mode,
        })
    }

    /// Returns the schema.
    #[getter]
    fn schema(&self) -> Schema {
        Schema {
            inner: Arc::clone(&self.schema),
        }
    }

    /// Returns the current row index.
    #[getter]
    fn current_index(&self) -> usize {
        // The reader tracks this internally but doesn't expose it publicly
        // For now, we'll need to track it ourselves or update the Rust API
        // This is a placeholder - actual implementation depends on Rust API
        0 // TODO: Expose current_row from Rust reader
    }

    /// Seeks to an absolute row index.
    ///
    /// Args:
    ///     index: The row index to seek to (0-based).
    ///
    /// Raises:
    ///     DecodingError: If the index is out of range.
    fn seek(&mut self, py: Python<'_>, index: usize) -> PyResult<()> {
        py.allow_threads(|| self.reader.seek_row(index))
            .map_err(to_py_err)
    }

    /// Seeks relative to the current position.
    ///
    /// Args:
    ///     delta: The number of rows to move (positive = forward, negative =
    /// backward).
    ///
    /// Raises:
    ///     DecodingError: If the resulting index is out of range.
    fn seek_relative(&mut self, py: Python<'_>, delta: i64) -> PyResult<()> {
        py.allow_threads(|| self.reader.seek_relative(delta))
            .map_err(to_py_err)
    }

    /// Seeks to the first row.
    fn seek_to_start(&mut self, py: Python<'_>) -> PyResult<()> {
        self.seek(py, 0)
    }

    /// Returns the raw bytes of the current row without decoding.
    ///
    /// This is useful for high-performance batching where you want to
    /// pass row bytes directly to another writer without decoding.
    ///
    /// Note: This does NOT advance the position.
    ///
    /// Returns:
    ///     bytes | None: The raw row bytes, or None if at end of file.
    fn current_row_bytes<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match self.reader.current_row().map_err(to_py_err)? {
            Some(bytes) => Ok(Some(PyBytes::new(py, bytes))),
            None => Ok(None),
        }
    }

    /// Reads and decodes the current row.
    ///
    /// Note: This does NOT advance the position. Use `seek_relative(1)` to
    /// move to the next row.
    ///
    /// Returns:
    ///     Row | None: The decoded row, or None if at end of file.
    ///
    /// Raises:
    ///     DecodingError: If decoding fails.
    fn read_current(&mut self, _py: Python<'_>) -> PyResult<Option<Row>> {
        let Some(bytes) = self.reader.current_row().map_err(to_py_err)? else {
            return Ok(None);
        };

        // Decode the raw bytes using RowBinaryValueReader
        let mut cursor = std::io::Cursor::new(bytes);
        let mut value_reader = RustValueReader::with_schema(
            &mut cursor,
            RustFormat::RowBinary,
            (*self.schema).clone(),
        )
        .map_err(to_py_err)?;

        match value_reader.read_row().map_err(to_py_err)? {
            Some(values) => Ok(Some(Row {
                schema: Arc::clone(&self.schema),
                values,
                string_mode: self.string_mode,
            })),
            None => Ok(None),
        }
    }

    /// Reads the next n rows as decoded Row objects.
    ///
    /// This is a convenience method that seeks forward after each read.
    ///
    /// Args:
    ///     count: Maximum number of rows to read.
    ///
    /// Returns:
    ///     list[Row]: The decoded rows (may be fewer than count if EOF
    /// reached).
    ///
    /// Raises:
    ///     DecodingError: If decoding fails.
    fn read_rows(&mut self, py: Python<'_>, count: usize) -> PyResult<Vec<Row>> {
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            match self.read_current(py)? {
                Some(row) => {
                    rows.push(row);
                    // Move to next row
                    if self.reader.seek_relative(1).is_err() {
                        break; // EOF or error
                    }
                }
                None => break,
            }
        }
        Ok(rows)
    }

    /// Iterator protocol.
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Iterator next - reads current and advances.
    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Row>> {
        let row = self.read_current(py)?;
        if row.is_some() {
            // Advance to next row (ignore error at EOF)
            let _ = self.reader.seek_relative(1);
        }
        Ok(row)
    }

    /// Context manager entry.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager exit.
    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        false // Don't suppress exceptions
    }

    fn __repr__(&self) -> String {
        format!(
            "SeekableReader(schema={:?}, string_mode={:?})",
            self.schema.fields().len(),
            match self.string_mode {
                StringMode::Bytes => "bytes",
                StringMode::Str => "str",
            }
        )
    }
}
