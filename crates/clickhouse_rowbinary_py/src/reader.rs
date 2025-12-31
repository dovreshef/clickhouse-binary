//! RowBinary reader wrapper for Python.

use std::{
    fs::File,
    io::{BufReader, Cursor},
    path::PathBuf,
    sync::Arc,
};

use pyo3::{
    prelude::*,
    types::{PyBytes, PyList},
};

use clickhouse_rowbinary::{
    RowBinaryFormat as RustFormat, RowBinaryValueReader as RustReader, Schema as RustSchema, Value,
};

use crate::{convert::StringMode, errors::to_py_err, format::Format, row::Row, schema::Schema};

/// A reader for decoding RowBinary data.
///
/// The reader decodes rows from bytes or a file. It supports both
/// iteration and batch reading.
///
/// Example:
///     >>> schema = Schema.from_clickhouse([("id", "UInt32"), ("name",
/// "String")])     >>> reader = RowBinaryReader(data, schema)
///     >>> for row in reader:
///     ...     print(row["id"], row["name"])
#[pyclass]
pub struct RowBinaryReader {
    // Store the reader state (Option to allow taking for GIL release)
    state: Option<ReaderState>,
    schema: Arc<RustSchema>,
    string_mode: StringMode,
}

enum ReaderState {
    Bytes(RustReader<Cursor<Vec<u8>>>),
    File(RustReader<BufReader<File>>),
}

#[pymethods]
impl RowBinaryReader {
    /// Creates a new RowBinary reader from bytes.
    ///
    /// Args:
    ///     data: The RowBinary data as bytes.
    ///     schema: The schema (required for RowBinary format, optional for
    ///         RowBinaryWithNamesAndTypes which includes the schema).
    ///     format: The RowBinary format variant (default: RowBinary).
    ///     string_mode: How to handle string fields: "bytes" (default) or
    /// "str".
    ///
    /// Returns:
    ///     RowBinaryReader: A new reader instance.
    ///
    /// Raises:
    ///     SchemaError: If schema is required but not provided.
    ///     DecodingError: If the data header is invalid.
    #[new]
    #[pyo3(signature = (data, schema = None, format = Format::RowBinary, string_mode = "bytes"))]
    fn new(
        data: &Bound<'_, PyBytes>,
        schema: Option<Schema>,
        format: Format,
        string_mode: &str,
    ) -> PyResult<Self> {
        let string_mode = StringMode::from_str(string_mode)?;
        let rust_format: RustFormat = format.into();
        let bytes = data.as_bytes().to_vec();
        let cursor = Cursor::new(bytes);

        let (reader, schema_inner) = if let Some(s) = schema {
            let reader = RustReader::with_schema(cursor, rust_format, (*s.inner).clone())
                .map_err(to_py_err)?;
            (reader, s.inner)
        } else {
            let reader = RustReader::new(cursor, rust_format).map_err(to_py_err)?;
            // For formats that include schema in header, we'd need to get it from reader
            // For now, use empty schema for schemaless formats
            (reader, Arc::new(RustSchema::new(vec![])))
        };

        Ok(Self {
            state: Some(ReaderState::Bytes(reader)),
            schema: schema_inner,
            string_mode,
        })
    }

    /// Creates a reader from a file path.
    ///
    /// Args:
    ///     path: Path to the RowBinary file.
    ///     schema: The schema (required for RowBinary format).
    ///     format: The RowBinary format variant (default: RowBinary).
    ///     string_mode: How to handle string fields: "bytes" (default) or
    /// "str".
    ///
    /// Returns:
    ///     RowBinaryReader: A new reader instance.
    ///
    /// Raises:
    ///     IOError: If the file cannot be opened.
    ///     SchemaError: If schema is required but not provided.
    #[staticmethod]
    #[pyo3(signature = (path, schema = None, format = Format::RowBinary, string_mode = "bytes"))]
    fn from_file(
        path: PathBuf,
        schema: Option<Schema>,
        format: Format,
        string_mode: &str,
    ) -> PyResult<Self> {
        let string_mode = StringMode::from_str(string_mode)?;
        let rust_format: RustFormat = format.into();

        let file = File::open(&path)?;
        let buf_reader = BufReader::new(file);

        let (reader, schema_inner) = if let Some(s) = schema {
            let reader = RustReader::with_schema(buf_reader, rust_format, (*s.inner).clone())
                .map_err(to_py_err)?;
            (reader, s.inner)
        } else {
            let reader = RustReader::new(buf_reader, rust_format).map_err(to_py_err)?;
            (reader, Arc::new(RustSchema::new(vec![])))
        };

        Ok(Self {
            state: Some(ReaderState::File(reader)),
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

    /// Reads a single row.
    ///
    /// Returns:
    ///     Row | None: The next row, or None if no more rows.
    ///
    /// Raises:
    ///     DecodingError: If decoding fails.
    fn read_row(&mut self) -> PyResult<Option<Row>> {
        self.next_row()
    }

    /// Reads all remaining rows.
    ///
    /// This method releases the GIL during the pure-Rust decoding phase,
    /// allowing other Python threads to run while reading large datasets.
    ///
    /// Returns:
    ///     list[Row]: All remaining rows.
    ///
    /// Raises:
    ///     DecodingError: If decoding fails.
    fn read_all(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Take ownership of the reader state to release GIL during decoding
        let state = self.state.take();

        let Some(state) = state else {
            // Reader exhausted, return empty list
            return Ok(PyList::empty(py).into_any().unbind());
        };

        // Read all values without GIL (pure Rust decoding)
        let result: Result<(Vec<Vec<Value>>, ReaderState), clickhouse_rowbinary::Error> =
            py.allow_threads(|| read_all_values(state));

        let (all_values, state) = result.map_err(to_py_err)?;

        // Put the state back
        self.state = Some(state);

        // Convert to Python objects (requires GIL)
        let list = PyList::empty(py);
        for values in all_values {
            let row = Row {
                schema: Arc::clone(&self.schema),
                values,
                string_mode: self.string_mode,
            };
            list.append(row.into_pyobject(py)?)?;
        }
        Ok(list.into_any().unbind())
    }

    /// Iterator protocol.
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Iterator next.
    fn __next__(&mut self) -> PyResult<Option<Row>> {
        self.next_row()
    }

    fn __repr__(&self) -> String {
        format!(
            "RowBinaryReader(schema={:?}, string_mode={:?})",
            self.schema.fields().len(),
            match self.string_mode {
                StringMode::Bytes => "bytes",
                StringMode::Str => "str",
            }
        )
    }
}

impl RowBinaryReader {
    fn next_row(&mut self) -> PyResult<Option<Row>> {
        let Some(state) = &mut self.state else {
            return Ok(None); // Reader exhausted
        };

        let values = match state {
            ReaderState::Bytes(reader) => match reader.read_row().map_err(to_py_err)? {
                Some(values) => values,
                None => return Ok(None),
            },
            ReaderState::File(reader) => match reader.read_row().map_err(to_py_err)? {
                Some(values) => values,
                None => return Ok(None),
            },
        };

        Ok(Some(Row {
            schema: Arc::clone(&self.schema),
            values,
            string_mode: self.string_mode,
        }))
    }
}

/// Helper function to read all values from a reader state (pure Rust, no GIL
/// needed).
fn read_all_values(
    mut state: ReaderState,
) -> Result<(Vec<Vec<Value>>, ReaderState), clickhouse_rowbinary::Error> {
    let mut results = Vec::new();

    loop {
        let row = match &mut state {
            ReaderState::Bytes(reader) => reader.read_row()?,
            ReaderState::File(reader) => reader.read_row()?,
        };

        match row {
            Some(values) => results.push(values),
            None => break,
        }
    }

    Ok((results, state))
}
