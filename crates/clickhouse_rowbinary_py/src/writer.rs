//! RowBinary writer wrapper for Python.

use pyo3::{
    prelude::*,
    types::{PyBytes, PyDict, PyList, PyTuple},
};

use clickhouse_rowbinary::{
    RowBinaryFormat as RustFormat, RowBinaryValueWriter as RustWriter, Value,
};

use crate::{convert::python_to_value, errors::to_py_err, format::Format, schema::Schema};

/// A writer for encoding rows into RowBinary format.
///
/// The writer encodes rows into an in-memory buffer. Use `take()` to
/// retrieve the encoded bytes.
///
/// Example:
///     >>> schema = Schema.from_clickhouse([("id", "UInt32"), ("name",
/// "String")])     >>> writer = RowBinaryWriter(schema)
///     >>> writer.write_row({"id": 1, "name": b"Alice"})
///     >>> writer.write_row([2, b"Bob"])
///     >>> data = writer.take()
#[pyclass]
pub struct RowBinaryWriter {
    inner: RustWriter<Vec<u8>>,
    schema: Schema,
    rows_written: usize,
}

#[pymethods]
impl RowBinaryWriter {
    /// Creates a new RowBinary writer.
    ///
    /// Args:
    ///     schema: The schema defining the columns to write.
    ///     format: The RowBinary format variant (default: RowBinary).
    ///
    /// Returns:
    ///     RowBinaryWriter: A new writer instance.
    #[new]
    #[pyo3(signature = (schema, format = Format::RowBinary))]
    fn new(schema: Schema, format: Format) -> PyResult<Self> {
        let rust_format: RustFormat = format.into();
        let inner = RustWriter::new(Vec::new(), rust_format, (*schema.inner).clone());

        Ok(Self {
            inner,
            schema,
            rows_written: 0,
        })
    }

    /// Writes the format header (if required by the format).
    ///
    /// Call this before writing any rows. For RowBinaryWithNames and
    /// RowBinaryWithNamesAndTypes formats, this writes the column
    /// names and/or types.
    ///
    /// Raises:
    ///     EncodingError: If writing the header fails.
    fn write_header(&mut self) -> PyResult<()> {
        self.inner.write_header().map_err(to_py_err)
    }

    /// Writes a single row.
    ///
    /// Args:
    ///     row: The row data as a dict, list, or tuple.
    ///         - dict: Maps column names to values
    ///         - list/tuple: Values in schema column order
    ///
    /// Raises:
    ///     ValidationError: If the row data doesn't match the schema.
    ///     EncodingError: If encoding fails.
    fn write_row(&mut self, py: Python<'_>, row: &Bound<'_, PyAny>) -> PyResult<()> {
        let values = self.row_to_values(py, row)?;
        self.inner.write_row(&values).map_err(to_py_err)?;
        self.rows_written += 1;
        Ok(())
    }

    /// Writes multiple rows.
    ///
    /// Args:
    ///     rows: An iterable of row data (dicts, lists, or tuples).
    ///
    /// Raises:
    ///     ValidationError: If any row data doesn't match the schema.
    ///     EncodingError: If encoding fails.
    fn write_rows(&mut self, py: Python<'_>, rows: &Bound<'_, PyAny>) -> PyResult<()> {
        let iter = rows.try_iter()?;
        for row in iter {
            let row = row?;
            let values = self.row_to_values(py, &row)?;
            self.inner.write_row(&values).map_err(to_py_err)?;
            self.rows_written += 1;
        }
        Ok(())
    }

    /// Writes pre-encoded row bytes directly.
    ///
    /// This is useful for high-performance batching where rows are already
    /// encoded (e.g., from SeekableReader.current_row_bytes()).
    ///
    /// Args:
    ///     data: The raw RowBinary-encoded row bytes.
    ///
    /// Raises:
    ///     EncodingError: If writing fails.
    fn write_row_bytes(&mut self, data: &[u8]) -> PyResult<()> {
        self.inner.write_row_bytes(data).map_err(to_py_err)?;
        self.rows_written += 1;
        Ok(())
    }

    /// Returns the number of rows written.
    #[getter]
    fn rows_written(&self) -> usize {
        self.rows_written
    }

    /// Takes the encoded bytes, resetting the writer.
    ///
    /// Returns:
    ///     bytes: The encoded RowBinary data.
    fn take(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let data = self.inner.take_inner();
        self.rows_written = 0;
        Ok(PyBytes::new(py, &data).into_any().unbind())
    }

    /// Finishes writing and returns the encoded bytes.
    ///
    /// This is an alias for `take()` that makes the intent clearer
    /// when you're done writing.
    ///
    /// Returns:
    ///     bytes: The encoded RowBinary data.
    fn finish(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.take(py)
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
        // Don't suppress exceptions
        false
    }

    fn __repr__(&self) -> String {
        format!("RowBinaryWriter(rows_written={})", self.rows_written,)
    }
}

impl RowBinaryWriter {
    fn row_to_values(&self, py: Python<'_>, row: &Bound<'_, PyAny>) -> PyResult<Vec<Value>> {
        let fields = self.schema.inner.fields();

        if let Ok(dict) = row.downcast::<PyDict>() {
            // Dict: map by name
            let mut values = Vec::with_capacity(fields.len());
            for field in fields {
                let item = dict.get_item(&field.name)?.ok_or_else(|| {
                    pyo3::exceptions::PyKeyError::new_err(format!(
                        "Missing column '{}' in row",
                        field.name
                    ))
                })?;
                values.push(python_to_value(py, &item, &field.ty)?);
            }
            Ok(values)
        } else if let Ok(list) = row.downcast::<PyList>() {
            // List: positional
            if list.len() != fields.len() {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Row has {} values, expected {}",
                    list.len(),
                    fields.len()
                )));
            }
            let mut values = Vec::with_capacity(fields.len());
            for (item, field) in list.iter().zip(fields.iter()) {
                values.push(python_to_value(py, &item, &field.ty)?);
            }
            Ok(values)
        } else if let Ok(tuple) = row.downcast::<PyTuple>() {
            // Tuple: positional
            if tuple.len() != fields.len() {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Row has {} values, expected {}",
                    tuple.len(),
                    fields.len()
                )));
            }
            let mut values = Vec::with_capacity(fields.len());
            for (item, field) in tuple.iter().zip(fields.iter()) {
                values.push(python_to_value(py, &item, &field.ty)?);
            }
            Ok(values)
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(
                "Row must be a dict, list, or tuple",
            ))
        }
    }
}
