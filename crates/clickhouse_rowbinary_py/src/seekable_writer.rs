//! Seekable Zstd writer for RowBinary data.

use std::{fs::File, io::BufWriter, path::PathBuf, sync::Arc};

use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyTuple},
};

use clickhouse_rowbinary::{
    RowBinaryFormat as RustFormat, RowBinaryWriter as RustWriter, Schema as RustSchema, Value,
};

use crate::{convert::python_to_value, errors::to_py_err, format::Format, schema::Schema};

/// A writer for creating Zstd-compressed RowBinary files with seek tables.
///
/// This writer produces seekable Zstd files that can be efficiently read
/// with random access using `SeekableReader`.
///
/// Example:
///     >>> schema = Schema.from_clickhouse([("id", "UInt64"), ("name",
/// "String")])     >>> with SeekableWriter.create("data.rowbinary.zst", schema)
/// as writer:     ...     writer.write_header()
///     ...     writer.write_row({"id": 1, "name": b"Alice"})
///     ...     writer.write_row({"id": 2, "name": b"Bob"})
///     ... # finish() called automatically
#[pyclass]
pub struct SeekableWriter {
    writer: Option<RustWriter<BufWriter<File>>>,
    schema: Arc<RustSchema>,
    rows_written: usize,
}

#[pymethods]
impl SeekableWriter {
    /// Creates a new Zstd-compressed RowBinary file.
    ///
    /// Args:
    ///     path: Path to the output file.
    ///     schema: The schema defining the columns to write.
    ///     format: The RowBinary format variant (default:
    /// RowBinaryWithNamesAndTypes).     compression_level: Zstd compression
    /// level 1-22 (default: 3).
    ///
    /// Returns:
    ///     SeekableWriter: A new writer instance.
    ///
    /// Raises:
    ///     IOError: If the file cannot be created.
    ///     EncodingError: If the writer cannot be initialized.
    #[staticmethod]
    #[pyo3(signature = (path, schema, format = Format::RowBinaryWithNamesAndTypes))]
    fn create(path: PathBuf, schema: &Schema, format: Format) -> PyResult<Self> {
        let rust_format: RustFormat = format.into();
        let file = File::create(&path)?;
        let buf_writer = BufWriter::new(file);
        let writer = RustWriter::new(buf_writer, rust_format).map_err(to_py_err)?;

        Ok(Self {
            writer: Some(writer),
            schema: Arc::clone(&schema.inner),
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
    ///     RuntimeError: If the writer has already been finished.
    ///     EncodingError: If writing the header fails.
    fn write_header(&mut self) -> PyResult<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Writer already finished"))?;
        writer.write_header(&self.schema).map_err(to_py_err)
    }

    /// Writes a single row.
    ///
    /// Args:
    ///     row: The row data as a dict, list, or tuple.
    ///         - dict: Maps column names to values
    ///         - list/tuple: Values in schema column order
    ///
    /// Raises:
    ///     RuntimeError: If the writer has already been finished.
    ///     ValidationError: If the row data doesn't match the schema.
    ///     EncodingError: If encoding fails.
    fn write_row(&mut self, py: Python<'_>, row: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.writer.is_none() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Writer already finished",
            ));
        }

        let values = self.row_to_values(py, row)?;
        let mut row_buf = Vec::new();
        {
            use clickhouse_rowbinary::RowBinaryValueWriter;
            let mut value_writer = RowBinaryValueWriter::new(
                &mut row_buf,
                RustFormat::RowBinary,
                (*self.schema).clone(),
            );
            value_writer.write_row(&values).map_err(to_py_err)?;
        }

        let writer = self.writer.as_mut().unwrap();
        writer.write_row_bytes(&row_buf).map_err(to_py_err)?;
        self.rows_written += 1;
        Ok(())
    }

    /// Writes multiple rows.
    ///
    /// Args:
    ///     rows: An iterable of row data (dicts, lists, or tuples).
    ///
    /// Raises:
    ///     RuntimeError: If the writer has already been finished.
    ///     ValidationError: If any row data doesn't match the schema.
    ///     EncodingError: If encoding fails.
    fn write_rows(&mut self, py: Python<'_>, rows: &Bound<'_, PyAny>) -> PyResult<()> {
        let iter = rows.try_iter()?;
        for row in iter {
            let row = row?;
            self.write_row(py, &row)?;
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
    ///     RuntimeError: If the writer has already been finished.
    ///     EncodingError: If writing fails.
    fn write_row_bytes(&mut self, data: &[u8]) -> PyResult<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Writer already finished"))?;
        writer.write_row_bytes(data).map_err(to_py_err)?;
        self.rows_written += 1;
        Ok(())
    }

    /// Writes multiple pre-encoded row bytes.
    ///
    /// Args:
    ///     data_list: A list of raw RowBinary-encoded row bytes.
    ///
    /// Raises:
    ///     RuntimeError: If the writer has already been finished.
    ///     EncodingError: If writing fails.
    fn write_rows_bytes(&mut self, data_list: &Bound<'_, PyList>) -> PyResult<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Writer already finished"))?;
        for item in data_list.iter() {
            let data: Vec<u8> = item.extract()?;
            writer.write_row_bytes(&data).map_err(to_py_err)?;
            self.rows_written += 1;
        }
        Ok(())
    }

    /// Returns the number of rows written.
    #[getter]
    fn rows_written(&self) -> usize {
        self.rows_written
    }

    /// Returns the number of compressed bytes written so far.
    ///
    /// Note: Call flush() first for an accurate count.
    #[getter]
    fn compressed_bytes(&self) -> PyResult<u64> {
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Writer already finished"))?;
        Ok(writer.compressed_bytes_written())
    }

    /// Flushes the underlying writer.
    ///
    /// Raises:
    ///     RuntimeError: If the writer has already been finished.
    ///     EncodingError: If flushing fails.
    fn flush(&mut self) -> PyResult<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Writer already finished"))?;
        writer.flush().map_err(to_py_err)
    }

    /// Finalizes the file and writes the seek table.
    ///
    /// This MUST be called when done writing. The context manager
    /// calls this automatically.
    ///
    /// Returns:
    ///     int: The total number of compressed bytes written.
    ///
    /// Raises:
    ///     RuntimeError: If the writer has already been finished.
    ///     EncodingError: If finalization fails.
    fn finish(&mut self, py: Python<'_>) -> PyResult<u64> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Writer already finished"))?;
        py.allow_threads(|| writer.finish()).map_err(to_py_err)
    }

    /// Context manager entry.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager exit - calls finish() if not already called.
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        if self.writer.is_some() {
            self.finish(py)?;
        }
        Ok(false) // Don't suppress exceptions
    }

    fn __repr__(&self) -> String {
        let status = if self.writer.is_some() {
            "open"
        } else {
            "finished"
        };
        format!(
            "SeekableWriter(rows_written={}, status={})",
            self.rows_written, status
        )
    }
}

impl SeekableWriter {
    fn row_to_values(&self, py: Python<'_>, row: &Bound<'_, PyAny>) -> PyResult<Vec<Value>> {
        let fields = self.schema.fields();

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
