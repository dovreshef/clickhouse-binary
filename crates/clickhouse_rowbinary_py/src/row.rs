//! Row class for Python bindings.

use std::sync::Arc;

use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyTuple},
};

use clickhouse_rowbinary::{Schema as RustSchema, Value};

use crate::{
    convert::{StringMode, value_to_python},
    errors::ValidationError,
};

/// A single row of RowBinary data.
///
/// Provides dict-like and attribute access to column values.
/// Column values are lazily converted to Python objects on access.
#[pyclass]
pub struct Row {
    pub(crate) schema: Arc<RustSchema>,
    pub(crate) values: Vec<Value>,
    pub(crate) string_mode: StringMode,
}

#[pymethods]
impl Row {
    /// Gets a value by column name or index.
    ///
    /// Args:
    ///     key: Either a string column name or integer index.
    ///
    /// Returns:
    ///     The value at the given column.
    ///
    /// Raises:
    ///     KeyError: If the column name is not found.
    ///     IndexError: If the index is out of range.
    fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if let Ok(index) = key.extract::<isize>() {
            let len = self.values.len() as isize;
            let idx = if index < 0 { len + index } else { index };

            if idx < 0 || idx >= len {
                return Err(pyo3::exceptions::PyIndexError::new_err(
                    "row index out of range",
                ));
            }

            let idx = idx as usize;
            let fields = self.schema.fields();
            value_to_python(py, &self.values[idx], &fields[idx].ty, self.string_mode)
        } else if let Ok(name) = key.extract::<&str>() {
            self.get_by_name(py, name)?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(name.to_string()))
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(
                "row indices must be integers or strings",
            ))
        }
    }

    /// Gets a value by column name, returning None if not found.
    ///
    /// Args:
    ///     key: The column name.
    ///     default: Value to return if key is not found (default: None).
    ///
    /// Returns:
    ///     The value or the default.
    #[pyo3(signature = (key, default = None))]
    fn get(
        &self,
        py: Python<'_>,
        key: &str,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Option<Py<PyAny>>> {
        match self.get_by_name(py, key)? {
            Some(v) => Ok(Some(v)),
            None => Ok(default),
        }
    }

    /// Gets a string value with explicit UTF-8 decoding.
    ///
    /// This method allows retrieving string fields with control over
    /// error handling, regardless of the reader's string_mode setting.
    ///
    /// Args:
    ///     key: The column name.
    ///     errors: Error handling mode: "strict" (default), "replace", or
    /// "ignore".
    ///
    /// Returns:
    ///     str: The decoded string value.
    ///
    /// Raises:
    ///     KeyError: If the column is not found.
    ///     ValidationError: If the value is not a string type.
    ///     UnicodeDecodeError: If decoding fails with errors="strict".
    #[pyo3(signature = (key, errors = "strict"))]
    fn get_str(&self, py: Python<'_>, key: &str, errors: &str) -> PyResult<Py<PyAny>> {
        let fields = self.schema.fields();
        let idx = fields
            .iter()
            .position(|f| f.name == key)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(key.to_string()))?;

        let value = &self.values[idx];
        let (Value::String(bytes) | Value::FixedString(bytes)) = value else {
            return Err(ValidationError::new_err(format!(
                "Column '{key}' is not a string type"
            )));
        };

        let s = match errors {
            "strict" => std::str::from_utf8(bytes)
                .map_err(|e| pyo3::exceptions::PyUnicodeDecodeError::new_err(e.to_string()))?
                .to_string(),
            "replace" => String::from_utf8_lossy(bytes).into_owned(),
            "ignore" => String::from_utf8_lossy(bytes)
                .chars()
                .filter(|c| *c != '\u{FFFD}')
                .collect(),
            _ => {
                return Err(ValidationError::new_err(format!(
                    "Invalid errors mode: '{errors}', expected 'strict', 'replace', or 'ignore'"
                )));
            }
        };

        Ok(s.into_pyobject(py)?.into_any().unbind())
    }

    /// Allows attribute-style access to columns.
    fn __getattr__(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        self.get_by_name(py, name)?.ok_or_else(|| {
            pyo3::exceptions::PyAttributeError::new_err(format!("Row has no column '{name}'"))
        })
    }

    /// Returns the row as a dictionary.
    ///
    /// Returns:
    ///     dict: A dictionary mapping column names to values.
    fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for (field, value) in self.schema.fields().iter().zip(self.values.iter()) {
            let py_val = value_to_python(py, value, &field.ty, self.string_mode)?;
            dict.set_item(&field.name, py_val)?;
        }
        Ok(dict.into_any().unbind())
    }

    /// Returns the row as a tuple.
    ///
    /// Returns:
    ///     tuple: A tuple of values in column order.
    fn as_tuple(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut values = Vec::with_capacity(self.values.len());
        for (field, value) in self.schema.fields().iter().zip(self.values.iter()) {
            values.push(value_to_python(py, value, &field.ty, self.string_mode)?);
        }
        let tuple = PyTuple::new(py, values)?;
        Ok(tuple.into_any().unbind())
    }

    /// Returns the column names.
    ///
    /// Returns:
    ///     list[str]: The column names.
    fn keys(&self) -> Vec<&str> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect()
    }

    /// Returns the values.
    ///
    /// Returns:
    ///     list: The column values.
    fn values(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::empty(py);
        for (field, value) in self.schema.fields().iter().zip(self.values.iter()) {
            let py_val = value_to_python(py, value, &field.ty, self.string_mode)?;
            list.append(py_val)?;
        }
        Ok(list.into_any().unbind())
    }

    /// Returns (name, value) pairs.
    ///
    /// Returns:
    ///     list[tuple[str, Any]]: List of (column_name, value) tuples.
    fn items(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::empty(py);
        for (field, value) in self.schema.fields().iter().zip(self.values.iter()) {
            let py_val = value_to_python(py, value, &field.ty, self.string_mode)?;
            let item = PyTuple::new(
                py,
                [
                    field.name.as_str().into_pyobject(py)?.into_any().unbind(),
                    py_val,
                ],
            )?;
            list.append(item)?;
        }
        Ok(list.into_any().unbind())
    }

    /// Returns the number of columns.
    fn __len__(&self) -> usize {
        self.values.len()
    }

    /// Checks if a column name exists.
    fn __contains__(&self, name: &str) -> bool {
        self.schema.fields().iter().any(|f| f.name == name)
    }

    /// Iterates over column names.
    fn __iter__(slf: PyRef<'_, Self>) -> RowKeyIter {
        RowKeyIter {
            schema: Arc::clone(&slf.schema),
            index: 0,
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let mut parts = Vec::new();
        for (field, value) in self.schema.fields().iter().zip(self.values.iter()) {
            let py_val = value_to_python(py, value, &field.ty, self.string_mode)?;
            parts.push(format!("{}={:?}", field.name, py_val));
        }
        Ok(format!("Row({})", parts.join(", ")))
    }
}

impl Row {
    fn get_by_name(&self, py: Python<'_>, name: &str) -> PyResult<Option<Py<PyAny>>> {
        for (idx, field) in self.schema.fields().iter().enumerate() {
            if field.name == name {
                let value = value_to_python(py, &self.values[idx], &field.ty, self.string_mode)?;
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
}

/// Iterator over row column names.
#[pyclass]
pub struct RowKeyIter {
    schema: Arc<RustSchema>,
    index: usize,
}

#[pymethods]
impl RowKeyIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<String> {
        let fields = slf.schema.fields();
        if slf.index < fields.len() {
            let name = fields[slf.index].name.clone();
            slf.index += 1;
            Some(name)
        } else {
            None
        }
    }
}
