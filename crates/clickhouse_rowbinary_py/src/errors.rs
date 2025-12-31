//! Exception types for Python bindings.
//!
//! This module defines the exception hierarchy exposed to Python:
//!
//! - `ClickHouseRowBinaryError` - Base exception for all library errors
//!   - `SchemaError` - Invalid type string or schema definition
//!   - `ValidationError` - Data doesn't match schema (wrong type, missing
//!     column)
//!   - `EncodingError` - Failed to encode value to RowBinary
//!   - `DecodingError` - Failed to decode RowBinary data (corrupted, truncated)

use pyo3::{create_exception, exceptions::PyException, prelude::*};

use clickhouse_rowbinary::Error as RustError;

// Base exception for all clickhouse_rowbinary errors.
// Useful for catching any error from this library.
create_exception!(clickhouse_rowbinary, ClickHouseRowBinaryError, PyException);

// Raised when a type string is invalid or malformed (e.g., "InvalidType").
create_exception!(clickhouse_rowbinary, SchemaError, ClickHouseRowBinaryError);

// Raised when row data doesn't match the expected schema.
// Examples: wrong type, missing column, wrong number of values.
create_exception!(
    clickhouse_rowbinary,
    ValidationError,
    ClickHouseRowBinaryError
);

// Raised when a value cannot be encoded to RowBinary.
// Examples: integer overflow, value out of range.
create_exception!(
    clickhouse_rowbinary,
    EncodingError,
    ClickHouseRowBinaryError
);

// Raised when RowBinary data cannot be decoded.
// Examples: truncated data, corrupted bytes, unexpected EOF.
create_exception!(
    clickhouse_rowbinary,
    DecodingError,
    ClickHouseRowBinaryError
);

/// Converts a Rust error to a Python exception.
pub fn to_py_err(err: RustError) -> PyErr {
    match &err {
        RustError::UnsupportedType(_) => SchemaError::new_err(err.to_string()),
        RustError::TypeMismatch { .. } | RustError::InvalidValue(_) => {
            ValidationError::new_err(err.to_string())
        }
        RustError::Io(_) => DecodingError::new_err(err.to_string()),
        RustError::Overflow(_)
        | RustError::Internal(_)
        | RustError::UnsupportedCombination(_)
        | RustError::Zstd(_) => ClickHouseRowBinaryError::new_err(err.to_string()),
    }
}
