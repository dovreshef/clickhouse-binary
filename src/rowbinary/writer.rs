//! `RowBinary` writer implementation.

use std::io::Write;

use crate::{
    error::{Error, Result},
    io::{write_string, write_uvarint},
    types::TypeDesc,
    value::Value,
};

use super::{
    format::RowBinaryFormat,
    schema::{Schema, ensure_nested_names, expand_schema_for_writing},
    value_rw::{write_nested_value, write_value},
};

/// `RowBinary` writer that streams rows into the provided writer.
pub struct RowBinaryWriter<W: Write> {
    inner: W,
    format: RowBinaryFormat,
    schema: Schema,
    wire_schema: Schema,
    header_written: bool,
}

impl<W: Write> RowBinaryWriter<W> {
    /// Creates a writer for the specified format and schema.
    #[must_use]
    pub fn new(inner: W, format: RowBinaryFormat, schema: Schema) -> Self {
        let wire_schema = expand_schema_for_writing(&schema);
        Self {
            inner,
            format,
            schema,
            wire_schema,
            header_written: false,
        }
    }

    /// Writes the `RowBinary` header (names/types) when required.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the underlying writer fails.
    pub fn write_header(&mut self) -> Result<()> {
        if self.header_written {
            return Ok(());
        }
        ensure_nested_names(&self.schema)?;
        match self.format {
            RowBinaryFormat::RowBinary => {}
            RowBinaryFormat::RowBinaryWithNames | RowBinaryFormat::RowBinaryWithNamesAndTypes => {
                write_uvarint(self.wire_schema.len() as u64, &mut self.inner)?;
                for field in self.wire_schema.fields() {
                    write_string(&field.name, &mut self.inner)?;
                }
                if self.format == RowBinaryFormat::RowBinaryWithNamesAndTypes {
                    for field in self.wire_schema.fields() {
                        write_string(&field.ty.type_name(), &mut self.inner)?;
                    }
                }
            }
        }
        self.header_written = true;
        Ok(())
    }

    /// Writes a single row.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the row is invalid or IO fails.
    pub fn write_row(&mut self, row: &[Value]) -> Result<()> {
        self.write_header()?;
        if row.len() != self.schema.len() {
            return Err(Error::InvalidValue("row length does not match schema"));
        }
        for (field, value) in self.schema.fields().iter().zip(row.iter()) {
            match &field.ty {
                TypeDesc::Nested(items) => {
                    write_nested_value(items, value, &mut self.inner)?;
                }
                _ => write_value(&field.ty, value, &mut self.inner)?,
            }
        }
        Ok(())
    }

    /// Writes multiple rows.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when any row is invalid or IO fails.
    pub fn write_rows<I, R>(&mut self, rows: I) -> Result<()>
    where
        I: IntoIterator<Item = R>,
        R: AsRef<[Value]>,
    {
        for row in rows {
            self.write_row(row.as_ref())?;
        }
        Ok(())
    }

    /// Flushes the underlying writer.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the flush fails.
    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush().map_err(Error::Io)
    }

    /// Returns the inner writer.
    pub fn into_inner(self) -> W {
        self.inner
    }
}
