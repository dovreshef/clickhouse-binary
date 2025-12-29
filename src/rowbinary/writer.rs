//! This module provides two writers:
//! - `RowBinaryValueWriter` encodes rows from `Value`s.
//! - `RowBinaryWriter` writes raw row bytes into a seekable Zstd stream.

use std::io::{BufWriter, Write};

use zeekstd::{Encoder, seek_table::Format};

use crate::{
    error::{Error, Result},
    io::{write_string, write_uvarint},
    types::TypeDesc,
    value::Value,
};

use super::{
    format::RowBinaryFormat,
    schema::{Row, Schema, ensure_nested_names, expand_schema_for_writing},
    value_rw::{write_nested_value, write_value},
};

/// `RowBinary` writer that streams rows into the provided writer.
pub struct RowBinaryValueWriter<W: Write> {
    inner: W,
    format: RowBinaryFormat,
    schema: Schema,
    wire_schema: Schema,
    header_written: bool,
}

impl<W: Write> RowBinaryValueWriter<W> {
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

    /// Creates a buffered writer for the specified format and schema.
    #[must_use]
    pub fn new_buffered(
        inner: W,
        format: RowBinaryFormat,
        schema: Schema,
    ) -> RowBinaryValueWriter<BufWriter<W>> {
        RowBinaryValueWriter::new(BufWriter::new(inner), format, schema)
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
    /// Call [`Self::write_header`] before writing the first row.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the row is invalid or IO fails.
    pub fn write_row(&mut self, row: &[Value]) -> Result<()> {
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

    /// Writes a single owned row.
    ///
    /// Call [`Self::write_header`] before writing the first row.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the row is invalid or IO fails.
    #[allow(clippy::needless_pass_by_value)]
    pub fn write_row_owned(&mut self, row: Row) -> Result<()> {
        self.write_row(&row)
    }

    /// Writes multiple rows.
    ///
    /// Call [`Self::write_header`] before writing the first row.
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

    /// Writes a raw `RowBinary` row payload (without header).
    ///
    /// Call [`Self::write_header`] before writing the first row.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the underlying writer fails.
    pub fn write_row_bytes(&mut self, row: &[u8]) -> Result<()> {
        self.inner.write_all(row)?;
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

    /// Replaces the inner writer and resets header state.
    pub fn reset(&mut self, inner: W) {
        self.inner = inner;
        self.header_written = false;
    }

    /// Takes the inner writer, replacing it with `Default::default()`.
    pub fn take_inner(&mut self) -> W
    where
        W: Default,
    {
        self.header_written = false;
        std::mem::take(&mut self.inner)
    }
}

/// Seekable Zstd writer that produces `RowBinary` payloads.
pub struct RowBinaryWriter<W: Write> {
    encoder: Encoder<'static, W>,
    format: RowBinaryFormat,
    header_written: bool,
    wrote_data: bool,
}

impl<W: Write> RowBinaryWriter<W> {
    /// Creates a new seekable writer for the specified format.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the encoder cannot be created.
    pub fn new(inner: W, format: RowBinaryFormat) -> Result<Self> {
        let encoder = Encoder::new(inner).map_err(Error::from)?;
        Ok(Self {
            encoder,
            format,
            header_written: false,
            wrote_data: false,
        })
    }

    /// Writes the `RowBinary` header at the start of the stream.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the header is written after data.
    pub fn write_header(&mut self, schema: &Schema) -> Result<()> {
        if self.header_written {
            return Ok(());
        }
        if self.wrote_data {
            return Err(Error::InvalidValue("header must be written before data"));
        }
        ensure_nested_names(schema)?;
        let wire_schema = expand_schema_for_writing(schema);
        match self.format {
            RowBinaryFormat::RowBinary => {}
            RowBinaryFormat::RowBinaryWithNames | RowBinaryFormat::RowBinaryWithNamesAndTypes => {
                crate::io::write_uvarint(wire_schema.len() as u64, &mut self.encoder)?;
                for field in wire_schema.fields() {
                    crate::io::write_string(&field.name, &mut self.encoder)?;
                }
                if self.format == RowBinaryFormat::RowBinaryWithNamesAndTypes {
                    for field in wire_schema.fields() {
                        crate::io::write_string(&field.ty.type_name(), &mut self.encoder)?;
                    }
                }
            }
        }
        self.header_written = true;
        Ok(())
    }

    /// Writes a single row payload as bytes.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when encoding fails.
    #[inline]
    pub fn write_row_bytes(&mut self, row: &[u8]) -> Result<()> {
        self.write_all_compressed(row)?;
        self.wrote_data = true;
        Ok(())
    }

    /// Writes multiple row payloads as bytes.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when encoding fails.
    #[inline]
    pub fn write_rows_bytes(&mut self, rows: &[u8]) -> Result<()> {
        self.write_row_bytes(rows)
    }

    /// Flushes the underlying encoder.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when flushing fails.
    pub fn flush(&mut self) -> Result<()> {
        self.encoder.flush().map_err(Error::from)
    }

    /// Returns the number of compressed bytes written so far.
    ///
    /// Call [`Self::flush`] first if you need an up-to-date value.
    pub fn compressed_bytes_written(&self) -> u64 {
        self.encoder.written_compressed()
    }

    /// Ends the current frame and writes the seek table (Foot format).
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when finalization fails.
    pub fn finish(self) -> Result<u64> {
        self.finish_format(Format::Foot)
    }

    /// Ends the current frame and writes the seek table in the specified
    /// format.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when finalization fails.
    pub fn finish_format(self, format: Format) -> Result<u64> {
        self.encoder.finish_format(format).map_err(Error::from)
    }

    fn write_all_compressed(&mut self, mut buf: &[u8]) -> Result<()> {
        while !buf.is_empty() {
            let consumed = self.encoder.compress(buf).map_err(Error::from)?;
            if consumed == 0 {
                return Err(Error::Internal("encoder made no progress"));
            }
            buf = &buf[consumed..];
        }
        Ok(())
    }
}
