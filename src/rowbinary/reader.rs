//! This module provides two readers:
//! - `RowBinaryValueReader` decodes rows into `Value`s.
//! - `RowBinaryReader` scans seekable streams and exposes raw row bytes.

use std::io::{self, Read, Seek, SeekFrom};

use zeekstd::{Decoder, Seekable};

use crate::{
    error::{Error, Result},
    io::{read_string, read_uvarint},
    types::{TypeDesc, parse_type_desc},
};

use super::{
    format::RowBinaryFormat,
    scan::{CaptureReader, skip_value_optional, skip_value_required},
    schema::{Field, Row, Schema},
    value_rw::{read_value_optional, read_value_required},
};

/// `RowBinary` reader that streams rows from the provided reader.
pub struct RowBinaryValueReader<R: Read> {
    inner: R,
    schema: Schema,
    header: Option<RowBinaryHeader>,
}

impl<R: Read> RowBinaryValueReader<R> {
    /// Creates a reader without a pre-defined schema.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when header parsing fails.
    pub fn new(inner: R, format: RowBinaryFormat) -> Result<Self> {
        Self::with_schema_optional(inner, format, None)
    }

    /// Creates a reader with an expected schema.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when header parsing fails.
    pub fn with_schema(inner: R, format: RowBinaryFormat, schema: Schema) -> Result<Self> {
        Self::with_schema_optional(inner, format, Some(schema))
    }

    /// Reads the next row.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when decoding fails or the stream ends
    /// unexpectedly.
    pub fn read_row(&mut self) -> Result<Option<Row>> {
        if self.schema.is_empty() {
            return Ok(None);
        }

        if matches!(self.schema.fields()[0].ty, crate::types::TypeDesc::Nothing) {
            return Err(Error::UnsupportedCombination(
                "RowBinary cannot stream Nothing as the leading column".into(),
            ));
        }
        let mut row = Vec::with_capacity(self.schema.len());
        for (index, field) in self.schema.fields().iter().enumerate() {
            let value = if index == 0 {
                match read_value_optional(&field.ty, &mut self.inner)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            } else {
                read_value_required(&field.ty, &mut self.inner)?
            };
            row.push(value);
        }
        Ok(Some(row))
    }

    /// Reads the next row into the provided buffer.
    ///
    /// Returns `Ok(true)` when a row was read, or `Ok(false)` on EOF.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when decoding fails or the stream ends
    /// unexpectedly.
    pub fn read_row_into(&mut self, row: &mut Row) -> Result<bool> {
        if self.schema.is_empty() {
            row.clear();
            return Ok(false);
        }

        if matches!(self.schema.fields()[0].ty, crate::types::TypeDesc::Nothing) {
            return Err(Error::UnsupportedCombination(
                "RowBinary cannot stream Nothing as the leading column".into(),
            ));
        }
        row.clear();
        row.reserve(self.schema.len());
        for (index, field) in self.schema.fields().iter().enumerate() {
            let value = if index == 0 {
                match read_value_optional(&field.ty, &mut self.inner)? {
                    Some(value) => value,
                    None => return Ok(false),
                }
            } else {
                read_value_required(&field.ty, &mut self.inner)?
            };
            row.push(value);
        }
        Ok(true)
    }

    /// Returns an iterator over decoded rows.
    pub fn rows(self) -> RowBinaryRows<R> {
        RowBinaryRows { reader: self }
    }

    /// Returns the parsed header, if present.
    #[must_use]
    pub fn header(&self) -> Option<&RowBinaryHeader> {
        self.header.as_ref()
    }

    fn with_schema_optional(
        mut inner: R,
        format: RowBinaryFormat,
        schema: Option<Schema>,
    ) -> Result<Self> {
        let (schema, header) = parse_header_from_reader(&mut inner, format, schema)?;
        Ok(Self {
            inner,
            schema,
            header,
        })
    }
}

/// Iterator over `RowBinary` rows.
pub struct RowBinaryRows<R: Read> {
    reader: RowBinaryValueReader<R>,
}

impl<R: Read> Iterator for RowBinaryRows<R> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_row() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

/// Header metadata for `RowBinary` formats with names and/or types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowBinaryHeader {
    /// Column names in stream order.
    pub names: Vec<String>,
    /// Column types when present in the header.
    pub types: Option<Vec<TypeDesc>>,
}

fn parse_header<S: Seekable>(
    decoder: &mut Decoder<'static, S>,
    format: RowBinaryFormat,
    schema: Option<Schema>,
) -> Result<(Schema, Option<RowBinaryHeader>, u64)> {
    decoder.seek(SeekFrom::Start(0))?;
    let (schema, header) = parse_header_from_reader(decoder, format, schema)?;
    let offset = decoder.offset();
    Ok((schema, header, offset))
}

fn parse_header_from_reader<R: Read + ?Sized>(
    reader: &mut R,
    format: RowBinaryFormat,
    schema: Option<Schema>,
) -> Result<(Schema, Option<RowBinaryHeader>)> {
    let has_schema = schema.is_some();
    let mut schema = schema.unwrap_or_else(|| Schema::new(Vec::new()));

    match format {
        RowBinaryFormat::RowBinary => {
            if !has_schema {
                return Err(Error::InvalidValue("schema required for RowBinary reader"));
            }
            if schema.is_empty() {
                return Err(Error::InvalidValue(
                    "schema must contain at least one column",
                ));
            }
            return Ok((schema, None));
        }
        RowBinaryFormat::RowBinaryWithNames | RowBinaryFormat::RowBinaryWithNamesAndTypes => {}
    }

    let column_count = read_uvarint(reader)?.ok_or_else(|| {
        Error::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "missing header",
        ))
    })?;
    let column_count = usize::try_from(column_count)
        .map_err(|_| Error::Overflow("header column count too large"))?;
    if column_count == 0 {
        return Err(Error::InvalidValue("header column count must be > 0"));
    }

    let mut names = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let name = match read_string(reader) {
            Ok(Some(value)) => value,
            Ok(None) => return Err(Error::InvalidValue("missing header")),
            Err(Error::Io(err)) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(Error::InvalidValue("missing header"));
            }
            Err(err) => return Err(err),
        };
        names.push(name);
    }

    let types = if format == RowBinaryFormat::RowBinaryWithNamesAndTypes {
        let mut types = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let type_name = match read_string(reader) {
                Ok(Some(value)) => value,
                Ok(None) => return Err(Error::InvalidValue("missing header")),
                Err(Error::Io(err)) if err.kind() == io::ErrorKind::UnexpectedEof => {
                    return Err(Error::InvalidValue("missing header"));
                }
                Err(err) => return Err(err),
            };
            types.push(parse_type_desc(&type_name)?);
        }
        Some(types)
    } else {
        None
    };

    if has_schema {
        if schema.len() != names.len() {
            return Err(Error::InvalidValue("header column count mismatch"));
        }
        if format == RowBinaryFormat::RowBinaryWithNames
            && schema
                .fields()
                .iter()
                .map(|field| field.name.as_str())
                .ne(names.iter().map(String::as_str))
        {
            return Err(Error::InvalidValue("header column names mismatch"));
        }
    } else if let Some(types) = types.clone() {
        schema = Schema::new(
            names
                .iter()
                .cloned()
                .zip(types)
                .map(|(name, ty)| Field { name, ty })
                .collect(),
        );
    } else {
        return Err(Error::InvalidValue(
            "schema required for RowBinaryWithNames reader",
        ));
    }

    if schema.is_empty() {
        return Err(Error::InvalidValue(
            "schema must contain at least one column",
        ));
    }

    let header = Some(RowBinaryHeader { names, types });
    Ok((schema, header))
}

/// Seekable Zstd reader for `RowBinary` payloads.
pub struct RowBinaryReader<S: Seekable> {
    /// Parsed schema for the stream.
    schema: Schema,
    /// Parsed header metadata when the format includes names and/or types.
    header: Option<RowBinaryHeader>,
    /// Seekable decoder for the compressed payload.
    decoder: Decoder<'static, S>,
    /// Row offset stride for the sparse in-memory index.
    row_stride: usize,
    /// Sparse row offsets: entry `i` is the offset for row `i * row_stride`.
    row_offsets: Vec<u64>,
    /// Current row index.
    current_row: usize,
    /// Buffer holding the current row bytes (empty when unloaded).
    row_buf: Vec<u8>,
}

const DEFAULT_ROW_OFFSET_STRIDE: usize = 1024;

impl<S: Seekable> RowBinaryReader<S> {
    /// Creates a new seekable reader for the specified format.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the decoder cannot be created.
    pub fn new(source: S, format: RowBinaryFormat, schema: Option<Schema>) -> Result<Self> {
        Self::new_with_stride(source, format, schema, DEFAULT_ROW_OFFSET_STRIDE)
    }

    /// Creates a new seekable reader with a custom row index stride.
    ///
    /// Smaller strides use more memory but make backward seeks faster.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the decoder cannot be created or
    /// the stride is zero.
    pub fn new_with_stride(
        source: S,
        format: RowBinaryFormat,
        schema: Option<Schema>,
        row_stride: usize,
    ) -> Result<Self> {
        if row_stride == 0 {
            return Err(Error::InvalidValue("row stride must be greater than 0"));
        }
        let mut decoder = Decoder::new(source).map_err(Error::from)?;
        let (schema, header, data_start_offset) = parse_header(&mut decoder, format, schema)?;
        let mut reader = Self {
            schema,
            header,
            decoder,
            row_stride,
            row_offsets: Vec::new(),
            current_row: 0,
            row_buf: Vec::new(),
        };
        reader.row_offsets.push(data_start_offset);
        let maybe_len = read_row_bytes(&reader.schema, &mut reader.decoder, &mut reader.row_buf)?;
        if maybe_len.is_none() {
            reader.row_buf.clear();
        } else {
            reader.record_next_offset_if_needed();
        }
        Ok(reader)
    }

    /// Returns the parsed header, if present.
    #[must_use]
    pub fn header(&self) -> Option<&RowBinaryHeader> {
        self.header.as_ref()
    }

    /// Seeks to a specific row index.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the row is out of range.
    pub fn seek_row(&mut self, index: usize) -> Result<()> {
        if index != self.current_row + 1 || self.row_buf.is_empty() {
            self.seek_to_row(index)?;
        }
        self.load_current_row()?;
        self.current_row = index;
        Ok(())
    }

    /// Seeks to a row relative to the current row index.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when the target row is out of range.
    pub fn seek_relative(&mut self, delta: i64) -> Result<()> {
        let base = i64::try_from(self.current_row)
            .map_err(|_| Error::Overflow("current row index too large"))?;
        let target = base
            .checked_add(delta)
            .ok_or(Error::InvalidValue("row index out of range"))?;
        if target < 0 {
            return Err(Error::InvalidValue("row index out of range"));
        }
        let target = usize::try_from(target).map_err(|_| Error::Overflow("row index too large"))?;
        self.seek_row(target)
    }

    /// Returns the current row as an uncompressed byte slice.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when decoding fails.
    pub fn current_row(&mut self) -> Result<Option<&[u8]>> {
        if self.row_buf.is_empty() {
            return Ok(None);
        }
        Ok(Some(&self.row_buf))
    }

    fn ensure_offsets_for(&mut self, index: usize) -> Result<()> {
        let block = index / self.row_stride;
        if block < self.row_offsets.len() {
            return Ok(());
        }
        let mut offset = self.row_offsets[self.row_offsets.len().saturating_sub(1)];
        self.decoder.seek(SeekFrom::Start(offset))?;
        while self.row_offsets.len() <= block {
            for _ in 0..self.row_stride {
                if let Err(err) = skip_row(&self.schema, &mut self.decoder) {
                    if matches!(
                        err,
                        Error::Io(ref io_err) if io_err.kind() == io::ErrorKind::UnexpectedEof
                    ) {
                        return Err(Error::InvalidValue("row index out of range"));
                    }
                    return Err(err);
                }
            }
            offset = self.decoder.offset();
            self.row_offsets.push(offset);
        }
        Ok(())
    }

    fn seek_to_row(&mut self, index: usize) -> Result<()> {
        self.ensure_offsets_for(index)?;
        let block = index / self.row_stride;
        let offset = self.row_offsets[block];
        self.decoder.seek(SeekFrom::Start(offset))?;
        let start = block * self.row_stride;
        if index > start {
            let mut row = start;
            while row < index {
                if let Err(err) = skip_row(&self.schema, &mut self.decoder) {
                    if matches!(
                        err,
                        Error::Io(ref io_err) if io_err.kind() == io::ErrorKind::UnexpectedEof
                    ) {
                        return Err(Error::InvalidValue("row index out of range"));
                    }
                    return Err(err);
                }
                row += 1;
            }
        }
        Ok(())
    }

    fn load_current_row(&mut self) -> Result<()> {
        let maybe_len = read_row_bytes(&self.schema, &mut self.decoder, &mut self.row_buf)?;
        if maybe_len.is_none() {
            self.row_buf.clear();
            return Err(Error::InvalidValue("row index out of range"));
        }
        self.record_next_offset_if_needed();
        Ok(())
    }

    fn record_next_offset_if_needed(&mut self) {
        let next_row = self.current_row + 1;
        if next_row.is_multiple_of(self.row_stride) {
            let next_block = next_row / self.row_stride;
            if self.row_offsets.len() == next_block {
                self.row_offsets.push(self.decoder.offset());
            }
        }
    }
}

fn read_row_bytes<R: Read + ?Sized>(
    schema: &Schema,
    reader: &mut R,
    buf: &mut Vec<u8>,
) -> Result<Option<usize>> {
    if matches!(schema.fields()[0].ty, TypeDesc::Nothing) {
        return Err(Error::UnsupportedCombination(
            "RowBinary cannot stream Nothing as the leading column".into(),
        ));
    }
    buf.clear();
    let mut capture = CaptureReader::new(reader, buf);
    let mut iter = schema.fields().iter();
    let Some(first) = iter.next() else {
        return Ok(None);
    };
    if let Some(()) = skip_value_optional(&first.ty, &mut capture)? {
    } else {
        buf.clear();
        return Ok(None);
    }
    for field in iter {
        skip_value_required(&field.ty, &mut capture)?;
    }
    Ok(Some(buf.len()))
}

fn skip_row<R: Read + ?Sized>(schema: &Schema, reader: &mut R) -> Result<()> {
    if schema.is_empty() {
        return Ok(());
    }
    let mut iter = schema.fields().iter();
    let Some(first) = iter.next() else {
        return Ok(());
    };
    let Some(()) = skip_value_optional(&first.ty, reader)? else {
        return Err(Error::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "unexpected EOF while reading row",
        )));
    };
    for field in iter {
        skip_value_required(&field.ty, reader)?;
    }
    Ok(())
}
