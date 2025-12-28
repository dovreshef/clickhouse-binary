//! `RowBinary` reader implementation.

use std::io::{self, Read};

use crate::{
    error::{Error, Result},
    io::{read_string, read_uvarint},
    types::parse_type_desc,
};

use super::{
    format::RowBinaryFormat,
    schema::{Field, Row, Schema},
    value_rw::{read_value_optional, read_value_required},
};

/// `RowBinary` reader that streams rows from the provided reader.
pub struct RowBinaryReader<R: Read> {
    inner: R,
    format: RowBinaryFormat,
    schema: Option<Schema>,
    header_read: bool,
}

impl<R: Read> RowBinaryReader<R> {
    /// Creates a reader without a pre-defined schema.
    #[must_use]
    pub fn new(inner: R, format: RowBinaryFormat) -> Self {
        Self {
            inner,
            format,
            schema: None,
            header_read: false,
        }
    }

    /// Creates a reader with an expected schema.
    #[must_use]
    pub fn with_schema(inner: R, format: RowBinaryFormat, schema: Schema) -> Self {
        Self {
            inner,
            format,
            schema: Some(schema),
            header_read: false,
        }
    }

    /// Reads the header for `RowBinary` formats with names and/or types.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when parsing fails or IO errors occur.
    pub fn read_header(&mut self) -> Result<()> {
        if self.header_read {
            return Ok(());
        }
        match self.format {
            RowBinaryFormat::RowBinary => {
                self.header_read = true;
                return Ok(());
            }
            RowBinaryFormat::RowBinaryWithNames | RowBinaryFormat::RowBinaryWithNamesAndTypes => {}
        }

        let column_count = read_uvarint(&mut self.inner)?.ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "missing header",
            ))
        })?;
        let column_count = usize::try_from(column_count)
            .map_err(|_| Error::Overflow("header column count too large"))?;

        let mut names = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let name = read_string(&mut self.inner)?.ok_or_else(|| {
                Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "missing column name",
                ))
            })?;
            names.push(name);
        }

        let types = if self.format == RowBinaryFormat::RowBinaryWithNamesAndTypes {
            let mut types = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                let type_name = read_string(&mut self.inner)?.ok_or_else(|| {
                    Error::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "missing column type",
                    ))
                })?;
                types.push(parse_type_desc(&type_name)?);
            }
            Some(types)
        } else {
            None
        };

        if let Some(types) = types {
            let header_schema = Schema::new(
                names
                    .into_iter()
                    .zip(types)
                    .map(|(name, ty)| Field { name, ty })
                    .collect(),
            );
            if let Some(existing) = &self.schema {
                if existing.len() != header_schema.len() {
                    return Err(Error::InvalidValue("header column count mismatch"));
                }
            } else {
                self.schema = Some(header_schema);
            }
        } else if let Some(existing) = &self.schema {
            if existing.len() != names.len() {
                return Err(Error::InvalidValue("header column count mismatch"));
            }
            if existing
                .fields()
                .iter()
                .map(|field| field.name.as_str())
                .ne(names.iter().map(String::as_str))
            {
                return Err(Error::InvalidValue("header column names mismatch"));
            }
        } else {
            return Err(Error::InvalidValue(
                "schema required to read RowBinaryWithNames",
            ));
        }

        self.header_read = true;
        Ok(())
    }

    /// Reads the next row.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when decoding fails or the stream ends
    /// unexpectedly.
    pub fn read_row(&mut self) -> Result<Option<Row>> {
        self.read_header()?;
        let Some(schema) = &self.schema else {
            return Err(Error::InvalidValue("schema required to read rows"));
        };
        if schema.is_empty() {
            return Ok(None);
        }

        let mut row = Vec::with_capacity(schema.len());
        for (index, field) in schema.fields().iter().enumerate() {
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
}
