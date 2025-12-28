//! `RowBinary` read/write support.

use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, Ipv6Addr},
};

use uuid::Uuid;

use crate::{
    error::{Error, Result},
    io::{read_bytes, read_string, read_uvarint, write_bytes, write_string, write_uvarint},
    types::{TypeDesc, parse_type_desc},
    value::Value,
};

/// `RowBinary` variants supported by the crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowBinaryFormat {
    /// Plain `RowBinary` (no names, no types).
    RowBinary,
    /// `RowBinary` with column names.
    RowBinaryWithNames,
    /// `RowBinary` with column names and types.
    RowBinaryWithNamesAndTypes,
}

impl std::fmt::Display for RowBinaryFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowBinaryFormat::RowBinary => f.write_str("RowBinary"),
            RowBinaryFormat::RowBinaryWithNames => f.write_str("RowBinaryWithNames"),
            RowBinaryFormat::RowBinaryWithNamesAndTypes => {
                f.write_str("RowBinaryWithNamesAndTypes")
            }
        }
    }
}

/// Column descriptor used by `RowBinary` readers and writers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    /// Column name.
    pub name: String,
    /// Column type.
    pub ty: TypeDesc,
}

/// Schema containing ordered fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    /// Creates a schema from fields.
    #[must_use]
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    /// Returns the ordered field list.
    #[must_use]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Returns the number of fields.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Reports whether the schema is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Creates a schema from name/type pairs.
    pub fn from_names_and_types<I, S>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (S, TypeDesc)>,
        S: Into<String>,
    {
        let fields = pairs
            .into_iter()
            .map(|(name, ty)| Field {
                name: name.into(),
                ty,
            })
            .collect();
        Self { fields }
    }

    /// Parses a schema from name/type strings.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when parsing type names fails.
    pub fn from_type_strings(pairs: &[(&str, &str)]) -> Result<Self> {
        let mut fields = Vec::with_capacity(pairs.len());
        for (name, ty) in pairs {
            fields.push(Field {
                name: (*name).to_string(),
                ty: parse_type_desc(ty)?,
            });
        }
        Ok(Self { fields })
    }
}

/// A single `RowBinary` row.
pub type Row = Vec<Value>;

/// `RowBinary` writer that streams rows into the provided writer.
pub struct RowBinaryWriter<W: Write> {
    inner: W,
    format: RowBinaryFormat,
    schema: Schema,
    header_written: bool,
}

impl<W: Write> RowBinaryWriter<W> {
    /// Creates a writer for the specified format and schema.
    #[must_use]
    pub fn new(inner: W, format: RowBinaryFormat, schema: Schema) -> Self {
        Self {
            inner,
            format,
            schema,
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
        match self.format {
            RowBinaryFormat::RowBinary => {}
            RowBinaryFormat::RowBinaryWithNames | RowBinaryFormat::RowBinaryWithNamesAndTypes => {
                write_uvarint(self.schema.len() as u64, &mut self.inner)?;
                for field in &self.schema.fields {
                    write_string(&field.name, &mut self.inner)?;
                }
                if self.format == RowBinaryFormat::RowBinaryWithNamesAndTypes {
                    for field in &self.schema.fields {
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
        for (field, value) in self.schema.fields.iter().zip(row.iter()) {
            write_value(&field.ty, value, &mut self.inner)?;
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
                .fields
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
        for (index, field) in schema.fields.iter().enumerate() {
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

fn read_value_required<R: Read + ?Sized>(ty: &TypeDesc, reader: &mut R) -> Result<Value> {
    match read_value_optional(ty, reader)? {
        Some(value) => Ok(value),
        None => Err(Error::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "unexpected EOF while reading row",
        ))),
    }
}

fn read_exact_or_eof<R: Read + ?Sized>(reader: &mut R, buf: &mut [u8]) -> Result<bool> {
    if buf.is_empty() {
        return Ok(false);
    }
    let mut first = [0_u8; 1];
    match reader.read(&mut first)? {
        0 => Ok(true),
        1 => {
            buf[0] = first[0];
            if buf.len() > 1 {
                reader.read_exact(&mut buf[1..])?;
            }
            Ok(false)
        }
        _ => Err(Error::Internal(
            "unexpected read size while decoding fixed value",
        )),
    }
}

#[allow(clippy::too_many_lines)]
fn read_value_optional<R: Read + ?Sized>(ty: &TypeDesc, reader: &mut R) -> Result<Option<Value>> {
    match ty {
        TypeDesc::UInt8 => read_fixed::<_, _, 1>(reader, |bytes| Value::UInt8(bytes[0])),
        TypeDesc::UInt16 => {
            read_fixed::<_, _, 2>(reader, |bytes| Value::UInt16(u16::from_le_bytes(bytes)))
        }
        TypeDesc::UInt32 => {
            read_fixed::<_, _, 4>(reader, |bytes| Value::UInt32(u32::from_le_bytes(bytes)))
        }
        TypeDesc::UInt64 => {
            read_fixed::<_, _, 8>(reader, |bytes| Value::UInt64(u64::from_le_bytes(bytes)))
        }
        TypeDesc::Int8 => {
            read_fixed::<_, _, 1>(reader, |bytes| Value::Int8(i8::from_le_bytes(bytes)))
        }
        TypeDesc::Int16 => {
            read_fixed::<_, _, 2>(reader, |bytes| Value::Int16(i16::from_le_bytes(bytes)))
        }
        TypeDesc::Int32 => {
            read_fixed::<_, _, 4>(reader, |bytes| Value::Int32(i32::from_le_bytes(bytes)))
        }
        TypeDesc::Int64 => {
            read_fixed::<_, _, 8>(reader, |bytes| Value::Int64(i64::from_le_bytes(bytes)))
        }
        TypeDesc::Float32 => read_fixed(reader, |bytes| Value::Float32(f32::from_le_bytes(bytes))),
        TypeDesc::Float64 => read_fixed(reader, |bytes| Value::Float64(f64::from_le_bytes(bytes))),
        TypeDesc::String => {
            let Some(bytes) = read_bytes(reader)? else {
                return Ok(None);
            };
            Ok(Some(Value::String(bytes)))
        }
        TypeDesc::FixedString { length } => {
            let mut buf = vec![0_u8; *length];
            if read_exact_or_eof(reader, &mut buf)? {
                return Ok(None);
            }
            Ok(Some(Value::FixedString(buf)))
        }
        TypeDesc::Date => {
            read_fixed::<_, _, 2>(reader, |bytes| Value::Date(u16::from_le_bytes(bytes)))
        }
        TypeDesc::Date32 => {
            read_fixed::<_, _, 4>(reader, |bytes| Value::Date32(i32::from_le_bytes(bytes)))
        }
        TypeDesc::DateTime { .. } => {
            read_fixed::<_, _, 4>(reader, |bytes| Value::DateTime(u32::from_le_bytes(bytes)))
        }
        TypeDesc::DateTime64 { .. } => {
            read_fixed::<_, _, 8>(reader, |bytes| Value::DateTime64(i64::from_le_bytes(bytes)))
        }
        TypeDesc::Uuid => read_fixed::<_, _, 16>(reader, |bytes| {
            let mut normalized = bytes;
            normalized[..8].reverse();
            normalized[8..].reverse();
            Value::Uuid(Uuid::from_bytes(normalized))
        }),
        TypeDesc::Ipv4 => read_fixed::<_, _, 4>(reader, |bytes| {
            Value::Ipv4(Ipv4Addr::from(u32::from_le_bytes(bytes)))
        }),
        TypeDesc::Ipv6 => {
            read_fixed::<_, _, 16>(reader, |bytes| Value::Ipv6(Ipv6Addr::from(bytes)))
        }
        TypeDesc::Nullable(inner) => {
            let Some(flag_value) = read_fixed::<_, _, 1>(reader, |bytes| Value::UInt8(bytes[0]))?
            else {
                return Ok(None);
            };
            let Value::UInt8(flag) = flag_value else {
                return Err(Error::Internal("nullable flag read failure"));
            };
            if flag > 1 {
                return Err(Error::InvalidValue("invalid nullable flag"));
            }
            if flag == 1 {
                Ok(Some(Value::Nullable(None)))
            } else {
                let inner_value = read_value_required(inner, reader)?;
                Ok(Some(Value::Nullable(Some(Box::new(inner_value)))))
            }
        }
        TypeDesc::LowCardinality(inner) => read_value_optional(inner, reader),
        TypeDesc::Array(inner) => {
            let Some(len) = read_uvarint(reader)? else {
                return Ok(None);
            };
            let len =
                usize::try_from(len).map_err(|_| Error::Overflow("array length too large"))?;
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                values.push(read_value_required(inner, reader)?);
            }
            Ok(Some(Value::Array(values)))
        }
        TypeDesc::Map { key, value } => {
            let Some(len) = read_uvarint(reader)? else {
                return Ok(None);
            };
            let len = usize::try_from(len).map_err(|_| Error::Overflow("map length too large"))?;
            let mut entries = Vec::with_capacity(len);
            for _ in 0..len {
                let key_value = read_value_required(key, reader)?;
                let value_value = read_value_required(value, reader)?;
                entries.push((key_value, value_value));
            }
            Ok(Some(Value::Map(entries)))
        }
    }
}

fn read_fixed<R, F, const N: usize>(reader: &mut R, map: F) -> Result<Option<Value>>
where
    R: Read + ?Sized,
    F: FnOnce([u8; N]) -> Value,
{
    let mut buf = [0_u8; N];
    if read_exact_or_eof(reader, &mut buf)? {
        return Ok(None);
    }
    Ok(Some(map(buf)))
}

fn write_value<W: Write + ?Sized>(ty: &TypeDesc, value: &Value, writer: &mut W) -> Result<()> {
    match (ty, value) {
        (TypeDesc::UInt8, Value::UInt8(value)) => writer.write_all(&[*value])?,
        (TypeDesc::UInt16, Value::UInt16(value)) | (TypeDesc::Date, Value::Date(value)) => {
            writer.write_all(&value.to_le_bytes())?;
        }
        (TypeDesc::UInt32, Value::UInt32(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::UInt64, Value::UInt64(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::Int8, Value::Int8(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::Int16, Value::Int16(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::Int32, Value::Int32(value)) | (TypeDesc::Date32, Value::Date32(value)) => {
            writer.write_all(&value.to_le_bytes())?;
        }
        (TypeDesc::Int64, Value::Int64(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::Float32, Value::Float32(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::Float64, Value::Float64(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::String, Value::String(value)) => write_bytes(value, writer)?,
        (TypeDesc::FixedString { length }, Value::FixedString(value)) => {
            if value.len() != *length {
                return Err(Error::InvalidValue("FixedString length mismatch"));
            }
            writer.write_all(value)?;
        }
        (TypeDesc::DateTime { .. }, Value::DateTime(value)) => {
            writer.write_all(&value.to_le_bytes())?;
        }
        (TypeDesc::DateTime64 { .. }, Value::DateTime64(value)) => {
            writer.write_all(&value.to_le_bytes())?;
        }
        (TypeDesc::Uuid, Value::Uuid(value)) => {
            let mut bytes = *value.as_bytes();
            bytes[..8].reverse();
            bytes[8..].reverse();
            writer.write_all(&bytes)?;
        }
        (TypeDesc::Ipv4, Value::Ipv4(value)) => {
            writer.write_all(&u32::from(*value).to_le_bytes())?;
        }
        (TypeDesc::Ipv6, Value::Ipv6(value)) => {
            writer.write_all(&value.octets())?;
        }
        (TypeDesc::Nullable(inner), Value::Nullable(value)) => {
            if let Some(inner_value) = value {
                writer.write_all(&[0])?;
                write_value(inner, inner_value, writer)?;
            } else {
                writer.write_all(&[1])?;
            }
        }
        (TypeDesc::LowCardinality(inner), value) => {
            write_value(inner, value, writer)?;
        }
        (TypeDesc::Array(inner), Value::Array(values)) => {
            write_uvarint(values.len() as u64, writer)?;
            for item in values {
                write_value(inner, item, writer)?;
            }
        }
        (TypeDesc::Map { key, value }, Value::Map(entries)) => {
            write_uvarint(entries.len() as u64, writer)?;
            for (entry_key, entry_value) in entries {
                write_value(key, entry_key, writer)?;
                write_value(value, entry_value, writer)?;
            }
        }
        (ty, value) => {
            return Err(Error::TypeMismatch {
                expected: ty.type_name(),
                actual: value.type_name().to_string(),
            });
        }
    }
    Ok(())
}
