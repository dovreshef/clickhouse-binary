//! `RowBinary` value-level read/write helpers.

use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, Ipv6Addr},
};

use uuid::Uuid;

use crate::{
    error::{Error, Result},
    io::{read_bytes, read_string, read_uvarint, write_bytes, write_string, write_uvarint},
    types::{DecimalSize, TypeDesc},
    value::Value,
};

use super::type_binary::{decode_type_binary_from_tag, encode_type_binary_option};

pub(crate) fn read_value_required<R: Read + ?Sized>(
    ty: &TypeDesc,
    reader: &mut R,
) -> Result<Value> {
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
pub(crate) fn read_value_optional<R: Read + ?Sized>(
    ty: &TypeDesc,
    reader: &mut R,
) -> Result<Option<Value>> {
    match ty {
        TypeDesc::UInt8 => read_fixed::<_, _, 1>(reader, |bytes| Value::UInt8(bytes[0])),
        TypeDesc::Bool => {
            let mut buf = [0_u8; 1];
            if read_exact_or_eof(reader, &mut buf)? {
                return Ok(None);
            }
            if buf[0] > 1 {
                return Err(Error::InvalidValue("invalid Bool value"));
            }
            Ok(Some(Value::Bool(buf[0] == 1)))
        }
        TypeDesc::UInt16 => {
            read_fixed::<_, _, 2>(reader, |bytes| Value::UInt16(u16::from_le_bytes(bytes)))
        }
        TypeDesc::UInt32 => {
            read_fixed::<_, _, 4>(reader, |bytes| Value::UInt32(u32::from_le_bytes(bytes)))
        }
        TypeDesc::UInt64 => {
            read_fixed::<_, _, 8>(reader, |bytes| Value::UInt64(u64::from_le_bytes(bytes)))
        }
        TypeDesc::UInt128 => {
            read_fixed::<_, _, 16>(reader, |bytes| Value::UInt128(u128::from_le_bytes(bytes)))
        }
        TypeDesc::UInt256 => read_fixed::<_, _, 32>(reader, Value::UInt256),
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
        TypeDesc::Int128 => {
            read_fixed::<_, _, 16>(reader, |bytes| Value::Int128(i128::from_le_bytes(bytes)))
        }
        TypeDesc::Int256 => read_fixed::<_, _, 32>(reader, Value::Int256),
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
        TypeDesc::Decimal32 { .. } => {
            read_fixed::<_, _, 4>(reader, |bytes| Value::Decimal32(i32::from_le_bytes(bytes)))
        }
        TypeDesc::Decimal64 { .. } => {
            read_fixed::<_, _, 8>(reader, |bytes| Value::Decimal64(i64::from_le_bytes(bytes)))
        }
        TypeDesc::Decimal128 { .. } => read_fixed::<_, _, 16>(reader, |bytes| {
            Value::Decimal128(i128::from_le_bytes(bytes))
        }),
        TypeDesc::Decimal256 { .. } => read_fixed::<_, _, 32>(reader, Value::Decimal256),
        TypeDesc::Decimal { size, .. } => match size {
            DecimalSize::Bits32 => {
                read_fixed::<_, _, 4>(reader, |bytes| Value::Decimal32(i32::from_le_bytes(bytes)))
            }
            DecimalSize::Bits64 => {
                read_fixed::<_, _, 8>(reader, |bytes| Value::Decimal64(i64::from_le_bytes(bytes)))
            }
            DecimalSize::Bits128 => read_fixed::<_, _, 16>(reader, |bytes| {
                Value::Decimal128(i128::from_le_bytes(bytes))
            }),
            DecimalSize::Bits256 => read_fixed::<_, _, 32>(reader, Value::Decimal256),
        },
        TypeDesc::Enum8(_) => {
            read_fixed::<_, _, 1>(reader, |bytes| Value::Enum8(i8::from_le_bytes(bytes)))
        }
        TypeDesc::Enum16(_) => {
            read_fixed::<_, _, 2>(reader, |bytes| Value::Enum16(i16::from_le_bytes(bytes)))
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
        TypeDesc::Tuple(items) => read_tuple_values(items, reader),
        TypeDesc::Nested(items) => {
            let Some(len) = read_uvarint(reader)? else {
                return Ok(None);
            };
            let len =
                usize::try_from(len).map_err(|_| Error::Overflow("array length too large"))?;
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                let tuple = read_tuple_values(items, reader)?.ok_or_else(|| {
                    Error::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "unexpected EOF while reading row",
                    ))
                })?;
                values.push(tuple);
            }
            Ok(Some(Value::Array(values)))
        }
        TypeDesc::Json { typed_paths, .. } => {
            let Some(path_count) = read_uvarint(reader)? else {
                return Ok(None);
            };
            let path_count = usize::try_from(path_count)
                .map_err(|_| Error::Overflow("JSON path count too large"))?;
            let mut entries = Vec::with_capacity(path_count);
            for _ in 0..path_count {
                let path = read_string(reader)?.ok_or_else(|| {
                    Error::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "missing JSON path",
                    ))
                })?;
                let value =
                    if let Some((_, ty)) = typed_paths.iter().find(|(name, _)| name == &path) {
                        read_value_required(ty, reader)?
                    } else {
                        read_value_required(&TypeDesc::Dynamic { max_types: None }, reader)?
                    };
                entries.push((path, value));
            }
            Ok(Some(Value::JsonObject(entries)))
        }
        TypeDesc::Dynamic { .. } => {
            let mut tag = [0_u8; 1];
            if read_exact_or_eof(reader, &mut tag)? {
                return Ok(None);
            }
            let ty = decode_type_binary_from_tag(tag[0], reader)?;
            let Some(ty) = ty else {
                return Ok(Some(Value::DynamicNull));
            };
            let value = read_value_required(&ty, reader)?;
            Ok(Some(Value::Dynamic {
                ty: Box::new(ty),
                value: Box::new(value),
            }))
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

#[allow(clippy::too_many_lines)]
pub(crate) fn write_value<W: Write + ?Sized>(
    ty: &TypeDesc,
    value: &Value,
    writer: &mut W,
) -> Result<()> {
    match (ty, value) {
        (TypeDesc::UInt8, Value::UInt8(value)) => writer.write_all(&[*value])?,
        (TypeDesc::Bool, Value::Bool(value)) => {
            writer.write_all(&[u8::from(*value)])?;
        }
        (TypeDesc::UInt16, Value::UInt16(value)) | (TypeDesc::Date, Value::Date(value)) => {
            writer.write_all(&value.to_le_bytes())?;
        }
        (TypeDesc::UInt32, Value::UInt32(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::UInt64, Value::UInt64(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::UInt128, Value::UInt128(value)) => {
            writer.write_all(&value.to_le_bytes())?;
        }
        (TypeDesc::UInt256, Value::UInt256(value))
        | (TypeDesc::Decimal256 { .. }, Value::Decimal256(value)) => {
            writer.write_all(value)?;
        }
        (TypeDesc::Int8, Value::Int8(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::Int16, Value::Int16(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::Int32, Value::Int32(value))
        | (TypeDesc::Date32, Value::Date32(value))
        | (TypeDesc::Decimal32 { .. }, Value::Decimal32(value)) => {
            writer.write_all(&value.to_le_bytes())?;
        }
        (TypeDesc::Int64, Value::Int64(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::Int128, Value::Int128(value)) => writer.write_all(&value.to_le_bytes())?,
        (TypeDesc::Int256, Value::Int256(value)) => writer.write_all(value)?,
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
        (TypeDesc::DateTime64 { .. }, Value::DateTime64(value))
        | (TypeDesc::Decimal64 { .. }, Value::Decimal64(value)) => {
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
        (TypeDesc::Decimal128 { .. }, Value::Decimal128(value)) => {
            writer.write_all(&value.to_le_bytes())?;
        }
        (TypeDesc::Decimal { size, .. }, value) => match (size, value) {
            (DecimalSize::Bits32, Value::Decimal32(value)) => {
                writer.write_all(&value.to_le_bytes())?;
            }
            (DecimalSize::Bits64, Value::Decimal64(value)) => {
                writer.write_all(&value.to_le_bytes())?;
            }
            (DecimalSize::Bits128, Value::Decimal128(value)) => {
                writer.write_all(&value.to_le_bytes())?;
            }
            (DecimalSize::Bits256, Value::Decimal256(value)) => {
                writer.write_all(value)?;
            }
            _ => {
                return Err(Error::TypeMismatch {
                    expected: ty.type_name(),
                    actual: value.type_name().to_string(),
                });
            }
        },
        (TypeDesc::Enum8(_), Value::Enum8(value)) => {
            writer.write_all(&value.to_le_bytes())?;
        }
        (TypeDesc::Enum16(_), Value::Enum16(value)) => {
            writer.write_all(&value.to_le_bytes())?;
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
        (TypeDesc::Tuple(items), Value::Tuple(values)) => {
            write_tuple_values(items, values, writer)?;
        }
        (TypeDesc::Nested(items), Value::Array(values)) => {
            write_uvarint(values.len() as u64, writer)?;
            for value in values {
                let Value::Tuple(values) = value else {
                    return Err(Error::TypeMismatch {
                        expected: "Tuple".to_string(),
                        actual: value.type_name().to_string(),
                    });
                };
                write_tuple_values(items, values, writer)?;
            }
        }
        (TypeDesc::Json { typed_paths, .. }, Value::JsonObject(entries)) => {
            write_uvarint(entries.len() as u64, writer)?;
            for (path, value) in entries {
                write_string(path, writer)?;
                if let Some((_, ty)) = typed_paths.iter().find(|(name, _)| name == path) {
                    write_value(ty, value, writer)?;
                } else {
                    write_value(&TypeDesc::Dynamic { max_types: None }, value, writer)?;
                }
            }
        }
        (TypeDesc::Dynamic { .. }, Value::DynamicNull)
        | (TypeDesc::Dynamic { .. }, Value::Nullable(None)) => {
            encode_type_binary_option(None, writer)?;
        }
        (TypeDesc::Dynamic { .. }, Value::Dynamic { ty, value }) => {
            if matches!(ty.as_ref(), TypeDesc::Dynamic { .. }) {
                return Err(Error::UnsupportedCombination(
                    "Dynamic value cannot be Dynamic".into(),
                ));
            }
            encode_type_binary_option(Some(ty.as_ref()), writer)?;
            write_value(ty.as_ref(), value, writer)?;
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

fn read_tuple_values<R: Read + ?Sized>(
    items: &[crate::types::TupleItem],
    reader: &mut R,
) -> Result<Option<Value>> {
    let mut iter = items.iter();
    let Some(first) = iter.next() else {
        return Ok(Some(Value::Tuple(Vec::new())));
    };
    let Some(first_value) = read_value_optional(&first.ty, reader)? else {
        return Ok(None);
    };
    let mut values = Vec::with_capacity(items.len());
    values.push(first_value);
    for item in iter {
        values.push(read_value_required(&item.ty, reader)?);
    }
    Ok(Some(Value::Tuple(values)))
}

fn write_tuple_values<W: Write + ?Sized>(
    items: &[crate::types::TupleItem],
    values: &[Value],
    writer: &mut W,
) -> Result<()> {
    if items.len() != values.len() {
        return Err(Error::InvalidValue("Tuple length mismatch"));
    }
    for (item, value) in items.iter().zip(values.iter()) {
        write_value(&item.ty, value, writer)?;
    }
    Ok(())
}

pub(crate) fn write_nested_value<W: Write + ?Sized>(
    items: &[crate::types::TupleItem],
    value: &Value,
    writer: &mut W,
) -> Result<()> {
    if items.is_empty() {
        return Err(Error::InvalidValue("Nested expects at least one field"));
    }
    let Value::Array(rows) = value else {
        return Err(Error::TypeMismatch {
            expected: "Array(Tuple(...))".to_string(),
            actual: value.type_name().to_string(),
        });
    };
    let mut columns: Vec<Vec<Value>> = vec![Vec::with_capacity(rows.len()); items.len()];
    for row in rows {
        let Value::Tuple(values) = row else {
            return Err(Error::TypeMismatch {
                expected: "Tuple".to_string(),
                actual: row.type_name().to_string(),
            });
        };
        if values.len() != items.len() {
            return Err(Error::InvalidValue("Nested tuple length mismatch"));
        }
        for (idx, item_value) in values.iter().enumerate() {
            columns[idx].push(item_value.clone());
        }
    }
    for (item, column) in items.iter().zip(columns.into_iter()) {
        if item.name.as_deref().unwrap_or("").is_empty() {
            return Err(Error::InvalidValue(
                "Nested fields must have names when writing",
            ));
        }
        let array_type = TypeDesc::Array(Box::new(item.ty.clone()));
        let array_value = Value::Array(column);
        write_value(&array_type, &array_value, writer)?;
    }
    Ok(())
}
