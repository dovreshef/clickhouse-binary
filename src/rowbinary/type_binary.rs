//! Binary encoding/decoding of `ClickHouse` types (used by Dynamic values).

use std::io::{self, Read, Write};

use crate::{
    error::{Error, Result},
    io::{read_string, read_uvarint, write_string, write_uvarint},
    types::{DecimalSize, TupleItem, TypeDesc, can_be_inside_low_cardinality, is_valid_map_key},
};

const MAX_TYPE_ITEMS: usize = 1_000_000;
const MAX_TYPE_COMPLEXITY: usize = 1_000;
const DECIMAL32_MAX_PRECISION: u8 = 9;
const DECIMAL64_MAX_PRECISION: u8 = 18;
const DECIMAL128_MAX_PRECISION: u8 = 38;
const DECIMAL256_MAX_PRECISION: u8 = 76;
const JSON_SERIALIZATION_VERSION: u8 = 0;
const JSON_MAX_TYPED_PATHS: usize = 1000;
const JSON_MAX_DYNAMIC_PATHS_LIMIT: usize = 10000;

#[repr(u8)]
enum BinaryTypeIndex {
    Nothing = 0x00,
    UInt8 = 0x01,
    UInt16 = 0x02,
    UInt32 = 0x03,
    UInt64 = 0x04,
    UInt128 = 0x05,
    UInt256 = 0x06,
    Int8 = 0x07,
    Int16 = 0x08,
    Int32 = 0x09,
    Int64 = 0x0A,
    Int128 = 0x0B,
    Int256 = 0x0C,
    Float32 = 0x0D,
    Float64 = 0x0E,
    Date = 0x0F,
    Date32 = 0x10,
    DateTimeUTC = 0x11,
    DateTimeWithTimezone = 0x12,
    DateTime64UTC = 0x13,
    DateTime64WithTimezone = 0x14,
    String = 0x15,
    FixedString = 0x16,
    Enum8 = 0x17,
    Enum16 = 0x18,
    Decimal32 = 0x19,
    Decimal64 = 0x1A,
    Decimal128 = 0x1B,
    Decimal256 = 0x1C,
    Uuid = 0x1D,
    Array = 0x1E,
    UnnamedTuple = 0x1F,
    NamedTuple = 0x20,
    Nullable = 0x23,
    LowCardinality = 0x26,
    Map = 0x27,
    Ipv4 = 0x28,
    Ipv6 = 0x29,
    Dynamic = 0x2B,
    Bool = 0x2D,
    Nested = 0x2F,
    Json = 0x30,
}

#[allow(clippy::too_many_lines)]
pub(crate) fn encode_type_binary<W: Write + ?Sized>(ty: &TypeDesc, writer: &mut W) -> Result<()> {
    match ty {
        TypeDesc::UInt8 => write_tag(BinaryTypeIndex::UInt8, writer),
        TypeDesc::Bool => write_tag(BinaryTypeIndex::Bool, writer),
        TypeDesc::UInt16 => write_tag(BinaryTypeIndex::UInt16, writer),
        TypeDesc::UInt32 => write_tag(BinaryTypeIndex::UInt32, writer),
        TypeDesc::UInt64 => write_tag(BinaryTypeIndex::UInt64, writer),
        TypeDesc::UInt128 => write_tag(BinaryTypeIndex::UInt128, writer),
        TypeDesc::UInt256 => write_tag(BinaryTypeIndex::UInt256, writer),
        TypeDesc::Int8 => write_tag(BinaryTypeIndex::Int8, writer),
        TypeDesc::Int16 => write_tag(BinaryTypeIndex::Int16, writer),
        TypeDesc::Int32 => write_tag(BinaryTypeIndex::Int32, writer),
        TypeDesc::Int64 => write_tag(BinaryTypeIndex::Int64, writer),
        TypeDesc::Int128 => write_tag(BinaryTypeIndex::Int128, writer),
        TypeDesc::Int256 => write_tag(BinaryTypeIndex::Int256, writer),
        TypeDesc::Float32 => write_tag(BinaryTypeIndex::Float32, writer),
        TypeDesc::Float64 => write_tag(BinaryTypeIndex::Float64, writer),
        TypeDesc::Date => write_tag(BinaryTypeIndex::Date, writer),
        TypeDesc::Date32 => write_tag(BinaryTypeIndex::Date32, writer),
        TypeDesc::DateTime { timezone } => match timezone {
            Some(tz) => {
                write_tag(BinaryTypeIndex::DateTimeWithTimezone, writer)?;
                write_string(tz, writer)?;
                Ok(())
            }
            None => write_tag(BinaryTypeIndex::DateTimeUTC, writer),
        },
        TypeDesc::DateTime64 {
            precision,
            timezone,
        } => {
            if let Some(tz) = timezone {
                write_tag(BinaryTypeIndex::DateTime64WithTimezone, writer)?;
                writer.write_all(&[*precision])?;
                write_string(tz, writer)?;
                Ok(())
            } else {
                write_tag(BinaryTypeIndex::DateTime64UTC, writer)?;
                writer.write_all(&[*precision])?;
                Ok(())
            }
        }
        TypeDesc::String => write_tag(BinaryTypeIndex::String, writer),
        TypeDesc::FixedString { length } => {
            if *length == 0 {
                return Err(Error::InvalidValue("FixedString length must be > 0"));
            }
            write_tag(BinaryTypeIndex::FixedString, writer)?;
            write_uvarint(*length as u64, writer)?;
            Ok(())
        }
        TypeDesc::Enum8(values) => {
            write_tag(BinaryTypeIndex::Enum8, writer)?;
            write_uvarint(values.len() as u64, writer)?;
            for (name, value) in values {
                write_string(name, writer)?;
                writer.write_all(&value.to_le_bytes())?;
            }
            Ok(())
        }
        TypeDesc::Enum16(values) => {
            write_tag(BinaryTypeIndex::Enum16, writer)?;
            write_uvarint(values.len() as u64, writer)?;
            for (name, value) in values {
                write_string(name, writer)?;
                writer.write_all(&value.to_le_bytes())?;
            }
            Ok(())
        }
        TypeDesc::Decimal {
            precision,
            scale,
            size,
        } => {
            let (tag, max_precision) = decimal_tag_and_precision(*size);
            validate_decimal(*precision, *scale, max_precision)?;
            write_tag(tag, writer)?;
            writer.write_all(&[*precision, *scale])?;
            Ok(())
        }
        TypeDesc::Decimal32 { scale } => encode_decimal_with_scale(
            BinaryTypeIndex::Decimal32,
            DECIMAL32_MAX_PRECISION,
            *scale,
            writer,
        ),
        TypeDesc::Decimal64 { scale } => encode_decimal_with_scale(
            BinaryTypeIndex::Decimal64,
            DECIMAL64_MAX_PRECISION,
            *scale,
            writer,
        ),
        TypeDesc::Decimal128 { scale } => encode_decimal_with_scale(
            BinaryTypeIndex::Decimal128,
            DECIMAL128_MAX_PRECISION,
            *scale,
            writer,
        ),
        TypeDesc::Decimal256 { scale } => encode_decimal_with_scale(
            BinaryTypeIndex::Decimal256,
            DECIMAL256_MAX_PRECISION,
            *scale,
            writer,
        ),
        TypeDesc::Uuid => write_tag(BinaryTypeIndex::Uuid, writer),
        TypeDesc::Ipv4 => write_tag(BinaryTypeIndex::Ipv4, writer),
        TypeDesc::Ipv6 => write_tag(BinaryTypeIndex::Ipv6, writer),
        TypeDesc::Array(inner) => {
            write_tag(BinaryTypeIndex::Array, writer)?;
            encode_type_binary(inner, writer)
        }
        TypeDesc::Map { key, value } => {
            if !is_valid_map_key(key) {
                return Err(Error::UnsupportedCombination(format!(
                    "Map cannot have a key of type {}",
                    key.type_name()
                )));
            }
            write_tag(BinaryTypeIndex::Map, writer)?;
            encode_type_binary(key, writer)?;
            encode_type_binary(value, writer)
        }
        TypeDesc::Tuple(items) => encode_tuple(items, writer),
        TypeDesc::Nested(items) => encode_nested(items, writer),
        TypeDesc::Nullable(inner) => {
            write_tag(BinaryTypeIndex::Nullable, writer)?;
            encode_type_binary(inner, writer)
        }
        TypeDesc::LowCardinality(inner) => {
            if !can_be_inside_low_cardinality(inner) {
                return Err(Error::UnsupportedCombination(format!(
                    "LowCardinality({}) is unsupported",
                    inner.type_name()
                )));
            }
            write_tag(BinaryTypeIndex::LowCardinality, writer)?;
            encode_type_binary(inner, writer)
        }
        TypeDesc::Json {
            max_dynamic_paths,
            max_dynamic_types,
            typed_paths,
            skip_paths,
            skip_regexps,
        } => {
            if *max_dynamic_paths > JSON_MAX_DYNAMIC_PATHS_LIMIT {
                return Err(Error::InvalidValue("JSON max_dynamic_paths too large"));
            }
            write_tag(BinaryTypeIndex::Json, writer)?;
            writer.write_all(&[JSON_SERIALIZATION_VERSION])?;
            write_uvarint(*max_dynamic_paths as u64, writer)?;
            writer.write_all(&[*max_dynamic_types])?;
            if typed_paths.len() > JSON_MAX_TYPED_PATHS {
                return Err(Error::InvalidValue("too many JSON typed paths"));
            }
            write_uvarint(typed_paths.len() as u64, writer)?;
            for (path, ty) in typed_paths {
                write_string(path, writer)?;
                encode_type_binary(ty, writer)?;
            }
            write_uvarint(skip_paths.len() as u64, writer)?;
            for path in skip_paths {
                write_string(path, writer)?;
            }
            write_uvarint(skip_regexps.len() as u64, writer)?;
            for regex in skip_regexps {
                write_string(regex, writer)?;
            }
            Ok(())
        }
        TypeDesc::Dynamic { .. } => Err(Error::UnsupportedType("Dynamic".into())),
    }
}

pub(crate) fn encode_type_binary_option<W: Write + ?Sized>(
    ty: Option<&TypeDesc>,
    writer: &mut W,
) -> Result<()> {
    match ty {
        Some(ty) => encode_type_binary(ty, writer),
        None => write_tag(BinaryTypeIndex::Nothing, writer),
    }
}

#[allow(clippy::too_many_lines)]
fn decode_type_binary_inner<R: Read + ?Sized>(
    reader: &mut R,
    complexity: &mut usize,
) -> Result<Option<TypeDesc>> {
    let tag = read_u8(reader)?;
    decode_type_binary_inner_with_tag(tag, reader, complexity)
}

pub(crate) fn decode_type_binary_from_tag<R: Read + ?Sized>(
    tag: u8,
    reader: &mut R,
) -> Result<Option<TypeDesc>> {
    let mut complexity = 0usize;
    decode_type_binary_inner_with_tag(tag, reader, &mut complexity)
}

#[allow(clippy::too_many_lines)]
fn decode_type_binary_inner_with_tag<R: Read + ?Sized>(
    tag: u8,
    reader: &mut R,
    complexity: &mut usize,
) -> Result<Option<TypeDesc>> {
    *complexity = complexity.saturating_add(1);
    if *complexity > MAX_TYPE_COMPLEXITY {
        return Err(Error::InvalidValue(
            "binary type decoding complexity limit exceeded",
        ));
    }
    match tag {
        x if x == BinaryTypeIndex::Nothing as u8 => Ok(None),
        x if x == BinaryTypeIndex::UInt8 as u8 => Ok(Some(TypeDesc::UInt8)),
        x if x == BinaryTypeIndex::Bool as u8 => Ok(Some(TypeDesc::Bool)),
        x if x == BinaryTypeIndex::UInt16 as u8 => Ok(Some(TypeDesc::UInt16)),
        x if x == BinaryTypeIndex::UInt32 as u8 => Ok(Some(TypeDesc::UInt32)),
        x if x == BinaryTypeIndex::UInt64 as u8 => Ok(Some(TypeDesc::UInt64)),
        x if x == BinaryTypeIndex::UInt128 as u8 => Ok(Some(TypeDesc::UInt128)),
        x if x == BinaryTypeIndex::UInt256 as u8 => Ok(Some(TypeDesc::UInt256)),
        x if x == BinaryTypeIndex::Int8 as u8 => Ok(Some(TypeDesc::Int8)),
        x if x == BinaryTypeIndex::Int16 as u8 => Ok(Some(TypeDesc::Int16)),
        x if x == BinaryTypeIndex::Int32 as u8 => Ok(Some(TypeDesc::Int32)),
        x if x == BinaryTypeIndex::Int64 as u8 => Ok(Some(TypeDesc::Int64)),
        x if x == BinaryTypeIndex::Int128 as u8 => Ok(Some(TypeDesc::Int128)),
        x if x == BinaryTypeIndex::Int256 as u8 => Ok(Some(TypeDesc::Int256)),
        x if x == BinaryTypeIndex::Float32 as u8 => Ok(Some(TypeDesc::Float32)),
        x if x == BinaryTypeIndex::Float64 as u8 => Ok(Some(TypeDesc::Float64)),
        x if x == BinaryTypeIndex::Date as u8 => Ok(Some(TypeDesc::Date)),
        x if x == BinaryTypeIndex::Date32 as u8 => Ok(Some(TypeDesc::Date32)),
        x if x == BinaryTypeIndex::DateTimeUTC as u8 => {
            Ok(Some(TypeDesc::DateTime { timezone: None }))
        }
        x if x == BinaryTypeIndex::DateTimeWithTimezone as u8 => {
            let tz = read_required_string(reader, "missing DateTime timezone")?;
            Ok(Some(TypeDesc::DateTime { timezone: Some(tz) }))
        }
        x if x == BinaryTypeIndex::DateTime64UTC as u8 => {
            let precision = read_u8(reader)?;
            Ok(Some(TypeDesc::DateTime64 {
                precision,
                timezone: None,
            }))
        }
        x if x == BinaryTypeIndex::DateTime64WithTimezone as u8 => {
            let precision = read_u8(reader)?;
            let tz = read_required_string(reader, "missing DateTime64 timezone")?;
            Ok(Some(TypeDesc::DateTime64 {
                precision,
                timezone: Some(tz),
            }))
        }
        x if x == BinaryTypeIndex::String as u8 => Ok(Some(TypeDesc::String)),
        x if x == BinaryTypeIndex::FixedString as u8 => {
            let size = read_required_uvarint(reader, "missing FixedString length")?;
            let size = usize::try_from(size)
                .map_err(|_| Error::Overflow("FixedString length too large"))?;
            if size == 0 {
                return Err(Error::InvalidValue("FixedString length must be > 0"));
            }
            Ok(Some(TypeDesc::FixedString { length: size }))
        }
        x if x == BinaryTypeIndex::Enum8 as u8 => {
            let count = read_required_uvarint(reader, "missing Enum8 count")?;
            let count = usize::try_from(count)
                .map_err(|_| Error::Overflow("Enum8 value count too large"))?;
            if count > MAX_TYPE_ITEMS {
                return Err(Error::Overflow("Enum8 value count too large"));
            }
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                let name = read_required_string(reader, "missing Enum8 name")?;
                let mut buf = [0_u8; 1];
                reader.read_exact(&mut buf)?;
                values.push((name, i8::from_le_bytes(buf)));
            }
            Ok(Some(TypeDesc::Enum8(values)))
        }
        x if x == BinaryTypeIndex::Enum16 as u8 => {
            let count = read_required_uvarint(reader, "missing Enum16 count")?;
            let count = usize::try_from(count)
                .map_err(|_| Error::Overflow("Enum16 value count too large"))?;
            if count > MAX_TYPE_ITEMS {
                return Err(Error::Overflow("Enum16 value count too large"));
            }
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                let name = read_required_string(reader, "missing Enum16 name")?;
                let mut buf = [0_u8; 2];
                reader.read_exact(&mut buf)?;
                values.push((name, i16::from_le_bytes(buf)));
            }
            Ok(Some(TypeDesc::Enum16(values)))
        }
        x if x == BinaryTypeIndex::Decimal32 as u8 => {
            let (precision, scale) = decode_decimal(reader)?;
            validate_decimal(precision, scale, DECIMAL32_MAX_PRECISION)?;
            Ok(Some(TypeDesc::Decimal {
                precision,
                scale,
                size: DecimalSize::Bits32,
            }))
        }
        x if x == BinaryTypeIndex::Decimal64 as u8 => {
            let (precision, scale) = decode_decimal(reader)?;
            validate_decimal(precision, scale, DECIMAL64_MAX_PRECISION)?;
            Ok(Some(TypeDesc::Decimal {
                precision,
                scale,
                size: DecimalSize::Bits64,
            }))
        }
        x if x == BinaryTypeIndex::Decimal128 as u8 => {
            let (precision, scale) = decode_decimal(reader)?;
            validate_decimal(precision, scale, DECIMAL128_MAX_PRECISION)?;
            Ok(Some(TypeDesc::Decimal {
                precision,
                scale,
                size: DecimalSize::Bits128,
            }))
        }
        x if x == BinaryTypeIndex::Decimal256 as u8 => {
            let (precision, scale) = decode_decimal(reader)?;
            validate_decimal(precision, scale, DECIMAL256_MAX_PRECISION)?;
            Ok(Some(TypeDesc::Decimal {
                precision,
                scale,
                size: DecimalSize::Bits256,
            }))
        }
        x if x == BinaryTypeIndex::Uuid as u8 => Ok(Some(TypeDesc::Uuid)),
        x if x == BinaryTypeIndex::Ipv4 as u8 => Ok(Some(TypeDesc::Ipv4)),
        x if x == BinaryTypeIndex::Ipv6 as u8 => Ok(Some(TypeDesc::Ipv6)),
        x if x == BinaryTypeIndex::Array as u8 => {
            let inner = decode_type_binary_inner(reader, complexity)?.ok_or_else(|| {
                Error::UnsupportedCombination("Array(Nothing) is unsupported".into())
            })?;
            Ok(Some(TypeDesc::Array(Box::new(inner))))
        }
        x if x == BinaryTypeIndex::Map as u8 => {
            let key = decode_type_binary_inner(reader, complexity)?.ok_or_else(|| {
                Error::UnsupportedCombination("Map(Nothing, T) is unsupported".into())
            })?;
            let value = decode_type_binary_inner(reader, complexity)?.ok_or_else(|| {
                Error::UnsupportedCombination("Map(T, Nothing) is unsupported".into())
            })?;
            if !is_valid_map_key(&key) {
                return Err(Error::UnsupportedCombination(format!(
                    "Map cannot have a key of type {}",
                    key.type_name()
                )));
            }
            Ok(Some(TypeDesc::Map {
                key: Box::new(key),
                value: Box::new(value),
            }))
        }
        x if x == BinaryTypeIndex::UnnamedTuple as u8 => {
            let count = read_required_uvarint(reader, "missing Tuple size")?;
            let count =
                usize::try_from(count).map_err(|_| Error::Overflow("Tuple size too large"))?;
            if count == 0 {
                return Err(Error::InvalidValue("Tuple expects at least one type"));
            }
            if count > MAX_TYPE_ITEMS {
                return Err(Error::Overflow("Tuple size too large"));
            }
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let ty = decode_type_binary_inner(reader, complexity)?.ok_or_else(|| {
                    Error::UnsupportedCombination("Tuple(Nothing) is unsupported".into())
                })?;
                items.push(TupleItem { name: None, ty });
            }
            Ok(Some(TypeDesc::Tuple(items)))
        }
        x if x == BinaryTypeIndex::NamedTuple as u8 => {
            let count = read_required_uvarint(reader, "missing Tuple size")?;
            let count =
                usize::try_from(count).map_err(|_| Error::Overflow("Tuple size too large"))?;
            if count == 0 {
                return Err(Error::InvalidValue("Tuple expects at least one type"));
            }
            if count > MAX_TYPE_ITEMS {
                return Err(Error::Overflow("Tuple size too large"));
            }
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let name = read_required_string(reader, "missing Tuple element name")?;
                let ty = decode_type_binary_inner(reader, complexity)?.ok_or_else(|| {
                    Error::UnsupportedCombination("Tuple(Nothing) is unsupported".into())
                })?;
                items.push(TupleItem {
                    name: Some(name),
                    ty,
                });
            }
            Ok(Some(TypeDesc::Tuple(items)))
        }
        x if x == BinaryTypeIndex::Nullable as u8 => {
            let inner = decode_type_binary_inner(reader, complexity)?.ok_or_else(|| {
                Error::UnsupportedCombination("Nullable(Nothing) is unsupported".into())
            })?;
            Ok(Some(TypeDesc::Nullable(Box::new(inner))))
        }
        x if x == BinaryTypeIndex::LowCardinality as u8 => {
            let inner = decode_type_binary_inner(reader, complexity)?.ok_or_else(|| {
                Error::UnsupportedCombination("LowCardinality(Nothing) is unsupported".into())
            })?;
            if !can_be_inside_low_cardinality(&inner) {
                return Err(Error::UnsupportedCombination(format!(
                    "LowCardinality({}) is unsupported",
                    inner.type_name()
                )));
            }
            Ok(Some(TypeDesc::LowCardinality(Box::new(inner))))
        }
        x if x == BinaryTypeIndex::Nested as u8 => {
            let count = read_required_uvarint(reader, "missing Nested size")?;
            let count =
                usize::try_from(count).map_err(|_| Error::Overflow("Nested size too large"))?;
            if count == 0 {
                return Err(Error::InvalidValue("Nested expects at least one element"));
            }
            if count > MAX_TYPE_ITEMS {
                return Err(Error::Overflow("Nested size too large"));
            }
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let name = read_required_string(reader, "missing Nested element name")?;
                let ty = decode_type_binary_inner(reader, complexity)?.ok_or_else(|| {
                    Error::UnsupportedCombination("Nested(Nothing) is unsupported".into())
                })?;
                items.push(TupleItem {
                    name: Some(name),
                    ty,
                });
            }
            Ok(Some(TypeDesc::Nested(items)))
        }
        x if x == BinaryTypeIndex::Json as u8 => {
            let version = read_u8(reader)?;
            if version > JSON_SERIALIZATION_VERSION {
                return Err(Error::InvalidValue("unexpected JSON serialization version"));
            }
            let max_dynamic_paths =
                read_required_uvarint(reader, "missing JSON max_dynamic_paths")?;
            let max_dynamic_paths = usize::try_from(max_dynamic_paths)
                .map_err(|_| Error::Overflow("JSON max_dynamic_paths too large"))?;
            if max_dynamic_paths > JSON_MAX_DYNAMIC_PATHS_LIMIT {
                return Err(Error::InvalidValue("JSON max_dynamic_paths too large"));
            }
            let max_dynamic_types = read_u8(reader)?;
            let typed_paths_count = read_required_uvarint(reader, "missing JSON typed path count")?;
            let typed_paths_count = usize::try_from(typed_paths_count)
                .map_err(|_| Error::Overflow("JSON typed path count too large"))?;
            if typed_paths_count > JSON_MAX_TYPED_PATHS {
                return Err(Error::InvalidValue("too many JSON typed paths"));
            }
            let mut typed_paths = Vec::with_capacity(typed_paths_count);
            for _ in 0..typed_paths_count {
                let path = read_required_string(reader, "missing JSON typed path")?;
                let ty = decode_type_binary_inner(reader, complexity)?.ok_or_else(|| {
                    Error::UnsupportedCombination("JSON typed path cannot be Nothing".into())
                })?;
                typed_paths.push((path, ty));
            }
            let skip_paths_count = read_required_uvarint(reader, "missing JSON skip paths count")?;
            let skip_paths_count = usize::try_from(skip_paths_count)
                .map_err(|_| Error::Overflow("JSON skip path count too large"))?;
            let mut skip_paths = Vec::with_capacity(skip_paths_count);
            for _ in 0..skip_paths_count {
                let path = read_required_string(reader, "missing JSON skip path")?;
                skip_paths.push(path);
            }
            let skip_regexps_count =
                read_required_uvarint(reader, "missing JSON skip regexps count")?;
            let skip_regexps_count = usize::try_from(skip_regexps_count)
                .map_err(|_| Error::Overflow("JSON skip regexp count too large"))?;
            let mut skip_regexps = Vec::with_capacity(skip_regexps_count);
            for _ in 0..skip_regexps_count {
                let regex = read_required_string(reader, "missing JSON skip regexp")?;
                skip_regexps.push(regex);
            }
            Ok(Some(TypeDesc::Json {
                max_dynamic_paths,
                max_dynamic_types,
                typed_paths,
                skip_paths,
                skip_regexps,
            }))
        }
        x if x == BinaryTypeIndex::Dynamic as u8 => Err(Error::UnsupportedType("Dynamic".into())),
        other => Err(Error::UnsupportedType(format!("binary type 0x{other:02x}"))),
    }
}

fn write_tag<W: Write + ?Sized>(tag: BinaryTypeIndex, writer: &mut W) -> Result<()> {
    writer.write_all(&[tag as u8])?;
    Ok(())
}

fn read_u8<R: Read + ?Sized>(reader: &mut R) -> Result<u8> {
    let mut buf = [0_u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_required_string<R: Read + ?Sized>(reader: &mut R, context: &'static str) -> Result<String> {
    read_string(reader)?
        .ok_or_else(|| Error::Io(io::Error::new(io::ErrorKind::UnexpectedEof, context)))
}

fn read_required_uvarint<R: Read + ?Sized>(reader: &mut R, context: &'static str) -> Result<u64> {
    read_uvarint(reader)?
        .ok_or_else(|| Error::Io(io::Error::new(io::ErrorKind::UnexpectedEof, context)))
}

fn encode_tuple<W: Write + ?Sized>(items: &[TupleItem], writer: &mut W) -> Result<()> {
    let has_names = items.iter().any(|item| item.name.is_some());
    if has_names && items.iter().any(|item| item.name.is_none()) {
        return Err(Error::UnsupportedCombination(
            "Tuple with mixed named/unnamed elements is unsupported".into(),
        ));
    }
    if items.is_empty() {
        return Err(Error::InvalidValue("Tuple expects at least one type"));
    }
    if has_names {
        write_tag(BinaryTypeIndex::NamedTuple, writer)?;
        write_uvarint(items.len() as u64, writer)?;
        for item in items {
            let Some(name) = &item.name else {
                return Err(Error::Internal("Tuple name missing"));
            };
            write_string(name, writer)?;
            encode_type_binary(&item.ty, writer)?;
        }
        Ok(())
    } else {
        write_tag(BinaryTypeIndex::UnnamedTuple, writer)?;
        write_uvarint(items.len() as u64, writer)?;
        for item in items {
            encode_type_binary(&item.ty, writer)?;
        }
        Ok(())
    }
}

fn encode_nested<W: Write + ?Sized>(items: &[TupleItem], writer: &mut W) -> Result<()> {
    if items.is_empty() {
        return Err(Error::InvalidValue("Nested expects at least one element"));
    }
    if items.iter().any(|item| item.name.is_none()) {
        return Err(Error::InvalidValue("Nested field must have a name"));
    }
    write_tag(BinaryTypeIndex::Nested, writer)?;
    write_uvarint(items.len() as u64, writer)?;
    for item in items {
        let Some(name) = &item.name else {
            return Err(Error::Internal("Nested name missing"));
        };
        write_string(name, writer)?;
        encode_type_binary(&item.ty, writer)?;
    }
    Ok(())
}

fn encode_decimal_with_scale<W: Write + ?Sized>(
    tag: BinaryTypeIndex,
    precision: u8,
    scale: u8,
    writer: &mut W,
) -> Result<()> {
    validate_decimal(precision, scale, precision)?;
    write_tag(tag, writer)?;
    writer.write_all(&[precision, scale])?;
    Ok(())
}

fn decimal_tag_and_precision(size: DecimalSize) -> (BinaryTypeIndex, u8) {
    match size {
        DecimalSize::Bits32 => (BinaryTypeIndex::Decimal32, DECIMAL32_MAX_PRECISION),
        DecimalSize::Bits64 => (BinaryTypeIndex::Decimal64, DECIMAL64_MAX_PRECISION),
        DecimalSize::Bits128 => (BinaryTypeIndex::Decimal128, DECIMAL128_MAX_PRECISION),
        DecimalSize::Bits256 => (BinaryTypeIndex::Decimal256, DECIMAL256_MAX_PRECISION),
    }
}

fn decode_decimal<R: Read + ?Sized>(reader: &mut R) -> Result<(u8, u8)> {
    let precision = read_u8(reader)?;
    let scale = read_u8(reader)?;
    Ok((precision, scale))
}

fn validate_decimal(precision: u8, scale: u8, max_precision: u8) -> Result<()> {
    if precision == 0 {
        return Err(Error::InvalidValue("Decimal precision must be > 0"));
    }
    if precision > max_precision {
        return Err(Error::InvalidValue(
            "Decimal precision exceeds max precision",
        ));
    }
    if scale > precision {
        return Err(Error::InvalidValue("Decimal scale must be <= precision"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read};

    fn roundtrip(ty: &TypeDesc) -> TypeDesc {
        let mut buf = Vec::new();
        encode_type_binary(ty, &mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        let mut tag = [0_u8; 1];
        cursor.read_exact(&mut tag).unwrap();
        decode_type_binary_from_tag(tag[0], &mut cursor)
            .unwrap()
            .expect("expected type")
    }

    #[test]
    fn roundtrip_simple_types() {
        let types = vec![
            TypeDesc::UInt8,
            TypeDesc::Bool,
            TypeDesc::Int32,
            TypeDesc::UInt64,
            TypeDesc::Float32,
            TypeDesc::Float64,
            TypeDesc::String,
            TypeDesc::FixedString { length: 8 },
            TypeDesc::Date,
            TypeDesc::Date32,
            TypeDesc::DateTime { timezone: None },
            TypeDesc::DateTime {
                timezone: Some("UTC".to_string()),
            },
            TypeDesc::DateTime64 {
                precision: 3,
                timezone: None,
            },
            TypeDesc::DateTime64 {
                precision: 6,
                timezone: Some("UTC".to_string()),
            },
            TypeDesc::Uuid,
            TypeDesc::Ipv4,
            TypeDesc::Ipv6,
        ];

        for ty in types {
            assert_eq!(roundtrip(&ty), ty);
        }
    }

    #[test]
    fn roundtrip_decimal_and_enum_types() {
        let types = vec![
            TypeDesc::Decimal {
                precision: 9,
                scale: 2,
                size: DecimalSize::Bits32,
            },
            TypeDesc::Decimal {
                precision: 18,
                scale: 4,
                size: DecimalSize::Bits64,
            },
            TypeDesc::Decimal {
                precision: 38,
                scale: 6,
                size: DecimalSize::Bits128,
            },
            TypeDesc::Decimal {
                precision: 76,
                scale: 7,
                size: DecimalSize::Bits256,
            },
            TypeDesc::Enum8(vec![("a".to_string(), 1), ("b".to_string(), 2)]),
            TypeDesc::Enum16(vec![("x".to_string(), -1), ("y".to_string(), 2)]),
        ];

        for ty in types {
            assert_eq!(roundtrip(&ty), ty);
        }
    }

    #[test]
    fn roundtrip_composite_types() {
        let tuple_items = vec![
            TupleItem {
                name: None,
                ty: TypeDesc::UInt8,
            },
            TupleItem {
                name: None,
                ty: TypeDesc::String,
            },
        ];
        let named_tuple_items = vec![
            TupleItem {
                name: Some("a".to_string()),
                ty: TypeDesc::UInt8,
            },
            TupleItem {
                name: Some("b".to_string()),
                ty: TypeDesc::String,
            },
        ];

        let types = vec![
            TypeDesc::Array(Box::new(TypeDesc::UInt8)),
            TypeDesc::Map {
                key: Box::new(TypeDesc::String),
                value: Box::new(TypeDesc::UInt8),
            },
            TypeDesc::Nullable(Box::new(TypeDesc::UInt8)),
            TypeDesc::LowCardinality(Box::new(TypeDesc::String)),
            TypeDesc::Tuple(tuple_items.clone()),
            TypeDesc::Tuple(named_tuple_items.clone()),
            TypeDesc::Nested(named_tuple_items),
            TypeDesc::Json {
                max_dynamic_paths: 10,
                max_dynamic_types: 4,
                typed_paths: vec![("a".to_string(), TypeDesc::UInt8)],
                skip_paths: vec!["b".to_string()],
                skip_regexps: vec!["^c".to_string()],
            },
        ];

        for ty in types {
            assert_eq!(roundtrip(&ty), ty);
        }
    }

    #[test]
    fn rejects_dynamic_type_encoding() {
        let err = encode_type_binary(&TypeDesc::Dynamic { max_types: None }, &mut Vec::new())
            .unwrap_err();
        assert!(matches!(err, Error::UnsupportedType(_)));
    }
}
