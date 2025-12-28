//! Type descriptors used by `RowBinary` read/write paths.

use std::fmt;

use crate::error::{Error, Result};

/// Parsed `ClickHouse` type descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeDesc {
    /// Unsigned 8-bit integer.
    UInt8,
    /// Boolean stored as 8-bit integer.
    Bool,
    /// Unsigned 16-bit integer.
    UInt16,
    /// Unsigned 32-bit integer.
    UInt32,
    /// Unsigned 64-bit integer.
    UInt64,
    /// Unsigned 128-bit integer.
    UInt128,
    /// Unsigned 256-bit integer.
    UInt256,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// Signed 128-bit integer.
    Int128,
    /// Signed 256-bit integer.
    Int256,
    /// 32-bit floating point number.
    Float32,
    /// 64-bit floating point number.
    Float64,
    /// Variable-length string.
    String,
    /// Fixed-width string.
    FixedString {
        /// Width in bytes.
        length: usize,
    },
    /// Date stored as days since Unix epoch (16-bit).
    Date,
    /// Date stored as days since Unix epoch (32-bit).
    Date32,
    /// `DateTime` stored as seconds since Unix epoch.
    DateTime {
        /// Optional timezone identifier.
        timezone: Option<String>,
    },
    /// `DateTime64` stored as scaled integer.
    DateTime64 {
        /// Fractional precision (decimal places).
        precision: u8,
        /// Optional timezone identifier.
        timezone: Option<String>,
    },
    /// UUID column.
    Uuid,
    /// IPv4 address column.
    Ipv4,
    /// IPv6 address column.
    Ipv6,
    /// Decimal type with explicit precision/scale.
    Decimal {
        /// Total precision (number of digits).
        precision: u8,
        /// Scale (digits after decimal point).
        scale: u8,
        /// Underlying storage size.
        size: DecimalSize,
    },
    /// Decimal32 stored as 32-bit integer with scale.
    Decimal32 {
        /// Scale (digits after decimal point).
        scale: u8,
    },
    /// Decimal64 stored as 64-bit integer with scale.
    Decimal64 {
        /// Scale (digits after decimal point).
        scale: u8,
    },
    /// Decimal128 stored as 128-bit integer with scale.
    Decimal128 {
        /// Scale (digits after decimal point).
        scale: u8,
    },
    /// Decimal256 stored as 256-bit integer with scale.
    Decimal256 {
        /// Scale (digits after decimal point).
        scale: u8,
    },
    /// Enum8 with label/value pairs.
    Enum8(Vec<(String, i8)>),
    /// Enum16 with label/value pairs.
    Enum16(Vec<(String, i16)>),
    /// Nullable wrapper around another type.
    Nullable(Box<TypeDesc>),
    /// Dictionary-encoded values stored as a low cardinality column.
    LowCardinality(Box<TypeDesc>),
    /// Array of nested values.
    Array(Box<TypeDesc>),
    /// Map implemented as Array(Tuple(key, value)).
    Map {
        /// Map key type.
        key: Box<TypeDesc>,
        /// Map value type.
        value: Box<TypeDesc>,
    },
    /// Tuple of ordered values.
    Tuple(Vec<TupleItem>),
    /// Nested type (Array of Tuple) with named elements.
    Nested(Vec<TupleItem>),
    /// Dynamic value storing per-row type info.
    Dynamic {
        /// Optional max dynamic types setting.
        max_types: Option<u8>,
    },
}

/// Named tuple element (name is optional).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TupleItem {
    /// Optional element name.
    pub name: Option<String>,
    /// Element type.
    pub ty: TypeDesc,
}

/// Backing storage size for Decimal types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecimalSize {
    /// 32-bit signed integer.
    Bits32,
    /// 64-bit signed integer.
    Bits64,
    /// 128-bit signed integer.
    Bits128,
    /// 256-bit signed integer.
    Bits256,
}

impl TypeDesc {
    /// Returns the `ClickHouse` type name.
    #[must_use]
    pub fn type_name(&self) -> String {
        match self {
            TypeDesc::UInt8 => "UInt8".into(),
            TypeDesc::Bool => "Bool".into(),
            TypeDesc::UInt16 => "UInt16".into(),
            TypeDesc::UInt32 => "UInt32".into(),
            TypeDesc::UInt64 => "UInt64".into(),
            TypeDesc::UInt128 => "UInt128".into(),
            TypeDesc::UInt256 => "UInt256".into(),
            TypeDesc::Int8 => "Int8".into(),
            TypeDesc::Int16 => "Int16".into(),
            TypeDesc::Int32 => "Int32".into(),
            TypeDesc::Int64 => "Int64".into(),
            TypeDesc::Int128 => "Int128".into(),
            TypeDesc::Int256 => "Int256".into(),
            TypeDesc::Float32 => "Float32".into(),
            TypeDesc::Float64 => "Float64".into(),
            TypeDesc::String => "String".into(),
            TypeDesc::FixedString { length } => format!("FixedString({length})"),
            TypeDesc::Date => "Date".into(),
            TypeDesc::Date32 => "Date32".into(),
            TypeDesc::DateTime { timezone } => match timezone {
                Some(value) => format!("DateTime('{value}')"),
                None => "DateTime".into(),
            },
            TypeDesc::DateTime64 {
                precision,
                timezone,
            } => match timezone {
                Some(value) => format!("DateTime64({precision}, '{value}')"),
                None => format!("DateTime64({precision})"),
            },
            TypeDesc::Uuid => "UUID".into(),
            TypeDesc::Ipv4 => "IPv4".into(),
            TypeDesc::Ipv6 => "IPv6".into(),
            TypeDesc::Decimal {
                precision, scale, ..
            } => format!("Decimal({precision}, {scale})"),
            TypeDesc::Decimal32 { scale } => format!("Decimal(9, {scale})"),
            TypeDesc::Decimal64 { scale } => format!("Decimal(18, {scale})"),
            TypeDesc::Decimal128 { scale } => format!("Decimal(38, {scale})"),
            TypeDesc::Decimal256 { scale } => format!("Decimal(76, {scale})"),
            TypeDesc::Enum8(values) => format_enum("Enum8", values),
            TypeDesc::Enum16(values) => format_enum("Enum16", values),
            TypeDesc::Nullable(inner) => format!("Nullable({})", inner.type_name()),
            TypeDesc::LowCardinality(inner) => format!("LowCardinality({})", inner.type_name()),
            TypeDesc::Array(inner) => format!("Array({})", inner.type_name()),
            TypeDesc::Map { key, value } => {
                format!("Map({}, {})", key.type_name(), value.type_name())
            }
            TypeDesc::Tuple(items) => format!("Tuple({})", format_tuple_items(items)),
            TypeDesc::Nested(items) => format!("Nested({})", format_tuple_items(items)),
            TypeDesc::Dynamic { max_types } => match max_types {
                Some(max_types) => format!("Dynamic(max_types={max_types})"),
                None => "Dynamic".into(),
            },
        }
    }
}

impl fmt::Display for TypeDesc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.type_name())
    }
}

/// Parses a textual `ClickHouse` type into a structured descriptor.
///
/// # Errors
///
/// Returns [`Error::InvalidValue`] when the descriptor is malformed or
/// [`Error::UnsupportedType`] for unsupported types.
#[allow(clippy::too_many_lines)]
pub fn parse_type_desc(input: &str) -> Result<TypeDesc> {
    let trimmed = input.trim();
    match trimmed {
        "UInt8" => Ok(TypeDesc::UInt8),
        "Bool" => Ok(TypeDesc::Bool),
        "UInt16" => Ok(TypeDesc::UInt16),
        "UInt32" => Ok(TypeDesc::UInt32),
        "UInt64" => Ok(TypeDesc::UInt64),
        "UInt128" => Ok(TypeDesc::UInt128),
        "UInt256" => Ok(TypeDesc::UInt256),
        "Int8" => Ok(TypeDesc::Int8),
        "Int16" => Ok(TypeDesc::Int16),
        "Int32" => Ok(TypeDesc::Int32),
        "Int64" => Ok(TypeDesc::Int64),
        "Int128" => Ok(TypeDesc::Int128),
        "Int256" => Ok(TypeDesc::Int256),
        "Float32" => Ok(TypeDesc::Float32),
        "Float64" => Ok(TypeDesc::Float64),
        "String" => Ok(TypeDesc::String),
        "Date" => Ok(TypeDesc::Date),
        "Date32" => Ok(TypeDesc::Date32),
        "DateTime" => Ok(TypeDesc::DateTime { timezone: None }),
        "UUID" => Ok(TypeDesc::Uuid),
        "IPv4" => Ok(TypeDesc::Ipv4),
        "IPv6" => Ok(TypeDesc::Ipv6),
        "Dynamic" => Ok(TypeDesc::Dynamic { max_types: None }),
        _ => {
            if let Some(inner) = trimmed.strip_prefix("Decimal(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Decimal type"))?;
                let (precision, scale) = parse_decimal_precision_scale(inner)?;
                let size = decimal_size_for_precision(precision)?;
                return Ok(TypeDesc::Decimal {
                    precision,
                    scale,
                    size,
                });
            }
            if let Some(inner) = trimmed.strip_prefix("Decimal32(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Decimal32 type"))?;
                let scale = parse_decimal_scale(inner, 9)?;
                return Ok(TypeDesc::Decimal32 { scale });
            }
            if let Some(inner) = trimmed.strip_prefix("Decimal64(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Decimal64 type"))?;
                let scale = parse_decimal_scale(inner, 18)?;
                return Ok(TypeDesc::Decimal64 { scale });
            }
            if let Some(inner) = trimmed.strip_prefix("Decimal128(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Decimal128 type"))?;
                let scale = parse_decimal_scale(inner, 38)?;
                return Ok(TypeDesc::Decimal128 { scale });
            }
            if let Some(inner) = trimmed.strip_prefix("Decimal256(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Decimal256 type"))?;
                let scale = parse_decimal_scale(inner, 76)?;
                return Ok(TypeDesc::Decimal256 { scale });
            }
            if let Some(inner) = trimmed.strip_prefix("Enum8(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Enum8 type"))?;
                let values = parse_enum_variants(inner, EnumStorage::Enum8)?;
                let values = values
                    .into_iter()
                    .map(|(name, value)| {
                        let value = i8::try_from(value)
                            .map_err(|_| Error::InvalidValue("Enum8 value out of range"))?;
                        Ok((name, value))
                    })
                    .collect::<Result<Vec<_>>>()?;
                return Ok(TypeDesc::Enum8(values));
            }
            if let Some(inner) = trimmed.strip_prefix("Enum16(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Enum16 type"))?;
                let values = parse_enum_variants(inner, EnumStorage::Enum16)?;
                return Ok(TypeDesc::Enum16(values));
            }
            if let Some(inner) = trimmed.strip_prefix("Dynamic(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Dynamic type"))?;
                let mut parts = inner.splitn(2, '=');
                let key = parts
                    .next()
                    .ok_or(Error::InvalidValue("Dynamic expects max_types=N"))?
                    .trim();
                let value = parts
                    .next()
                    .ok_or(Error::InvalidValue("Dynamic expects max_types=N"))?
                    .trim();
                if key != "max_types" {
                    return Err(Error::InvalidValue("Dynamic expects max_types=N"));
                }
                let max_types: u64 = value
                    .parse()
                    .map_err(|_| Error::InvalidValue("invalid Dynamic max_types"))?;
                if max_types > u64::from(u8::MAX) {
                    return Err(Error::InvalidValue("Dynamic max_types out of range"));
                }
                let max_types = u8::try_from(max_types)
                    .map_err(|_| Error::InvalidValue("Dynamic max_types out of range"))?;
                return Ok(TypeDesc::Dynamic {
                    max_types: Some(max_types),
                });
            }
            if let Some(inner) = trimmed.strip_prefix("LowCardinality(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated LowCardinality type"))?;
                let desc = parse_type_desc(inner)?;
                if matches!(desc, TypeDesc::LowCardinality(_)) {
                    return Err(Error::UnsupportedCombination(
                        "LowCardinality(LowCardinality(T)) is unsupported".into(),
                    ));
                }
                if !can_be_inside_low_cardinality(&desc) {
                    return Err(Error::UnsupportedCombination(format!(
                        "LowCardinality({}) is unsupported",
                        desc.type_name()
                    )));
                }
                return Ok(TypeDesc::LowCardinality(Box::new(desc)));
            }
            if let Some(inner) = trimmed.strip_prefix("Nullable(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Nullable type"))?;
                let desc = parse_type_desc(inner)?;
                if matches!(desc, TypeDesc::Nullable(_)) {
                    return Err(Error::UnsupportedCombination(
                        "Nullable(Nullable(T)) is unsupported".into(),
                    ));
                }
                if matches!(desc, TypeDesc::Tuple(_)) {
                    return Err(Error::UnsupportedCombination(
                        "Nullable(Tuple(...)) is unsupported".into(),
                    ));
                }
                return Ok(TypeDesc::Nullable(Box::new(desc)));
            }
            if let Some(inner) = trimmed.strip_prefix("Array(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Array type"))?;
                let desc = parse_type_desc(inner)?;
                return Ok(TypeDesc::Array(Box::new(desc)));
            }
            if let Some(inner) = trimmed.strip_prefix("Map(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Map type"))?;
                let (key, value) = parse_map_descriptor(inner)?;
                if !is_valid_map_key(&key) {
                    return Err(Error::UnsupportedCombination(format!(
                        "Map cannot have a key of type {}",
                        key.type_name()
                    )));
                }
                return Ok(TypeDesc::Map {
                    key: Box::new(key),
                    value: Box::new(value),
                });
            }
            if let Some(inner) = trimmed.strip_prefix("Tuple(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Tuple type"))?;
                let items = parse_tuple_descriptor(inner)?;
                return Ok(TypeDesc::Tuple(items));
            }
            if let Some(inner) = trimmed.strip_prefix("Nested(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated Nested type"))?;
                let items = parse_nested_descriptor(inner)?;
                return Ok(TypeDesc::Nested(items));
            }
            if let Some(inner) = trimmed.strip_prefix("DateTime(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated DateTime type"))?;
                let timezone = parse_timezone(inner)?;
                return Ok(TypeDesc::DateTime {
                    timezone: Some(timezone),
                });
            }
            if let Some(inner) = trimmed.strip_prefix("DateTime64(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated DateTime64 type"))?;
                let (precision, timezone) = parse_datetime64(inner)?;
                return Ok(TypeDesc::DateTime64 {
                    precision,
                    timezone,
                });
            }
            if let Some(inner) = trimmed.strip_prefix("FixedString(") {
                let inner = inner
                    .strip_suffix(')')
                    .ok_or(Error::InvalidValue("unterminated FixedString type"))?;
                let length: usize = inner
                    .trim()
                    .parse()
                    .map_err(|_| Error::InvalidValue("invalid FixedString length"))?;
                if length == 0 {
                    return Err(Error::InvalidValue("FixedString length must be > 0"));
                }
                return Ok(TypeDesc::FixedString { length });
            }
            Err(Error::UnsupportedType(trimmed.to_string()))
        }
    }
}

pub(crate) fn can_be_inside_low_cardinality(desc: &TypeDesc) -> bool {
    match desc {
        TypeDesc::UInt8
        | TypeDesc::Bool
        | TypeDesc::UInt16
        | TypeDesc::UInt32
        | TypeDesc::UInt64
        | TypeDesc::UInt128
        | TypeDesc::UInt256
        | TypeDesc::Int8
        | TypeDesc::Int16
        | TypeDesc::Int32
        | TypeDesc::Int64
        | TypeDesc::Int128
        | TypeDesc::Int256
        | TypeDesc::Float32
        | TypeDesc::Float64
        | TypeDesc::String
        | TypeDesc::FixedString { .. }
        | TypeDesc::Date
        | TypeDesc::Date32
        | TypeDesc::DateTime { .. }
        | TypeDesc::Uuid
        | TypeDesc::Ipv4
        | TypeDesc::Ipv6 => true,
        TypeDesc::Nullable(inner) => can_be_inside_low_cardinality(inner),
        TypeDesc::LowCardinality(_)
        | TypeDesc::Array(_)
        | TypeDesc::Map { .. }
        | TypeDesc::Tuple(_)
        | TypeDesc::Nested(_)
        | TypeDesc::Dynamic { .. }
        | TypeDesc::DateTime64 { .. }
        | TypeDesc::Decimal { .. }
        | TypeDesc::Decimal32 { .. }
        | TypeDesc::Decimal64 { .. }
        | TypeDesc::Decimal128 { .. }
        | TypeDesc::Decimal256 { .. }
        | TypeDesc::Enum8(_)
        | TypeDesc::Enum16(_) => false,
    }
}

pub(crate) fn is_valid_map_key(desc: &TypeDesc) -> bool {
    match desc {
        TypeDesc::Nullable(_) => false,
        TypeDesc::LowCardinality(inner) => !matches!(inner.as_ref(), TypeDesc::Nullable(_)),
        _ => true,
    }
}

fn parse_timezone(input: &str) -> Result<String> {
    let trimmed = input.trim();
    let trimmed = trimmed
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .ok_or(Error::InvalidValue("timezone must be quoted"))?;
    if trimmed.is_empty() {
        return Err(Error::InvalidValue("timezone cannot be empty"));
    }
    Ok(trimmed.to_string())
}

fn parse_datetime64(input: &str) -> Result<(u8, Option<String>)> {
    let mut parts = input.splitn(2, ',').map(str::trim);
    let precision_part = parts
        .next()
        .ok_or(Error::InvalidValue("missing DateTime64 precision"))?;
    let precision: u8 = precision_part
        .parse()
        .map_err(|_| Error::InvalidValue("invalid DateTime64 precision"))?;
    let timezone = if let Some(tz_part) = parts.next() {
        Some(parse_timezone(tz_part)?)
    } else {
        None
    };
    Ok((precision, timezone))
}

fn parse_map_descriptor(input: &str) -> Result<(TypeDesc, TypeDesc)> {
    let mut depth = 0_i32;
    let mut split = None;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                split = Some(idx);
                break;
            }
            _ => {}
        }
    }
    let split = split.ok_or(Error::InvalidValue("Map expects two type arguments"))?;
    let (left, right) = input.split_at(split);
    let key = parse_type_desc(left.trim())?;
    let value = parse_type_desc(right[1..].trim())?;
    Ok((key, value))
}

fn parse_tuple_descriptor(input: &str) -> Result<Vec<TupleItem>> {
    let items = split_top_level_commas_with_parens(input);
    if items.is_empty() {
        return Err(Error::InvalidValue("Tuple expects at least one type"));
    }
    let mut types = Vec::with_capacity(items.len());
    for item in items {
        let item = parse_tuple_item(item)?;
        types.push(item);
    }
    Ok(types)
}

fn parse_nested_descriptor(input: &str) -> Result<Vec<TupleItem>> {
    let items = split_top_level_commas_with_parens(input);
    if items.is_empty() {
        return Err(Error::InvalidValue("Nested expects at least one element"));
    }
    let mut fields = Vec::with_capacity(items.len());
    for item in items {
        let field = parse_tuple_item(item)?;
        if field.name.is_none() {
            return Err(Error::InvalidValue("Nested field must have a name"));
        }
        fields.push(field);
    }
    Ok(fields)
}

fn parse_decimal_precision_scale(input: &str) -> Result<(u8, u8)> {
    let mut parts = input.splitn(2, ',').map(str::trim);
    let precision_part = parts
        .next()
        .ok_or(Error::InvalidValue("missing Decimal precision"))?;
    let scale_part = parts
        .next()
        .ok_or(Error::InvalidValue("missing Decimal scale"))?;
    let precision: u8 = precision_part
        .parse()
        .map_err(|_| Error::InvalidValue("invalid Decimal precision"))?;
    let scale: u8 = scale_part
        .parse()
        .map_err(|_| Error::InvalidValue("invalid Decimal scale"))?;
    if precision == 0 {
        return Err(Error::InvalidValue("Decimal precision must be > 0"));
    }
    if scale > precision {
        return Err(Error::InvalidValue("Decimal scale must be <= precision"));
    }
    Ok((precision, scale))
}

fn parse_decimal_scale(input: &str, max_scale: u8) -> Result<u8> {
    let scale: u8 = input
        .trim()
        .parse()
        .map_err(|_| Error::InvalidValue("invalid Decimal scale"))?;
    if scale > max_scale {
        return Err(Error::InvalidValue("Decimal scale exceeds max precision"));
    }
    Ok(scale)
}

fn decimal_size_for_precision(precision: u8) -> Result<DecimalSize> {
    match precision {
        1..=9 => Ok(DecimalSize::Bits32),
        10..=18 => Ok(DecimalSize::Bits64),
        19..=38 => Ok(DecimalSize::Bits128),
        39..=76 => Ok(DecimalSize::Bits256),
        _ => Err(Error::InvalidValue(
            "Decimal precision must be between 1 and 76",
        )),
    }
}

#[derive(Clone, Copy)]
enum EnumStorage {
    Enum8,
    Enum16,
}

fn parse_enum_variants(input: &str, storage: EnumStorage) -> Result<Vec<(String, i16)>> {
    let entries = split_top_level_commas(input);
    if entries.is_empty() {
        return Err(Error::InvalidValue("Enum must have at least one value"));
    }
    let mut variants = Vec::with_capacity(entries.len());
    for entry in entries {
        let (name, value) = parse_enum_entry(entry)?;
        let value = match storage {
            EnumStorage::Enum8 => {
                if value < i64::from(i8::MIN) || value > i64::from(i8::MAX) {
                    return Err(Error::InvalidValue("Enum8 value out of range"));
                }
                i16::from(
                    i8::try_from(value)
                        .map_err(|_| Error::InvalidValue("Enum8 value out of range"))?,
                )
            }
            EnumStorage::Enum16 => {
                if value < i64::from(i16::MIN) || value > i64::from(i16::MAX) {
                    return Err(Error::InvalidValue("Enum16 value out of range"));
                }
                i16::try_from(value)
                    .map_err(|_| Error::InvalidValue("Enum16 value out of range"))?
            }
        };
        variants.push((name, value));
    }
    Ok(variants)
}

fn parse_enum_entry(input: &str) -> Result<(String, i64)> {
    let mut in_quote = false;
    let mut escape = false;
    let mut split = None;
    for (idx, ch) in input.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_quote => escape = true,
            '\'' => in_quote = !in_quote,
            '=' if !in_quote => {
                split = Some(idx);
                break;
            }
            _ => {}
        }
    }
    let split = split.ok_or(Error::InvalidValue("Enum entry must contain '='"))?;
    let (left, right) = input.split_at(split);
    let name = parse_quoted_string(left.trim())?;
    let value: i64 = right[1..]
        .trim()
        .parse()
        .map_err(|_| Error::InvalidValue("invalid Enum value"))?;
    Ok((name, value))
}

fn parse_quoted_string(input: &str) -> Result<String> {
    let mut chars = input.chars();
    if chars.next() != Some('\'') || !input.ends_with('\'') || input.len() < 2 {
        return Err(Error::InvalidValue("Enum name must be single-quoted"));
    }
    let mut result = String::new();
    let mut escape = false;
    for ch in input[1..input.len() - 1].chars() {
        if escape {
            result.push(ch);
            escape = false;
        } else if ch == '\\' {
            escape = true;
        } else {
            result.push(ch);
        }
    }
    if escape {
        return Err(Error::InvalidValue("invalid escape in Enum name"));
    }
    Ok(result)
}

fn split_top_level_commas(input: &str) -> Vec<&str> {
    let mut entries = Vec::new();
    let mut in_quote = false;
    let mut escape = false;
    let mut start = 0;
    for (idx, ch) in input.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_quote => escape = true,
            '\'' => in_quote = !in_quote,
            ',' if !in_quote => {
                entries.push(input[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
    }
    if start < input.len() {
        let tail = input[start..].trim();
        if !tail.is_empty() {
            entries.push(tail);
        }
    }
    entries
}

fn split_top_level_commas_with_parens(input: &str) -> Vec<&str> {
    let mut entries = Vec::new();
    let mut in_quote = false;
    let mut escape = false;
    let mut depth = 0_i32;
    let mut start = 0;
    for (idx, ch) in input.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_quote => escape = true,
            '\'' => in_quote = !in_quote,
            '(' if !in_quote => depth += 1,
            ')' if !in_quote => depth -= 1,
            ',' if !in_quote && depth == 0 => {
                entries.push(input[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
    }
    if start < input.len() {
        let tail = input[start..].trim();
        if !tail.is_empty() {
            entries.push(tail);
        }
    }
    entries
}

fn parse_tuple_item(input: &str) -> Result<TupleItem> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidValue("Tuple element cannot be empty"));
    }
    if let Some((name, ty)) = split_name_and_type(trimmed)? {
        return Ok(TupleItem {
            name: Some(parse_identifier(name)?),
            ty: parse_type_desc(ty)?,
        });
    }
    Ok(TupleItem {
        name: None,
        ty: parse_type_desc(trimmed)?,
    })
}

fn split_name_and_type(input: &str) -> Result<Option<(&str, &str)>> {
    let mut in_quote = false;
    let mut escape = false;
    let mut depth = 0_i32;
    for (idx, ch) in input.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_quote => escape = true,
            '\'' => in_quote = !in_quote,
            '(' if !in_quote => depth += 1,
            ')' if !in_quote => depth -= 1,
            ch if ch.is_whitespace() && !in_quote && depth == 0 => {
                let (left, right) = input.split_at(idx);
                let name = left.trim();
                let ty = right.trim();
                if name.is_empty() || ty.is_empty() {
                    return Err(Error::InvalidValue("Tuple element name/type missing"));
                }
                return Ok(Some((name, ty)));
            }
            _ => {}
        }
    }
    Ok(None)
}

fn parse_identifier(input: &str) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidValue("empty identifier"));
    }
    let unquoted = if (trimmed.starts_with('`') && trimmed.ends_with('`'))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };
    if unquoted.is_empty() {
        return Err(Error::InvalidValue("empty identifier"));
    }
    Ok(unquoted.to_string())
}

fn format_tuple_items(items: &[TupleItem]) -> String {
    items
        .iter()
        .map(|item| match &item.name {
            Some(name) => format!("{} {}", format_identifier(name), item.ty.type_name()),
            None => item.ty.type_name(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_identifier(name: &str) -> String {
    let simple = name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_');
    if simple && !name.is_empty() {
        name.to_string()
    } else {
        let escaped = name.replace('`', "``");
        format!("`{escaped}`")
    }
}

fn format_enum<T: fmt::Display>(prefix: &str, values: &[(String, T)]) -> String {
    let entries: Vec<String> = values
        .iter()
        .map(|(name, value)| format!("'{}' = {}", escape_enum_name(name), value))
        .collect();
    format!("{prefix}({})", entries.join(", "))
}

fn escape_enum_name(name: &str) -> String {
    name.replace('\'', "\\'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_low_cardinality_of_unsupported_types() {
        let err = parse_type_desc("LowCardinality(DateTime64(3))").unwrap_err();
        assert!(matches!(err, Error::UnsupportedCombination(_)));
        let err = parse_type_desc("LowCardinality(Array(UInt8))").unwrap_err();
        assert!(matches!(err, Error::UnsupportedCombination(_)));
    }

    #[test]
    fn rejects_map_keys_that_are_nullable() {
        let err = parse_type_desc("Map(Nullable(UInt8), UInt8)").unwrap_err();
        assert!(matches!(err, Error::UnsupportedCombination(_)));
        let err = parse_type_desc("Map(LowCardinality(Nullable(String)), UInt8)").unwrap_err();
        assert!(matches!(err, Error::UnsupportedCombination(_)));
    }

    #[test]
    fn rejects_low_cardinality_decimal_and_enum() {
        let decimals = [
            "LowCardinality(Decimal(9,2))",
            "LowCardinality(Decimal32(2))",
            "LowCardinality(Decimal64(2))",
            "LowCardinality(Decimal128(2))",
            "LowCardinality(Decimal256(2))",
        ];
        for ty in decimals {
            let err = parse_type_desc(ty).unwrap_err();
            assert!(matches!(err, Error::UnsupportedCombination(_)));
        }

        let enums = [
            "LowCardinality(Enum8('a' = 1, 'b' = 2))",
            "LowCardinality(Enum16('a' = 1, 'b' = 2))",
        ];
        for ty in enums {
            let err = parse_type_desc(ty).unwrap_err();
            assert!(matches!(err, Error::UnsupportedCombination(_)));
        }
    }

    #[test]
    fn rejects_low_cardinality_tuple() {
        let err = parse_type_desc("LowCardinality(Tuple(UInt8, String))").unwrap_err();
        assert!(matches!(err, Error::UnsupportedCombination(_)));
    }

    #[test]
    fn parses_dynamic_types() {
        assert_eq!(
            parse_type_desc("Dynamic").unwrap(),
            TypeDesc::Dynamic { max_types: None }
        );
        assert_eq!(
            parse_type_desc("Dynamic(max_types=12)").unwrap(),
            TypeDesc::Dynamic {
                max_types: Some(12)
            }
        );
    }
}
