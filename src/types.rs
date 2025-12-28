//! Type descriptors used by `RowBinary` read/write paths.

use std::fmt;

use crate::error::{Error, Result};

/// Parsed `ClickHouse` type descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeDesc {
    /// Unsigned 8-bit integer.
    UInt8,
    /// Unsigned 16-bit integer.
    UInt16,
    /// Unsigned 32-bit integer.
    UInt32,
    /// Unsigned 64-bit integer.
    UInt64,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
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
}

impl TypeDesc {
    /// Returns the `ClickHouse` type name.
    #[must_use]
    pub fn type_name(&self) -> String {
        match self {
            TypeDesc::UInt8 => "UInt8".into(),
            TypeDesc::UInt16 => "UInt16".into(),
            TypeDesc::UInt32 => "UInt32".into(),
            TypeDesc::UInt64 => "UInt64".into(),
            TypeDesc::Int8 => "Int8".into(),
            TypeDesc::Int16 => "Int16".into(),
            TypeDesc::Int32 => "Int32".into(),
            TypeDesc::Int64 => "Int64".into(),
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
            TypeDesc::Nullable(inner) => format!("Nullable({})", inner.type_name()),
            TypeDesc::LowCardinality(inner) => format!("LowCardinality({})", inner.type_name()),
            TypeDesc::Array(inner) => format!("Array({})", inner.type_name()),
            TypeDesc::Map { key, value } => {
                format!("Map({}, {})", key.type_name(), value.type_name())
            }
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
        "UInt16" => Ok(TypeDesc::UInt16),
        "UInt32" => Ok(TypeDesc::UInt32),
        "UInt64" => Ok(TypeDesc::UInt64),
        "Int8" => Ok(TypeDesc::Int8),
        "Int16" => Ok(TypeDesc::Int16),
        "Int32" => Ok(TypeDesc::Int32),
        "Int64" => Ok(TypeDesc::Int64),
        "Float32" => Ok(TypeDesc::Float32),
        "Float64" => Ok(TypeDesc::Float64),
        "String" => Ok(TypeDesc::String),
        "Date" => Ok(TypeDesc::Date),
        "Date32" => Ok(TypeDesc::Date32),
        "DateTime" => Ok(TypeDesc::DateTime { timezone: None }),
        "UUID" => Ok(TypeDesc::Uuid),
        "IPv4" => Ok(TypeDesc::Ipv4),
        "IPv6" => Ok(TypeDesc::Ipv6),
        _ => {
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

fn can_be_inside_low_cardinality(desc: &TypeDesc) -> bool {
    match desc {
        TypeDesc::UInt8
        | TypeDesc::UInt16
        | TypeDesc::UInt32
        | TypeDesc::UInt64
        | TypeDesc::Int8
        | TypeDesc::Int16
        | TypeDesc::Int32
        | TypeDesc::Int64
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
        | TypeDesc::DateTime64 { .. } => false,
    }
}

fn is_valid_map_key(desc: &TypeDesc) -> bool {
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
}
