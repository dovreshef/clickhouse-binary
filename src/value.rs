//! `RowBinary` value representation.

use std::net::{Ipv4Addr, Ipv6Addr};

use uuid::Uuid;

use crate::types::TypeDesc;

/// Runtime value used for `RowBinary` read/write APIs.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// Unsigned 8-bit integer.
    UInt8(u8),
    /// Boolean stored as 8-bit integer.
    Bool(bool),
    /// Unsigned 16-bit integer.
    UInt16(u16),
    /// Unsigned 32-bit integer.
    UInt32(u32),
    /// Unsigned 64-bit integer.
    UInt64(u64),
    /// Unsigned 128-bit integer.
    UInt128(u128),
    /// Unsigned 256-bit integer (little-endian).
    UInt256([u8; 32]),
    /// Signed 8-bit integer.
    Int8(i8),
    /// Signed 16-bit integer.
    Int16(i16),
    /// Signed 32-bit integer.
    Int32(i32),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Signed 128-bit integer.
    Int128(i128),
    /// Signed 256-bit integer (little-endian two's complement).
    Int256([u8; 32]),
    /// 32-bit floating point number.
    Float32(f32),
    /// 64-bit floating point number.
    Float64(f64),
    /// Variable-length string (binary safe).
    String(Vec<u8>),
    /// Fixed-width string (binary safe).
    FixedString(Vec<u8>),
    /// Date stored as days since Unix epoch (16-bit).
    Date(u16),
    /// Date stored as days since Unix epoch (32-bit).
    Date32(i32),
    /// `DateTime` stored as seconds since Unix epoch.
    DateTime(u32),
    /// `DateTime64` stored as scaled integer.
    DateTime64(i64),
    /// UUID column.
    Uuid(Uuid),
    /// IPv4 address column.
    Ipv4(Ipv4Addr),
    /// IPv6 address column.
    Ipv6(Ipv6Addr),
    /// Decimal32 stored as a scaled 32-bit integer.
    Decimal32(i32),
    /// Decimal64 stored as a scaled 64-bit integer.
    Decimal64(i64),
    /// Decimal128 stored as a scaled 128-bit integer.
    Decimal128(i128),
    /// Decimal256 stored as little-endian two's complement bytes.
    Decimal256([u8; 32]),
    /// Enum8 stored as an 8-bit integer.
    Enum8(i8),
    /// Enum16 stored as a 16-bit integer.
    Enum16(i16),
    /// Nullable wrapper around another value.
    Nullable(Option<Box<Value>>),
    /// Array of nested values.
    Array(Vec<Value>),
    /// Map represented as key/value pairs.
    Map(Vec<(Value, Value)>),
    /// Tuple represented as ordered values.
    Tuple(Vec<Value>),
    /// Dynamic value with an explicit runtime type.
    Dynamic {
        /// The concrete type encoded for this value.
        ty: TypeDesc,
        /// The concrete value payload.
        value: Box<Value>,
    },
    /// Dynamic NULL (encoded as `Nothing` with no payload).
    DynamicNull,
}

impl Value {
    /// Returns the `ClickHouse` type name for the value variant.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::UInt8(_) => "UInt8",
            Value::Bool(_) => "Bool",
            Value::UInt16(_) => "UInt16",
            Value::UInt32(_) => "UInt32",
            Value::UInt64(_) => "UInt64",
            Value::UInt128(_) => "UInt128",
            Value::UInt256(_) => "UInt256",
            Value::Int8(_) => "Int8",
            Value::Int16(_) => "Int16",
            Value::Int32(_) => "Int32",
            Value::Int64(_) => "Int64",
            Value::Int128(_) => "Int128",
            Value::Int256(_) => "Int256",
            Value::Float32(_) => "Float32",
            Value::Float64(_) => "Float64",
            Value::String(_) => "String",
            Value::FixedString(_) => "FixedString",
            Value::Date(_) => "Date",
            Value::Date32(_) => "Date32",
            Value::DateTime(_) => "DateTime",
            Value::DateTime64(_) => "DateTime64",
            Value::Uuid(_) => "UUID",
            Value::Ipv4(_) => "IPv4",
            Value::Ipv6(_) => "IPv6",
            Value::Decimal32(_) => "Decimal32",
            Value::Decimal64(_) => "Decimal64",
            Value::Decimal128(_) => "Decimal128",
            Value::Decimal256(_) => "Decimal256",
            Value::Enum8(_) => "Enum8",
            Value::Enum16(_) => "Enum16",
            Value::Nullable(_) => "Nullable",
            Value::Array(_) => "Array",
            Value::Map(_) => "Map",
            Value::Tuple(_) => "Tuple",
            Value::Dynamic { .. } | Value::DynamicNull => "Dynamic",
        }
    }
}

impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Value::UInt8(value)
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Value::UInt16(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Value::UInt32(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::UInt64(value)
    }
}

impl From<i8> for Value {
    fn from(value: i8) -> Self {
        Value::Int8(value)
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Value::Int16(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::Int32(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int64(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float32(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float64(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value.into_bytes())
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.as_bytes().to_vec())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::String(value)
    }
}

#[cfg(test)]
mod tests {
    use super::Value;
    use std::mem::size_of;

    #[test]
    fn value_size_is_stable() {
        assert_eq!(size_of::<Value>(), 48);
    }
}
