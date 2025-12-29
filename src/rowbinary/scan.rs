//! Helpers for scanning `RowBinary` streams without allocating values.

use std::io::{self, Read};

use crate::{
    error::{Error, Result},
    io::{read_string, read_uvarint},
    types::{DecimalSize, TupleItem, TypeDesc},
};

use super::type_binary::decode_type_binary_from_tag;

const DISCARD_CHUNK: usize = 8 * 1024;

/// Reader wrapper that captures all bytes read into a buffer.
pub(crate) struct CaptureReader<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut Vec<u8>,
}

impl<'a, R: ?Sized> CaptureReader<'a, R> {
    pub(crate) fn new(reader: &'a mut R, buf: &'a mut Vec<u8>) -> Self {
        Self { reader, buf }
    }
}

impl<R: Read + ?Sized> Read for CaptureReader<'_, R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.reader.read(out)?;
        if n > 0 {
            self.buf.extend_from_slice(&out[..n]);
        }
        Ok(n)
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn skip_value_optional<R: Read + ?Sized>(
    ty: &TypeDesc,
    reader: &mut R,
) -> Result<Option<()>> {
    if let Some(len) = fixed_len_for_type(ty) {
        return read_fixed(reader, len);
    }

    match ty {
        TypeDesc::String => skip_bytes(reader),
        TypeDesc::FixedString { length } => {
            if *length == 0 {
                return Err(Error::InvalidValue("FixedString length must be > 0"));
            }
            let mut first = [0_u8; 1];
            if read_exact_or_eof(reader, &mut first)? {
                return Ok(None);
            }
            discard_exact(reader, length - 1)?;
            Ok(Some(()))
        }
        TypeDesc::Nullable(inner) => {
            let mut flag = [0_u8; 1];
            if read_exact_or_eof(reader, &mut flag)? {
                return Ok(None);
            }
            match flag[0] {
                0 => {
                    skip_value_required(inner, reader)?;
                    Ok(Some(()))
                }
                1 => Ok(Some(())),
                _ => Err(Error::InvalidValue("invalid nullable flag")),
            }
        }
        TypeDesc::LowCardinality(inner) => skip_value_optional(inner, reader),
        TypeDesc::Array(inner) => {
            let Some(len) = read_uvarint(reader)? else {
                return Ok(None);
            };
            let len =
                usize::try_from(len).map_err(|_| Error::Overflow("array length too large"))?;
            for _ in 0..len {
                skip_value_required(inner, reader)?;
            }
            Ok(Some(()))
        }
        TypeDesc::Map { key, value } => {
            let Some(len) = read_uvarint(reader)? else {
                return Ok(None);
            };
            let len = usize::try_from(len).map_err(|_| Error::Overflow("map length too large"))?;
            for _ in 0..len {
                skip_value_required(key, reader)?;
                skip_value_required(value, reader)?;
            }
            Ok(Some(()))
        }
        TypeDesc::Tuple(items) => skip_tuple(items, reader),
        TypeDesc::Nested(items) => {
            let Some(len) = read_uvarint(reader)? else {
                return Ok(None);
            };
            let len =
                usize::try_from(len).map_err(|_| Error::Overflow("array length too large"))?;
            for _ in 0..len {
                skip_tuple_required(items, reader)?;
            }
            Ok(Some(()))
        }
        TypeDesc::Variant(variants) => {
            let mut tag = [0_u8; 1];
            if read_exact_or_eof(reader, &mut tag)? {
                return Ok(None);
            }
            if tag[0] == u8::MAX {
                return Ok(Some(()));
            }
            let index = usize::from(tag[0]);
            let variant = variants
                .get(index)
                .ok_or(Error::InvalidValue("Variant discriminator out of range"))?;
            skip_value_required(variant, reader)?;
            Ok(Some(()))
        }
        TypeDesc::Json { typed_paths, .. } => {
            let Some(path_count) = read_uvarint(reader)? else {
                return Ok(None);
            };
            let path_count = usize::try_from(path_count)
                .map_err(|_| Error::Overflow("JSON path count too large"))?;
            for _ in 0..path_count {
                let path = read_string(reader)?.ok_or_else(|| {
                    Error::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "missing JSON path",
                    ))
                })?;
                if let Some((_, ty)) = typed_paths.iter().find(|(name, _)| name == &path) {
                    skip_value_required(ty, reader)?;
                } else {
                    skip_value_required(&TypeDesc::Dynamic { max_types: None }, reader)?;
                }
            }
            Ok(Some(()))
        }
        TypeDesc::Dynamic { .. } => {
            let mut tag = [0_u8; 1];
            if read_exact_or_eof(reader, &mut tag)? {
                return Ok(None);
            }
            let ty = decode_type_binary_from_tag(tag[0], reader)?;
            let Some(ty) = ty else {
                return Ok(Some(()));
            };
            skip_value_required(&ty, reader)?;
            Ok(Some(()))
        }
        TypeDesc::Decimal { size, .. } => {
            let len = match size {
                DecimalSize::Bits32 => 4,
                DecimalSize::Bits64 => 8,
                DecimalSize::Bits128 => 16,
                DecimalSize::Bits256 => 32,
            };
            read_fixed(reader, len)
        }
        TypeDesc::Nothing => Ok(Some(())),
        _ => Err(Error::Internal("unsupported skip type")),
    }
}

pub(crate) fn skip_value_required<R: Read + ?Sized>(ty: &TypeDesc, reader: &mut R) -> Result<()> {
    match skip_value_optional(ty, reader)? {
        Some(()) => Ok(()),
        None => Err(Error::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "unexpected EOF while reading row",
        ))),
    }
}

fn skip_tuple<R: Read + ?Sized>(items: &[TupleItem], reader: &mut R) -> Result<Option<()>> {
    let mut iter = items.iter();
    let Some(first) = iter.next() else {
        return Ok(Some(()));
    };
    let Some(()) = skip_value_optional(&first.ty, reader)? else {
        return Ok(None);
    };
    for item in iter {
        skip_value_required(&item.ty, reader)?;
    }
    Ok(Some(()))
}

fn skip_tuple_required<R: Read + ?Sized>(items: &[TupleItem], reader: &mut R) -> Result<()> {
    match skip_tuple(items, reader)? {
        Some(()) => Ok(()),
        None => Err(Error::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "unexpected EOF while reading row",
        ))),
    }
}

fn skip_bytes<R: Read + ?Sized>(reader: &mut R) -> Result<Option<()>> {
    let Some(len) = read_uvarint(reader)? else {
        return Ok(None);
    };
    let len = usize::try_from(len).map_err(|_| Error::Overflow("byte length too large"))?;
    discard_exact(reader, len)?;
    Ok(Some(()))
}

fn read_fixed<R: Read + ?Sized>(reader: &mut R, len: usize) -> Result<Option<()>> {
    let mut buf = vec![0_u8; len];
    if read_exact_or_eof(reader, &mut buf)? {
        return Ok(None);
    }
    Ok(Some(()))
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

fn discard_exact<R: Read + ?Sized>(reader: &mut R, mut len: usize) -> Result<()> {
    let mut buf = [0_u8; DISCARD_CHUNK];
    while len > 0 {
        let take = len.min(buf.len());
        reader.read_exact(&mut buf[..take])?;
        len -= take;
    }
    Ok(())
}

fn fixed_len_for_type(ty: &TypeDesc) -> Option<usize> {
    match ty {
        TypeDesc::UInt8 | TypeDesc::Bool | TypeDesc::Int8 | TypeDesc::Enum8(_) => Some(1),
        TypeDesc::UInt16
        | TypeDesc::Int16
        | TypeDesc::Enum16(_)
        | TypeDesc::Date
        | TypeDesc::Float16
        | TypeDesc::BFloat16 => Some(2),
        TypeDesc::UInt32
        | TypeDesc::Int32
        | TypeDesc::Float32
        | TypeDesc::Date32
        | TypeDesc::DateTime { .. }
        | TypeDesc::Decimal32 { .. }
        | TypeDesc::Ipv4 => Some(4),
        TypeDesc::UInt64
        | TypeDesc::Int64
        | TypeDesc::Float64
        | TypeDesc::DateTime64 { .. }
        | TypeDesc::Decimal64 { .. } => Some(8),
        TypeDesc::UInt128
        | TypeDesc::Int128
        | TypeDesc::Decimal128 { .. }
        | TypeDesc::Uuid
        | TypeDesc::Ipv6 => Some(16),
        TypeDesc::UInt256 | TypeDesc::Int256 | TypeDesc::Decimal256 { .. } => Some(32),
        _ => None,
    }
}
