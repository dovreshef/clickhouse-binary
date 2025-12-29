#![deny(missing_docs)]
//! `RowBinary` read/write support for `ClickHouse` formats.

pub mod error;
pub mod io;
pub mod rowbinary;
pub mod types;
pub mod value;

pub use error::{Error, Result};
pub use rowbinary::{
    Field, Row, RowBinaryFileReader, RowBinaryFileWriter, RowBinaryFormat, RowBinaryHeader,
    RowBinaryReader, RowBinaryValueReader, RowBinaryValueWriter, RowBinaryWriter, Schema,
};
pub use types::{TypeDesc, parse_type_desc};
pub use value::Value;
