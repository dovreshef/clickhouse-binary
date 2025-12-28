//! `RowBinary` read/write support.

mod format;
mod reader;
mod schema;
mod value_rw;
mod writer;

pub use format::RowBinaryFormat;
pub use reader::RowBinaryReader;
pub use schema::{Field, Row, Schema};
pub use writer::RowBinaryWriter;
