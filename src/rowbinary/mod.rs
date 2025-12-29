//! `RowBinary` read/write support.

mod format;
mod reader;
mod scan;
mod schema;
mod type_binary;
mod value_rw;
mod writer;

pub use format::RowBinaryFormat;
pub use reader::{RowBinaryHeader, RowBinaryReader, RowBinaryValueReader};
pub use schema::{Field, Row, Schema};
pub use writer::{RowBinaryValueWriter, RowBinaryWriter};

/// File-backed seekable Zstd reader.
pub type RowBinaryFileReader = RowBinaryReader<std::io::BufReader<std::fs::File>>;

/// File-backed seekable Zstd writer.
pub type RowBinaryFileWriter = RowBinaryWriter<std::io::BufWriter<std::fs::File>>;
