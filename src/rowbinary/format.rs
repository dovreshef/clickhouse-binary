//! `RowBinary` format definitions.

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
