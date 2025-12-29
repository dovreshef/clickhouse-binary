//! Public error type used by every module in the crate.

/// Convenient alias over [`std::result::Result`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// High-level error definition covering IO and data mistakes.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// IO error bubbling from [`std::io`].
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Zstd seekable error bubbling from `zeekstd`.
    #[error("zstd seekable error: {0}")]
    Zstd(#[from] zeekstd::Error),
    /// Returned when a value is outside of the supported domain of a type.
    #[error("invalid value: {0}")]
    InvalidValue(&'static str),
    /// Returned when input or schema contains unsupported types.
    #[error("unsupported type: {0}")]
    UnsupportedType(String),
    /// Returned when a combination of types or wrappers is not supported.
    #[error("unsupported type combination: {0}")]
    UnsupportedCombination(String),
    /// Returned when a value does not match the schema type.
    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        /// Expected type name.
        expected: String,
        /// Actual value type name.
        actual: String,
    },
    /// Returned when data exceeds protocol or platform limits (e.g., string
    /// offsets larger than `u64`).
    #[error("value overflow: {0}")]
    Overflow(&'static str),
    /// Raised when an invariant that "should never happen" fires (internal
    /// bug or upstream issue).
    #[error("internal error: {0}")]
    Internal(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_human_friendly() {
        // Ensure every variant renders a descriptive message so users see an
        // actionable error when formatting via Display.
        let io_err = Error::Io(std::io::Error::other("boom"));
        assert!(format!("{io_err}").contains("io error"));

        let zstd_err = Error::Zstd(zeekstd::Error::from(std::io::Error::other("zstd")));
        assert!(format!("{zstd_err}").contains("zstd seekable error"));

        let invalid = Error::InvalidValue("oops");
        assert!(format!("{invalid}").contains("oops"));

        let unsupported = Error::UnsupportedType("Decimal(1)".into());
        assert!(format!("{unsupported}").contains("unsupported type"));

        let combo = Error::UnsupportedCombination("Nullable(Array(T))".into());
        assert!(format!("{combo}").contains("unsupported type combination"));

        let mismatch = Error::TypeMismatch {
            expected: "UInt8".into(),
            actual: "String".into(),
        };
        assert!(format!("{mismatch}").contains("expected"));

        let overflow = Error::Overflow("too big");
        assert!(format!("{overflow}").contains("too big"));

        let internal = Error::Internal("bug");
        assert!(format!("{internal}").contains("bug"));
    }
}
