use std::path::PathBuf;

/// Errors produced while reading a Prometheus/Thanos block.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid TSDB data in {path} at offset {offset}: {message}")]
    Parse {
        path: PathBuf,
        offset: usize,
        message: String,
    },

    #[error("checksum mismatch in {path} {section} at offset {offset}")]
    Checksum {
        path: PathBuf,
        section: &'static str,
        offset: usize,
    },

    #[error("unsupported TSDB feature in {path}: {feature}")]
    Unsupported { path: PathBuf, feature: String },

    #[error("block {ulid}: {message}")]
    Block { ulid: String, message: String },

    #[error("source error: {0}")]
    Source(String),

    #[error("no Prometheus/Thanos blocks found under {0}")]
    EmptySource(PathBuf),

    #[error("series matcher is invalid: {0}")]
    Matcher(String),

    #[error("sample at {timestamp_ms} ms is outside Parqtel's representable range")]
    TimestampOutOfRange { timestamp_ms: i64 },
}

impl Error {
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    pub fn parse(path: impl Into<PathBuf>, offset: usize, message: impl Into<String>) -> Self {
        Self::Parse {
            path: path.into(),
            offset,
            message: message.into(),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
