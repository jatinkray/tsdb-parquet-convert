//! Pure library for reading Prometheus / Thanos TSDB block sources.
//!
//! Phase 1 of `tsdb2parqtel` lands the source-side seams here: the error type,
//! the result alias and the [`BlockSource`] trait that concrete readers
//! (local Prometheus blocks, Thanos buckets) implement.

use async_trait::async_trait;

/// Errors produced while reading a source.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The source could not be opened or read.
    #[error("source error: {0}")]
    Source(String),
}

/// Result alias for source readers.
pub type Result<T> = std::result::Result<T, Error>;

/// Identity of a source as seen by a reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceInfo {
    /// Root path or object-store prefix of the source.
    pub root: String,
    /// Signal carried by the blocks under this source.
    pub signal: String,
}

/// Read-only access to a Prometheus / Thanos block source.
#[async_trait]
pub trait BlockSource {
    /// Opens the source and reports what is inside it.
    async fn open(&self) -> Result<SourceInfo>;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    struct StaticSource;

    #[async_trait]
    impl BlockSource for StaticSource {
        async fn open(&self) -> Result<SourceInfo> {
            Ok(SourceInfo {
                root: "data".into(),
                signal: "metrics".into(),
            })
        }
    }

    #[tokio::test]
    async fn open_reports_source_identity() {
        let info = StaticSource.open().await.unwrap();
        assert_eq!(info.root, "data");
        assert_eq!(info.signal, "metrics");
    }
}
