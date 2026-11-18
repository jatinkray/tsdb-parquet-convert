//! One Prometheus/Thanos block: meta, index, chunks and tombstones.

use crate::chunks::{ChunkReader, FloatSample};
use crate::error::{Error, Result};
use crate::index::{Index, SeriesEntry};
use crate::meta::BlockMeta;
use crate::tombstones::{Interval, Tombstones};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct Block {
    pub dir: PathBuf,
    pub meta: BlockMeta,
    pub index: Index,
    pub tombstones: Tombstones,
    chunks: ChunkReader,
}

impl Block {
    pub fn open(dir: &Path) -> Result<Self> {
        let (meta, _) = BlockMeta::read(dir)?;
        let index_path = dir.join("index");
        let index = Index::open(&index_path)?;
        let tombstones = Tombstones::read(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            meta,
            index,
            tombstones,
            chunks: ChunkReader::new(dir),
        })
    }

    pub fn ulid(&self) -> &str {
        &self.meta.ulid
    }

    pub fn series(&self) -> &[SeriesEntry] {
        self.index.series()
    }

    pub fn read_series(
        &self,
        series: &SeriesEntry,
        min_time_ms: i64,
        max_time_ms: i64,
        stale: StaleMarkerPolicy,
    ) -> Result<Vec<FloatSample>> {
        let mut samples = Vec::new();
        for chunk in &series.chunks {
            if chunk.max_time_ms < min_time_ms || chunk.min_time_ms > max_time_ms {
                continue;
            }
            let (_, decoded) = self
                .chunks
                .read_chunk(chunk.reference)
                .map_err(|e| block_error(self, series, e))?;
            let chunk_samples = match decoded {
                crate::chunks::ChunkSamples::Floats(samples) => samples,
                crate::chunks::ChunkSamples::Histograms(histograms) => {
                    return Err(Error::Block {
                        ulid: self.ulid().to_string(),
                        message: format!(
                            "series {} carries {} native histogram sample(s); Parqtel has no schema-preserving native-histogram column (see docs/LIMITATIONS.md). Use --on-error=skip to omit it.",
                            series.id,
                            histograms.len()
                        ),
                    });
                }
            };
            samples.extend(chunk_samples.into_iter().filter(|sample| {
                sample.timestamp_ms >= min_time_ms
                    && sample.timestamp_ms <= max_time_ms
                    && !self.tombstones.is_deleted(series.id, sample.timestamp_ms)
                    && (stale == StaleMarkerPolicy::Keep || !sample.is_stale_marker())
            }));
        }
        samples.sort_by_key(|sample| sample.timestamp_ms);
        Ok(samples)
    }

    pub fn deleted_intervals(&self, series: &SeriesEntry) -> &[Interval] {
        self.tombstones.intervals_for(series.id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleMarkerPolicy {
    /// Prometheus uses a reserved NaN payload to mean "series disappeared".
    /// Parqtel has no stale-marker semantic, so dropping the marker is the
    /// least surprising representation of a gap.
    Drop,
    Keep,
}

impl StaleMarkerPolicy {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "drop" => Ok(Self::Drop),
            "keep" => Ok(Self::Keep),
            other => Err(Error::Matcher(format!(
                "stale-markers must be drop or keep, got {other:?}"
            ))),
        }
    }
}

fn block_error(block: &Block, series: &SeriesEntry, error: Error) -> Error {
    Error::Block {
        ulid: block.ulid().to_string(),
        message: format!(
            "series ref {} ({}): {error}",
            series.id,
            series.metric_name().unwrap_or("<no __name__>")
        ),
    }
}
