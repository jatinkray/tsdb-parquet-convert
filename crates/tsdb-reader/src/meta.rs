//! `meta.json` for Prometheus and Thanos blocks.

use crate::error::{Error, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct BlockMeta {
    #[serde(default = "default_version")]
    pub version: u8,
    pub ulid: String,
    #[serde(rename = "minTime")]
    pub min_time_ms: i64,
    #[serde(rename = "maxTime")]
    pub max_time_ms: i64,
    #[serde(default)]
    pub stats: BlockStats,
    #[serde(default)]
    pub compaction: Compaction,
    #[serde(default)]
    pub thanos: Option<ThanosMeta>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct BlockStats {
    #[serde(rename = "numSamples", default)]
    pub num_samples: u64,
    #[serde(rename = "numSeries", default)]
    pub num_series: u64,
    #[serde(rename = "numChunks", default)]
    pub num_chunks: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Compaction {
    #[serde(default)]
    pub level: u8,
    #[serde(default)]
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ThanosMeta {
    #[serde(default)]
    pub version: u8,
    #[serde(default)]
    pub seq: u64,
    #[serde(default)]
    pub source: String,
    #[serde(default, alias = "externalLabels", alias = "external_labels")]
    pub external_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub downsample: Option<DownsampleMeta>,
    #[serde(default)]
    pub files: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct DownsampleMeta {
    #[serde(default)]
    pub resolution: i64,
}

impl BlockMeta {
    pub fn read(block_dir: &Path) -> Result<(Self, PathBuf)> {
        let path = block_dir.join("meta.json");
        let bytes = std::fs::read(&path).map_err(|e| Error::io(&path, e))?;
        let meta: Self = serde_json::from_slice(&bytes)
            .map_err(|e| Error::parse(&path, 0, format!("invalid meta.json: {e}")))?;
        if meta.ulid.trim().is_empty() {
            return Err(Error::Block {
                ulid: meta.ulid,
                message: format!("{} has an empty ULID", path.display()),
            });
        }
        if meta.max_time_ms < meta.min_time_ms {
            return Err(Error::Block {
                ulid: meta.ulid,
                message: format!(
                    "maxTime {} is before minTime {}",
                    meta.max_time_ms, meta.min_time_ms
                ),
            });
        }
        Ok((meta, path))
    }

    pub fn resolution(&self) -> i64 {
        self.thanos
            .as_ref()
            .and_then(|t| t.downsample.as_ref())
            .map_or(0, |d| d.resolution)
    }

    pub fn is_downsampled(&self) -> bool {
        self.resolution() > 0
    }

    pub fn external_labels(&self) -> &BTreeMap<String, String> {
        static EMPTY: std::sync::OnceLock<BTreeMap<String, String>> = std::sync::OnceLock::new();
        self.thanos
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(BTreeMap::new), |t| &t.external_labels)
    }
}

fn default_version() -> u8 {
    1
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn parses_thanos_metadata_with_aliases() {
        let raw = br#"{
            "ulid":"01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "minTime":0,"maxTime":7200000,
            "thanos":{
                "source":"compactor",
                "external_labels":{"cluster":"prod"},
                "downsample":{"resolution":300000}
            }
        }"#;
        let meta: BlockMeta = serde_json::from_slice(raw).unwrap();
        assert_eq!(meta.resolution(), 300000);
        assert!(meta.is_downsampled());
        assert_eq!(meta.external_labels().get("cluster").unwrap(), "prod");
    }

    #[test]
    fn rejects_reversed_time_range() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("meta.json"),
            br#"{"ulid":"01ARZ3NDEKTSV4RRFFQ69G5FAV","minTime":2,"maxTime":1}"#,
        )
        .unwrap();
        assert!(BlockMeta::read(dir.path()).is_err());
    }
}
