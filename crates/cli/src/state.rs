//! Incremental-run bookkeeping.
//!
//! A cron job should convert each source block once. The state file records
//! which block ULIDs are already in the destination, together with the schema
//! hash that produced them, so a changed schema cannot silently reuse a run
//! that wrote a different layout.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub version: u32,
    /// Hash of the schema that produced the recorded blocks.
    pub schema_hash: String,
    pub blocks: BTreeMap<String, BlockRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockRecord {
    pub max_time_ms: i64,
    pub updated_at: String,
}

impl State {
    /// Loads a state file, or starts an empty one when it does not exist.
    pub fn load(path: &Path, schema_hash: &str) -> Result<Self> {
        if !path.exists() {
            return Ok(Self {
                version: STATE_VERSION,
                schema_hash: schema_hash.to_string(),
                blocks: BTreeMap::new(),
            });
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading state {}", path.display()))?;
        let state: Self = serde_json::from_str(&text)
            .with_context(|| format!("parsing state {}", path.display()))?;
        if state.version != STATE_VERSION {
            anyhow::bail!(
                "state {} has version {}; this build writes version {STATE_VERSION}",
                path.display(),
                state.version
            );
        }
        if state.schema_hash != schema_hash {
            anyhow::bail!(
                "state {} was written with schema hash {} but this run uses {}; point --state at a new file or keep the schema stable",
                path.display(),
                state.schema_hash,
                schema_hash
            );
        }
        Ok(state)
    }

    /// A block counts as converted only when it was fully covered.
    pub fn covers(&self, ulid: &str, max_time_ms: i64) -> bool {
        self.blocks
            .get(ulid)
            .is_some_and(|record| record.max_time_ms >= max_time_ms)
    }

    pub fn record(&mut self, ulid: &str, max_time_ms: i64) {
        self.blocks.insert(
            ulid.to_string(),
            BlockRecord {
                max_time_ms,
                updated_at: chrono::Utc::now().to_rfc3339(),
            },
        );
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
        }
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)
            .with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("installing state {}", path.display()))?;
        Ok(())
    }
}

/// Stable hash of a schema, shared by the state file and the manifest.
pub fn schema_hash(spec: &parquet_sink::SchemaSpec) -> Result<String> {
    let canonical = serde_json::to_string(spec)?;
    Ok(format!(
        "{:016x}",
        xxhash_rust::xxh3::xxh3_64(canonical.as_bytes())
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(schema_hash: &str) -> State {
        State {
            version: STATE_VERSION,
            schema_hash: schema_hash.to_string(),
            blocks: BTreeMap::new(),
        }
    }

    #[test]
    fn round_trips_through_disk() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("state.json");
        let mut state = state("abc");
        state.record("01ARZ3NDEKTSV4RRFFQ69G5FAV", 1_650_009_599_999);
        state.save(&path)?;
        let loaded = State::load(&path, "abc")?;
        assert!(loaded.covers("01ARZ3NDEKTSV4RRFFQ69G5FAV", 1_650_009_599_999));
        assert!(!loaded.covers("01ARZ3NDEKTSV4RRFFQ69G5FAV", 1_650_009_600_000));
        Ok(())
    }

    #[test]
    fn missing_file_starts_empty() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let state = State::load(&dir.path().join("absent.json"), "abc")?;
        assert!(state.blocks.is_empty());
        Ok(())
    }

    #[test]
    fn schema_change_is_refused() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("state.json");
        state("abc").save(&path)?;
        let error = State::load(&path, "def")
            .err()
            .map(|error| error.to_string());
        assert!(
            error
                .unwrap_or_default()
                .contains("was written with schema hash abc"),
            "unexpected error"
        );
        Ok(())
    }
}
