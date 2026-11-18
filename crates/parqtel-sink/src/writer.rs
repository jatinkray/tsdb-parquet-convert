//! Writes one metric window as a Parqtel block.

use crate::{BlockMetadata, DataPoint, LabelSet, MetricKind, SignalType};
use parqtel_core::models::storage::MetricRun;
use parqtel_core::storage::FooterIndex;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const LABEL_VALUE_CAP: usize = 10_000;
const UUID_NAMESPACE: Uuid = Uuid::NAMESPACE_URL;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parqtel writer error: {0}")]
    Core(String),
    #[error("invalid writer configuration: {0}")]
    Config(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub compression: String,
    pub compression_level: Option<i32>,
    pub row_group_size: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            compression: "zstd".to_string(),
            compression_level: None,
            row_group_size: 100_000,
        }
    }
}

impl WriterConfig {
    pub fn validate(&self) -> Result<()> {
        if !matches!(
            self.compression.as_str(),
            "zstd" | "snappy" | "lz4" | "none"
        ) {
            return Err(Error::Config(format!(
                "compression must be zstd, snappy, lz4 or none, got {:?}",
                self.compression
            )));
        }
        if let Some(level) = self.compression_level {
            if !(1..=22).contains(&level) {
                return Err(Error::Config(format!(
                    "zstd compression level must be 1..=22, got {level}"
                )));
            }
        }
        if self.row_group_size == 0 {
            return Err(Error::Config(
                "row_group_size must be greater than 0".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SeriesData {
    pub metric_name: String,
    pub kind: MetricKind,
    pub resource: LabelSet,
    pub points: Vec<DataPoint>,
}

#[derive(Debug, Clone)]
pub struct BlockSpec {
    /// Stable source identity; used for deterministic output names.
    pub source_id: String,
    pub window_start_ns: i64,
    pub window_end_ns: i64,
    /// Part number within a window. Callers must keep this stable across runs.
    pub part: u32,
}

#[derive(Debug, Clone)]
pub struct BlockFile {
    pub metadata: BlockMetadata,
    pub filename: String,
}

/// Deterministic Parqtel block filename.
pub fn block_filename(spec: &BlockSpec, start_ns: i64, end_ns: i64) -> String {
    let name = format!(
        "tsdb2parqtel:{}:{}:{}:{}",
        spec.source_id, spec.window_start_ns, spec.window_end_ns, spec.part
    );
    let id = Uuid::new_v5(&UUID_NAMESPACE, name.as_bytes()).simple();
    format!("m_l0_{start_ns}_{end_ns}_{id}.parquet")
}

/// Writes one block. Series are grouped by metric identity and every group is
/// ordered by series fingerprint then timestamp, matching Parqtel's writer.
pub fn write_metric_block(
    out_dir: &Path,
    spec: &BlockSpec,
    mut series: Vec<SeriesData>,
    config: &WriterConfig,
) -> Result<BlockFile> {
    config.validate()?;
    if series.is_empty() {
        return Err(Error::Config("cannot write an empty block".into()));
    }
    std::fs::create_dir_all(out_dir).map_err(|e| Error::Io {
        path: out_dir.to_path_buf(),
        source: e,
    })?;

    series.retain_mut(|series| {
        series.points.retain(|point| point.timestamp_ns > 0);
        !series.points.is_empty()
    });
    if series.is_empty() {
        return Err(Error::Config(
            "all samples were outside the timestamp range".into(),
        ));
    }

    let mut groups: BTreeMap<(String, String, String), Vec<DataPoint>> = BTreeMap::new();
    for series in series {
        let key = (
            series.metric_name.clone(),
            metric_kind_name(series.kind).to_string(),
            series.resource.json_str().to_string(),
        );
        groups.entry(key).or_default().extend(series.points);
    }

    let mut owned: Vec<(String, MetricKind, LabelSet, Vec<DataPoint>)> =
        Vec::with_capacity(groups.len());
    for ((name, kind, resource), mut points) in groups {
        points.sort_by_key(|point| (point.labels.fingerprint(), point.timestamp_ns));
        let kind = parse_kind(&kind)?;
        let resource = LabelSet::from_json(&resource).map_err(|e| Error::Core(e.to_string()))?;
        owned.push((name, kind, resource, points));
    }
    owned.sort_by(|a, b| {
        (&a.0, metric_kind_name(a.1), a.2.json_str()).cmp(&(
            &b.0,
            metric_kind_name(b.1),
            b.2.json_str(),
        ))
    });

    let mut runs: Vec<MetricRun<'_>> = Vec::with_capacity(owned.len());
    for (name, kind, resource, points) in &owned {
        runs.push(MetricRun {
            name,
            kind: *kind,
            resource,
            points: points.iter().collect(),
        });
    }
    let (batch, table) = parqtel_core::models::storage::metric_runs_to_block(&runs)
        .map_err(|e| Error::Core(e.to_string()))?;
    let (start_ns, end_ns) = block_range(&owned)?;
    let filename = block_filename(spec, start_ns, end_ns);
    let final_path = out_dir.join(&filename);
    let temp_path = out_dir.join(format!(".tmp_{filename}"));

    parqtel_core::storage::write_block_file(
        &temp_path,
        batch,
        &config.compression,
        config.compression_level,
        config.row_group_size,
        None,
        Some(FooterIndex::Series(&table)),
    )
    .map_err(|e| Error::Core(e.to_string()))?;
    sync_file(&temp_path)?;
    std::fs::rename(&temp_path, &final_path).map_err(|e| Error::Io {
        path: final_path.clone(),
        source: e,
    })?;
    sync_parent(&final_path)?;

    let size_bytes = std::fs::metadata(&final_path)
        .map_err(|e| Error::Io {
            path: final_path.clone(),
            source: e,
        })?
        .len();
    let mut metadata = BlockMetadata {
        path: final_path,
        start_timestamp_ns: start_ns,
        end_timestamp_ns: end_ns,
        row_count: owned.iter().map(|group| group.3.len()).sum(),
        size_bytes,
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: BTreeMap::new(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    };
    for (name, _, resource, points) in &owned {
        metadata.metric_names.insert(name.clone());
        for (key, value) in resource.iter() {
            metadata.label_names.insert(key.to_string());
            cap_insert(&mut metadata.label_values, key, value);
        }
        for point in points {
            for (key, value) in point.labels.iter() {
                metadata.label_names.insert(key.to_string());
                cap_insert(&mut metadata.label_values, key, value);
            }
            if let Some(service) = resource.get("service.name") {
                metadata.label_names.insert("service.name".to_string());
                cap_insert(&mut metadata.label_values, "service.name", service);
            }
        }
    }
    Ok(BlockFile { metadata, filename })
}

fn block_range(owned: &[(String, MetricKind, LabelSet, Vec<DataPoint>)]) -> Result<(i64, i64)> {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for (_, _, _, points) in owned {
        for point in points {
            min = min.min(point.timestamp_ns);
            max = max.max(point.timestamp_ns);
        }
    }
    if min == i64::MAX {
        return Err(Error::Config("block has no points".into()));
    }
    Ok((min, max))
}

fn metric_kind_name(kind: MetricKind) -> &'static str {
    match kind {
        MetricKind::Gauge => "Gauge",
        MetricKind::Sum => "Sum",
        MetricKind::Histogram => "Histogram",
        MetricKind::Summary => "Summary",
    }
}

fn parse_kind(kind: &str) -> Result<MetricKind> {
    match kind {
        "Gauge" => Ok(MetricKind::Gauge),
        "Sum" => Ok(MetricKind::Sum),
        "Histogram" => Ok(MetricKind::Histogram),
        "Summary" => Ok(MetricKind::Summary),
        other => Err(Error::Core(format!("unknown metric kind {other}"))),
    }
}

fn cap_insert(values: &mut BTreeMap<String, BTreeSet<String>>, key: &str, value: &str) {
    let set = values.entry(key.to_string()).or_default();
    if set.len() < LABEL_VALUE_CAP {
        set.insert(value.to_string());
    }
}

fn sync_file(path: &Path) -> Result<()> {
    let file = std::fs::File::open(path).map_err(|e| Error::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    file.sync_all().map_err(|e| Error::Io {
        path: path.to_path_buf(),
        source: e,
    })
}

fn sync_parent(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let dir = std::fs::File::open(parent).map_err(|e| Error::Io {
            path: parent.to_path_buf(),
            source: e,
        })?;
        dir.sync_all().map_err(|e| Error::Io {
            path: parent.to_path_buf(),
            source: e,
        })?;
    }
    Ok(())
}

/// Writes a fragment that can be merged into a Parqtel `index.json`.
pub fn write_sidecar_fragment(out_dir: &Path, blocks: &[BlockMetadata]) -> Result<PathBuf> {
    let path = out_dir.join("index.fragment.json");
    let body = serde_json::json!({ "blocks": blocks });
    let temp = out_dir.join(".tmp_index.fragment.json");
    std::fs::write(
        &temp,
        serde_json::to_vec_pretty(&body).map_err(|e| Error::Core(e.to_string()))?,
    )
    .map_err(|e| Error::Io {
        path: temp.clone(),
        source: e,
    })?;
    std::fs::rename(&temp, &path).map_err(|e| Error::Io {
        path: path.clone(),
        source: e,
    })?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn deterministic_filename_is_32_hex_and_stable() {
        let spec = BlockSpec {
            source_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".into(),
            window_start_ns: 1,
            window_end_ns: 2,
            part: 0,
        };
        let a = block_filename(&spec, 10, 20);
        let b = block_filename(&spec, 10, 20);
        assert_eq!(a, b);
        let uuid = a
            .strip_prefix("m_l0_10_20_")
            .and_then(|name| name.strip_suffix(".parquet"))
            .unwrap();
        assert_eq!(uuid.len(), 32);
        assert!(uuid
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }
}
