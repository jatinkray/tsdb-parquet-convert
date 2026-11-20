//! Local Prometheus/Thanos block sources.

use crate::block::{Block, StaleMarkerPolicy};
use crate::chunks::FloatSample;
use crate::error::{Error, Result};
use crate::index::Labels;
use crate::matcher::Selector;
use crate::meta::BlockMeta;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::PathBuf;
use walkdir::WalkDir;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownsamplePolicy {
    Skip,
    PreferRaw,
    Include,
    Only,
}

impl DownsamplePolicy {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "skip" => Ok(Self::Skip),
            "prefer-raw" => Ok(Self::PreferRaw),
            "include" => Ok(Self::Include),
            "only" => Ok(Self::Only),
            other => Err(Error::Matcher(format!(
                "downsampled must be skip, prefer-raw, include or only, got {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalLabels {
    Include,
    Ignore,
}

impl ExternalLabels {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "include" => Ok(Self::Include),
            "ignore" => Ok(Self::Ignore),
            other => Err(Error::Matcher(format!(
                "external-labels must be include or ignore, got {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnError {
    Skip,
    Fail,
}

impl OnError {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "skip" => Ok(Self::Skip),
            "fail" => Ok(Self::Fail),
            other => Err(Error::Matcher(format!(
                "on-error must be skip or fail, got {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceOptions {
    pub root: PathBuf,
    pub min_time_ms: Option<i64>,
    pub max_time_ms: Option<i64>,
    pub block_ulids: HashSet<String>,
    pub matcher: Option<Selector>,
    pub external_label_filter: Option<Selector>,
    pub external_labels: ExternalLabels,
    pub downsampled: DownsamplePolicy,
    pub stale_markers: StaleMarkerPolicy,
    pub dedup_replica_labels: Vec<String>,
    pub on_error: OnError,
}

impl SourceOptions {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            min_time_ms: None,
            max_time_ms: None,
            block_ulids: HashSet::new(),
            matcher: None,
            external_label_filter: None,
            external_labels: ExternalLabels::Include,
            downsampled: DownsamplePolicy::PreferRaw,
            stale_markers: StaleMarkerPolicy::Drop,
            dedup_replica_labels: Vec::new(),
            on_error: OnError::Fail,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub dir: PathBuf,
    pub meta: BlockMeta,
    pub series: usize,
    pub chunks: usize,
    pub samples: u64,
    pub resolution: i64,
    pub external_labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ScanStats {
    pub blocks: usize,
    pub series: usize,
    pub samples: u64,
    pub skipped_blocks: usize,
    pub skipped_series: usize,
    pub stale_dropped: u64,
    pub deleted_samples: u64,
    pub errors: Vec<ErrorRecord>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ErrorRecord {
    pub block_ulid: String,
    pub series_id: Option<u64>,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct SeriesRecord {
    pub block_ulid: String,
    pub series_id: u64,
    pub labels: Labels,
    pub samples: Vec<FloatSample>,
}

impl SeriesRecord {
    pub fn metric_name(&self) -> Option<&str> {
        self.labels.get("__name__").map(String::as_str)
    }
}

pub struct Source {
    options: SourceOptions,
    blocks: Vec<Block>,
    _staging: Option<tempfile::TempDir>,
}

impl Source {
    pub fn open(options: SourceOptions) -> Result<Self> {
        let root = options.root.to_string_lossy();
        if let Some((scheme, rest)) = root.split_once("://") {
            let (staged, temp) = stage_remote(scheme, rest, &options)?;
            let mut local = options;
            local.root = staged;
            let mut source = Self::open_local(local)?;
            source._staging = Some(temp);
            return Ok(source);
        }
        Self::open_local(options)
    }

    fn open_local(options: SourceOptions) -> Result<Self> {
        let mut dirs = Vec::new();
        if options.root.join("meta.json").is_file() {
            dirs.push(options.root.clone());
        } else {
            for entry in WalkDir::new(&options.root)
                .max_depth(4)
                .follow_links(false)
                .into_iter()
            {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) => {
                        if options.on_error == OnError::Fail {
                            return Err(Error::Source(format!(
                                "walking {}: {error}",
                                options.root.display()
                            )));
                        }
                        tracing::warn!(root = %options.root.display(), %error, "skipping unreadable directory");
                        continue;
                    }
                };
                if entry.file_type().is_file() && entry.file_name() == "meta.json" {
                    if let Some(parent) = entry.path().parent() {
                        dirs.push(parent.to_path_buf());
                    }
                }
            }
        }
        dirs.sort();
        dirs.dedup();

        let mut blocks = Vec::new();
        for dir in dirs {
            let meta = match BlockMeta::read(&dir) {
                Ok((meta, _)) => meta,
                Err(error) if options.on_error == OnError::Skip => {
                    tracing::warn!(block = %dir.display(), %error, "skipping block with unreadable meta.json");
                    continue;
                }
                Err(error) => return Err(error),
            };
            if !options.block_ulids.is_empty() && !options.block_ulids.contains(&meta.ulid) {
                continue;
            }
            if options
                .min_time_ms
                .is_some_and(|min| meta.max_time_ms <= min)
                || options
                    .max_time_ms
                    .is_some_and(|max| meta.min_time_ms >= max)
            {
                continue;
            }
            if !downsample_wanted(meta.resolution(), options.downsampled) {
                continue;
            }
            if let Some(filter) = &options.external_label_filter {
                if !filter.matches(meta.external_labels()) {
                    continue;
                }
            }
            match Block::open(&dir) {
                Ok(block) => blocks.push(block),
                Err(error) if options.on_error == OnError::Skip => {
                    tracing::warn!(block = %dir.display(), %error, "skipping unreadable block");
                }
                Err(error) => return Err(error),
            }
        }
        if blocks.is_empty() {
            return Err(Error::EmptySource(options.root));
        }
        if options.downsampled == DownsamplePolicy::PreferRaw {
            blocks = prefer_raw(blocks);
        }
        blocks.sort_by_key(|block| (block.meta.min_time_ms, block.meta.ulid.clone()));
        Ok(Self {
            options,
            blocks,
            _staging: None,
        })
    }

    pub fn block_infos(&self) -> Vec<BlockInfo> {
        self.blocks
            .iter()
            .map(|block| BlockInfo {
                dir: block.dir.clone(),
                meta: block.meta.clone(),
                series: block.series().len(),
                chunks: block
                    .series()
                    .iter()
                    .map(|series| series.chunks.len())
                    .sum(),
                samples: block.meta.stats.num_samples,
                resolution: block.meta.resolution(),
                external_labels: block.meta.external_labels().clone(),
            })
            .collect()
    }

    /// Label names present in the selected blocks, with the number of series
    /// that carry each one. Index-only: no chunk data is decoded. Thanos
    /// external labels are not included because they are added at scan time.
    pub fn label_names(&self) -> BTreeMap<String, u64> {
        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        for block in &self.blocks {
            for series in block.series() {
                for key in series.labels.keys() {
                    *counts.entry(key.clone()).or_insert(0) += 1;
                }
            }
        }
        counts
    }

    /// Keeps only the blocks named in `keep`. Incremental runs use this to
    /// drop blocks a previous run already wrote, so their chunks are never
    /// decoded again.
    pub fn retain_blocks(mut self, keep: &BTreeSet<String>) -> Self {
        self.blocks.retain(|block| keep.contains(block.ulid()));
        self
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Inclusive time bounds of the selected blocks, clipped by the source
    /// options. A block's `maxTime` is exclusive, so this returns `max - 1`.
    pub fn time_bounds_ms(&self) -> Option<(i64, i64)> {
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for block in &self.blocks {
            min = min.min(block.meta.min_time_ms);
            max = max.max(block.meta.max_time_ms.saturating_sub(1));
        }
        if min == i64::MAX {
            return None;
        }
        let min = self.options.min_time_ms.map_or(min, |bound| min.max(bound));
        let max = self
            .options
            .max_time_ms
            .map_or(max, |bound| max.min(bound.saturating_sub(1)));
        (min <= max).then_some((min, max))
    }

    pub fn scan<F>(&self, emit: F) -> Result<ScanStats>
    where
        F: FnMut(SeriesRecord) -> Result<()>,
    {
        self.scan_range(
            self.options.min_time_ms.unwrap_or(i64::MIN),
            self.options.max_time_ms.unwrap_or(i64::MAX),
            emit,
        )
    }

    /// Scans only samples in `[min_time_ms, max_time_ms)`. Converters call this
    /// once per output window so window state is bounded by one window rather
    /// than by the whole source.
    pub fn scan_range<F>(&self, min_time: i64, max_time: i64, mut emit: F) -> Result<ScanStats>
    where
        F: FnMut(SeriesRecord) -> Result<()>,
    {
        let mut stats = ScanStats {
            blocks: self.blocks.len(),
            ..ScanStats::default()
        };
        let mut buffered: Option<BTreeMap<Labels, Vec<SeriesRecord>>> = None;
        if !self.options.dedup_replica_labels.is_empty() {
            buffered = Some(BTreeMap::new());
        }

        for block in &self.blocks {
            for series in block.series() {
                let Some(metric_name) = series.metric_name() else {
                    stats.skipped_series += 1;
                    continue;
                };
                if metric_name.is_empty() {
                    stats.skipped_series += 1;
                    continue;
                }
                let mut labels = series.labels.clone();
                if self.options.external_labels == ExternalLabels::Include {
                    for (key, value) in block.meta.external_labels() {
                        labels.insert(key.clone(), value.clone());
                    }
                }
                if self
                    .options
                    .matcher
                    .as_ref()
                    .is_some_and(|matcher| !matcher.matches(&labels))
                {
                    stats.skipped_series += 1;
                    continue;
                }
                let samples =
                    match block.read_series(series, min_time, max_time, self.options.stale_markers)
                    {
                        Ok(samples) => samples,
                        Err(error) if self.options.on_error == OnError::Skip => {
                            stats.errors.push(ErrorRecord {
                                block_ulid: block.ulid().to_string(),
                                series_id: Some(series.id),
                                message: error.to_string(),
                            });
                            stats.skipped_series += 1;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                let before = samples.len();
                let samples: Vec<FloatSample> = samples
                    .into_iter()
                    .filter(|sample| {
                        sample.timestamp_ms >= min_time
                            && sample.timestamp_ms < max_time
                            && (self.options.stale_markers == StaleMarkerPolicy::Keep
                                || !sample.is_stale_marker())
                    })
                    .collect();
                stats.stale_dropped += (before - samples.len()) as u64;
                stats.deleted_samples += block.deleted_intervals(series).len() as u64;
                if samples.is_empty() {
                    stats.skipped_series += 1;
                    continue;
                }
                stats.series += 1;
                stats.samples += samples.len() as u64;
                let record = SeriesRecord {
                    block_ulid: block.ulid().to_string(),
                    series_id: series.id,
                    labels,
                    samples,
                };
                if let Some(buffer) = &mut buffered {
                    let key = dedup_key(&record.labels, &self.options.dedup_replica_labels);
                    buffer.entry(key).or_default().push(record);
                } else {
                    emit(record)?;
                }
            }
        }

        if let Some(buffer) = buffered {
            for (_, mut records) in buffer {
                let winner = |a: &SeriesRecord, b: &SeriesRecord| {
                    (a.block_ulid.clone(), a.series_id).cmp(&(b.block_ulid.clone(), b.series_id))
                };
                records.sort_by(winner);
                let source_block = records
                    .first()
                    .map(|record| record.block_ulid.clone())
                    .unwrap_or_default();
                let mut labels = records
                    .first()
                    .map(|record| record.labels.clone())
                    .unwrap_or_default();
                labels.retain(|key, _| {
                    !self
                        .options
                        .dedup_replica_labels
                        .iter()
                        .any(|replica| replica == key)
                });
                let mut by_time: BTreeMap<i64, (String, u64, f64)> = BTreeMap::new();
                for record in records {
                    for sample in record.samples {
                        by_time.entry(sample.timestamp_ms).or_insert((
                            record.block_ulid.clone(),
                            record.series_id,
                            sample.value,
                        ));
                    }
                }
                let samples = by_time
                    .into_iter()
                    .map(|(timestamp_ms, (_, _, value))| FloatSample {
                        timestamp_ms,
                        value,
                    })
                    .collect();
                emit(SeriesRecord {
                    block_ulid: source_block,
                    series_id: 0,
                    labels,
                    samples,
                })?;
            }
        }
        Ok(stats)
    }
}

fn stage_remote(
    scheme: &str,
    rest: &str,
    options: &SourceOptions,
) -> Result<(PathBuf, tempfile::TempDir)> {
    use futures::TryStreamExt;
    use object_store::{path::Path as ObjectPath, ObjectStore, ObjectStoreExt};
    use std::sync::Arc;

    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return Err(Error::Source(format!("{scheme} source has no bucket")));
    }
    let store: Arc<dyn ObjectStore> = match scheme {
        "s3" => Arc::new(
            object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|error| Error::Source(format!("opening S3 source: {error}")))?,
        ),
        "gs" => Arc::new(
            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|error| Error::Source(format!("opening GCS source: {error}")))?,
        ),
        "az" => Arc::new(
            object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_container_name(bucket)
                .build()
                .map_err(|error| Error::Source(format!("opening Azure source: {error}")))?,
        ),
        other => {
            return Err(Error::Source(format!(
                "unsupported object-store scheme {other:?}; use s3://, gs:// or az://"
            )))
        }
    };
    let temp = tempfile::tempdir().map_err(|error| Error::Source(error.to_string()))?;
    let root = temp.path().to_path_buf();
    let prefix_path = ObjectPath::from(prefix.trim_matches('/'));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Source(format!("building object-store runtime: {error}")))?;
    runtime.block_on(async {
        let objects: Vec<_> = store
            .list(Some(&prefix_path))
            .try_collect()
            .await
            .map_err(|error| Error::Source(format!("listing {scheme} source: {error}")))?;
        let keys: BTreeSet<String> = objects
            .iter()
            .map(|object| object.location.to_string())
            .collect();
        for object in &objects {
            let key = object.location.to_string();
            if !key.ends_with("/meta.json") && key != "meta.json" {
                continue;
            }
            let bytes = store
                .get(&object.location)
                .await
                .map_err(|error| Error::Source(format!("reading {key}: {error}")))?
                .bytes()
                .await
                .map_err(|error| Error::Source(format!("reading {key}: {error}")))?;
            let meta = match BlockMeta::parse(std::path::Path::new(&key), &bytes) {
                Ok(meta) => meta,
                Err(error) if options.on_error == OnError::Skip => {
                    tracing::warn!(%key, %error, "skipping remote block with unreadable meta.json");
                    continue;
                }
                Err(error) => return Err(error),
            };
            if !options.block_ulids.is_empty() && !options.block_ulids.contains(&meta.ulid) {
                continue;
            }
            if options
                .min_time_ms
                .is_some_and(|min| meta.max_time_ms <= min)
                || options
                    .max_time_ms
                    .is_some_and(|max| meta.min_time_ms >= max)
                || !downsample_wanted(meta.resolution(), options.downsampled)
            {
                continue;
            }
            let block_prefix = key.trim_end_matches("meta.json").trim_end_matches('/');
            let block_dir = root.join(&meta.ulid);
            std::fs::create_dir_all(block_dir.join("chunks"))
                .map_err(|error| Error::Source(error.to_string()))?;
            std::fs::write(block_dir.join("meta.json"), &bytes)
                .map_err(|error| Error::Source(error.to_string()))?;
            for (suffix, required) in [("index", true), ("tombstones", false)] {
                let object_key = format!("{block_prefix}/{suffix}");
                if keys.contains(&object_key) {
                    let data = store
                        .get(&ObjectPath::from(object_key.as_str()))
                        .await
                        .map_err(|error| Error::Source(format!("reading {object_key}: {error}")))?
                        .bytes()
                        .await
                        .map_err(|error| Error::Source(format!("reading {object_key}: {error}")))?;
                    std::fs::write(block_dir.join(suffix), data)
                        .map_err(|error| Error::Source(error.to_string()))?;
                } else if required {
                    return Err(Error::Source(format!(
                        "remote block {} has no index",
                        meta.ulid
                    )));
                }
            }
            let chunk_prefix = format!("{block_prefix}/chunks/");
            for chunk_key in keys.iter().filter(|key| key.starts_with(&chunk_prefix)) {
                let name = chunk_key.strip_prefix(&chunk_prefix).unwrap_or("segment");
                let data = store
                    .get(&ObjectPath::from(chunk_key.as_str()))
                    .await
                    .map_err(|error| Error::Source(format!("reading {chunk_key}: {error}")))?
                    .bytes()
                    .await
                    .map_err(|error| Error::Source(format!("reading {chunk_key}: {error}")))?;
                std::fs::write(block_dir.join("chunks").join(name), data)
                    .map_err(|error| Error::Source(error.to_string()))?;
            }
        }
        Ok(())
    })?;
    Ok((root, temp))
}

fn downsample_wanted(resolution: i64, policy: DownsamplePolicy) -> bool {
    match policy {
        DownsamplePolicy::Skip => resolution == 0,
        DownsamplePolicy::Only => resolution > 0,
        DownsamplePolicy::Include | DownsamplePolicy::PreferRaw => true,
    }
}

fn prefer_raw(blocks: Vec<Block>) -> Vec<Block> {
    let raw: Vec<(i64, i64)> = blocks
        .iter()
        .filter(|block| block.meta.resolution() == 0)
        .map(|block| (block.meta.min_time_ms, block.meta.max_time_ms))
        .collect();
    blocks
        .into_iter()
        .filter(|block| {
            if block.meta.resolution() == 0 {
                return true;
            }
            !raw.iter()
                .any(|(min, max)| *min <= block.meta.min_time_ms && *max >= block.meta.max_time_ms)
        })
        .collect()
}

fn dedup_key(labels: &Labels, replica_labels: &[String]) -> Labels {
    labels
        .iter()
        .filter(|(key, _)| !replica_labels.iter().any(|replica| replica == *key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Fills a `BTreeSet` of every label name encountered by a source.
pub fn label_names(records: &[SeriesRecord]) -> BTreeSet<String> {
    records
        .iter()
        .flat_map(|record| record.labels.keys().cloned())
        .collect()
}
