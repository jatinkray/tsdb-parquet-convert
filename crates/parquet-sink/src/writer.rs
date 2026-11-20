//! Assembles rows from series data and writes one Parquet file.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::spec::{ColumnSource, SchemaSpec};
use crate::value::{ColumnData, SourceValue, ValueContext};
use crate::WriteError;

/// Compression choices accepted from the CLI.
pub const COMPRESSIONS: &[&str] = &["zstd", "snappy", "lz4", "gzip", "none"];

/// Writer settings shared by every file of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOptions {
    /// `zstd`, `snappy`, `lz4`, `gzip` or `none`.
    pub compression: String,
    /// `zstd` / `gzip` level; ignored by the other codecs.
    pub compression_level: Option<i32>,
    pub row_group_size: usize,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            compression: "zstd".to_string(),
            compression_level: None,
            row_group_size: 100_000,
        }
    }
}

impl WriteOptions {
    /// Rejects unknown codecs and levels instead of silently ignoring them.
    pub fn validate(&self) -> Result<(), WriteError> {
        let compression = compression(self)?;
        let level = match compression {
            Compression::ZSTD(level) => Some(level.compression_level()),
            Compression::GZIP(level) => Some(i32::try_from(level.compression_level()).unwrap_or(6)),
            _ => None,
        };
        if let Some(level) = level {
            if !(1..=22).contains(&level) {
                return Err(WriteError::Column {
                    column: "<options>".to_string(),
                    message: format!("compression level {level} is outside 1..=22"),
                });
            }
        }
        if self.row_group_size == 0 {
            return Err(WriteError::Column {
                column: "<options>".to_string(),
                message: "row-group-size must be greater than 0".to_string(),
            });
        }
        Ok(())
    }
}

fn compression(options: &WriteOptions) -> Result<Compression, WriteError> {
    let level = options.compression_level;
    Ok(match options.compression.as_str() {
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        "snappy" => Compression::SNAPPY,
        "lz4" => Compression::LZ4_RAW,
        "gzip" => match level {
            Some(level) => Compression::GZIP(parquet::basic::GzipLevel::try_new(
                u32::try_from(level).unwrap_or(6),
            )?),
            None => Compression::GZIP(Default::default()),
        },
        "zstd" => match level {
            Some(level) => Compression::ZSTD(parquet::basic::ZstdLevel::try_new(level)?),
            None => Compression::ZSTD(parquet::basic::ZstdLevel::default()),
        },
        other => {
            return Err(WriteError::Column {
                column: "<options>".to_string(),
                message: format!(
                    "unknown compression `{other}`; use {}",
                    COMPRESSIONS.join(", ")
                ),
            })
        }
    })
}

/// One series' samples, already aligned with the schema's timestamp unit input
/// (nanoseconds since the Unix epoch).
#[derive(Debug, Clone, Copy)]
pub struct SeriesInput<'a> {
    /// Source block ULID; feeds `source: block_ulid`.
    pub block_ulid: &'a str,
    /// Stable fingerprint of the label set; feeds `source: series_fingerprint`
    /// and identifies the series in error messages.
    pub series_fingerprint: u64,
    /// Ordinal of the series inside this output file.
    pub series_index: u64,
    /// `__name__`; feeds `source: metric_name`.
    pub metric_name: Option<&'a str>,
    /// Series labels, including `__name__`.
    pub labels: &'a BTreeMap<String, String>,
    /// `(timestamp_ns, value)` pairs.
    pub samples: &'a [(i64, f64)],
}

/// A file that was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileInfo {
    pub path: PathBuf,
    pub filename: String,
    pub rows: usize,
    pub bytes: u64,
}

/// Deterministic output name: re-running the same window overwrites the same
/// file instead of duplicating rows.
pub fn block_filename(start_ns: i64, end_ns: i64, part: u32) -> String {
    format!("data_{start_ns}_{end_ns}_{part:05}.parquet")
}

/// Buffers rows for one output Parquet file.
pub struct BlockWriter {
    spec: SchemaSpec,
    sources: Vec<ColumnSource>,
    columns: Vec<ColumnData>,
    schema: Arc<Schema>,
    options: WriteOptions,
    rows: usize,
}

impl BlockWriter {
    /// Builds a writer for one file.
    pub fn new(spec: SchemaSpec, options: WriteOptions) -> Result<Self, WriteError> {
        spec.validate()?;
        options.validate()?;
        let sources = spec
            .columns
            .iter()
            .map(|column| column.parsed_source())
            .collect::<Result<Vec<_>, _>>()?;
        let columns: Vec<ColumnData> = spec.columns.iter().map(ColumnData::new).collect();
        let schema = Arc::new(spec.arrow_schema());
        Ok(Self {
            spec,
            sources,
            columns,
            schema,
            options,
            rows: 0,
        })
    }

    /// Arrow schema that will be written.
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Rows buffered so far.
    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Appends every sample of one series.
    pub fn push_series(&mut self, input: SeriesInput<'_>) -> Result<usize, WriteError> {
        let Self {
            spec,
            sources,
            columns,
            ..
        } = self;
        let mut pushed = 0;
        for (timestamp_ns, value) in input.samples {
            for (index, source) in sources.iter().enumerate() {
                let column_spec = &spec.columns[index];
                let ctx = ValueContext {
                    column: &column_spec.name,
                    series: input.series_fingerprint,
                    nullable: column_spec.nullable,
                };
                let source_value = match source {
                    ColumnSource::Timestamp => SourceValue::Int(*timestamp_ns),
                    ColumnSource::MetricName => input
                        .metric_name
                        .map(SourceValue::Text)
                        .unwrap_or(SourceValue::Missing),
                    ColumnSource::Value => SourceValue::Float(*value),
                    ColumnSource::SeriesFingerprint => SourceValue::UInt(input.series_fingerprint),
                    ColumnSource::SeriesIndex => SourceValue::UInt(input.series_index),
                    ColumnSource::BlockUlid => SourceValue::Text(input.block_ulid),
                    ColumnSource::Label(key) => input
                        .labels
                        .get(key)
                        .map(|value| SourceValue::Text(value.as_str()))
                        .unwrap_or(SourceValue::Missing),
                    ColumnSource::Constant(text) => SourceValue::Text(text.as_str()),
                };
                columns[index].push(source_value, ctx)?;
            }
            pushed += 1;
        }
        self.rows += pushed;
        Ok(pushed)
    }

    /// Writes the buffered rows to `path` atomically (temp file, fsync, rename).
    pub fn write(self, path: &Path) -> Result<FileInfo, WriteError> {
        if self.rows == 0 {
            return Err(WriteError::Column {
                column: "<file>".to_string(),
                message: "refusing to write an empty Parquet file".to_string(),
            });
        }
        let filename = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "data.parquet".to_string());
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|source| WriteError::Io {
                    path: parent.display().to_string(),
                    source,
                })?;
            }
        }
        let arrays = self
            .columns
            .iter()
            .map(ColumnData::to_array)
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)?;
        let properties = self.writer_properties()?;
        let tmp = path.with_file_name(format!(".tmp_{filename}"));
        {
            let file = File::create(&tmp).map_err(|source| WriteError::Io {
                path: tmp.display().to_string(),
                source,
            })?;
            let mut writer =
                ArrowWriter::try_new(file, Arc::clone(&self.schema), Some(properties))?;
            writer.write(&batch)?;
            writer.close()?;
        }
        sync_file(&tmp)?;
        std::fs::rename(&tmp, path).map_err(|source| WriteError::Io {
            path: path.display().to_string(),
            source,
        })?;
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
        }
        let bytes = std::fs::metadata(path)
            .map_err(|source| WriteError::Io {
                path: path.display().to_string(),
                source,
            })?
            .len();
        Ok(FileInfo {
            path: path.to_path_buf(),
            filename,
            rows: self.rows,
            bytes,
        })
    }

    fn writer_properties(&self) -> Result<WriterProperties, WriteError> {
        let mut metadata = vec![
            KeyValue::new(
                "tsdb2parqtel.schema".to_string(),
                serde_json::to_string(&self.spec).map_err(|error| WriteError::Column {
                    column: "metadata".to_string(),
                    message: format!("serializing the schema: {error}"),
                })?,
            ),
            KeyValue::new(
                "tsdb2parqtel.writer".to_string(),
                format!("tsdb2parqtel {} parquet-sink", env!("CARGO_PKG_VERSION")),
            ),
        ];
        let schema_metadata: HashMap<String, String> = self.schema.metadata().clone().into();
        let mut schema_metadata: Vec<_> = schema_metadata.into_iter().collect();
        schema_metadata.sort();
        for (key, value) in schema_metadata {
            metadata.push(KeyValue::new(key, value));
        }
        Ok(WriterProperties::builder()
            .set_compression(compression(&self.options)?)
            .set_max_row_group_row_count(Some(self.options.row_group_size))
            .set_dictionary_enabled(true)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_key_value_metadata(Some(metadata))
            .set_created_by(format!(
                "tsdb2parqtel {} (schema-driven parquet-sink)",
                env!("CARGO_PKG_VERSION")
            ))
            .build())
    }
}

fn sync_file(path: &Path) -> Result<(), WriteError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| WriteError::Io {
            path: path.display().to_string(),
            source,
        })
}

fn sync_dir(path: &Path) -> Result<(), WriteError> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|source| WriteError::Io {
            path: path.display().to_string(),
            source,
        })
}
