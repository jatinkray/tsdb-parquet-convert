//! Declarative output-schema specification.
//!
//! A schema file names the Parquet columns to emit and, for each column, which
//! part of the source series fills it. The format is deliberately explicit
//! rather than inferred: the same series must produce the same columns on every
//! run, and a missing column is a schema error rather than a silent guess.

use std::collections::BTreeSet;
use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Only schema layout this crate understands.
pub const CURRENT_VERSION: u32 = 1;

/// Default `precision` for `decimal128` columns.
pub const DEFAULT_DECIMAL_PRECISION: u8 = 38;

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("reading schema {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parsing schema {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("unsupported schema version {found}; this build reads version {CURRENT_VERSION}")]
    Version { found: u32 },
    #[error("schema defines no columns")]
    NoColumns,
    #[error("duplicate column name `{0}`")]
    DuplicateColumn(String),
    #[error("column `{column}`: {message}")]
    Column { column: String, message: String },
}

fn current_version() -> u32 {
    CURRENT_VERSION
}

/// One output column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSpec {
    /// Column name as written in the Parquet file.
    pub name: String,
    /// Arrow/Parquet type of the column.
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    /// Where the value comes from (`timestamp`, `label:job`, `const:eu`, ...).
    pub source: String,
    /// Whether a missing source value becomes a NULL instead of an error.
    #[serde(default)]
    pub nullable: bool,
    /// Decimal precision, required for `decimal128`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<u8>,
    /// Decimal scale, default `0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<i8>,
    /// Timezone for `timestamp_*` columns (e.g. `UTC`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// Free-form note, copied into the Arrow field metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl ColumnSpec {
    pub(crate) fn decimal_precision(&self) -> u8 {
        self.precision.unwrap_or(DEFAULT_DECIMAL_PRECISION)
    }

    pub(crate) fn decimal_scale(&self) -> i8 {
        self.scale.unwrap_or(0)
    }

    /// Parses the `source` string.
    pub fn parsed_source(&self) -> Result<ColumnSource, SchemaError> {
        ColumnSource::parse(&self.source).map_err(|message| SchemaError::Column {
            column: self.name.clone(),
            message,
        })
    }

    /// Arrow data type, after validation.
    fn data_type(&self) -> DataType {
        let timezone = self.timezone.clone().map(Arc::from);
        match self.column_type {
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::Int8 => DataType::Int8,
            ColumnType::Int16 => DataType::Int16,
            ColumnType::Int32 => DataType::Int32,
            ColumnType::Int64 => DataType::Int64,
            ColumnType::UInt8 => DataType::UInt8,
            ColumnType::UInt16 => DataType::UInt16,
            ColumnType::UInt32 => DataType::UInt32,
            ColumnType::UInt64 => DataType::UInt64,
            ColumnType::Float32 => DataType::Float32,
            ColumnType::Float64 => DataType::Float64,
            ColumnType::Utf8 => DataType::Utf8,
            ColumnType::Binary => DataType::Binary,
            ColumnType::Date32 => DataType::Date32,
            ColumnType::TimestampMs => DataType::Timestamp(TimeUnit::Millisecond, timezone),
            ColumnType::TimestampUs => DataType::Timestamp(TimeUnit::Microsecond, timezone),
            ColumnType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, timezone),
            ColumnType::Decimal128 => {
                DataType::Decimal128(self.decimal_precision(), self.decimal_scale())
            }
        }
    }
}

/// The complete output schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSpec {
    #[serde(default = "current_version")]
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub columns: Vec<ColumnSpec>,
}

impl SchemaSpec {
    /// Loads YAML (or JSON, which YAML accepts) and validates it.
    pub fn from_path(path: &Path) -> Result<Self, SchemaError> {
        let text = std::fs::read_to_string(path).map_err(|source| SchemaError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_str(&text).map_err(|error| match error {
            SchemaError::Parse { .. } => error,
            other => other,
        })
    }

    /// Parses YAML text and validates the result.
    fn from_text(text: &str) -> Result<Self, SchemaError> {
        let spec: Self = serde_yaml::from_str(text).map_err(|source| SchemaError::Parse {
            path: "<inline>".to_string(),
            source,
        })?;
        spec.validate()?;
        Ok(spec)
    }

    /// Checks every structural rule the writer relies on.
    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.version != CURRENT_VERSION {
            return Err(SchemaError::Version {
                found: self.version,
            });
        }
        if self.columns.is_empty() {
            return Err(SchemaError::NoColumns);
        }
        let mut seen = BTreeSet::new();
        let mut has_timestamp = false;
        for column in &self.columns {
            if column.name.trim().is_empty() {
                return Err(SchemaError::Column {
                    column: column.name.clone(),
                    message: "column name must not be empty".to_string(),
                });
            }
            if !seen.insert(column.name.clone()) {
                return Err(SchemaError::DuplicateColumn(column.name.clone()));
            }
            let source = column.parsed_source()?;
            if matches!(column.column_type, ColumnType::Decimal128) {
                if let Some(precision) = column.precision {
                    if precision == 0 || precision > 38 {
                        return Err(column_error(
                            column,
                            format!("decimal precision {precision} must be 1..=38"),
                        ));
                    }
                }
                let precision = i16::from(column.decimal_precision());
                let scale = i16::from(column.decimal_scale());
                if scale < 0 || scale > precision {
                    return Err(column_error(
                        column,
                        format!("decimal scale {scale} must be 0..={precision}"),
                    ));
                }
            } else if column.precision.is_some() || column.scale.is_some() {
                return Err(column_error(
                    column,
                    "precision and scale only apply to `decimal128`".to_string(),
                ));
            }
            if column.timezone.is_some() && !column.column_type.is_timestamp() {
                return Err(column_error(
                    column,
                    "timezone only applies to `timestamp_ms`, `timestamp_us` or `timestamp_ns`"
                        .to_string(),
                ));
            }
            if column.column_type.is_timestamp() {
                if !matches!(source, ColumnSource::Timestamp) {
                    return Err(column_error(
                        column,
                        format!(
                            "`{}` columns must use `source: timestamp`; `{}` would need a unit conversion the schema cannot express",
                            column.column_type, column.source
                        ),
                    ));
                }
                has_timestamp = true;
            } else if matches!(source, ColumnSource::Timestamp) {
                return Err(column_error(
                    column,
                    "`source: timestamp` requires a `timestamp_ms`, `timestamp_us` or `timestamp_ns` column"
                        .to_string(),
                ));
            }
        }
        if !has_timestamp {
            return Err(SchemaError::Column {
                column: "<schema>".to_string(),
                message: "at least one `timestamp_ms`, `timestamp_us` or `timestamp_ns` column with `source: timestamp` is required".to_string(),
            });
        }
        Ok(())
    }

    /// Arrow schema emitted into the Parquet footer.
    ///
    /// Each field carries `tsdb2parqtel.source` metadata so a reader can tell
    /// how the column was produced.
    pub fn arrow_schema(&self) -> Schema {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|column| {
                let mut metadata = std::collections::HashMap::new();
                metadata.insert("tsdb2parqtel.source".to_string(), column.source.clone());
                if let Some(description) = &column.description {
                    metadata.insert("description".to_string(), description.clone());
                }
                Field::new(column.name.clone(), column.data_type(), column.nullable)
                    .with_metadata(metadata)
            })
            .collect();
        let mut metadata = std::collections::HashMap::new();
        if let Some(name) = &self.name {
            metadata.insert("tsdb2parqtel.schema_name".to_string(), name.clone());
        }
        if let Some(description) = &self.description {
            metadata.insert(
                "tsdb2parqtel.schema_description".to_string(),
                description.clone(),
            );
        }
        metadata.insert(
            "tsdb2parqtel.schema_version".to_string(),
            self.version.to_string(),
        );
        Schema::new_with_metadata(fields, metadata)
    }
}

impl FromStr for SchemaSpec {
    type Err = SchemaError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::from_text(text)
    }
}

fn column_error(column: &ColumnSpec, message: String) -> SchemaError {
    SchemaError::Column {
        column: column.name.clone(),
        message,
    }
}

/// Where a column's value comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnSource {
    /// Sample timestamp (`timestamp_ms`/`us`/`ns` column).
    Timestamp,
    /// `__name__` of the series.
    MetricName,
    /// Sample value.
    Value,
    /// Stable xxHash3 fingerprint of the series label set.
    SeriesFingerprint,
    /// Series ordinal inside the output file.
    SeriesIndex,
    /// Source block ULID.
    BlockUlid,
    /// Value of one label.
    Label(String),
    /// Same literal for every row.
    Constant(String),
}

impl ColumnSource {
    /// Parses the compact source syntax used in schema files.
    pub fn parse(input: &str) -> Result<Self, String> {
        let trimmed = input.trim();
        let (kind, argument) = match trimmed.split_once(':') {
            Some((kind, argument)) => (kind.trim(), Some(argument)),
            None => (trimmed, None),
        };
        match (kind, argument) {
            ("timestamp", None) => Ok(Self::Timestamp),
            ("metric_name", None) => Ok(Self::MetricName),
            ("value", None) => Ok(Self::Value),
            ("series_fingerprint", None) => Ok(Self::SeriesFingerprint),
            ("series_index", None) => Ok(Self::SeriesIndex),
            ("block_ulid", None) => Ok(Self::BlockUlid),
            ("label", Some(key)) if !key.is_empty() => Ok(Self::Label(key.to_string())),
            ("const", Some(value)) => Ok(Self::Constant(value.to_string())),
            _ => Err(format!(
                "unknown source `{input}`; use timestamp, metric_name, value, series_fingerprint, series_index, block_ulid, label:<key> or const:<value>"
            )),
        }
    }
}

impl fmt::Display for ColumnSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timestamp => f.write_str("timestamp"),
            Self::MetricName => f.write_str("metric_name"),
            Self::Value => f.write_str("value"),
            Self::SeriesFingerprint => f.write_str("series_fingerprint"),
            Self::SeriesIndex => f.write_str("series_index"),
            Self::BlockUlid => f.write_str("block_ulid"),
            Self::Label(key) => write!(f, "label:{key}"),
            Self::Constant(value) => write!(f, "const:{value}"),
        }
    }
}

/// Types a schema file may name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Date32,
    TimestampMs,
    TimestampUs,
    TimestampNs,
    Decimal128,
}

impl ColumnType {
    /// Every accepted spelling, for error messages.
    pub const NAMES: &'static [&'static str] = &[
        "boolean",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float32",
        "float64",
        "utf8",
        "binary",
        "date32",
        "timestamp_ms",
        "timestamp_us",
        "timestamp_ns",
        "decimal128",
    ];

    pub fn is_timestamp(self) -> bool {
        matches!(
            self,
            Self::TimestampMs | Self::TimestampUs | Self::TimestampNs
        )
    }
}

impl FromStr for ColumnType {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(match input.trim() {
            "boolean" | "bool" => Self::Boolean,
            "int8" => Self::Int8,
            "int16" => Self::Int16,
            "int32" => Self::Int32,
            "int64" => Self::Int64,
            "uint8" => Self::UInt8,
            "uint16" => Self::UInt16,
            "uint32" => Self::UInt32,
            "uint64" => Self::UInt64,
            "float32" | "float" => Self::Float32,
            "float64" | "double" => Self::Float64,
            "utf8" | "string" => Self::Utf8,
            "binary" => Self::Binary,
            "date32" => Self::Date32,
            "timestamp_ms" => Self::TimestampMs,
            "timestamp_us" => Self::TimestampUs,
            "timestamp_ns" | "timestamp" => Self::TimestampNs,
            "decimal128" => Self::Decimal128,
            other => {
                return Err(format!(
                    "unknown type `{other}`; known types: {}",
                    Self::NAMES.join(", ")
                ))
            }
        })
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Boolean => "boolean",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::UInt8 => "uint8",
            Self::UInt16 => "uint16",
            Self::UInt32 => "uint32",
            Self::UInt64 => "uint64",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Utf8 => "utf8",
            Self::Binary => "binary",
            Self::Date32 => "date32",
            Self::TimestampMs => "timestamp_ms",
            Self::TimestampUs => "timestamp_us",
            Self::TimestampNs => "timestamp_ns",
            Self::Decimal128 => "decimal128",
        };
        f.write_str(name)
    }
}

impl Serialize for ColumnType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ColumnType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::from_str(&text).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(yaml: &str) -> Result<SchemaSpec, SchemaError> {
        SchemaSpec::from_str(yaml)
    }

    #[test]
    fn accepts_minimal_schema() -> Result<(), SchemaError> {
        let parsed = spec(
            "columns:\n  - name: ts\n    type: timestamp_ns\n    source: timestamp\n  - name: job\n    type: utf8\n    source: label:job\n    nullable: true\n",
        )?;
        assert_eq!(parsed.version, CURRENT_VERSION);
        assert_eq!(parsed.columns.len(), 2);
        let schema = parsed.arrow_schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert!(schema.field(1).is_nullable());
        assert_eq!(
            schema.field(1).metadata().get("tsdb2parqtel.source"),
            Some(&"label:job".to_string())
        );
        Ok(())
    }

    #[test]
    fn rejects_unknown_type_with_known_list() {
        let error = spec("columns:\n  - name: ts\n    type: datetime\n    source: timestamp\n");
        let message = error.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(message.contains("unknown type `datetime`"), "{message}");
        assert!(message.contains("timestamp_ns"), "{message}");
    }

    #[test]
    fn rejects_duplicate_column() {
        let error = spec(
            "columns:\n  - name: ts\n    type: timestamp_ns\n    source: timestamp\n  - name: ts\n    type: utf8\n    source: metric_name\n",
        );
        assert!(matches!(error, Err(SchemaError::DuplicateColumn(_))));
    }

    #[test]
    fn requires_a_timestamp_column() {
        let error = spec("columns:\n  - name: job\n    type: utf8\n    source: label:job\n");
        let message = error.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(message.contains("timestamp"), "{message}");
    }

    #[test]
    fn rejects_label_source_on_timestamp_column() {
        let error = spec("columns:\n  - name: ts\n    type: timestamp_ms\n    source: label:ts\n");
        let message = error.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(message.contains("source: timestamp"), "{message}");
    }

    #[test]
    fn rejects_version_mismatch() {
        let error = spec(
            "version: 2\ncolumns:\n  - name: ts\n    type: timestamp_ns\n    source: timestamp\n",
        );
        assert!(matches!(error, Err(SchemaError::Version { found: 2 })));
    }

    #[test]
    fn validates_decimal_precision_and_scale() -> Result<(), SchemaError> {
        let parsed = spec(
            "columns:\n  - name: ts\n    type: timestamp_ns\n    source: timestamp\n  - name: amount\n    type: decimal128\n    source: value\n    precision: 18\n    scale: 4\n",
        )?;
        assert_eq!(
            parsed.arrow_schema().field(1).data_type(),
            &DataType::Decimal128(18, 4)
        );
        let error = spec(
            "columns:\n  - name: ts\n    type: timestamp_ns\n    source: timestamp\n  - name: amount\n    type: decimal128\n    source: value\n    precision: 4\n    scale: 9\n",
        );
        assert!(error.is_err());
        Ok(())
    }

    #[test]
    fn parses_sources() {
        assert_eq!(
            ColumnSource::parse("label:k8s.pod.name"),
            Ok(ColumnSource::Label("k8s.pod.name".to_string()))
        );
        assert_eq!(
            ColumnSource::parse("const:eu-north-1"),
            Ok(ColumnSource::Constant("eu-north-1".to_string()))
        );
        assert!(ColumnSource::parse("label:").is_err());
        assert!(ColumnSource::parse("nope").is_err());
    }
}
