//! Schema-driven Parquet output for TSDB series.
//!
//! The Parqtel block writer in `parqtel-sink` is tied to one fixed schema. This
//! crate is the general path: the caller supplies a [`SchemaSpec`] naming the
//! columns it wants, and each column declares where its value comes from
//! (`timestamp`, `value`, `metric_name`, `label:<key>`, `const:<value>`, ...).
//! Nothing here depends on Parqtel.
//!
//! ```no_run
//! use std::str::FromStr;
//!
//! use parquet_sink::{BlockWriter, SchemaSpec, SeriesInput, WriteOptions};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let spec = SchemaSpec::from_str(
//!     "columns:\n  - name: ts\n    type: timestamp_ms\n    source: timestamp\n  - name: job\n    type: utf8\n    source: label:job\n    nullable: true\n",
//! )?;
//! let mut writer = BlockWriter::new(spec, WriteOptions::default())?;
//! let labels = std::collections::BTreeMap::from([("job".to_string(), "api".to_string())]);
//! writer.push_series(SeriesInput {
//!     block_ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
//!     series_fingerprint: 7,
//!     series_index: 0,
//!     metric_name: Some("up"),
//!     labels: &labels,
//!     samples: &[(1_700_000_000_000_000_000, 1.0)],
//! })?;
//! # Ok(())
//! # }
//! ```

#![deny(unsafe_code)]

pub mod spec;
mod value;
pub mod writer;

pub use spec::{
    ColumnSource, ColumnSpec, ColumnType, SchemaError, SchemaSpec, CURRENT_VERSION,
    DEFAULT_DECIMAL_PRECISION,
};
pub use writer::{
    block_filename, writer_properties, BlockWriter, FileInfo, SeriesInput, WriteOptions,
};

/// Everything that can fail while assembling or writing an output file.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error(transparent)]
    Schema(#[from] SchemaError),
    #[error("writing {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("column `{column}`: {message}")]
    Column { column: String, message: String },
    #[error("column `{column}` of series {series}: {message}")]
    Row {
        column: String,
        series: u64,
        message: String,
    },
}
