//! Parqtel-compatible metric block schema and writer.
//!
//! The writer is a thin, pinned wrapper over `parqtel-core` at the audited
//! commit rather than a reimplementation. That keeps the Arrow schema, block
//! footer series table and Parquet writer properties in lock-step with the
//! server that will read the output.

#![deny(unsafe_code)]

pub use parqtel_core::models::labels::LabelSet;
pub use parqtel_core::models::metrics::{
    DataPoint, HistogramValue, Metric, MetricKind, MetricValue, SummaryValue,
};
pub use parqtel_core::models::storage::{BlockMetadata, SignalType};

pub mod writer;

pub use writer::{
    block_filename, write_metric_block, write_sidecar_fragment, BlockFile, BlockSpec, SeriesData,
    WriterConfig,
};
