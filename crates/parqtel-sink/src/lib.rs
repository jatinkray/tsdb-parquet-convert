//! Parqtel-compatible metric block schema types.
//!
//! Phase 1 of `tsdb2parqtel` vendors the subset of the Parqtel core model
//! that the sink needs (Phase 0 recon: `parqtel-oss` @ `63a9cf1c`). The types
//! are pinned copies: the JSON encodings below are asserted in tests so an
//! upstream drift fails loudly instead of silently producing unreadable blocks.

use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// FNV-1a-64 over length-prefixed sorted key/value pairs, then a splitmix64
/// finalizer. Pinned in Parqtel (`parqtel-core/src/models/labels.rs`).
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A resolved label set: sorted unique (key, value) pairs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct LabelSet {
    entries: BTreeMap<String, String>,
}

impl LabelSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_pairs<I>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        Self {
            entries: pairs.into_iter().collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries.get(key).map(String::as_str)
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.entries.keys()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.entries.iter()
    }

    /// Replaces (or inserts) a single key. Used to overlay the resource-derived
    /// `service.name` on the query-visible label set; the overlay wins.
    pub fn with(&self, key: &str, value: &str) -> Self {
        let mut entries = self.entries.clone();
        entries.insert(key.to_string(), value.to_string());
        Self { entries }
    }

    /// Merge: on equal keys the value from `other` takes precedence.
    pub fn merge(&self, other: &Self) -> Self {
        let mut entries = self.entries.clone();
        entries.append(&mut other.entries.clone());
        Self { entries }
    }

    /// Compact JSON map, e.g. `{"job":"api"}`.
    pub fn json(&self) -> String {
        serde_json::to_string(&self.entries).unwrap_or_else(|_| "{}".to_string())
    }

    /// Stable 64-bit series fingerprint (FNV-1a + splitmix64 finalizer).
    pub fn fingerprint(&self) -> u64 {
        let mut h: u64 = FNV_OFFSET;
        let mix = |hash: &mut u64, bytes: &[u8]| {
            let len = bytes.len() as u64;
            for byte in len.to_le_bytes().iter().chain(bytes.iter()) {
                *hash = (*hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME);
            }
        };
        for (key, value) in &self.entries {
            mix(&mut h, key.as_bytes());
            mix(&mut h, value.as_bytes());
        }
        let mut z = h;
        z ^= z >> 30;
        z = z.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z ^= z >> 27;
        z = z.wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

impl Serialize for LabelSet {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.entries.len()))?;
        for (key, value) in &self.entries {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for LabelSet {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LabelVisitor;

        impl<'de> serde::de::Visitor<'de> for LabelVisitor {
            type Value = LabelSet;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map of label key to value")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut entries = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, String>()? {
                    entries.insert(key, value);
                }
                Ok(LabelSet { entries })
            }
        }

        deserializer.deserialize_map(LabelVisitor)
    }
}

/// The metric kind as Parqtel stores it on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum MetricKind {
    /// A gauge represents a single numerical value that can arbitrarily go up
    /// and down.
    #[default]
    Gauge,
    /// A sum represents the total of a property over time.
    Sum,
    /// A histogram represents a distribution of values.
    Histogram,
    /// A summary represents a distribution of values with quantiles.
    Summary,
}

impl fmt::Display for MetricKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Gauge => "Gauge",
            Self::Sum => "Sum",
            Self::Histogram => "Histogram",
            Self::Summary => "Summary",
        })
    }
}

impl std::str::FromStr for MetricKind {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "Gauge" => Ok(Self::Gauge),
            "Sum" => Ok(Self::Sum),
            "Histogram" => Ok(Self::Histogram),
            "Summary" => Ok(Self::Summary),
            other => Err(format!("unknown metric kind: {other}")),
        }
    }
}

/// Histogram bucket data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistogramValue {
    /// Number of samples.
    pub count: u64,
    /// Sum of all samples.
    pub sum: f64,
    /// Optional minimum value in the distribution.
    pub min: Option<f64>,
    /// Optional maximum value in the distribution.
    pub max: Option<f64>,
    /// Explicit bucket boundaries.
    pub boundaries: Vec<f64>,
    /// Counts for each bucket defined by boundaries.
    pub counts: Vec<u64>,
}

impl HistogramValue {
    pub fn new(count: u64, sum: f64) -> Self {
        Self {
            count,
            sum,
            min: None,
            max: None,
            boundaries: Vec::new(),
            counts: Vec::new(),
        }
    }
}

/// Summary quantile data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SummaryValue {
    /// Number of samples.
    pub count: u64,
    /// Sum of all samples.
    pub sum: f64,
    /// Quantile values.
    pub quantiles: Vec<(f64, f64)>,
}

impl SummaryValue {
    pub fn new(count: u64, sum: f64) -> Self {
        Self {
            count,
            sum,
            quantiles: Vec::new(),
        }
    }
}

/// The value of a single data point, varying by [`MetricKind`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetricValue {
    /// A single floating point value (Gauge or Sum).
    Double(f64),
    /// A single integer value (Sum).
    Int(i64),
    /// Histogram bucket data.
    Histogram(Box<HistogramValue>),
    /// Summary quantile data.
    Summary(Box<SummaryValue>),
}

impl MetricValue {
    pub fn histogram(count: u64, sum: f64, boundaries: Vec<f64>, counts: Vec<u64>) -> Self {
        Self::Histogram(Box::new(HistogramValue {
            count,
            sum,
            min: None,
            max: None,
            boundaries,
            counts,
        }))
    }

    pub fn summary(count: u64, sum: f64, quantiles: Vec<(f64, f64)>) -> Self {
        Self::Summary(Box::new(SummaryValue {
            count,
            sum,
            quantiles,
        }))
    }
}

/// A single data point for a metric.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataPoint {
    /// Nanosecond timestamp of the data point.
    pub timestamp_ns: i64,
    /// Value of the data point.
    pub value: MetricValue,
    /// Labels associated with this specific data point.
    pub labels: LabelSet,
}

/// A metric and its data points, grouped by resource.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub description: String,
    pub unit: String,
    pub kind: MetricKind,
    pub resource_attributes: LabelSet,
    pub data_points: Vec<DataPoint>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn metric_kind_serde_uses_parqtel_pascal_case_strings() {
        let kind = serde_json::to_string(&MetricKind::Gauge).unwrap();
        assert_eq!(kind, "\"Gauge\"");
        let round: MetricKind = serde_json::from_str("\"Summary\"").unwrap();
        assert_eq!(round, MetricKind::Summary);
    }

    #[test]
    fn double_value_serializes_with_the_variant_key() {
        assert_eq!(
            serde_json::to_string(&MetricValue::Double(3.15)).unwrap(),
            r#"{"Double":3.15}"#
        );
    }

    #[test]
    fn int_value_serializes_with_the_variant_key() {
        assert_eq!(
            serde_json::to_string(&MetricValue::Int(42)).unwrap(),
            r#"{"Int":42}"#
        );
    }

    #[test]
    fn histogram_value_json_shape_is_pinned() {
        let value = MetricValue::histogram(3, 1.5, vec![1.0], vec![1, 2]);
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            r#"{"Histogram":{"count":3,"sum":1.5,"min":null,"max":null,"boundaries":[1.0],"counts":[1,2]}}"#
        );
    }

    #[test]
    fn summary_value_json_shape_is_pinned() {
        let value = MetricValue::summary(2, 3.0, vec![(0.5, 10.0), (0.99, 20.0)]);
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            r#"{"Summary":{"count":2,"sum":3.0,"quantiles":[[0.5,10.0],[0.99,20.0]]}}"#
        );
    }

    #[test]
    fn label_set_json_is_compact_sorted_map() {
        let labels = LabelSet::from_pairs([
            ("job".to_string(), "api".to_string()),
            ("instance".to_string(), "x:1".to_string()),
        ]);
        assert_eq!(labels.json(), r#"{"instance":"x:1","job":"api"}"#);
    }

    #[test]
    fn label_set_with_overlays_service_name() {
        let labels = LabelSet::from_pairs([("job".to_string(), "api".to_string())]);
        let merged = labels.with("service.name", "checkout");
        assert_eq!(merged.get("service.name"), Some("checkout"));
        assert_eq!(merged.get("job"), Some("api"));
    }

    #[test]
    fn label_set_merge_lets_other_win() {
        let base = LabelSet::from_pairs([("job".to_string(), "api".to_string())]);
        let overlay = LabelSet::from_pairs([("job".to_string(), "web".to_string())]);
        assert_eq!(base.merge(&overlay).get("job"), Some("web"));
    }
}
