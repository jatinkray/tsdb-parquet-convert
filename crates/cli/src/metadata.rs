//! Metric kind inference and optional `/api/v1/metadata` overrides.

use anyhow::{Context, Result};
use parqtel_sink::MetricKind;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, Default)]
pub struct Metadata {
    overrides: BTreeMap<String, MetricKind>,
}

impl Metadata {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading metadata file {}", path.display()))?;
        let value: serde_json::Value = if path
            .extension()
            .is_some_and(|ext| ext == "yaml" || ext == "yml")
        {
            serde_yaml::from_str(&text)
                .with_context(|| format!("parsing YAML metadata {}", path.display()))?
        } else {
            serde_json::from_str(&text)
                .with_context(|| format!("parsing JSON metadata {}", path.display()))?
        };
        let mut out = Self::default();
        collect_metadata(&value, &mut out.overrides);
        Ok(out)
    }

    pub fn kind_for(&self, metric: &str) -> MetricKind {
        if let Some(kind) = self.overrides.get(metric) {
            return *kind;
        }
        infer_kind(metric)
    }
}

fn collect_metadata(value: &serde_json::Value, out: &mut BTreeMap<String, MetricKind>) {
    let data = value.get("data").unwrap_or(value);
    let Some(map) = data.as_object() else {
        return;
    };
    for (name, entry) in map {
        let kind = entry
            .as_array()
            .and_then(|values| values.first())
            .and_then(|first| first.get("type"))
            .or_else(|| entry.get("type"))
            .and_then(serde_json::Value::as_str);
        let Some(kind) = kind else {
            continue;
        };
        let kind = match kind.to_ascii_lowercase().as_str() {
            "counter" => MetricKind::Sum,
            "gauge" | "info" | "stateset" => MetricKind::Gauge,
            // Classic histograms and summaries are flat `_bucket/_sum/_count`
            // series in Prometheus. The individual series are counters; a
            // `_sum`/`_count` suffix rule already says Sum. For an override we
            // keep Sum as the closest truthful representation.
            "histogram" | "summary" => MetricKind::Sum,
            _ => continue,
        };
        out.insert(name.clone(), kind);
    }
}

pub fn infer_kind(metric: &str) -> MetricKind {
    if metric.ends_with("_total")
        || metric.ends_with("_count")
        || metric.ends_with("_sum")
        || metric.ends_with("_bucket")
        || metric.ends_with("_created")
    {
        MetricKind::Sum
    } else {
        MetricKind::Gauge
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn suffix_rules_match_prometheus_counter_families() {
        assert_eq!(infer_kind("http_requests_total"), MetricKind::Sum);
        assert_eq!(
            infer_kind("http_request_duration_seconds_bucket"),
            MetricKind::Sum
        );
        assert_eq!(
            infer_kind("http_request_duration_seconds_sum"),
            MetricKind::Sum
        );
        assert_eq!(
            infer_kind("http_request_duration_seconds_count"),
            MetricKind::Sum
        );
        assert_eq!(
            infer_kind("http_request_duration_seconds_created"),
            MetricKind::Sum
        );
        assert_eq!(infer_kind("up"), MetricKind::Gauge);
    }

    #[test]
    fn metadata_overrides_suffix_rules() {
        let value = serde_json::json!({
            "status":"success",
            "data":{"up":[{"type":"gauge","help":"","unit":""}],
                    "http_requests_total":[{"type":"gauge","help":"","unit":""}]}
        });
        let mut map = BTreeMap::new();
        collect_metadata(&value, &mut map);
        let metadata = Metadata { overrides: map };
        assert_eq!(metadata.kind_for("http_requests_total"), MetricKind::Gauge);
        assert_eq!(metadata.kind_for("other_total"), MetricKind::Sum);
    }
}
