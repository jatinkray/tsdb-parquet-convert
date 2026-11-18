//! OTLP/HTTP JSON live clone.
//!
//! Parqtel accepts OTLP JSON at `/v1/metrics/json`. We deliberately send JSON
//! rather than protobuf here: the wire shape is inspectable, the body limit is
//! easy to enforce, and the target endpoint is the same ingest path the server
//! already documents. The sink keeps request bodies under `max_body_bytes`
//! (default 4 MiB), retries transient failures with jitter, and checkpoints the
//! last acknowledged timestamp per series.

#![deny(unsafe_code)]

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_MAX_BODY: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Gauge,
    Sum,
}

#[derive(Debug, Clone)]
pub struct OtlpSink {
    endpoint: String,
    client: reqwest::blocking::Client,
    max_body_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Checkpoint {
    path: PathBuf,
    last_ts_ns: BTreeMap<String, i64>,
}

impl Checkpoint {
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => {
                let mut checkpoint: Self = serde_json::from_slice(&bytes)
                    .with_context(|| format!("parsing checkpoint {}", path.display()))?;
                checkpoint.path = path.to_path_buf();
                Ok(checkpoint)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self {
                path: path.to_path_buf(),
                last_ts_ns: BTreeMap::new(),
            }),
            Err(error) => {
                Err(error).with_context(|| format!("reading checkpoint {}", path.display()))
            }
        }
    }

    pub fn save(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temp = self.path.with_extension("tmp");
        std::fs::write(&temp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(&temp, &self.path)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Option<i64> {
        self.last_ts_ns.get(key).copied()
    }

    fn set(&mut self, key: &str, timestamp_ns: i64) {
        self.last_ts_ns.insert(key.to_string(), timestamp_ns);
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CloneStats {
    pub requests: u64,
    pub series: u64,
    pub samples: u64,
    pub skipped_samples: u64,
    pub retries: u64,
}

impl OtlpSink {
    pub fn new(endpoint: &str) -> Result<Self> {
        let endpoint = normalize_endpoint(endpoint);
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .context("building OTLP HTTP client")?;
        Ok(Self {
            endpoint,
            client,
            max_body_bytes: DEFAULT_MAX_BODY,
        })
    }

    pub fn with_max_body_bytes(mut self, bytes: usize) -> Self {
        self.max_body_bytes = bytes.max(64 * 1024);
        self
    }

    pub fn send_series(
        &self,
        metric: &str,
        kind: MetricKind,
        labels: &BTreeMap<String, String>,
        resource: &BTreeMap<String, String>,
        samples: &[(i64, f64)],
        checkpoint: &mut Checkpoint,
    ) -> Result<CloneStats> {
        let key = format!(
            "{metric}{}",
            serde_json::to_string(labels).unwrap_or_default()
        );
        let mut stats = CloneStats::default();
        let mut start = 0usize;
        while start < samples.len() {
            let cutoff = checkpoint.get(&key);
            let mut batch = Vec::new();
            let mut index = start;
            while index < samples.len() {
                let (timestamp_ns, value) = samples[index];
                if cutoff.is_some_and(|cutoff| timestamp_ns <= cutoff) {
                    stats.skipped_samples += 1;
                    index += 1;
                    continue;
                }
                batch.push((timestamp_ns, value));
                index += 1;
                if batch.len() >= 10_000 {
                    break;
                }
            }
            if batch.is_empty() {
                start = index;
                continue;
            }
            let body = build_body(metric, kind, labels, resource, &batch)?;
            if body.len() > self.max_body_bytes && batch.len() > 1 {
                let keep = (batch.len() / 2).max(1);
                batch.truncate(keep);
                index = start + keep;
            }
            let body = build_body(metric, kind, labels, resource, &batch)?;
            if body.len() > self.max_body_bytes {
                anyhow::bail!(
                    "single OTLP data point is {} bytes, above the {} byte body limit",
                    body.len(),
                    self.max_body_bytes
                );
            }
            self.post_with_retry(&body, &mut stats)?;
            if let Some((timestamp_ns, _)) = batch.last() {
                checkpoint.set(&key, *timestamp_ns);
                checkpoint.save()?;
            }
            stats.samples += batch.len() as u64;
            stats.series = 1;
            start = index;
        }
        Ok(stats)
    }

    fn post_with_retry(&self, body: &[u8], stats: &mut CloneStats) -> Result<()> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            stats.requests += 1;
            let response = self
                .client
                .post(&self.endpoint)
                .header("Content-Type", "application/json")
                .body(body.to_vec())
                .send();
            match response {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => {
                    let status = response.status();
                    let retry_after = response
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok());
                    let text = response.text().unwrap_or_default();
                    if status.as_u16() == 413 {
                        anyhow::bail!("target rejected the body with 413: {text}");
                    }
                    if status.as_u16() != 429 && !status.is_server_error() {
                        anyhow::bail!("OTLP ingest failed with {status}: {text}");
                    }
                    if attempt >= 8 {
                        anyhow::bail!(
                            "OTLP ingest failed after {attempt} attempts: {status} {text}"
                        );
                    }
                    stats.retries += 1;
                    let backoff = retry_after.unwrap_or_else(|| {
                        let base = 250u64.saturating_mul(1 << attempt.min(6));
                        base.min(8_000)
                    });
                    std::thread::sleep(Duration::from_millis(backoff + jitter()));
                }
                Err(error) => {
                    if attempt >= 8 {
                        return Err(error).context("OTLP ingest request failed");
                    }
                    stats.retries += 1;
                    std::thread::sleep(Duration::from_millis(
                        250 * (1 << attempt.min(6)) + jitter(),
                    ));
                }
            }
        }
    }
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.ends_with("/v1/metrics/json") {
        endpoint.to_string()
    } else {
        format!("{}/v1/metrics/json", endpoint.trim_end_matches('/'))
    }
}

fn build_body(
    metric: &str,
    kind: MetricKind,
    labels: &BTreeMap<String, String>,
    resource: &BTreeMap<String, String>,
    samples: &[(i64, f64)],
) -> Result<Vec<u8>> {
    let data_points: Vec<_> = samples
        .iter()
        .map(|(timestamp_ns, value)| {
            json!({
                "timeUnixNano": timestamp_ns.to_string(),
                "asDouble": value,
                "attributes": attributes(labels),
            })
        })
        .collect();
    let metric_body = match kind {
        MetricKind::Gauge => json!({
            "name": metric,
            "gauge": { "dataPoints": data_points }
        }),
        MetricKind::Sum => json!({
            "name": metric,
            "sum": {
                "dataPoints": data_points,
                "isMonotonic": true,
                "aggregationTemporality": 2
            }
        }),
    };
    let body = json!({
        "resourceMetrics": [{
            "resource": { "attributes": attributes(resource) },
            "scopeMetrics": [{ "metrics": [metric_body] }]
        }]
    });
    serde_json::to_vec(&body).context("serialising OTLP request")
}

fn attributes(labels: &BTreeMap<String, String>) -> Vec<serde_json::Value> {
    labels
        .iter()
        .map(|(key, value)| json!({ "key": key, "value": { "stringValue": value } }))
        .collect()
}

fn jitter() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos()),
    );
    hasher.finish() % 251
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn normalizes_endpoint() {
        assert_eq!(
            normalize_endpoint("http://localhost:8080"),
            "http://localhost:8080/v1/metrics/json"
        );
        assert_eq!(
            normalize_endpoint("http://localhost:8080/v1/metrics/json"),
            "http://localhost:8080/v1/metrics/json"
        );
    }

    #[test]
    fn builds_otlp_gauge_and_sum() {
        let labels = BTreeMap::from([("job".to_string(), "api".to_string())]);
        let resource = BTreeMap::from([("service.name".to_string(), "api".to_string())]);
        let gauge = build_body("up", MetricKind::Gauge, &labels, &resource, &[(1, 1.0)]).unwrap();
        let sum = build_body(
            "requests_total",
            MetricKind::Sum,
            &labels,
            &resource,
            &[(1, 2.0)],
        )
        .unwrap();
        assert!(std::str::from_utf8(&gauge).unwrap().contains("\"gauge\""));
        assert!(std::str::from_utf8(&sum)
            .unwrap()
            .contains("\"isMonotonic\":true"));
    }

    #[test]
    fn checkpoint_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let mut checkpoint = Checkpoint::load(&path).unwrap();
        checkpoint.set("up{}", 10);
        checkpoint.save().unwrap();
        let loaded = Checkpoint::load(&path).unwrap();
        assert_eq!(loaded.get("up{}"), Some(10));
    }
}
