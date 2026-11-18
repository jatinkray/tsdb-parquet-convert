//! Prometheus label → Parqtel row mapping.

use anyhow::{Context, Result};
use parqtel_sink::LabelSet;
use std::collections::BTreeMap;
use std::path::Path;
use tsdb_reader::Labels;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelMode {
    /// Keep every label in the row-level `labels` JSON. This preserves series
    /// identity exactly, including `job`, `namespace`, `pod`, `le`, etc.
    Preserve,
    /// Lift well-known labels into Parqtel correlation columns. This changes
    /// the returned label names (`job` becomes `service.name`) and is opt-in.
    Otel,
}

impl LabelMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "preserve" => Ok(Self::Preserve),
            "otel" => Ok(Self::Otel),
            other => anyhow::bail!("label-mode must be preserve or otel, got {other:?}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LabelMapper {
    mode: LabelMode,
    mapping: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappedLabels {
    pub labels: LabelSet,
    pub resource: LabelSet,
}

impl LabelMapper {
    pub fn new(mode: LabelMode, mapping_file: Option<&Path>) -> Result<Self> {
        let mut mapping = default_mapping();
        if let Some(path) = mapping_file {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading label mapping {}", path.display()))?;
            let parsed: BTreeMap<String, String> = if path
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml")
            {
                serde_yaml::from_str(&text)
                    .with_context(|| format!("parsing YAML label mapping {}", path.display()))?
            } else {
                serde_json::from_str(&text)
                    .with_context(|| format!("parsing JSON label mapping {}", path.display()))?
            };
            mapping = parsed;
        }
        Ok(Self { mode, mapping })
    }

    pub fn map(&self, input: &Labels) -> Result<MappedLabels> {
        let mut labels = input.clone();
        labels.remove("__name__");
        if self.mode == LabelMode::Preserve {
            return Ok(MappedLabels {
                labels: to_set(&labels)?,
                resource: LabelSet::default(),
            });
        }

        let mut resource = BTreeMap::new();
        for (source, target) in &self.mapping {
            let Some(value) = labels.remove(source) else {
                continue;
            };
            if target.starts_with("k8s.") || target == "service.name" || target == "service.version"
            {
                resource.insert(target.clone(), value);
            } else {
                labels.insert(target.clone(), value);
            }
        }
        Ok(MappedLabels {
            labels: to_set(&labels)?,
            resource: to_set(&resource)?,
        })
    }
}

fn to_set(labels: &Labels) -> Result<LabelSet> {
    LabelSet::try_from_iter(
        labels
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>(),
    )
    .map_err(|e| anyhow::anyhow!("building label set: {e}"))
}

fn default_mapping() -> BTreeMap<String, String> {
    [
        ("job", "service.name"),
        ("namespace", "k8s.namespace.name"),
        ("pod", "k8s.pod.name"),
        ("container", "k8s.container.name"),
        ("node", "k8s.node.name"),
    ]
    .into_iter()
    .map(|(source, target)| (source.to_string(), target.to_string()))
    .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn preserve_keeps_every_label_in_the_row() {
        let mapper = LabelMapper::new(LabelMode::Preserve, None).unwrap();
        let mapped = mapper
            .map(&labels(&[("__name__", "up"), ("job", "api"), ("le", "1")]))
            .unwrap();
        assert_eq!(mapped.labels.get("job"), Some("api"));
        assert_eq!(mapped.labels.get("le"), Some("1"));
        assert!(mapped.resource.is_empty());
    }

    #[test]
    fn otel_lifts_known_labels_to_resource() {
        let mapper = LabelMapper::new(LabelMode::Otel, None).unwrap();
        let mapped = mapper
            .map(&labels(&[
                ("__name__", "up"),
                ("job", "api"),
                ("pod", "p1"),
            ]))
            .unwrap();
        assert_eq!(mapped.labels.get("job"), None);
        assert_eq!(mapped.labels.get("pod"), None);
        assert_eq!(mapped.resource.get("service.name"), Some("api"));
        assert_eq!(mapped.resource.get("k8s.pod.name"), Some("p1"));
    }
}
