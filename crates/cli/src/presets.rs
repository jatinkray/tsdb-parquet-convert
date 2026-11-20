//! Built-in schemas, compiled into the binary so a first run needs no files.

use anyhow::{anyhow, Result};
use parquet_sink::SchemaSpec;

/// `(name, description, schema text)`, in listing order.
pub const PRESETS: &[(&str, &str, &str)] = &[
    (
        "lean",
        "timestamp, value and series fingerprint: the smallest export",
        include_str!("../presets/lean.yaml"),
    ),
    (
        "series",
        "adds the metric name to `lean`",
        include_str!("../presets/series.yaml"),
    ),
    (
        "otel",
        "adds nullable OTel resource columns (service, k8s) to `series`",
        include_str!("../presets/otel.yaml"),
    ),
];

/// Names accepted by `--preset`.
pub fn names() -> Vec<&'static str> {
    PRESETS.iter().map(|(name, _, _)| *name).collect()
}

/// Loads and validates a preset by name.
pub fn load(name: &str) -> Result<SchemaSpec> {
    let (_, _, text) = PRESETS
        .iter()
        .find(|(candidate, _, _)| *candidate == name)
        .ok_or_else(|| anyhow!("unknown preset `{name}`; available: {}", names().join(", ")))?;
    text.parse()
        .map_err(|error| anyhow!("preset `{name}`: {error}"))
}

/// Human-readable listing for `tsdb2parqtel presets`.
pub fn listing() -> String {
    let width = PRESETS
        .iter()
        .map(|(name, _, _)| name.len())
        .max()
        .unwrap_or(0);
    PRESETS
        .iter()
        .map(|(name, description, _)| format!("{name:<width$}  {description}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_preset_loads_and_validates() -> Result<()> {
        for name in names() {
            let spec = load(name)?;
            assert_eq!(spec.name.as_deref(), Some(&*format!("tsdb_{name}")));
            assert!(spec.arrow_schema().fields().len() >= 3);
        }
        Ok(())
    }

    #[test]
    fn unknown_preset_lists_the_known_ones() {
        let message = load("nope")
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(message.contains("unknown preset `nope`"), "{message}");
        assert!(message.contains("lean"), "{message}");
    }
}
