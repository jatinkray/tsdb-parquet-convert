//! OpenTelemetry to Parqtel metric kind conversion for live clones.

use parqtel_sink::MetricKind;

/// Maps an OTel metric kind string (`gauge`, `sum`, `histogram`, `summary`)
/// onto the Parqtel on-disk [`MetricKind`].
pub fn parqtel_kind(otel_kind: &str) -> Option<MetricKind> {
    match otel_kind {
        "gauge" => Some(MetricKind::Gauge),
        "sum" => Some(MetricKind::Sum),
        "histogram" => Some(MetricKind::Histogram),
        "summary" => Some(MetricKind::Summary),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn maps_known_otel_kinds() {
        assert_eq!(parqtel_kind("gauge"), Some(MetricKind::Gauge));
        assert_eq!(parqtel_kind("sum"), Some(MetricKind::Sum));
        assert_eq!(parqtel_kind("histogram"), Some(MetricKind::Histogram));
        assert_eq!(parqtel_kind("summary"), Some(MetricKind::Summary));
    }

    #[test]
    fn unknown_kinds_map_to_none() {
        assert_eq!(parqtel_kind("exponential_histogram"), None);
        assert_eq!(parqtel_kind(""), None);
    }
}
