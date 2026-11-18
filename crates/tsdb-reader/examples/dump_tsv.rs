//! Dump source samples as `labels_json<TAB>timestamp_ms<TAB>value_bits_hex`.
//!
//! This is a test/verification utility used to compare the native reader with
//! `promtool tsdb dump`; it is not part of the converter's public CLI.

use std::io::{BufWriter, Write};
use tsdb_reader::{Source, SourceOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: dump_tsv <block-root>")?;
    let mut options = SourceOptions::new(path);
    options.stale_markers = tsdb_reader::StaleMarkerPolicy::Keep;
    let source = Source::open(options)?;
    let stdout = std::io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    source.scan(|record| {
        let labels = serde_json::to_string(&record.labels)
            .map_err(|error| tsdb_reader::Error::Source(format!("serialising labels: {error}")))?;
        for sample in record.samples {
            writeln!(
                out,
                "{labels}\t{}\t{:016x}",
                sample.timestamp_ms,
                sample.value.to_bits()
            )
            .map_err(|error| tsdb_reader::Error::Source(error.to_string()))?;
        }
        Ok(())
    })?;
    Ok(())
}
