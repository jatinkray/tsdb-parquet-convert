# tsdb2parqtel

Convert persisted Prometheus / Thanos TSDB blocks into Parqtel-compatible
Parquet blocks or Parquet for a schema of your own, and replay the same series
into a running Parqtel over OTLP. This page is the overview; runbooks, the schema
reference, the design record, the loss list and the verification report are
linked from it and are not repeated here.

```bash
cargo build --release
./target/release/tsdb2parqtel inspect /var/lib/prometheus/blocks
./target/release/tsdb2parqtel convert /var/lib/prometheus/blocks \
  --out ./parqtel-blocks --sidecar
./target/release/tsdb2parqtel verify /var/lib/prometheus/blocks ./parqtel-blocks
```

## What it does

- Reads real Prometheus index v1/v2, chunk segments, XOR/XOR2/histogram chunk
  encodings and tombstones natively.
- Reads local Prometheus/Thanos blocks and staged `s3://`, `gs://`, `az://`
  sources through the standard provider credential chains.
- Filters by time, block ULID, PromQL-style label matcher and Thanos external
  labels; supports replica dedup and downsample selection.
- Writes blocks through the pinned `parqtel-core` writer, so the Arrow schema,
  footer series table and Parquet writer properties match the server exactly.
- Scans one output window at a time and flushes at
  `--max-rows-per-block`, so peak memory is independent of total source size.
- Uses deterministic UUIDv5 filenames, so a re-run overwrites the same block
  names instead of duplicating samples.
- Produces `index.fragment.json`, installs into a stopped data directory with
  a WAL guard, and replays over OTLP/HTTP with checkpoint/resume.
- Verifies per series: labels, sample count, first/last timestamp and an ordered
  xxhash of `(timestamp_ns, value_bits)`.
- Writes ordinary Parquet for a schema you supply (`convert --schema`), with one
  column per timestamp, metric name, sample value, label or constant; the
  format is documented in [docs/SCHEMA.md](docs/SCHEMA.md).

## Two output modes

Without `--schema`, output is Parqtel metric blocks: the pinned writer, the
footer series index and the sidecar fragment. With `--schema <file>`, output is
ordinary Parquet that follows that file, and every Parqtel-only flag is rejected
rather than ignored. `clone` and `verify` operate on Parqtel only.

## Commands

| Command | Purpose |
|---|---|
| `inspect <source>` | Blocks, ranges, series/chunk/sample counts, top label values |
| `plan <source>` | Windows, row estimates, mapping preview, retention conflict |
| `convert <source> --out <dir>` | Write Parquet blocks and optional sidecar fragment |
| `convert <source> --out <dir> --schema <file>` | Write Parquet for that schema |
| `schema <source> [--out <file>]` | Starter schema: one nullable column per label |
| `clone <source> --target <dir\|url>` | Install into a data dir or OTLP replay |
| `verify <source> <dir\|url>` | Per-series reconciliation; exit 4 on mismatch |

Global: `--threads`, `--memory-limit`, `--log-format text|json`, `--progress`,
`--dry-run`. See [docs/USAGE.md](docs/USAGE.md) for runbooks.

## Compatibility evidence

- `crates/tsdb-reader/tests/fixture_v1.rs` and `fixture_v2.rs` read real
  Prometheus/Thanos fixtures and compare counts/streams.
- `crates/tsdb-reader/tests/native_hist.rs` proves histogram chunk dispatch and
  the explicit unrepresentable-series error.
- `crates/parqtel-sink/tests/write_block.rs` asserts the Parquet schema, footer
  series index, row group and file naming.
- `verify` was run against a real generated block and against a live Parqtel
  instance (`source_series=1 output_series=1 source_samples=66 output_samples=66
  mismatched=0`).
- An OTLP clone into a fresh Parqtel was queried back through
  `/api/v1/query_range` with identical timestamps and values.
- `crates/parquet-sink/tests/roundtrip.rs` writes schema-driven files and reads
  them back: declared types, nullability, label values and cast errors.

The exact audited Parqtel source and the docs/code discrepancies are recorded in
[docs/DESIGN.md](docs/DESIGN.md). The schema-file format and conversion rules are
in [docs/SCHEMA.md](docs/SCHEMA.md). Known losses are in
[docs/LIMITATIONS.md](docs/LIMITATIONS.md); native histograms are decoded but
not converted because Parqtel has no schema-preserving native-histogram column.
Commands, results and the measured 5.55M-sample run are in
[docs/REPORT.md](docs/REPORT.md).

## Development

```bash
make fmt
make test
make lint
make docs-check
```

The workspace pins `parqtel-core` by git revision and Arrow/Parquet 60.1.0.
Changing either requires re-running the schema conformance test. MSRV is Rust
1.89 (the dependency set no longer builds on 1.88).

## License

MIT/Apache-2.0. The chunk decoder is used through the pinned MIT/Apache-2.0
`tsdb-reader` crate; the Parquet writer is used through `parqtel-core`
(Apache-2.0).
