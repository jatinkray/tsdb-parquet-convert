# tsdb2parqtel

Own your metric history. `tsdb2parqtel` turns Prometheus/Thanos TSDB blocks into
Parquet you keep, on storage that costs cents per gigabyte-month, and can still
convert to Parqtel blocks or replay into a running Parqtel over OTLP. This page
is the overview; the runbooks, schema reference, design record, loss list and
verification report are linked from it and are not repeated here.

```bash
# One command: export a bucket to a data lake with a cost estimate and a
# per-series verification gate.
tsdb2parqtel migrate s3://thanos/prod --out s3://lake/metrics \
  --preset series --rollup 5m --state /var/lib/migrate/prod.json --verify

# Or stay explicit.
tsdb2parqtel inspect /var/lib/prometheus/blocks
tsdb2parqtel convert /var/lib/prometheus/blocks --out ./lake --preset series
tsdb2parqtel verify /var/lib/prometheus/blocks ./lake --preset series
./target/release/tsdb2parqtel --help
```

## What it does

- Reads real Prometheus index v1/v2, chunk segments, XOR/XOR2/histogram chunk
  encodings and tombstones natively, plus local Prometheus/Thanos blocks and
  staged `s3://`, `gs://`, `az://` sources through the standard provider
  credential chains.
- Filters by time, block ULID, PromQL-style label matcher and Thanos external
  labels; supports replica dedup and downsample selection.
- Writes **ordinary Parquet for a schema you supply** (`--schema` or a built-in
  preset): one column per timestamp, metric name, sample value, label or
  constant, with dictionary encoding and page statistics.
- Writes straight to an object store (`--out s3://…`), leaving a
  `manifest.json` per run with the schema hash, file list, rows and bytes.
- Rolls samples up (`--rollup 5m --rollup-func avg`) and merges small files
  (`compact`), which is where a lakehouse query cost is actually won.
- Keeps incremental runs cheap: `--state` records which blocks are already in
  the destination and skips them before any chunk is decoded.
- Emits the query-engine DDL for what it wrote (`emit-ddl --target
  duckdb|athena|bigquery`).
- Verifies per series — labels, sample count, first/last timestamp and an
  ordered xxhash of `(timestamp_ns, value_bits)` — for Parqtel blocks and, with
  `--schema`/`--preset`, for schema-mode Parquet.
- Still writes Parqtel blocks and replays over OTLP/HTTP with checkpoint/resume
  when Parqtel is the target.

## Two output modes

Without `--schema`/`--preset`, output is Parqtel metric blocks: the pinned
writer, the footer series index and the sidecar fragment. With a schema, output
is ordinary Parquet that follows that file, and every Parqtel-only flag is
rejected rather than ignored. `clone` operates on Parqtel only; `verify` covers
both.

## Commands

| Command | Purpose |
|---|---|
| `inspect <source>` | Blocks, ranges, series/chunk/sample counts, top label values |
| `plan <source>` | Windows, row estimates, mapping preview, retention conflict |
| `convert <source> --out <dir\|url>` | Parquet for `--schema`/`--preset`, or Parqtel blocks |
| `migrate <source> --out <dir\|url> --preset …` | Convert + rollup/compaction + manifest + cost estimate + optional `--verify` |
| `compact <dir> --out <dir> --preset …` | Merge small files, proving rows are unchanged |
| `schema <source>` | Starter schema: one nullable column per label |
| `presets` | List the built-in schemas |
| `emit-ddl --preset … --target duckdb\|athena\|bigquery` | View or external table for what you wrote |
| `clone <source> --target <dir\|url>` | Install into a Parqtel data dir or OTLP replay |
| `verify <source> <dir\|url> [--schema\|--preset]` | Per-series reconciliation; exit 4 on mismatch |

Global: `--threads`, `--memory-limit`, `--log-format text|json`, `--progress`,
`--dry-run`. See [docs/USAGE.md](docs/USAGE.md) for runbooks and
[docs/SCHEMA.md](docs/SCHEMA.md) for the schema file.

## Install

```bash
# Prebuilt static binaries (Linux musl x86_64/aarch64, macOS, Windows)
gh release download --pattern '*x86_64-unknown-linux-musl.tar.gz'
tar -xzf tsdb2parqtel-*-x86_64-unknown-linux-musl.tar.gz && ./tsdb2parqtel --version

# Container
docker run --rm ghcr.io/jatinkray/tsdb-parquet-convert:latest --version

# From source
cargo install --git https://github.com/jatinkray/tsdb-parquet-convert tsdb2parqtel
```

Every release carries `SHA256SUMS`, an SPDX SBOM per archive, cosign keyless
signatures and a build-provenance attestation.

## Compatibility evidence

- `crates/tsdb-reader/tests/fixture_v1.rs` and `fixture_v2.rs` read real
  Prometheus/Thanos fixtures and compare counts/streams.
- `crates/tsdb-reader/tests/native_hist.rs` proves histogram chunk dispatch and
  the explicit unrepresentable-series error.
- `crates/parqtel-sink/tests/write_block.rs` asserts the Parquet schema, footer
  series index, row group and file naming.
- `crates/parquet-sink/tests/roundtrip.rs` writes schema-driven files and reads
  them back: declared types, nullability, label values and cast errors.
- `verify --preset lean` on the full fixture reconciles 35,354 series and
  5,550,882 samples with 0 missing, 0 extra and 0 mismatched.
- `bench/baseline.json` pins 1.62M samples/s and 0.789 B/sample; the `Bench`
  workflow fails on a throughput or size regression beyond its thresholds.

The exact audited Parqtel source and the docs/code discrepancies are recorded in
[docs/DESIGN.md](docs/DESIGN.md). The schema-file format and conversion rules are
in [docs/SCHEMA.md](docs/SCHEMA.md). Known losses are in
[docs/LIMITATIONS.md](docs/LIMITATIONS.md); native histograms are decoded but
not converted because Parqtel has no schema-preserving native-histogram column.
Commands, results and the measured runs are in [docs/REPORT.md](docs/REPORT.md).

## Development

```bash
make fmt
make test
make lint
make docs-check
python3 scripts/bench.py target/release/tsdb2parqtel \
  crates/tsdb-reader/tests/fixtures/testblock bench/baseline.json
```

The workspace pins `parqtel-core` by git revision and Arrow/Parquet 60.1.0.
Changing either requires re-running the schema conformance test. MSRV is Rust
1.89 (the dependency set no longer builds on 1.88).

## License

MIT/Apache-2.0. The chunk decoder is used through the pinned MIT/Apache-2.0
`tsdb-reader` crate; the Parquet writer is used through `parqtel-core`
(Apache-2.0).
