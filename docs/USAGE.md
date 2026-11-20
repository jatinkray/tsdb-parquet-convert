# Usage — `tsdb2parqtel`

`tsdb2parqtel` converts persisted Prometheus/Thanos TSDB blocks into Parqtel
Parquet blocks, writes ordinary Parquet for a schema you supply, or replays the
same series into a running Parqtel over OTLP/HTTP.

## Install

Every release publishes static binaries for Linux (x86_64, aarch64, musl),
macOS (Intel, Apple silicon) and Windows, plus `SHA256SUMS`; the container image
is on GitHub Container Registry.

```bash
# Prebuilt archive (pick the asset for your platform)
gh release download v0.2.0 --pattern '*x86_64-unknown-linux-musl.tar.gz'
tar -xzf tsdb2parqtel-0.2.0-x86_64-unknown-linux-musl.tar.gz
./tsdb2parqtel --version

# Container
docker run --rm ghcr.io/jatinkray/tsdb-parquet-convert:latest --version

# From source
cargo install --git https://github.com/jatinkray/tsdb-parquet-convert tsdb2parqtel
```

Verify what you downloaded before running it:

```bash
# Checksums cover every asset, including SBOMs and signature bundles
sha256sum -c <(grep 'x86_64-unknown-linux-musl.tar.gz$' SHA256SUMS)

# Keyless signature from the release workflow
cosign verify-blob \
  --bundle tsdb2parqtel-1.0.0-x86_64-unknown-linux-musl.tar.gz.sigstore.json \
  --certificate-identity-regexp 'https://github.com/.*/release.yml@refs/heads/main' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  tsdb2parqtel-1.0.0-x86_64-unknown-linux-musl.tar.gz

# Build provenance (gh >= 2.49)
gh attestation verify tsdb2parqtel-1.0.0-x86_64-unknown-linux-musl.tar.gz \
  --repo jatinkray/tsdb-parquet-convert
```

Each archive also ships as `<archive>.spdx.json` (SBOM) with its own signature
bundle.

Homebrew users render a formula from the release checksums (no tap is
published):

```bash
python3 scripts/render_homebrew_formula.py 0.2.0 SHA256SUMS \
  https://github.com/jatinkray/tsdb-parquet-convert/releases/download > tsdb2parqtel.rb
brew install --formula ./tsdb2parqtel.rb
```

Build from a checkout:

```bash
cargo build --release
./target/release/tsdb2parqtel --help
```

| Exit code | Meaning |
|---|---|
| `0` | Success |
| `1` | Generic error, including a rejected flag combination |
| `2` | Source not found or unreadable |
| `3` | Partial success with `--on-error=skip` |
| `4` | Verification mismatch |
| `5` | Retention refusal |

## Migrate a local Prometheus data directory

```bash
tsdb2parqtel inspect /var/lib/prometheus/blocks --top-labels 10
tsdb2parqtel plan /var/lib/prometheus/blocks \
  --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z
tsdb2parqtel convert /var/lib/prometheus/blocks \
  --out /var/lib/parqtel/staging \
  --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z \
  --sidecar
tsdb2parqtel verify /var/lib/prometheus/blocks /var/lib/parqtel/staging
```

`files` output is `m_l0_{start_ns}_{end_ns}_{uuid32}.parquet`; the UUID is a
UUIDv5 of source block ULID + window + part, so re-running the same source with
the same flags overwrites the same names instead of duplicating samples.

To install into a stopped Parqtel data directory and merge its index:

```bash
tsdb2parqtel clone /var/lib/prometheus/blocks \
  --target /var/lib/parqtel/data \
  --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z
# restart parqtel; a live server does not reload index.json
```

The tool refuses to mutate a target with WAL files unless `--force`.

## Convert to Parquet for a custom schema

Use this when the target is not Parqtel: you name the columns, their types and
where each value comes from, and the tool writes plain Parquet. The format,
types, sources, cast rules and failure messages are in
[docs/SCHEMA.md](SCHEMA.md).

Generate a starting schema from the block index (no chunk data is decoded):

```bash
tsdb2parqtel schema /var/lib/prometheus/blocks --out schema.yaml
```

Edit it (`--max-labels N`, `--no-labels` and `--name` control the generated
file), or start from `examples/schema-labelled.yaml`, then convert:

```bash
tsdb2parqtel convert /var/lib/prometheus/blocks \
  --out ./parquet \
  --schema schema.yaml \
  --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z
```

Files are named `data_{start_ns}_{end_ns}_{part:05}.parquet`, a window is split
into files of at most `--max-rows-per-block` rows, and a rerun overwrites the
same names. The schema is embedded in each file's Parquet metadata.

Constraints, all reported as errors instead of being ignored:

- `--sidecar`, `--label-mode otel`, `--label-mapping`, `--metadata-file` and
  `--target-retention-days` are Parqtel-only; remove them with `--schema`.
- `clone` and `verify` have no schema mode; `verify` reconciles Parqtel blocks
  through their footer series index.
- Schema mode reads the raw series labels, so what you map with `label:<key>` is
  exactly what the TSDB index stores.

## Verify schema-mode output

```bash
tsdb2parqtel verify /var/lib/prometheus/blocks ./lake --preset series
```

Verification reads the Parquet back and rebuilds a per-series digest (sample
count, first/last timestamp, ordered xxhash of `(timestamp_ns, value_bits)`),
then compares it with the source. It needs two columns in the schema:

- `source: series_fingerprint` typed `uint64`, which is how series are matched
  without carrying labels;
- one `source: value` column typed `float64`, because a cast copy cannot be
  compared bit-for-bit.

Exit code 4 means a missing, extra or mismatched series; the JSON report lists
each one with both digests. Rolled-up output cannot be verified this way — the
verifier compares every source sample.

## Export to a data lake

```bash
tsdb2parqtel migrate s3://thanos/prod --out s3://lake/metrics \
  --preset series --rollup 5m --state /var/lib/migrate/prod.json --verify
```

`migrate` runs convert, optional rollup and compaction, writes the manifest, and
prints a JSON summary with rows scanned and written, file count, bytes, and the
monthly storage cost at S3 Standard and Glacier Instant Retrieval list prices.
`--verify` gates the run on the per-series reconciliation above.

Object-store output assembles each file locally and uploads it, so peak local
disk is one output file, not one run. Every schema-mode run writes
`manifest.json` and `manifests/<run-id>.json` next to the data:

```json
{
  "tool": "tsdb2parqtel 1.0.0",
  "run_id": "20261120T201001Z-b239fe101527d074",
  "source": "s3://thanos/prod",
  "destination": "s3://lake/metrics",
  "schema": {"name": "tsdb_series", "hash": "b239fe101527d074", "columns": 4},
  "rollup": {"seconds": 300, "function": "avg"},
  "totals": {"windows": 6, "files": 6, "rows": 560727, "bytes": 984886},
  "windows": [],
  "files": []
}
```

## Roll up old data

```bash
tsdb2parqtel convert s3://thanos/prod --out s3://lake/metrics-5m \
  --preset series --rollup 5m --rollup-func avg
```

One row per series and bucket, at the bucket start. Functions: `avg` (mean of
the samples in the bucket), `sum`, `min`, `max`, `last` (sample with the latest
timestamp), `count` (number of samples). Buckets are aligned with floor
division, so `5m` buckets start at multiples of 300 s. The manifest records the
width and function; a raw export is still the source of truth for verification.

Measured on the repository fixture (5,550,882 samples): a 5m `avg` rollup wrote
560,727 rows and 0.98 MB against 4.38 MB raw — 9.9x fewer rows, 4.45x fewer
bytes.

## Merge small files

```bash
tsdb2parqtel compact ./lake --out ./lake/compacted --preset series \
  --rows-per-file 2000000
```

Compaction merges files without reordering rows and refuses to publish a result
whose per-series digest differs from the input, so a silent row loss or
reordering aborts the run. It does not cluster by label: pruning relies on the
row-group statistics each file already carries.

## Run on a schedule

```bash
# every 15 minutes, only new blocks are read
tsdb2parqtel convert s3://thanos/prod --out s3://lake/metrics \
  --preset series --state /var/lib/migrate/prod.json
```

The state file records each converted block ULID with its max timestamp and the
schema hash that produced it. A block already recorded is skipped before any
chunk is decoded; a state file written with a different schema hash is refused
instead of mixing layouts.

## Query what you wrote

```bash
tsdb2parqtel emit-ddl --preset series --target duckdb --table metrics \
  --location ./lake > lake.sql
duckdb < lake.sql
```

```sql
-- inside duckdb after the view exists
SELECT metric, avg(value) AS avg_value
FROM metrics
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY metric
ORDER BY avg_value DESC
LIMIT 10;
```

`--target athena` needs an `s3://` location and prints `CREATE EXTERNAL TABLE …
STORED AS PARQUET`; `--target bigquery` needs a `gs://` location and prints an
external table with `format = 'PARQUET'`. Nanosecond timestamps stay `INT64`
where the engine cannot represent them.

## Migrate a Thanos bucket range

```bash
export AWS_REGION=eu-north-1
# credentials from the standard AWS provider chain; never put them in argv
tsdb2parqtel inspect s3://my-thanos-bucket/prod \
  --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z
tsdb2parqtel convert s3://my-thanos-bucket/prod \
  --out ./thanos-parquet \
  --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z \
  --external-labels include \
  --downsampled prefer-raw
```

GCS and Azure use `gs://bucket/prefix` and `az://container/prefix`; the same
standard provider chains apply. The source is staged into a temporary directory
and deleted when the command exits.

## Select blocks and series

```bash
# One or more block ULIDs
--block-ulid 01ARZ3NDEKTSV4RRFFQ69G5FAV --block-ulid 01ARZ3NDEKTSV4RRFFQ69G5FAW

# PromQL-style label matcher (sequential scan; postings are not used)
--matcher '{job="api",severity!="info"}'

# Filter on Thanos external labels
--external-label-filter '{cluster="prod"}'

# Replica dedup, removing the replica label from output
--dedup-replica-label replica

# Corrupt input behavior
--on-error fail|skip
```

Default stale-marker behavior is `drop`; use `--stale-markers keep` for a
bit-exact archive. Default external-label behavior is `include`; use
`--external-labels ignore` to compare raw Prometheus series.

## Live clone into a running Parqtel

```bash
tsdb2parqtel clone /var/lib/thanos/bucket/prod \
  --target http://parqtel.internal:8080 \
  --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z \
  --checkpoint /var/lib/parqtel/clones/prod.json
```

The sink sends OTLP/HTTP JSON to `/v1/metrics/json` by default, or protobuf to
`/v1/metrics` with `--otlp-protocol protobuf`. It keeps each body under 4 MiB,
splits a series into timestamp-ordered batches, splits again on `413`, retries
`429`/`5xx`/timeouts with jittered backoff, and checkpoints the last acknowledged
timestamp per series. Re-running resumes after that timestamp.

Verify a live instance (one instant query per source sample; slow but exact):

```bash
tsdb2parqtel verify /var/lib/thanos/bucket/prod http://parqtel.internal:8080 \
  --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z
```

## Verify a converted directory

```bash
tsdb2parqtel verify /var/lib/prometheus/blocks ./thanos-parquet \
  --json --report /tmp/verify.json
```

The verifier compares per series: label set, sample count, first/last timestamp
and an ordered xxhash of `(timestamp_ns, value_bits)`. It exits `4` on any
missing, extra or mismatched series. A Parquet output must carry the
`parqtel.series_index` footer; legacy row-level labels are not read by `verify`.

## Configuration and retention

Global flags: `--threads N` sizes the Rayon pool used for parallel output
windows (`1` = sequential, lowest peak memory); `--memory-limit` is advisory
and logged; `--log-format json` emits structured logs; `--progress` logs one line per
converted window; `--dry-run` performs the full selection and plan without
writing files.

Useful output flags:

```text
--block-duration-secs 7200       Parqtel metric window
--max-rows-per-block 1000000
--row-group-size 100000
--compression zstd|snappy|lz4|none
--compression-level 1..22
--label-mode preserve|otel       preserve is identity-preserving
--label-mapping mapping.json
--metadata-file metadata.json
--target-retention-days 7        warn/refuse if imported data is already expired
--retention-action warn|refuse
--sidecar                        write index.fragment.json
--force                          allow index install despite WAL presence
```

The exact Parqtel key to raise for metric retention is
`storage.retention_days`; logs use `logs.retention_days`. Parqtel's retention
task runs hourly and deletes blocks whose entire range is older than the cutoff.
The converter warns when the oldest imported window is older than the configured
target retention and exits `5` when `--retention-action refuse` is set.

## Common failure messages

| Message | Meaning |
|---|---|
| `series ref N ... native histogram sample(s)` | Parqtel has no schema-preserving native histogram column; use `--on-error=skip` and read `docs/LIMITATIONS.md` |
| `checksum mismatch` | source block is corrupt; never trust that series |
| `unsupported object-store scheme` | use `s3://`, `gs://` or `az://` |
| `contains WAL data; stop the server or pass --force` | target may be live; index install is unsafe |
| `retention warning` | imported range is older than the target retention |

## End-to-end verification

`scripts/e2e.sh` (or `make e2e`) starts a pinned Parqtel container, imports the
real fixture through both `files` and `otlp` modes, re-runs the import to prove
idempotence, compacts the imported blocks, and reconciles after every step:

```bash
PARQTEL_IMAGE=parqtel-e2e bash scripts/e2e.sh
```

The compose file is `deploy/e2e/docker-compose.yml`; it publishes the HTTP API
on `127.0.0.1:18080` and OTLP gRPC on `14317`. Live reconciliation uses a
two-minute source slice because Parqtel's instant-query path is intentionally
one request per sample.

Schema — [docs/SCHEMA.md](SCHEMA.md) · Design — [docs/DESIGN.md](DESIGN.md) ·
Limitations — [docs/LIMITATIONS.md](LIMITATIONS.md) · Report — [docs/REPORT.md](REPORT.md)
