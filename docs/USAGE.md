# Usage — `tsdb2parqtel`

`tsdb2parqtel` converts persisted Prometheus/Thanos TSDB blocks into Parqtel
Parquet blocks, or replays them into a running Parqtel over OTLP/HTTP.

Build:

```bash
cargo build --release
./target/release/tsdb2parqtel --help
```

Exit codes: `0` success, `1` generic error, `2` source not found/unreadable,
`3` partial success with `--on-error=skip`, `4` verification mismatch, `5`
retention refusal.

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

The sink sends OTLP/HTTP JSON to `/v1/metrics/json`, keeps each body under 4 MiB,
splits a series into timestamp-ordered batches, retries `429`/`5xx`/timeouts
with jittered backoff, and checkpoints the last acknowledged timestamp per
series. Re-running resumes after that timestamp.

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
