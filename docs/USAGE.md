# USAGE — `tsdb2parqtel` runbooks

- **Audience:** operators and engineers running the CLI.
- **Audience note:** these are runbooks for the tool we are building. They are
  written against the Phase 0 design (`docs/DESIGN.md`) and will be updated as
  the implementation lands.

---

## 1. `inspect <source>`

**Purpose:** no writes. Report what the source contains, so a migration can be
planned without touching data.

```
cargo run -p tsdb-reader -- inspect /var/lib/parqtel/prometheus/blocks
```

Reports, per block:

- block start/end timestamp (ns),
- row count (samples) and estimated output size,
- external labels / downsample resolution / source (Thanos),
- series count, sample count, chunk statistics,
- top label cardinality (by value) so an operator knows what they are importing.

Exit codes: `0` ok, `2` source unreadable / empty, `3` partial (see `--on-error=skip`).

---

## 2. `plan <source> ...`

**Purpose:** dry run. No files written. Prints the intended output windows,
estimated rows/files, the label mapping preview for sampled series, and any
retention conflict (data older than the target retention would be swept on first
query).

```
cargo run -p tsdb-reader -- plan /var/lib/thanos/buckets/prod \
    --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z \
    --matcher 'job="api"' --downsampled=prefer-raw
```

`--downsampled=prefer-raw` is the default; the flag exists because Parqtel's
compaction/flush story assumes raw blocks for recent windows.

---

## 3. `convert <source> --out <dir> ...`

**Purpose:** write Parqtel metric blocks into a data directory (mode `files`) or
emit them into a running Parqtel (mode `otlp`).

```
# mode: files (write into a staging dir + optional index sidecar fragment)
cargo run -p tsdb2parqtel -- convert /var/lib/parqtel/prometheus/blocks \
    --out /tmp/parqtel-converted \
    --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z \
    --matcher 'job="api"' \
    --label-mapping /etc/parqtel/label-map.yaml \
    --label-mode preserve

# mode: otlp (live clone into a running Parqtel)
cargo run -p tsdb2parqtel -- clone /var/lib/thanos/buckets/prod \
    --target otlp://127.0.0.1:4317 \
    --min-time ... --max-time ... \
    --checkpoint /var/lib/parqtel/clones/api-checkpoint.json
```

`files` mode:

- writes `m_l0_{start}_{end}_{uuid32hex}.parquet` under `--out`,
- writes `.tmp_<name>` then `rename`,
- optional sidecar fragment (index entry) when `--install` is passed;
  never overwrites a live server index without `--force` (and refuses when a
  running server's WAL is detected).

`otlp` mode:

- replays series through Parqtel's OTLP ingest endpoints,
- bodies kept under the server's 10 MB limit; backpressure surfaced as `413`/`429`
  with jittered retries; idempotent via checkpoint.

---

## 4. `clone <source> --target <data-dir | otlp-url> ...`

**Purpose:** convert + install / ingest in one pass, with checkpoint/resume.

```
cargo run -p tsdb2parqtel -- clone /var/lib/thanos/buckets/prod \
    --target /var/lib/parqtel \
    --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z \
    --checkpoint /var/lib/parqtel/clones/api-checkpoint.json
```

The checkpoint stores per-series ingest position so a re-run resumes rather than
duplicates.

---

## 5. `verify <source> <parqtel-blocks | parqtel-url>`

```
# against a directory of Parqtel-produced blocks
cargo run -p tsdb2parqtel -- verify /var/lib/thanos/buckets/prod \
    /tmp/parqtel-converted --json /tmp/verify-report.json

# against a live Parqtel instance (via its query API)
cargo run -p tsdb2parqtel -- verify /var/lib/thanos/buckets/prod \
    otlp://http://127.0.0.1:8080 --json /tmp/verify-report.json
```

Compares, per series: label set, sample count, first/last timestamps, and a
xxhash of the `(timestamp_ns, value)` stream. Exits non-zero on any mismatch.

---

## 6. Retention warning

Before `clone --target <data-dir>`, check the target config:

```
cargo run -p tsdb2parqtel -- inspect /var/lib/parqtel --config /etc/parqtel/config.toml
```

If `storage.retention_days < (range span / 86400)`, data will be swept
immediately on first flush. The tool prints the exact config key to raise and
exits non-zero (or warns, per `--retention-action warn|refuse`).

---

## 7. Typical migration runbook

1. `inspect <source>` → size, ranges, series counts.
2. `plan <source>` → windows, mapping preview, retention check.
3. `convert <source> --out <dir> --install` → write blocks + sidecar fragment.
4. Start / restart Parqtel against `<dir>`.
5. `verify <source> <parqtel-blocks>` → per-series equality.
6. Re-run `convert` → confirm idempotence (no duplicate samples).

---

## 8. Exit codes (final design)

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | generic error (source unreadable, writer failed) |
| 2 | source unreadable / empty / not found |
| 3 | partial success with `--on-error=skip` |
| 4 | verification mismatch (from `verify`) |
| 5 | retention refusal (from `clone --target`) |
