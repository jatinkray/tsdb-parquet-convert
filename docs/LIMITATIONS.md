# Limitations — `tsdb2parqtel`

Status: implemented; schema-driven output verified 2026-11-20.

This is the honest loss list. If a feature cannot be represented in Parqtel,
the tool reports it with block/series context instead of silently changing
semantics. It does not describe how the tool works; see `docs/DESIGN.md` for
that.

## Source coverage

- **Prometheus head / WAL data is not read.** Only persisted blocks
  (`meta.json`, `index`, `chunks/`, `tombstones`) are exported. A migration that
  must be exact "now" should flush the head first or replay the WAL separately.
- **Exemplars are dropped.** Parqtel metric blocks have no exemplar column.
- **Metadata is not a per-row column.** Unit/help/description are not stored by
  Parqtel; use `--metadata-file` only to improve `metric_kind` inference.
- **Native histogram chunks decode but are not converted.** The pinned
  `tsdb-reader` chunk decoder supports histogram, float histogram and their ST
  variants. Parqtel's `HistogramValue` has explicit boundaries and counts, not a
  schema/zero-threshold/spans representation, so the converter refuses the
  series by default with a message. `--on-error=skip` omits those series and
  records the count in `ScanStats`. This is a real data loss and is why the
  default is fail, not skip.
- **XOR2/ST chunks** use the pinned upstream decoder; they are not silently
  treated as XOR.
- **Index v1 quirks** are supported for the real v1 fixture (absolute symbol
  and series offsets). The v1 fixture has no `__name__` on its synthetic series,
  so source scanning skips those; real Prometheus v1 blocks do carry it.

## Representation

- **Classic histograms stay flat.** `_bucket`, `_sum`, `_count` remain separate
  series with `le`; they are not rewritten into one `HistogramValue`.
- **Floats only for TSDB samples.** Prometheus TSDB stores float samples; the
  converter writes `value_float` and leaves `value_int`/`value_complex` null.
  Integer-valued counters are not guessed into `value_int`.
- **`service.name` overwrite.** Parqtel's scan merges the `service_name` column
  over the labels JSON. Preserve mode avoids the conflict; OTEL mode deliberately
  renames labels and is opt-in.
- **Series cap.** `query.max_series` defaults to 1000; a source selection with
  more result series is truncated at query time until the query config is
  raised. Ingest `series_limit_per_metric` is 1M.
- **Instant lookback.** Parqtel instant queries see 5 minutes by default. Use
  range queries for historical verification; `verify` uses instant queries only
  when explicitly pointed at a live instance, one query per sample.

## Thanos

- **Downsample aggregate flag is interpretive.** 5m/1h chunks already carry the
  aggregate value; `--downsample-aggregate` documents which interpretation is
  expected but cannot reconstruct a different aggregate from the stored chunk.
- **Replica dedup is block-order deterministic.** Overlaps choose the
  lexicographically smaller block ULID, then the lower series ref. This is
  documented but is not Thanos's full query-time dedup algorithm.
- **External labels win on collision.** This matches a Thanos query surface, but
  differs from a raw Prometheus block that accidentally has the same key.

## Object storage

- **Staging is local.** S3/GCS/Azure sources are downloaded to a temporary
  directory; a range larger than free disk space is not supported.
- **Credentials come from provider chains.** There is no credential file format
  of the tool's own; do not put secrets in command-line arguments.
- **No object-store source for a single block file**; a prefix containing
  `meta.json` plus `index` and `chunks/` is required.
- **No writer fencing in Parqtel targets.** The tool writes one data root and
  refuses a target with WAL data unless `--force`.

## Live clone

- **HTTP only.** The live sink supports OTLP/HTTP JSON and protobuf. OTLP/gRPC
  is not implemented; use the HTTP endpoint. Both transports checkpoint per
  series and preserve finite values exactly.

## Installation / visibility

- **Index install requires a restart.** `clone --target <dir>` merges
  `index.json`, but a running Parqtel process has already loaded its index and
  will not see the change until restart. The WAL guard is not a process lock; it
  is a conservative safety check.
- **Sidecar fragments are not live merges.** `convert --sidecar` writes
  `index.fragment.json`; it does not touch a server's `index.json`.
- **`verify` requires the footer series index.** It does not reconstruct labels
  from a legacy row-level `labels` Parquet file.

## Schema-driven output

- **`verify` does not cover it.** `verify` reconciles Parqtel blocks through the
  footer series index; schema-driven Parquet files have no such index. Check
  them with any Parquet reader instead.
- **Raw labels only.** `--label-mode otel`, `--label-mapping`, `--metadata-file`,
  `--target-retention-days` and `--sidecar` are rejected with `--schema`: they
  describe Parqtel block semantics, and accepting them would change the labels
  and columns the schema file names.
- **Flat columns only.** No nested, list, map, struct or extension types; a
  column is one value per sample row.
- **One timestamp column minimum.** The schema must declare a `timestamp_ms`,
  `timestamp_us` or `timestamp_ns` column fed by `source: timestamp`; that
  column is what places a row in time.
- **Classic histograms stay flat.** `_bucket`, `_sum`, `_count` remain separate
  series with `le` in schema mode as well; there is no complex-value column.
- **`decimal128` goes through `f64`.** Values beyond 53 bits of integer
  precision are not exact; use `float64` or a scaled integer column for wide
  counters.
- **`+Inf`, `-Inf` and `NaN` pass into float columns.** Text columns and
  `decimal128` reject non-finite input, and `utf8` formatting of a non-finite
  value is an error.
- **Compression list is narrower than the writer.** The writer understands
  `gzip`, but the CLI exposes `zstd`, `snappy`, `lz4` and `none` only.

## Lakehouse output

- **Compaction does not cluster.** `compact` merges files and preserves row
  order; it does not sort by label, so pruning relies on the row-group
  statistics the writer already records.
- **Rollups cannot be verified sample-for-sample.** `verify` compares every
  source sample; `migrate --verify` refuses to run together with `--rollup`.
  Keep a raw export if you need the exact check.
- **Object-store output stages locally.** Each file is assembled in a temporary
  directory and uploaded, so peak local disk is one output file, not one run.
- **`--compact` needs local output.** It reads the written files back, so run it
  after downloading an object-store export.
- **Manifests are per run, not a catalog.** `manifest.json` describes one run;
  there is no incremental table format (Iceberg/Delta) yet.

## Out of scope

- Logs/traces conversion.
- Prometheus remote-write as an input source (Parqtel has a receiver, but this
  tool consumes TSDB blocks).
- A TSDB writer, GUI, or multi-writer distributed coordinator.
- Exact display of native histograms in Grafana through Parqtel, because the
  target has no native-histogram query type.

## Verification

- **Files mode is bit-exact.** `verify <source> <blocks>` compares
  `(timestamp_ns, value_bits)` including NaN payloads, ±Inf and subnormals.
- **Live mode normalises NaN payloads.** Parqtel's JSON query API returns
  `"NaN"` without the original payload. `verify <source> <http-url>` compares
  every NaN as canonical NaN and reports `nan_payloads_normalized` in the JSON
  report; finite values remain bit-exact.
- **Live mode queries 1 µs after the source timestamp.** Parqtel parses `time`
  as f64 seconds; at 2022 epochs an exact boundary can evaluate a few hundred
  nanoseconds early and miss the row.

## Reader implementation

- **Full index scan.** The reader parses the TOC, symbol table and series
  entries; it does not use postings or label indexes to narrow a selector.
  `--matcher` is applied after series labels are decoded. This is a performance
  choice, not a correctness difference.
- **Bounded file reads, not mmap.** Chunk records are read with checked
  `Read`/`Seek` calls. The process therefore never has to keep a whole segment
  mapped, at the cost of one read syscall per chunk.

Usage — [docs/USAGE.md](USAGE.md) · Schema — [docs/SCHEMA.md](SCHEMA.md) ·
Design — [docs/DESIGN.md](DESIGN.md) · Report — [docs/REPORT.md](REPORT.md)
