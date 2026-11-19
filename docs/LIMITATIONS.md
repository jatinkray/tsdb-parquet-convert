# Limitations

This is the honest loss list. If a feature cannot be represented in Parqtel,
the tool reports it with block/series context instead of silently changing
semantics.

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
