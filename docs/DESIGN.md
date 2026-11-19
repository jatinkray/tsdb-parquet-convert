# Design — `tsdb2parqtel`

Status: implemented; source audited at
`parqtel-oss@63a9cf1cdc9e8cc4855565b6d0b67fa6c366cef4`.

The source tree is canonical. This document records what the code does, where
the docs disagree, and the lossy cases that are intentionally rejected or
flagged.

## 1. Target block format

A Parqtel metric block is **14 columns plus a footer `SeriesTable`**, not a
15-column row-level `labels` block. The 15-column `metrics_schema()` is retained
for chunks and legacy blocks; the writer emits the block form through
`metric_runs_to_block` + `write_block_file`.

| # | Column | Arrow type | Null |
|---|---|---|---|
| 0 | `timestamp_ns` | `Timestamp(Nanosecond)` | no |
| 1 | `metric_name` | `Dictionary(Int32, Utf8)` | no |
| 2 | `metric_kind` | `Utf8` | no |
| 3 | `service_name` | `Dictionary(Int32, Utf8)` | yes |
| 4 | `service_version` | same | yes |
| 5–9 | `k8s_namespace`, `k8s_pod_name`, `k8s_pod_uid`, `k8s_container_name`, `k8s_node_name` | same | yes |
| 10 | `resource_attributes` | `Dictionary(Int32, Utf8)`, JSON | no |
| 11 | `value_float` | `Float64` | yes |
| 12 | `value_int` | `Int64` | yes |
| 13 | `value_complex` | `Utf8`, JSON | yes |

The series labels are stored in the footer KV `parqtel.series_index`
(zstd-compressed JSON, `PQZ1`). The reader also accepts legacy blocks with a
row-level `labels` `Utf8` column.

Source: `crates/parqtel-core/src/models/storage/schema.rs`,
`models/storage/writer.rs`, `storage/series_index.rs`.

### Writer properties

- Parquet writer version `PARQUET_2_0`.
- Compression `zstd` (level 1–22), `snappy`, `lz4` or `none`.
- Row group size 100 000 (must be ≤ max rows per block).
- Bloom filters are configured for `metric_name` and `service_name`, fpp
  `0.01`. The upstream builder then calls the global `set_bloom_filter_fpp`,
  which implicitly enables a filter on **every** column; the converter keeps
  that exact behavior for byte-compatible output. This is an upstream
  discrepancy between the stated intent and the emitted block.
- Page statistics, column-index truncate length 64, data page limit 512 KiB.
- No dictionary for the log `body` column; metric columns use dictionary
  encoding as normally inferred by Arrow.

## 2. Label round-trip

`label-mode=preserve` is the default and preserves series identity exactly:

| Prometheus label | Preserve mode | Query-visible result |
|---|---|---|
| `__name__` | `metric_name` column | selector name / `__name__` in result |
| `job`, `instance`, `namespace`, `pod`, `container`, `node`, `le`, `quantile`, custom | `labels` JSON in the footer series table | same key/value |
| `service.name` | labels JSON unless lifted | same key/value |

`label-mode=otel` is opt-in. It moves the configured mappings (`job` →
`service.name`, `namespace` → `k8s.namespace.name`, etc.) into correlation
columns/`resource_attributes`; on read, `service.name` and the `k8s.*` columns
are merged back into the label set. This **changes the returned label names**
and is therefore not the default. A mapping file (`--label-mapping`) can replace
the default profile.

The reader merges the `service_name` column with `l.with("service.name", svc)`;
the column wins over a conflicting labels-JSON value. The converter never
writes the same key into both places in preserve mode.

## 3. `metric_kind` and complex values

Stored strings: `Gauge`, `Sum`, `Histogram`, `Summary` (strict on read).

Inference without metadata:

- `_total`, `_count`, `_sum`, `_bucket`, `_created` → `Sum` (a counter family).
- everything else → `Gauge`.
- native histogram chunks → not convertible yet (see §6).

`--metadata-file` accepts `/api/v1/metadata` JSON or a YAML/JSON map. A counter
metadata entry maps to `Sum`; gauge/info/stateset to `Gauge`; histogram/summary
to `Sum` for classic flat series. Classic histograms remain the source series
`_bucket`/`_sum`/`_count` with `le` as a normal label; they are not rewritten
into a single complex value.

`value_complex` JSON matches Parqtel:

```json
{"Histogram":{"count":3,"sum":1.5,"min":null,"max":2.0,"boundaries":[1.0],"counts":[1,2]}}
{"Summary":{"count":2,"sum":3.0,"quantiles":[[0.5,1.0]]}}
```

The converter currently emits `value_float` for Prometheus float samples. It
does not emit `value_int` for TSDB data because TSDB has no integer type on the
wire; the policy is documented rather than guessing.

## 4. Visibility

A block becomes visible when it is on disk and in the metric index:

1. Writer writes `.tmp_<name>.parquet`, fsyncs, renames to
   `m_l0_{start}_{end}_{uuid32}.parquet`.
2. `BlockMetadata` (path, range, rows, bytes, metric names, label names,
   capped label values, `SignalType::Metrics`, no WAL range) is added to
   `BlockIndex`.
3. `index.json` is written as a zstd `PQZ1` sidecar plus sibling
   `label_values.json`.
4. On startup, reconciliation adopts unknown `.parquet` files; without an index
   entry a block gets an empty metric-name set (visible to all queries, no
   name pruning) until a rewrite/compaction.

`convert --sidecar` writes `index.fragment.json` + the block files. `clone
--target <data-dir>` writes blocks and merges the index through
`parqtel_core::BlockIndex` + `persist_blocking`; it refuses to touch a target
with WAL data unless `--force`. A running server must be restarted to pick up a
sidecar changed underneath it.

## 5. Reconstruction path

- Local source discovery: walk up to four levels for `meta.json`; a single block
  directory is also accepted.
- Index v1/v2 parsed natively (magic `0xBAAAD700`, version byte, TOC, symbols,
  series, CRC32C). V1 symbol refs are absolute file offsets and series refs are
  absolute offsets; V2 refs are sequential/`offset/16`.
- Stacked chunk segments: reference `(segment << 32) | offset`; header magic
  `0x85BD40DD`; record CRC32C; XOR decoder validates sample count and bitstream.
- Tombstones: v1, CRC32C, closed intervals applied per series ref.
- XOR2 and native/float histogram chunks are decoded by the pinned
  `tsdb-reader` 0.1.0 chunk decoder (MIT/Apache-2.0) behind our block reader.
  Native histogram samples are detected and rejected with context because
  Parqtel's `HistogramValue` has explicit boundaries only; use
  `--on-error=skip` to omit such series and record the loss.
- Stale markers: default `drop`. Parqtel has no stale-marker semantic, and a
  stored NaN would be visible as a NaN point/range-function input. `keep` is
  available for bit-exact archives.
- External Thanos labels are merged into every series by default; a conflicting
  external key wins, matching what a Thanos query surface returns.
- Replica dedup: series are grouped without the requested replica labels;
  overlapping timestamps choose the lexicographically smaller block ULID, then
  the lower series ref. Replica labels are removed from the output label set.

## 6. Object storage and Thanos

`s3://`, `gs://` and `az://` sources are staged into a temporary directory
using `object_store` builders from the standard environment/provider chains.
Only the selected blocks' `meta.json`, `index`, `tombstones` and `chunks/*` are
downloaded; credentials are never logged. The staging directory lives as long as
the `Source`.

Thanos downsampled blocks are selected with `--downsampled=skip|prefer-raw|
include|only`. The aggregate is already encoded in the chunk; the
`--downsample-aggregate` flag records the documented interpretation and does not
invent a second aggregation. `prefer-raw` drops a downsampled block only when a
raw block covers its entire range.

## 7. Docs-vs-code discrepancies found

- `docs/` and the README have described metric blocks as 15 columns; the
  indexed writer emits 14 + footer series table. The 15-column schema is the
  legacy/public form.
- Old docs called the in-memory buffer unbounded; current code has
  `buffer_max_points` (5M), `buffer_max_bytes` (2GiB), overflow policy and a
  per-metric series cap.
- The README says Arrow 59 in places; the audited `Cargo.lock` pins Arrow and
  Parquet **60.1.0** (the converter uses exactly 60).
- The docs describe object storage as “landing” in the older edge-mode page;
  it exists and has a shared catalog, but no writer fencing.
- `docs/` says `noise_score` is reserved in older alert/MCP pages; the current
  alert engine derives it from transition history.
- Parqtel docs mention an index lock/WAL check for live installs; this tool
  implements a WAL-presence guard, not a process lock. Restart is required.
- Parqtel's writer comment says bloom filters are per-column for
  `metric_name`/`service_name`, but the global `set_bloom_filter_fpp` call
  enables bloom filters on every column. Conformance tests assert this
  upstream behavior rather than the comment.

- Parqtel's instant-query API parses `time` as floating-point seconds. At
  2022-era epochs, f64 cannot represent every nanosecond, so a query exactly on
  a stored sample can evaluate a few hundred nanoseconds before the row and
  miss it. `verify` evaluates 1 µs after the source timestamp and documents the
  workaround; this is a query-parser precision issue, not a storage loss.

## 8. Assumptions

- One writer per Parqtel data root. The converter does not attempt multi-writer
  compaction coordination.
- Source blocks are immutable while read.
- The target Parqtel version is the pinned audited commit; upgraded Parquet
  writing requirements require re-running the schema conformance test.
- Timestamps are accepted as Prometheus milliseconds and converted to ns with
  checked multiplication. Instants older than the target retention are warned
  about (or refused when `--retention-action refuse`).
- The tool never writes to the source.


## 9. Streaming and memory bound

`Source::scan_range(start_ms, end_ms)` applies the source selection and reads
only chunks overlapping the requested range. `convert` builds the output window
ranges, scans each window, writes it (splitting at `max_rows_per_block`), and
then drops the window state. Independent windows run on Rayon with
`--threads`; peak memory is bounded by the number of concurrent windows, not by
the total source size. On the 100M-sample synthetic block, eight concurrent 1M
row windows peaked at 1.09 GiB and ran at ≈15.3M samples/s; `--threads 1` used
109 MiB.
