# DESIGN — `tsdb2parqtel` (Prometheus / Thanos TSDB → Parquet for Parqtel)

- **Status:** Phase 0 recon deliverable (finalized).
- **Source audited:** `parqtel-oss` @ `63a9cf1cdc9e8cc4855565b6d0b67fa6c366cef4`.
- **Source of truth:** Parqtel source. Where docs and code disagree, the code
  wins; discrepancies are recorded below.

---

## 1. Mission

Build a production-grade CLI, working name `tsdb2parqtel`, that converts
Prometheus / Thanos TSDB blocks into Parquet blocks that are
**schema-compatible** with Parqtel's own metric blocks, and that can **clone**
those series into a running Parqtel so they are queryable there exactly as they
were in Prometheus / Thanos.

Out of scope (unless trivial): logs/traces conversion, a TSDB writer, a GUI.
(Parqtel does ship a Prometheus `remote_write` receiver at
`POST /api/v1/write` — `crates/parqtel-server/src/handlers/remote_write.rs:48-179`,
snappy-compressed `WriteRequest`, samples only, ms→ns, Gauge kind — which is a
candidate ingest route for the `clone` subcommand, not a conversion change.)

---

## 2. Phase 0 — recon findings (source-anchored)

### 2.1 Exact output schema — 15 metric columns (`crates/parqtel-core/src/models/storage/schema.rs`)

Public schema (`metrics_schema()`, lines 71–73 → `metrics_schema_with_labels(true)`):

| # | Field | Arrow type | Nullable | Comments |
|---|---|---|---|---|
| 0 | `timestamp_ns` | `Timestamp(Nanosecond)` | no | Storage unit is **nanoseconds**; Prometheus is seconds/ms. |
| 1 | `metric_name` | Dictionary(Int32, Utf8) | no | Series identity key. |
| 2 | `metric_kind` | Utf8 (plain) | no | `"Gauge"`, `"Sum"`, `"Histogram"`, `"Summary"`. |
| 3 | `service_name` | Dictionary(Int32, Utf8) | **yes** | Resource service → becomes `service.name` label on read. |
| 4 | `service_version` | Dictionary(Int32, Utf8) | **yes** | Resource version. |
| 5–9 | `k8s_namespace`, `k8s_pod_name`, `k8s_pod_uid`, `k8s_container_name`, `k8s_node_name` | Dictionary(Int32, Utf8) | **yes** | Correlation columns. |
| 10 | `resource_attributes` | Dictionary(Int32, Utf8) | **no** | JSON of remaining resource attributes. |
| 11 | `labels` | Utf8 (plain) | no | Query-visible labels JSON, e.g. `{"job":"api"}`. **Public only.** |
| 12 | `value_float` | Float64 | **yes** | Gauge / sum-as-double. |
| 13 | `value_int` | Int64 | **yes** | Sum as int. |
| 14 | `value_complex` | Utf8 (JSON) | **yes** | Histogram / Summary; null for simple values. |

The **block** schema (`metrics_block_schema()`, `pub(crate)`, lines 76–78 →
`metrics_schema_with_labels(false)`) omits the `labels` column and instead carries
a `SeriesTable` in the Parquet footer KV key `parqtel.series_index`
(`crates/parqtel-core/src/storage/series_index.rs:34-39`; writer
`crates/parqtel-core/src/models/storage/writer.rs:117-211` picks block schema when
producing a block).

Block-writing path (`models/storage/writer.rs:123-211`, `build_metric_batch`):
group by `(metric_name, kind, resource)`; within a group sort by final
query-visible series fingerprint then timestamp; emit one row per sample.

### 2.2 Writer properties — `crates/parqtel-core/src/storage/block_file.rs`

`writer_properties()` (lines 238–274):

- compression from `BlockConfig::compression` + `compression_level`
  (`zstd` default, level `None` → codec default; codecs `zstd|snappy|lz4|none`;
  zstd levels 1–22),
- `WriterVersion::PARQUET_2_0`,
- `set_max_row_group_row_count(row_group_size)` → metrics **100 000**, logs **20 000**
  (`config/storage.rs:57-73,152-167`); validation `> 0` and `<= max_rows_per_block`.
- bloom filters on `metric_name` and `service_name`, fpp **0.01**
  (`block_file.rs:20-31, 266-274`),
- `EnabledStatistics::Page`, column-index truncate length **64**, data page limit
  **512 KiB**, dictionary disabled only for the log `body` column.

### 2.3 Filenames, atomic write, BlockMetadata

- Metric: `m_l0_{start_ts_ns}_{end_ts_ns}_{uuid32hex}.parquet`
  (`ingest/src/writer.rs:263-268`, `Uuid::new_v4().simple()` = 32 lowercase hex);
  logs: `logs_{start}_{end}_{uuid}.parquet`; traces: `traces_...`.
- Temp + rename: `.tmp_{filename}` → `fs::rename` → `block_store.publish`.
- Compaction output: `m_l{level}_{start}_{end}_{uuid}.parquet`.
- `BlockMetadata` (`models/storage/schema.rs:7-30`): `path`, `start_timestamp_ns`,
  `end_timestamp_ns`, `row_count`, `size_bytes`, `metric_names: HashSet<String>`,
  `label_names: HashSet<String>`, `label_values: BTreeMap<String, BTreeSet<String>>`,
  `signal_type`, `wal_range: Option<(u64,u64)>`.

### 2.4 Block visibility

- Flush → in-memory `BlockIndex::add` → debounced persist (2 s default) to
  `<data_dir>/index.json` (`{"blocks":[...]}` sidecar ref + sibling
  `<data_dir>/label_values.json`; sidecar magic `PQZ1`, zstd-3, JSON fallback) →
  `spawn_blocking` write with tmp+rename.
- Startup `reconcile` (storage/index.rs:375-449): drops entries whose file is gone
  or superseded, adopts unknown on-disk blocks (skipping `.tmp_`, signal matched by
  **schema** via `detect_signal`), fills start/end from footer column stats or the
  filename heuristic (`m_l0_/m_n_/m_l1_/m_l2_/logs_/traces_ → {start}_{end}_…`).

### 2.5 Label round-trip (series identity)

Series identity = query-visible `LabelSet` after the resource-derived
`service.name` is merged. On read, the dedicated `service_name` column
(`SeriesResolver::build`, `scanner/metrics.rs:125-134`) does
`labels.with("service.name", svc)` — i.e. it **overwrites** any value already in
the labels JSON.

| Prometheus/OTLP source | Lands where on write | Returned as on query |
|---|---|---|
| `__name__` | `metric_name` dictionary column (series key) | metric name selector / result key |
| `service.name` (+ `service.version`) | resource → `service_name` / `service_version` (+ merge into labels) | merged into label set as `service.name` / `service.version` |
| `k8s.namespace_name` / `k8s.namespace` etc. → mapped keys | resource → correlation columns `k8s_namespace` … `k8s_node_name` | correlation columns, injected into labels |
| remaining resource attributes | `resource_attributes` JSON column | leftover resource JSON |
| `job`, `instance`, `pod`, `container`, `node`, `le`, `quantile`, custom | `labels` JSON column | query-visible label set |

Round-trip requirement: for the same selector over the same range, Parqtel must
return the same label sets, timestamps and values as the source. The `le` /
`quantile` labels stay in the labels JSON (classic histogram), while native
histograms are carried in `value_complex` (`{"Histogram":{...}}`).

### 2.6 `metric_kind` strings and `value_complex` JSON

- Stored kinds (writer): `"Gauge"`, `"Sum"`, `"Histogram"`, `"Summary"`
  (`writer.rs:80–87`). Strict on read (`models/storage/reader.rs:143–153`);
  unknown → error.
- OTLP-JSON kind keys (ingest decode): `"gauge"`, `"sum"`, `"histogram"`,
  `"summary"` (`ingest/decode/metrics.rs:62–67`).
- `value_complex` JSON:
  - Histogram: `{"Histogram":{"count":3,"sum":1.5,"min":null,"max":2.0,"boundaries":[1.0],"counts":[1,2]}}`
  - Summary: `{"Summary":{"count":2,"sum":3.0,"quantiles":[[0.5,1.0]]}}`
  (`models/metrics.rs:266–280`). NaN is persisted as JSON `null` then restored.
- Values: `Double` → `value_float` (+`value_int`/`value_complex` null);
  `Int` → `value_int` (+float/complex null); complex → `value_complex` JSON only.
- Label fingerprint: FNV-1a-64 over length-prefixed sorted key/value pairs + splitmix64
  finalizer, cached (`models/labels.rs:136–157`).

### 2.7 Docs-vs-code discrepancies

- Buffer: `docs/operations/known-issues.md` says unbounded; build now carries
  `buffer_max_points` (5 M, `0`=off), `buffer_max_bytes` (2 GiB, `0`=off),
  `buffer_overflow_policy` (`drop_oldest|reject_new`), `series_limit_per_metric`
  (1 M, `0`=off).
- Auth: `docs/operations/security.md` documents `[auth]`, scoped tokens
  (`read<write<admin`), datasets, public routes, fail-closed `deny_all`;
  `crates/parqtel-server/src/auth.rs` implements it, **disabled by default**.
- Benchmarks: README headline 972 samples/s vs `docs/performance/README.md` M1 run
  1.85 M pts/s ingest / 101 M pts/s in-process scans; backlog 11.7 K pts/s/core.
- Writer-vs-reader: blocks may omit `labels` (indexed) or carry it (public/legacy);
  scans use `METRIC_SCAN_COLUMNS` vs `INDEXED_SCAN_COLUMNS`.

---

## 3. Design & explicit assumptions (Phase 1 inputs)

1. Emit the writer form: `metric_runs_to_block` grouping + Parquet writer props
   + `m_l0_` filename + `.tmp_`/`rename` + footer KV (`parqtel.series_index`) or
   legacy `labels` column.
2. Deterministic UUID (v5) over source block ULID + window to keep re-runs
   idempotent.
3. Block windows from **target** `block_duration_secs` (7200 s) / `max_rows_per_block`
   (1 M); compacted sources split across windows.
4. Label mapping: `--label-mapping` default profile (`job`→`service.name`,
   `namespace`→`k8s.namespace_name`, etc.) with `--label-mode preserve|otel`.
5. Stale markers: `--stale-markers=drop|keep`, default to be verified against the
   PromQL eval of NaN.

Open decisions to record in Phase 1: which source block format (ULID dir vs Thanos
`meta.json` / object store) is the first supported; whether to reuse
`parqtel_core` schema types via git dependency or vendor + conformance test.

---

## 4. Phase 0 conclusion

The Parquet target format is fully specified and small: a 15-column Arrow schema,
writer properties (Parquet 2.0, zstd, row group 100 k, bloom fpp 0.01 on
`metric_name`+`service_name`, Page statistics, column index 64, data page 512 KiB,
per-column dictionary policy), a `m_l0_{start}_{end}_{uuid}.parquet` filename, tmp+rename
publish, footer KV or `labels` column, plus an index sidecar contract
(`{"blocks":[...]}` + `label_values.json`) and a startup reconcile that adopts
unindexed blocks.

The converter can therefore emit byte-format-compatible blocks with high
confidence, provided the reader path (`service_name` + labels JSON merge, kind
strings, value_complex JSON, fingerprint) is replicated exactly — all facts
captured in §2.
