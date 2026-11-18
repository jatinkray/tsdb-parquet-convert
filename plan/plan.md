# ROLE

You are a principal-level Rust engineer with deep expertise in the Prometheus TSDB on-disk format, Thanos block layout, Apache Arrow/Parquet internals, and high-throughput, memory-bounded data pipelines. You write idiomatic, safe, well-tested Rust, and you verify claims against source and real data instead of assuming.

# MISSION

Build a production-grade CLI utility, working name `tsdb2parqtel`, that converts Prometheus / Thanos TSDB blocks into Parquet blocks that are byte-for-byte *schema-compatible* with Parqtel (https://github.com/parqtel/parqtel-oss), and that can **clone** the same time series (identical series identity, timestamps, values) into a Parqtel instance so they are queryable there exactly as they were in Prometheus/Thanos.

Docs: https://parqtel.com/docs/ (start with /storage, /durability, /concepts, /ingestion, /architecture, /configuration, /operations) and https://github.com/parqtel/parqtel-oss/tree/main/docs.
**The source code is canonical. Where docs and code disagree, follow the code and record the discrepancy in docs/DESIGN.md.**

# PHASE 0 — RECON (mandatory, before writing any converter code)

Clone parqtel-oss and read, at minimum:
- `parqtel-core/src/models/storage/schema.rs` (canonical schemas), `parqtel-core/src/storage/` (`scanner.rs`, `index.rs`, `compactor.rs`, `retention.rs`, `persist.rs`), the metrics writer/flush path, and `parqtel-core/src/wal.rs`.
- The metrics decoder (how rows become series on read): how `service_name`, `resource_attributes` and `labels` are merged back into a label set, how the 64-bit label fingerprint is computed, and how `metric_kind`, `value_float`, `value_int`, `value_complex` are populated and read for gauge, counter, histogram and summary.
- OTLP ingest (`parqtel-ingest`) so you know exactly how OTLP data points map to rows (naming, unit/suffix handling, label key normalization, temporality, monotonic sums).
- `Cargo.lock`: pin the **same `arrow` / `parquet` versions** parqtel-oss uses (docs say the arrow 59 stack). Blocks written by a different encoding generation can be unreadable.

Deliverable of this phase: `docs/DESIGN.md` containing (1) the exact output schema (15 metric columns, types, nullability, dictionary encoding, writer properties) as found in code, (2) a **label round-trip table**: for every Prometheus label (`__name__`, `job`, `instance`, `namespace`, `pod`, `container`, `node`, `le`, `quantile`, custom) where it lands in the Parquet row and what the Parqtel query API returns for it, (3) the `metric_kind` string values and the native-histogram/summary JSON shape expected in `value_complex`, (4) how a block becomes visible to a running server (index sidecar `index.json`, reconciliation-on-startup adoption, `BlockMetadata` fields), (5) discrepancies between docs and code, (6) your design and explicit assumptions. Do not ask me questions; make the most defensible choice, document it, and make it configurable.

# SOURCE SIDE — INPUT FORMATS

Support, with a clean `BlockSource` abstraction:
1. **Local directory** of Prometheus TSDB blocks (ULID-named dirs with `meta.json`, `index`, `chunks/NNNNNN`, `tombstones`).
2. **Thanos blocks**, locally or in object storage (S3, GCS, Azure, via the `object_store` crate; accept credentials from the standard env/provider chains, never log secrets). Parse `meta.json` `thanos` section: external `labels`, `downsample.resolution`, `source`, `files`.
3. Selection: `--min-time/--max-time`, `--block-ulid ...`, `--matcher '{job="x"}'` (PromQL selector syntax), `--external-label-filter`.

Implement a native reader (no Go, no FFI to Prometheus). Requirements:
- Index format v2 (and v1 if feasible): TOC, symbol table, series entries, label indexes, postings, offset tables; verify CRC32 (Castagnoli) on every section you read.
- Chunks: XOR float chunks; also native-histogram and float-histogram chunk encodings (counter reset hints, spans/deltas, custom buckets); chunk CRC32 checks; multi-segment chunk files with 512 MiB-style segment layout.
- Tombstones: apply deletions; never emit deleted intervals.
- Thanos downsampled blocks (5m/1h aggregate chunks: count/sum/min/max/counter). Default `--downsampled=skip`; options `prefer-raw` (default when raw covers the range), `include`, `only`, with an explicit `--downsample-aggregate=counter|sum|count|min|max|avg`.
- Thanos replica dedup: `--dedup-replica-label <name>` (repeatable) merging overlapping series with a deterministic, documented tie-break; external labels from `meta.json` are **added to every series by default** (`--external-labels=include|ignore`) because that is what a Thanos query would return.
- Prometheus **stale markers** (NaN payload 0x7ff0000000000002): `--stale-markers=drop|keep`, default per what Parqtel's PromQL engine actually does with NaN (verify in recon).
- Robustness: truncated/corrupt blocks and unreadable chunks are reported with block ULID + series ref + offset, counted, and either skipped (`--on-error=skip`) or fatal (`--on-error=fail`, default). Never panic on malformed input; fuzz the readers.
- Prometheus blocks carry no metric metadata. Infer `metric_kind` via an overridable rule set (defaults: `*_total`, `*_count`, `*_sum`, `*_bucket`, `*_created`... => counter; native histogram => histogram; everything else => gauge). Allow `--metadata-file` (JSON/YAML from `/api/v1/metadata`) to override. Document that classic histograms stay as flat `_bucket/_sum/_count` series with `le` as a label (identical to the source series).

# SINK SIDE — OUTPUT, COMPATIBLE WITH PARQTEL

Write Parqtel metric blocks exactly as Parqtel's own writer does (confirm each point in code):
- Schema: the 15-column metrics schema (`timestamp_ns` Timestamp(ns); `metric_name`, `service_name`, `service_version`, `k8s_*` dictionary columns; `metric_kind`; `resource_attributes` JSON dictionary; `labels` JSON Utf8; `value_float`/`value_int`/`value_complex`). Samples in ms from Prometheus become ns.
- Writer properties: Parquet 2.0, zstd (level configurable 1–22), row group size 100 000 (validate ≤ max rows), bloom filters on `metric_name` and `service_name` (fpp 0.01), page-level statistics, column-index truncate length 64, data page limit 512 KiB.
- **Row ordering:** group rows by `(metric_name, metric_kind, resource_attributes)` so row-group min/max for `metric_name`/`service_name` are exact — Parqtel's pruning depends on this. TSDB index order already groups by name; exploit it with a streaming k-way/series-ordered pass rather than a global sort.
- **Block windows:** align to Parqtel's `block_duration_secs` (default 7200 s) and `max_rows_per_block` (default 1 000 000); one writer per open window, bounded number of concurrently open writers; compacted multi-day TSDB blocks are split across windows.
- **Naming and atomicity:** `{start_ns}_{end_ns}_{uuid32hex}.parquet` written as `.tmp_<name>` then renamed. Derive the UUID **deterministically** (UUIDv5 over source block ULID + window + part index, formatted as 32 lowercase hex) so re-runs are idempotent and never produce duplicate samples.
- **Label mapping** per the round-trip table from Phase 0: provide `--label-mapping <file>` (and a sane default profile, e.g. `job`→`service.name`, `namespace`→`k8s.namespace.name`, `pod`→`k8s.pod.name`, `container`→`k8s.container.name`, `node`→`k8s.node.name`) but the hard requirement is: **a PromQL/ParQL query for the same selector over the same range against Parqtel returns the same label sets, sample timestamps and values as the source**. If lifting a label into a correlation column changes how it is returned on read, choose the mapping that preserves series identity, and ship a `--label-mode preserve|otel` switch with the trade-off documented.
- **Make blocks visible to Parqtel:**
  - Mode `files`: write into a staging dir, plus an optional **index sidecar fragment** (`index.json`-compatible `BlockMetadata` with `metric_names`, `label_names`, capped `label_values`) so the blocks are fully indexed instead of relying on startup reconciliation (which adopts them with an empty metric-name set until compaction rewrites them). Never overwrite a live `index.json`; merge atomically (tmp + rename) only with `--install` and refuse if a running server's lock/WAL is detected unless `--force`.
  - Mode `otlp` (live clone): replay the same series into a running Parqtel through its OTLP endpoints (HTTP protobuf / JSON / gRPC) using `opentelemetry-proto`/`tonic`. Keep request bodies **under 4 MB** (server limit 10 MB), order by time, honor backpressure/`413`/`429`/5xx with jittered retries, preserve counter monotonicity/temporality semantics, and make resumption idempotent via a local checkpoint file. Document any timestamp-acceptance limits found in recon.
- **Retention warning:** Parqtel retention (default 7 days, hourly task) deletes blocks entirely older than the cutoff. The tool must read the target config when available (or accept `--target-retention-days`) and loudly warn/refuse when imported data would be immediately deleted, and print the exact config key to raise.

# CLI

`tsdb2parqtel` subcommands (clap, with `--help` examples for every one):
- `inspect <source>`: block list, time ranges, series/sample/chunk counts, external labels, downsample resolution, estimated output size, label-cardinality top-N, warnings.
- `plan <source> ...`: dry run showing output windows, estimated rows/files, label mapping preview for sampled series, retention conflicts.
- `convert <source> --out <dir> ...`: produce Parqtel Parquet blocks (+ optional sidecar fragment).
- `clone <source> --target <data-dir | otlp-url> ...`: convert and install/ingest, with checkpoint/resume.
- `verify <source> <parqtel-blocks | parqtel-url>`: independent reconciliation. Compare per-series (labels, sample count, first/last ts, xxhash of (ts,value) stream) between source and output, plus totals; non-zero exit on any mismatch; machine-readable JSON report.
Global: `--threads`, `--memory-limit`, `--log-format text|json`, `--progress`, `--dry-run`, exit codes documented.

# ENGINEERING REQUIREMENTS

- Workspace layout: `tsdb-reader` (pure library: index/chunk/tombstone/Thanos meta), `parqtel-sink` (schema + writer + sidecar), `otlp-sink`, `cli`. Prefer depending on `parqtel-core` schema types via a git dependency pinned to a commit **if** it is cleanly consumable; otherwise vendor the schema and add a conformance test that fails when upstream drifts.
- Memory-bounded and streaming: never load a whole block into RAM; parallelize per block/per window with a bounded worker pool (rayon/tokio); mmap input chunk files (`memmap2`) with safe wrappers. `#![forbid(unsafe_code)]` outside one audited mmap module.
- Errors with `thiserror` (library) / `anyhow` (CLI); structured `tracing` logs; no secrets in logs; sources are strictly **read-only**.
- Numeric correctness: bit-exact `f64` round-trips (including NaN payload handling, ±Inf, subnormals); integer-valued counters policy for `value_int` vs `value_float` taken from Parqtel's own writer behavior, documented.
- Lints: `cargo fmt`, `clippy -D warnings`, `cargo deny`, `cargo audit` clean. MSRV stated. Linux/macOS builds; static musl release binary.

# TESTING (non-negotiable)

1. **Reader oracle tests:** generate fixture blocks with real Prometheus (`promtool tsdb create-blocks-from openmetrics`, or a short-lived Prometheus + `tsdb dump`) including: classic histograms, native histograms, counter resets, stale markers, tombstoned ranges, multi-segment chunks, high cardinality, empty blocks. Assert the Rust reader equals `promtool tsdb dump` output sample-for-sample. Add Thanos fixtures (raw, 5m/1h downsampled, with external labels and replica labels).
2. **Property/fuzz tests** (`proptest`, `cargo-fuzz`) for varint/XOR/histogram decoders and index parsing; corrupted-input suites must never panic.
3. **Schema conformance:** read each output file with `parquet`/`arrow` and assert exact schema, writer properties (bloom filters, statistics, row group size, codec), row ordering/grouping, filename pattern, atomic rename behavior.
4. **End-to-end against a real Parqtel:** docker-compose that starts parqtel-oss with a pinned image/commit, imports fixture blocks via both `files` and `otlp` modes, then runs the same PromQL range/instant queries against Prometheus (source) and Parqtel (target) and asserts identical label sets, timestamps and values (tolerance 0 for floats). Also test restart-time reconciliation adoption, idempotent re-run (no duplicate samples), and compaction of imported blocks followed by re-query.
5. **Performance:** `criterion` benchmarks plus a documented large-block benchmark (≥ 100 M samples). Report samples/s/core, peak RSS, output/input size ratio. Establish a baseline, then optimize only hot spots shown by profiling (`cargo flamegraph`/`samply`).

# DELIVERABLES

- Source repo with the layout above, CI config (build, test, lint, e2e), and `Dockerfile`.
- `docs/DESIGN.md` (Phase 0 output, kept current), `docs/USAGE.md` (runbooks: "migrate a Prometheus data dir", "migrate a Thanos bucket range", "live clone into a running Parqtel", "verify"), `docs/LIMITATIONS.md` (what is intentionally not handled, e.g. Prometheus head/WAL data not yet in blocks, exemplars, metadata, native-histogram edge cases, anything Parqtel cannot represent).
- A final report: what was verified against what (with commands and results), measured performance, open risks, and a prioritized backlog.

# WORKING RULES

- Work in small, reviewable commits; each phase ends green (build + tests) with a short status note.
- Prove, don't claim: every compatibility statement must be backed by a test or a cited line of parqtel-oss source.
- If a requirement cannot be met as written (for example Parqtel cannot represent something), state it, implement the closest faithful behavior behind a flag, and document the loss precisely.
- Out of scope unless trivial: logs/traces conversion, Prometheus remote-write, a TSDB writer, GUI.
