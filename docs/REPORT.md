# Verification report

## Source and target

- Source audited: `parqtel-oss@63a9cf1cdc9e8cc4855565b6d0b67fa6c366cef4`
- Parquet/Arrow: `60.1.0`, pinned through the Parqtel workspace
- Converter: this repository, `tsdb2parqtel`
- Fixtures: real Prometheus/Thanos blocks from the MIT-licensed `tsdb` and
  `tsdb-reader` crates (v1, v2 and native-histogram blocks)

## What was run

| Check | Command | Result |
|---|---|---|
| v1 index/chunk/tombstone parse | `cargo test --manifest-path crates/tsdb-reader/Cargo.toml --test fixture_v1` | pass |
| v2 real block, 35,354 series / 5.55M samples | `--test fixture_v2` | pass |
| **promtool oracle** | `promtool tsdb dump` vs native `dump_tsv`, then `scripts/check_promtool_oracle.py` | **5,551,013/5,551,013 samples match sample-for-sample** |
| native histogram decode dispatch | `--test native_hist` | pass; explicit unrepresentable-series error |
| malformed input/property tests | `--test properties` | pass; no panics |
| Parquet schema/footer/naming | `cargo test -p parqtel-sink --test write_block` | pass |
| full workspace | `cargo test --workspace` | pass |
| clippy | `cargo clippy --workspace --all-targets -- -D warnings` | pass |
| fmt | `cargo fmt --all -- --check` | pass |
| audit | `cargo audit` | no vulnerabilities; 3 unmaintained warnings (chrono, humantime, unsafe-libyaml) |
| deny | `cargo deny check` | advisories, bans, licenses and sources pass (unmaintained advisories documented in `deny.toml`) |
| files conversion | `tsdb2parqtel convert tests/fixtures/testblock --out /tmp/... --max-rows-per-block 1000000` | pass |
| block reconciliation | `tsdb2parqtel verify ... /tmp/...` | 865/865 samples, 23/23 series, 0 mismatches |
| install + Parqtel restart | `clone --target /tmp/...`; query `/api/v1/query_range` | labels, timestamps and values matched |
| OTLP clone | `clone --target http://127.0.0.1:18091` | 66 samples, 0 retries; queried back identically |
| live verify | `verify ... http://127.0.0.1:18092` | 66/66 samples, 0 mismatches |

The files-mode block was opened by a real Parqtel debug server with
`PARQTEL__STORAGE__DATA_DIR` pointed at the output directory; `/ready` returned
ready and `/api/v1/query_range?query=up` returned the expected label sets and
values. A second conversion produced the same deterministic filename and the
verifier still reported no duplicates.

## Performance

Measured on the supplied v2 fixture: 5,551,013 samples, 35,354 series, release
build (`lto=fat`, one codegen unit), local NVMe, 8-core host.

| Metric | Result |
|---|---|
| Samples | 5,551,013 |
| Wall time | 7.31 s |
| Throughput | ≈759,000 samples/s |
| Peak RSS | 273,844 KiB (≈267 MiB) |
| Output | 5.4 MiB, 6 Parquet files |
| Output/input size | 5.4 MiB / 9.3 MiB fixture (≈0.58× before query indexes; fixture includes source index/chunks) |
| Docker image | `tsdb2parqtel:local`, 160 MB, non-root, CA bundle, `--help` verified |
| `cargo bench` index parse | 94.7–98.8 ms per 4.4 MB index (96.6 ms median, 10 samples) |

The debug build of the same run was 68.36 s and 1.86 GiB RSS; do not benchmark
debug builds.

A second run used `scripts/generate_synthetic_tsdb.py` to build a 50 MB TSDB
block with 100,000,000 samples across 1,000 series and 834,000 chunks. Values
are constant per series, so the output is smaller than real telemetry; the
purpose is throughput and memory, not compression ratio.

| Metric | Result |
|---|---|
| Samples | 100,000,000 |
| Wall time | 47.04 s |
| Throughput | ≈2,126,000 samples/s |
| Peak RSS | 3,494,392 KiB (≈3.33 GiB) |
| Output | 17 MiB, 209 Parquet files |
| Reconcile | 100,000,000/100,000,000 samples, 1000/1000 series, 0 mismatches (21.4 s) |

Peak RSS grows with total rows because `convert` retains every window in
`BTreeMap<WindowKey, WindowState>` until the full source scan finishes. This is
a known scalability limit: budget roughly 36 bytes per sample, or split the
source by block/time range for very large migrations.

## Open risks / backlog

1. **Native histograms are decoded but not converted.** Parqtel has explicit
   histogram boundaries only; schema/zero-threshold/negative-span semantics are
   lost. Current behavior is a clear error unless `--on-error=skip`.
2. **Head/WAL and exemplars are out of scope.**
3. **Parquet writer compatibility is pinned to `63a9cf1`.** A newer Parqtel may
   change the block schema or writer properties; re-run the conformance test.
4. **One writer per data root.** The tool's WAL guard is conservative, not a
   process lock; a live server must be stopped and restarted around install.
5. **Remote source staging consumes local disk** equal to the selected block
   bytes (index + chunks + tombstones).
6. **Live verification is one HTTP request per sample** and is intended for
   targeted checks, not billion-sample audits.
7. **No `cargo-deny` binary in the local image**; CI runs the official action
   with `deny.toml`. `cargo audit` was run locally and found no vulnerabilities.
