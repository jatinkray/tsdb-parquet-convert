# Schema reference — `tsdb2parqtel`

`convert --schema <file>` writes ordinary Parquet for a schema you supply,
instead of Parqtel metric blocks. This document is the reference for that file
and for the conversion rules behind it; the Parqtel block path is described in
`docs/DESIGN.md` and is not affected by anything here.

Implementation: `crates/parquet-sink/src/spec.rs` (format and validation),
`crates/parquet-sink/src/value.rs` (conversion), `crates/parquet-sink/src/writer.rs`
(files), `crates/cli/src/convert.rs` (pipeline), `crates/cli/src/main.rs` (flags
and their rejections).

## Workflow

Start from a generated template: one nullable utf8 column per label.

```bash
tsdb2parqtel schema /var/lib/prometheus/blocks --out schema.yaml
```

Edit types, sources and nullability, then convert. Validation runs as part of
`convert`; there is no separate validate step.

```bash
tsdb2parqtel convert /var/lib/prometheus/blocks \
  --out ./parquet \
  --schema schema.yaml \
  --min-time 2026-01-01T00:00:00Z --max-time 2026-02-01T00:00:00Z
```

The result is ordinary Parquet; open it with any Parquet reader.

`schema` flags: `--out <file>` (stdout when omitted), `--name <name>` (default
`tsdb_generic`), `--no-labels`, `--max-labels N` (0 = all, highest series count
first). Labels are read from the block index, so generating a template does not
decode chunks.

`--schema` is a `convert` flag. `plan` dry-runs the Parqtel path, and `clone` /
`verify` are Parqtel-only. These combinations are rejected with an explicit
error rather than ignored: `--sidecar`, `--label-mode otel`, `--label-mapping`,
`--metadata-file`, `--target-retention-days`.

## Example

The repository ships a validated example, `examples/schema-labelled.yaml` (a
test in `crates/parquet-sink/tests/roundtrip.rs` keeps it loadable). A smaller
one:

```yaml
version: 1
name: tsdb_labelled
description: labelled samples with the series identity kept as columns
columns:
  - name: timestamp
    type: timestamp_ms
    source: timestamp
    timezone: UTC
  - name: metric
    type: utf8
    source: metric_name
  - name: job
    type: utf8
    source: label:job
    nullable: true
  - name: region
    type: utf8
    source: "const:eu-north-1"
  - name: value
    type: float64
    source: value
  - name: series
    type: int64
    source: series_index
  - name: cost
    type: decimal128
    source: value
    precision: 12
    scale: 2
```

YAML and JSON are both accepted; JSON is parsed by the same YAML loader.

## Column fields

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | Column name in the Parquet file; must be unique and non-empty |
| `type` | yes | One of the types below |
| `source` | yes | Where the value comes from: `timestamp`, `metric_name`, `value`, `series_fingerprint`, `series_index`, `block_ulid`, `label:<key>`, `const:<value>` |
| `nullable` | no (default `false`) | Missing source value becomes NULL instead of an error |
| `precision` | `decimal128` only | 1–38, default 38 |
| `scale` | `decimal128` only | 0–`precision`, default 0 |
| `timezone` | timestamp columns only | Written into the Arrow type, e.g. `UTC` |
| `description` | no | Copied into the Arrow field metadata |

## Types

| `type` | Arrow type | Alias |
|---|---|---|
| `boolean` | `Boolean` | `bool` |
| `int8`, `int16`, `int32`, `int64` | signed integers | — |
| `uint8`, `uint16`, `uint32`, `uint64` | unsigned integers | — |
| `float32`, `float64` | IEEE floats | `float`, `double` |
| `utf8` | `Utf8` | `string` |
| `binary` | `Binary` | — |
| `date32` | `Date32` (days since 1970-01-01) | — |
| `timestamp_ms`, `timestamp_us`, `timestamp_ns` | `Timestamp(ms/us/ns, timezone)` | `timestamp` (means `timestamp_ns`) |
| `decimal128` | `Decimal128(precision, scale)` | — |

No nested, list, map or struct types: one column is one value per sample row.

## Sources

| Source | Value |
|---|---|
| `timestamp` | The sample timestamp; required on every `timestamp_*` and `date32` column |
| `metric_name` | `__name__` of the series |
| `value` | The sample value (`f64` in Prometheus TSDB) |
| `series_fingerprint` | xxHash3 of the raw label set, stable across runs |
| `series_index` | Ordinal of the series inside the output file |
| `block_ulid` | ULID of the source block |
| `label:<key>` | One label value, e.g. `label:k8s.pod.name` |
| `const:<value>` | The same literal for every row |

## Casting rules

Rules live in `crates/parquet-sink/src/value.rs`; label values, `metric_name`
and `const:` literals are text, sample values are `f64`.

- Integers: an `f64` is rounded half away from zero, then range-checked; a value
  that does not fit the declared integer type is an error. Unsigned columns
  reject negative values.
- Floats: `+Inf`, `-Inf` and `NaN` pass through unchanged; a text value that
  does not parse as a number is an error naming the column and series.
- Booleans: `true`/`t`/`1`/`yes` and `false`/`f`/`0`/`no`; numeric sources are
  true when non-zero.
- `utf8` from a number uses the shortest decimal form and is an error for
  non-finite values; `binary` from a number stores its little-endian bytes
  (`f64` → 8 bytes, `i64` → 8 bytes).
- `date32`: integer days since 1970-01-01, computed from the sample timestamp.
- Timestamps: the sample timestamp converted with floor division to the column
  unit. Text sources accept RFC 3339 or Unix seconds; numeric sources are Unix
  seconds.
- `decimal128`: `value × 10^scale`, rounded; the magnitude must fit `precision`.
  Values beyond 53 bits of integer precision are not exact.
- A missing label is NULL for a `nullable` column and an error otherwise; the
  error names the column and the series fingerprint.

## Validation

Loading fails, naming the column, when: `version` is not `1`; there are no
columns; a name is empty or duplicated; a type or source is unknown; no
`timestamp_*` column uses `source: timestamp`; a `timestamp_*` or `date32`
column uses another source; `source: timestamp` appears on any other type;
`precision` or `scale` appears outside `decimal128`; a decimal precision is
outside 1–38 or its scale outside 0–precision; `timezone` appears outside
timestamp columns.

## Output layout

Written by `crates/parquet-sink/src/writer.rs`.

| Aspect | Behaviour |
|---|---|
| File name | `data_{start_ns}_{end_ns}_{part:05}.parquet`; a rerun overwrites the same names |
| Write | `.tmp_<name>`, fsync, rename, directory fsync |
| Splitting | A window is split into files of at most `--max-rows-per-block` rows (default 1 000 000) |
| Row groups | `--row-group-size` (default 100 000) |
| Encoding | Parquet dictionary encoding with fallback, page statistics enabled |
| Compression | `--compression zstd\|snappy\|lz4\|none`, `--compression-level 1..=22` for zstd |

Key-value metadata: `tsdb2parqtel.schema` (full schema JSON),
`tsdb2parqtel.schema_name`, `tsdb2parqtel.schema_description`,
`tsdb2parqtel.schema_version`, `tsdb2parqtel.writer`. Each Arrow field carries
`tsdb2parqtel.source` plus the optional `description`.

Empty files are refused, and a window with no rows produces no file.

## Failure messages

| Message | Meaning | Fix |
|---|---|---|
| `unknown type '<x>'; known types: …` | `type` typo | Use one of the listed names |
| `duplicate column name` | Two columns share a name | Rename one |
| `at least one timestamp_ms, timestamp_us or timestamp_ns column` | Schema has no time column | Add one with `source: timestamp` |
| `must use source: timestamp` | Timestamp column fed from a label | Use `source: timestamp` |
| `source value is missing and the column is not nullable` | A label used by a non-nullable column is absent | Set `nullable: true` or filter the series |
| `is not a number, so it cannot fill this numeric column` | A label such as `le="half"` mapped to a numeric type | Keep it `utf8` or clean the label |
| `does not fit the column's integer type` | Value overflows the declared integer width | Widen the column |
| `cannot be combined with --schema` | Parqtel-only flag supplied | Remove the flag |

## Limits

1. `verify` does not cover schema output: there is no series index to reconcile
   against. Read the files with a Parquet reader instead.
2. Raw labels only; `--label-mode otel` and `--label-mapping` are rejected in
   `crates/cli/src/main.rs`.
3. Flat columns only: no nested, list, map or struct types.
4. One timestamp column minimum; `crates/parquet-sink/src/spec.rs` rejects a
   timestamp column fed by anything but `source: timestamp`, so the schema
   cannot express a unit conversion from a label or constant.
5. This YAML/JSON layout is the only accepted one; Arrow IPC or Arrow JSON
   schema files are not imported.
6. Classic histograms stay flat (`_bucket`/`_sum`/`_count` with `le`); there is no
   complex-value column.
7. `decimal128` is computed through `f64`, so wide counters lose exactness.
8. The CLI exposes `zstd`, `snappy`, `lz4` and `none`; the writer also
   understands `gzip`, which is not reachable from the command line.

Usage — [docs/USAGE.md](USAGE.md) · Design — [docs/DESIGN.md](DESIGN.md) ·
Limitations — [docs/LIMITATIONS.md](LIMITATIONS.md) · Report — [docs/REPORT.md](REPORT.md)
