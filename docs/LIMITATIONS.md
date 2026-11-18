# LIMITATIONS — `tsdb2parqtel`

Explicit, honest list of what the tool does **not** handle, and where the
faithful fallback lives.

## Source side

1. **Prometheus head / WAL data** — only *blocks* are converted. The in-memory
   head and the write-ahead log ahead of the last block boundary are out of
   scope; a clone that must be exact at "now" should pre-flush via the target
   ingest path or accept the head gap.
2. **Exemplars** — not represented in Parqtel metric blocks; dropped with a
   warning.
3. **Metadata / unit / description** — carried only insofar as Parqtel stores
   them (mostly not): `Metric.description`/`unit` are written into the metric
   group but not into per-series columns; conversion should emit a
   `--metadata-file` from `/api/v1/metadata` where available.
4. **Classic vs native histograms** — classic histograms (`le`, `_bucket`,
   `_sum`, `_count`) stay as flat series with `le` as a label (identical to
   source); native histograms are serialized into `value_complex`
   (`{"Histogram":{...}}`). Mixed inputs need explicit `--label-mode`.
5. **Counter reset handling** — retained: per-series counter increase is computed
   at query time (Parqtel `eval/range_fn.rs` segment logic); the converter
   preserves raw cumulative values and resets.

## Sink side

1. **`service.name` overwrite** — the converter must merge resource
   `service.name` into labels; Parqtel's read path overwrites a `service.name`
   already present in the labels JSON, so the converter must not pre-set it in a
   way that changes identity.
2. **Series cap** — Parqtel query `max_series = 1000` (`query.max_series`) and
   ingest `series_limit_per_metric = 1_000_000`; a source block with more series
   than the query cap will be truncated on read. Migration should warn or split.
3. **Instant lookback** — Parqtel instant queries look back 5 min
   (`query.lookback_delta_ns = 300_000_000_000`) unless raised; clone/verify must
   use range queries for historical data.

## Known losses

- **Row-group granularity** — Parqtel writer emits one row group per
  `row_group_size` (100 k for metrics); the converter must aggregate rows per
  window accordingly, else reader pruning differs.
- **Dictionary policy** — dictionary encoding is per-column; the log `body`
  column disables it, metric columns use it. The converter must mirror.
- **Blooms** — Parqtel writes bloom filters on `metric_name` + `service_name`
  with fpp 0.01; the converter should emit the same via writer properties.

---

## Intentionally out of scope

- Logs and traces conversion.
- Prometheus remote-write ingestion of the converted output (the tool writes
  blocks; use Parqtel's `POST /api/v1/write` for live replay).
- GUI.
- Parqtel-side changes (the tool assumes the Parqtel version audited in Phase 0;
  upgrade tracking is a backlog item).
