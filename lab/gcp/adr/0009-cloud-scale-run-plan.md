# ADR 0009 · The 2B-row run: generated in the cloud, bounded autoscaling, staged rollout

**Status:** accepted · 2026-10-28

## Context
The `cloud` rung is 30 days × 67M events = 2.01B rows, ~20M customers. Uploading that from a laptop is pointless; generating it where it will be processed is the whole point of Spark.

## Decision
- Rollout order, each proving the next: `gcp/submit.sh all smoke` (2M rows, minutes, cents) → `gcp/submit.sh all cloud` (generate + pipeline in one batch).
- `cloud` batch: `maxExecutors=32` × 4 cores, `shuffle.partitions=1024`, AQE on. Expected: generation of 30 days at 256 files/day (~7,700 Parquet files, ~60–100 GB), pipeline shuffles of a few tens of GB. Expected wall time: tens of minutes; expected cost: low single-digit dollars (ADR 0001).
- If a `cloud` batch fails after generation, re-run with `pipeline` only; the generated month is reusable.
- Generation writes each day into its own directory, so a partial failure leaves complete days behind and the next run overwrites only what it regenerates.

## Measured (batch `shopco-cloud-all-20261027t192144`, 2026-10-27)
- State `SUCCEEDED`; wall time **17.8 min** from first step to last; 44 steps, 63 jobs, 12,450 tasks.
- Generation: 30 days × 67M rows at 256 files/day, ~15–60 s per day (the first days are slower while autoscaling adds executors); 25.6 GiB of Parquet.
- Pipeline: 38.5 GB read, 3.3 GB shuffled, 0 bytes spilled; `write_features` 65 s for 4.93M customers; BigQuery sink 37 s.
- Usage: 28,613,800 milli-DCU-seconds = **7.95 DCU-hours ≈ $0.48** at the standard rate, plus 805 GB-hours of shuffle storage (cents). Autoscaling averaged ~6 executors; the 32 cap was never the limit.
- Skew ratios ≥ 5× appeared in 13 generation days (uneven executor start-up), 25× in `purchases` (cache materialisation), 5.5× in `write_features`. All are recorded in `examples/gcp-cloud-2B-events.report.md`.

## Consequences
- Both the generate and pipeline steps appear in one report with per-day timings, which is the "batches finished in X time" record the request asked for.
- The run is reproducible (fixed seed) so two runs of the month can be compared step by step via `shopco.run_ledger`.
