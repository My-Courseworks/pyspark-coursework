# ADR 0006 · Local SparkSession settings

**Status:** accepted · 2026-10-28

## Context
Laptop: 12 cores, 24 GB RAM, 64 GB free disk. In `local[*]` mode the driver *is* the executor, so one memory number is the whole Spark heap. Default shuffle partitions (200) are more than a 12-core machine needs, and shuffle spill files default to `/tmp`.

## Decision
`src/shopco/session.py`, local mode:
- `spark.driver.memory=12g` (half the machine; leaves room for the OS, Python and the Arrow buffers). Override with `--driver-memory`.
- `spark.driver.maxResultSize=2g`: fail fast on an accidental `collect()`.
- `spark.sql.shuffle.partitions = 4 × cores` (48 here), with AQE on so Spark coalesces them further at runtime.
- `spark.local.dir=data/<scale>/tmp`: spill and shuffle files on the big disk, inside the gitignored data folder.
- `spark.eventLog.enabled=true` → `runs/spark-events/`.
- Everywhere: AQE on, Arrow on for `toPandas`, UTC session time zone, no console progress bar (the monitor prints its own lines).

## Consequences
- `local` scale (60M events) runs without meaningful disk spill; `cloud` scale would not fit and is blocked from local by common sense, not code.
- Two Spark apps at once on this laptop will fight for memory; run one at a time.

## Alternatives rejected
- Leaving defaults: 1 GB driver memory makes the `local` rung OOM in the window step.
