# ADR 0004 · Storage: Parquet on GCS for the pipeline, BigQuery for what people query

**Status:** accepted · 2026-10-28

## Context
The course's M1 says: keep data in open-format files in cloud storage for Spark, and put query-shaped outputs where analysts and models read them.

## Decision
One bucket `gs://shopco-pyspark-lab-2026-data` (uniform access, public access prevented, same region):

```
code/              main.py + shopco.zip, re-uploaded on every submit
data/<scale>/      events/dt=…/  customers/  products/  customer_features/  training_set/  tmp/
runs/<batch id>/   run.json  ledger/<step>.json  report.md
```
- `data/tmp/` is deleted after 7 days by a lifecycle rule; everything else is kept.
- BigQuery dataset `shopco` (same region) receives `training_set` (overwritten per run, carries `cutoff`) and `run_ledger` (append; one row per step per run) through the BigQuery connector's **direct** write method (Storage Write API, no staging bucket).
- Plain Parquet, no Delta/Iceberg: the lab has one writer and no concurrent readers; a table format would add a lesson the course already covers conceptually.

## Consequences
- `SELECT * FROM shopco.run_ledger ORDER BY started_at` is the cross-run monitoring view; `report.md` is the per-run one.
- Scales never collide (separate prefixes); the smoke run does not touch the 2B-row month.
- Reading `training_set` from elsewhere (a Vertex AI notebook) is a plain Parquet or BigQuery read.
