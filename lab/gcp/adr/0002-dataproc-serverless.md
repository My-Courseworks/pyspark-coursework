# ADR 0002 · Dataproc Serverless, not clusters, Composer or Databricks

**Status:** accepted · 2026-10-28

## Context
Google Cloud offers several ways to run PySpark: Dataproc clusters you manage, Dataproc Serverless (now branded "Google Cloud Serverless for Apache Spark"), Spark inside BigQuery Studio, Spark on GKE, Databricks on GCP. The lab's jobs are scheduled batches with no interactive users.

## Decision
Dataproc Serverless **batches**: submit a PySpark script plus a zip of the package, Google provisions and autoscales executors, and the bill stops when the batch ends.
- Runtime `2.2` (Spark 3.5, Python 3.12, BigQuery and GCS connectors preinstalled), matching the local pin (`local/adr/0003`).
- Executors: 4 cores each; `spark.dynamicAllocation.maxExecutors` = 4 (smoke) / 8 (local) / 32 (cloud); `spark.sql.shuffle.partitions` = 64 / 256 / 1024 with AQE coalescing.
- Each batch is identified by `shopco-<scale>-<stage>-<utc stamp>` and labelled `scale`, `stage`, `trigger`, so cost and logs can be filtered by label.

## Consequences
- No cluster to leave running by accident; cost is proportional to work.
- Cold start is ~1 minute per batch; fine for nightly jobs, wrong for interactive use.
- The Spark UI is available per batch in the console ("View Spark UI") while running and for a while after; there is no persistent History Server (ADR 0008).
- The subnet needs Private Google Access (ADR 0006).

## Alternatives rejected
- Dataproc cluster: always-on cost, cluster sizing to decide, nothing extra learned for a batch pipeline.
- Cloud Composer (managed Airflow) for orchestration: ~$300+/month minimum; wildly over budget for one DAG. Cloud Workflows + Scheduler do the job (ADR 0007).
- Databricks on GCP: separate contract and UI; not what "PySpark on GCP" usually means in an interview.
- BigQuery Studio Spark procedures: good for SQL-adjacent work, but hides the batch mechanics the lab is about.
