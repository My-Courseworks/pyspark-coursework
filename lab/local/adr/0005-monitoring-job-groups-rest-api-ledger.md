# ADR 0005 · Monitoring: job groups + Spark REST API → per-step ledger → Markdown report

**Status:** accepted · 2026-10-28

## Context
The request: "the whole monitoring of processing and jobs completed and batches finished in X time and all being properly monitored and noted." PySpark has no Python-side `SparkListener` (listeners are JVM objects), so the usual Scala approach is out. What PySpark *does* have: job groups, a `StatusTracker`, the Spark UI's REST API, and event logs.

## Decision
`src/shopco/monitor.py`:
1. Each pipeline step runs inside `with monitor.step("name")`, which calls `sc.setJobGroup(step_id, …)`. Every Spark job the step triggers is tagged with that group.
2. When the step ends, the monitor queries the Spark UI REST API (`sc.uiWebUrl + /api/v1/applications/<app>/jobs`, then `/stages/<id>?withSummaries=true&quantiles=0.0,0.5,1.0`) for the jobs in the group and their stages: tasks, input/output bytes, shuffle read/write, memory/disk spill, and the task run-time distribution. `max/median` task time is recorded as the **skew ratio** (course M3's long-tail diagnosis).
3. The record (plus anything the step `note()`d, like row counts) is written as `ledger/<step_id>.json` under the run folder via Hadoop FS, and printed to stdout as one line `SHOPCO_STEP {json}`. The run itself writes `run.json` at start and end and `report.md` at the end.
4. If the REST API cannot be reached, the monitor falls back to `StatusTracker` (job/stage/task counts only) and records a `metrics_warning`, so a missing number is never silent.
5. Local mode also enables `spark.eventLog` into `runs/spark-events/` so any run can be replayed in the History Server (`local/scripts/history_server.sh`).
6. A failed step records `status=failed` with the exception, finalises the run as failed, writes the report, and re-raises.

`src/shopco/report.py` renders the ledger: per-step table (lazy steps marked), totals, slowest stages, metric warnings, and a legend tied to the course modules.

## Consequences
- "Jobs completed and in what time" is answered per step, per run, in a file next to the data, and in stdout (which Cloud Logging turns into metrics on GCP: `gcp/adr/0008`).
- Lazy steps show 0 jobs: that is the point, not a bug; the report says so.
- The REST call adds ~50–200 ms per step; negligible.
- Metrics are per *stage*, so a step whose stages are shared with a later step (Spark reuses stages across jobs) can attribute some work to the step that first triggered it. Acceptable for a per-step picture; the History Server has the exact DAG.

## Alternatives rejected
- A JVM `SparkListener` via py4j callback server: fragile, and unsupported on Dataproc Serverless.
- Parsing event logs after the run: works, but only after the run; no live per-step line.
- Only wall-clock timers: says *how long*, not *why*.
