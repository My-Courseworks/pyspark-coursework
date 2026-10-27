# ADR 0008 · Observability: structured stdout → Cloud Logging → log-based metrics → alert + dashboard

**Status:** accepted · 2026-10-28

## Context
On the laptop, monitoring is the per-step ledger plus the Spark UI/History Server (`local/adr/0005`). In the cloud, the driver's stdout is captured by Cloud Logging under `resource.type="cloud_dataproc_batch"`, and the Spark UI is available per batch in the console.

## Decision
Three layers, all driven by the same monitor code:
1. **Per run:** `run.json`, `ledger/*.json`, `report.md` in `gs://…/runs/<batch id>/`, plus one row per step appended to BigQuery `shopco.run_ledger` (ADR 0004). `gcp/submit.sh` prints the batch's approximate DCU usage and the report at the end.
2. **Metrics:** two log-based metrics built from the monitor's stdout lines (`gcp/monitoring/*.yaml`):
   - `shopco_step_seconds` (distribution, labels `step`, `status`) from `SHOPCO_STEP {…}` lines;
   - `shopco_run_failures` (counter) from `SHOPCO_RUN {…"status": "failed"…}` lines.
3. **Alert + dashboard:** an alert policy that emails the account when `shopco_run_failures > 0`, and a dashboard "ShopCo PySpark pipeline" with step durations by step, failed runs, and a log panel of run lines.

No Persistent History Server (PHS): it is a VM that costs money around the clock, and the per-batch Spark UI plus the ledger cover the lab's needs. The ADR to revisit if several people need to debug old batches.

## Consequences
- Adding a metric is a yaml file and re-running `50_monitoring.sh`; the pipeline code does not change.
- The regexes in the metric definitions depend on the JSON key order the monitor prints (`"step": …`, `"status": …`, `"elapsed_s": …`); the keys are written in a fixed order for that reason.
- Driver stdout lands in Logging as `jsonPayload.message` under log name `dataproc.googleapis.com/output` (checked with `gcloud logging read` after the first smoke batch on 2026-10-28; the first draft of the filters assumed `textPayload` and matched nothing). All filters and extractors use `jsonPayload.message`.
