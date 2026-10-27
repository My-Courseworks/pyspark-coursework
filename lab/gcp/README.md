# ShopCo on Google Cloud: runbook

Project `shopco-pyspark-lab-2026`, region `us-central1`. Everything below is idempotent; re-running a script is safe.

## 1. One-time infrastructure

```bash
gcp/infra/00_project.sh        # project, billing link, APIs
gcp/infra/10_network.sh        # Private Google Access on the default subnet (Dataproc Serverless needs it)
gcp/infra/20_storage.sh        # gs://shopco-pyspark-lab-2026-data + BigQuery dataset `shopco`
gcp/infra/30_iam.sh            # service account shopco-batch@… with least-privilege roles
gcp/infra/40_budget.sh         # $20/month budget alert on this project
gcp/infra/50_monitoring.sh     # log-based metrics, failed-run email alert, dashboard
gcp/infra/60_orchestration.sh  # Cloud Workflow + nightly Cloud Scheduler job (created PAUSED)
```

## 2. Run a batch by hand

```bash
gcp/submit.sh all smoke        # 2M events: proves the whole path in ~3 min
gcp/submit.sh all cloud        # generate + process one ShopCo month (2B events)
gcp/submit.sh pipeline cloud   # re-run only the pipeline on the generated month
```

`submit.sh` zips `src/shopco`, uploads it, submits a Dataproc Serverless batch named
`shopco-<scale>-<stage>-<utc stamp>`, streams the driver output, then prints the batch's
state and approximate DCU usage and the run report (also saved under `gcp/.state/`).

## 3. Run through the workflow (what the scheduler does)

```bash
gcloud workflows run shopco-pipeline --location=us-central1 --project=shopco-pyspark-lab-2026 \
  --data='{"scale":"smoke","stage":"all"}'
gcloud scheduler jobs resume shopco-nightly --location=us-central1 --project=shopco-pyspark-lab-2026   # turns the nightly run ON
```

## 4. Where to look

| what | where |
|---|---|
| per-run report | `gs://shopco-pyspark-lab-2026-data/runs/<batch id>/report.md` |
| per-step ledger, all runs | BigQuery `shopco.run_ledger` |
| training set | BigQuery `shopco.training_set`, Parquet under `data/<scale>/training_set/` |
| Spark UI for a batch | Console → Dataproc → Batches → the batch → "View Spark UI" |
| driver logs | Logging: `resource.type="cloud_dataproc_batch" AND jsonPayload.message:"SHOPCO_"` |
| dashboard | Monitoring → Dashboards → "ShopCo PySpark pipeline" |
| alert | email to the account when a run prints `status: failed` |
| cost | Billing → Reports, filter project; budget "shopco-pyspark-lab cap" |

## 5. What has been run (2026-10-27)

| batch | trigger | result |
|---|---|---|
| `shopco-smoke-all-…191745` | `submit.sh` | SUCCEEDED, 72 s, 34,163 training rows → BigQuery |
| `shopco-smoke-pipeline-…192304` | Cloud Workflow execution | SUCCEEDED (proves the scheduler path) |
| `shopco-cloud-all-…192144` | `submit.sh` | SUCCEEDED, **17.8 min**, 2.01B events, 4.93M training rows, 7.95 DCU-hours |

Reports: [examples/](../examples/). The nightly scheduler job exists and is **paused**.

## 6. Tear-down

```bash
gcloud projects delete shopco-pyspark-lab-2026
```
