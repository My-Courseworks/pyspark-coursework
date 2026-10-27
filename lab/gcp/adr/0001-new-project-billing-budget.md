# ADR 0001 · A fresh project, linked billing, a ₹2,000 (≈ $24) budget alert

**Status:** accepted · 2026-10-28

## Context
The request authorised creating a new project under the owner's Google account and enabling whatever APIs the work needs. The account has one open billing account and six unrelated existing projects.

## Decision
- New project `shopco-pyspark-lab-2026` (number 829103128025), name "ShopCo PySpark Lab", linked to the account's open billing account. Nothing from the other projects is reused, so this one can be deleted wholesale.
- APIs enabled (`gcp/infra/00_project.sh`): Compute, Dataproc, Cloud Storage, BigQuery + BigQuery Storage, Logging, Monitoring, Resource Manager, IAM, Service Usage, Workflows + Workflow Executions, Cloud Scheduler, Billing Budgets.
- A budget of **₹2,000/month (≈ $24)** scoped to this project with alerts at 50/90/100% (`gcp/infra/40_budget.sh`). Budgets notify billing admins by email; they do **not** stop spend.
  - The billing account is denominated in **INR**; a USD budget is rejected with a bare `INVALID_ARGUMENT` by both `gcloud` and the REST API. The script now reads the account currency and creates the budget through the REST API (which at least returns the created object). A first attempt accidentally created a ₹20 budget; it was deleted and replaced.

## Cost model (list prices, us-central1, approximate)
- Dataproc Serverless standard: ~$0.06 per DCU-hour; an executor with 4 cores ≈ 4 DCU. The `cloud` run caps at 32 executors (128 DCU ≈ $7.7/hour); generating and processing one 2B-row month is expected to take well under an hour, so **a few dollars per full run**. Smoke runs cost cents.
- GCS: ~100 GB of Parquet ≈ $2/month; BigQuery: the training set is ~20M rows ≈ 1 GB ≈ pennies.
- Workflows, Scheduler, Logging, Monitoring: within free tiers at this volume.

## Consequences
- `gcp/env.sh` exports `CLOUDSDK_CORE_PROJECT=$PROJECT_ID`, so every `gcloud` call in these scripts targets this project and bills its API quota here. (First run of the budget script failed because gcloud's default project was an unrelated one without the Budgets API.)
- Tear-down is `gcloud projects delete shopco-pyspark-lab-2026`.
- If the budget email arrives, stop submitting `cloud`-scale batches; the scheduler is paused by default (ADR 0007).
