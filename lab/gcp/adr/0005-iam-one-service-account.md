# ADR 0005 · IAM: one dedicated service account with the minimum roles

**Status:** accepted · 2026-10-28

## Context
Dataproc Serverless batches run as a service account; so do Workflows executions and the Scheduler's OAuth call. The default compute service account is Editor on the project, which is more than a batch job should have.

## Decision
`shopco-batch@shopco-pyspark-lab-2026.iam.gserviceaccount.com` (`gcp/infra/30_iam.sh`) with:

| role | scope | why |
|---|---|---|
| `roles/dataproc.worker` | project | required for the batch's own runtime |
| `roles/dataproc.editor` | project | the workflow submits batches |
| `roles/storage.objectAdmin` | the bucket only | read data, write features/ledger |
| `roles/bigquery.dataEditor`, `roles/bigquery.jobUser` | project | direct writes to the dataset |
| `roles/logging.logWriter`, `roles/monitoring.metricWriter` | project | driver logs and metrics |
| `roles/workflows.invoker` | project | Scheduler triggers the workflow as this account |
| `roles/iam.serviceAccountUser` | on itself | the workflow (running as SA) submits batches that run as SA |

The human operator uses their own identity (`gcloud auth`) for the infra scripts; no keys are created or downloaded.

## Consequences
- A leaked batch job can touch one bucket and one dataset, not the project.
- Adding a new sink (e.g. Pub/Sub) means adding one role here, deliberately.
