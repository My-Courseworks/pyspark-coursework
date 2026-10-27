# ADR 0007 · Orchestration: Cloud Workflows + Cloud Scheduler, created paused

**Status:** accepted · 2026-10-28

## Context
"Production grade" means the pipeline runs on a schedule without a human typing `gcloud`. The obvious tool, Cloud Composer, is far over the lab's budget (ADR 0001).

## Decision
- `gcp/orchestration/workflow.yaml`: a Cloud Workflow that takes `{scale, stage}`, derives a batch id, sizes executors for the scale, submits the batch and **polls** it every 30 s until it ends. A failed or cancelled batch raises, so the execution fails visibly in the Workflows console and in Logging.
- The submit and poll are plain `http.post`/`http.get` calls to the Dataproc REST API with the workflow's OAuth2 token. The Workflows Dataproc connector (`googleapis.dataproc.v1`) was tried first and rejected at deploy time: it has no `projects.locations.batches` resource (found out on 2026-10-28; the serverless API is not in the connector's catalogue).
- Cloud Scheduler job `shopco-nightly` at 03:00 UTC calls the workflow with `{scale: cloud, stage: pipeline}` (re-run the pipeline on the existing month; generation is a one-off).
- The scheduler job is created and immediately **paused**. A nightly cloud-scale run would be ~$2–5/night, which exceeds the $20 budget inside a week. Resume it deliberately: `gcloud scheduler jobs resume shopco-nightly --location=us-central1`.
- Manual runs use `gcp/submit.sh` (same batch shape, plus it prints usage and the report), or `gcloud workflows run shopco-pipeline --data='{"scale":"smoke","stage":"all"}'`.

## Consequences
- Scheduling, retries (Workflows can wrap the submit in a retry policy) and auditing come from managed services with no servers.
- The workflow and `submit.sh` must be kept in step; both read the same `env.sh` conventions, and the workflow derives the bucket/SA from the project id.

## Alternatives rejected
- Cloud Composer: cost. Airflow is the right answer at a company with 50 DAGs, not for one.
- Cron on a VM: a server to patch.
- Scheduler calling Dataproc directly: possible, but no wait/fail semantics; the workflow gives them.
