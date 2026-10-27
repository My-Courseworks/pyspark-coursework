# ADR 0003 · Region `us-central1`, everything co-located

**Status:** accepted · 2026-10-28

## Context
Dataproc Serverless, the GCS bucket, the BigQuery dataset, Workflows and Scheduler all take a location. Cross-region traffic costs money and adds latency.

## Decision
`us-central1` for all of them (`REGION` in `gcp/env.sh`): one of the cheapest regions, all needed services available, and the BigQuery dataset is created in the same region (not the `US` multi-region) so Spark's direct writes stay local.

## Consequences
- Changing region = change one variable and re-run the infra scripts into a new bucket/dataset; the old ones are not migrated.
