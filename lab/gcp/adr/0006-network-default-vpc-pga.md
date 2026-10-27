# ADR 0006 · Networking: the default VPC with Private Google Access

**Status:** accepted · 2026-10-28

## Context
Dataproc Serverless executors have no external IPs. They reach GCS, BigQuery and Logging through Google's private backbone only if the subnet has **Private Google Access** on, and they talk to each other only if a firewall rule allows internal ingress.

## Decision
- Use the project's auto-created `default` network and its `us-central1` subnet (`10.128.0.0/20`), with PGA switched on (`gcp/infra/10_network.sh`).
- Rely on the auto-created `default-allow-internal` rule (ingress from `10.128.0.0/9`); the script recreates it if missing.
- No Cloud NAT: the batches need nothing outside Google APIs (the Python package arrives via `--py-files`; no `pip install` at runtime).

## Consequences
- Zero network cost and nothing to design; good enough for a single-tenant lab.
- A production estate would use a dedicated VPC and subnet; the only change here is `--subnet` in `gcp/submit.sh` and `subnetworkUri` in the workflow.
