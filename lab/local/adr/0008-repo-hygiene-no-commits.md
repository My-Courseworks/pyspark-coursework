# ADR 0008 · Repo hygiene: generated data and runs are never committed; the assistant makes no commits

**Status:** accepted · 2026-10-28

## Context
Generated data is gigabytes; run ledgers are per-machine. The owner's standing rule for the assistant: no git commits or pushes without explicit approval.

## Decision
- `data/`, `runs/`, `.venv/`, `*.zip`, `gcp/.state/` are gitignored.
- The repo is initialised (`git init`) with no commits; the owner reviews and commits.
- Every environment-specific value (project id, region, bucket) is in `gcp/env.sh` and overridable from the environment; no secrets are in the repo (the billing account id is looked up with `gcloud` at run time).

## Consequences
- A fresh clone needs `local/scripts/setup.sh` then one `run.sh` to have data.
- Reports worth keeping should be copied out of `runs/` by hand (or into the README as examples).
