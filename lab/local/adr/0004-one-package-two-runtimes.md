# ADR 0004 · One `shopco` package, two runtimes

**Status:** accepted · 2026-10-28

## Context
The request asked for local and GCP work "naturally under separate folders". The course's own claim (M1) is that the same PySpark code runs on a laptop and on a cluster, with only the cluster size changing. Duplicating the pipeline per environment would contradict that and double the maintenance.

## Decision
- All Spark code lives once in `src/shopco/` (generator, pipeline, monitor, report, CLI).
- `local/` holds only what is laptop-specific: setup, run scripts, History Server, these ADRs.
- `gcp/` holds only what is cloud-specific: infra scripts, batch entry point (`gcp/jobs/main.py`, which forwards argv to `shopco.cli.main`), submission, orchestration, monitoring config, its ADRs.
- The only switches are CLI flags: `--mode local|cloud`, `--data-root <dir|gs://…>`, `--runs-root`, `--sink bigquery`.
- File I/O for ledgers goes through Hadoop's FileSystem (`src/shopco/fs.py`), which speaks `file://` and `gs://` alike, so no cloud SDK is needed in the package.

## Consequences
- A bug fixed locally is fixed in the cloud by re-uploading one zip (`gcp/submit.sh` does this every time).
- The cloud entry point is three lines; everything testable is tested locally by `tests/`.
- Cloud-only features (BigQuery sink) are behind a flag and never execute locally.

## Alternatives rejected
- Separate `local/pipeline.py` and `gcp/pipeline.py`: drift within a week.
- Notebooks: not reproducible, and Dataproc Serverless batches want a script.
