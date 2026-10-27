# ADR 0003 · Java 17 from Homebrew, `uv` virtualenv, PySpark 3.5 pinned to the cloud runtime

**Status:** accepted · 2026-10-28

## Context
The machine had no Java (PySpark needs a JVM), Python 3.12 with `uv`, and Homebrew. Dataproc Serverless runtime 2.2 ships Spark 3.5, and the same code must run in both places.

## Decision
- `brew install openjdk@17`; `JAVA_HOME=/opt/homebrew/opt/openjdk@17` set by `local/scripts/env.sh` (keg-only, so it does not touch the system Java).
- `uv venv .venv --python 3.12` and `uv pip install -e ".[dev]"` from `pyproject.toml`.
- `pyspark==3.5.3` pinned: the same minor line as the Dataproc runtime, so plan output, AQE behaviour and the REST API shape match what the cloud run produces.
- No Spark download: the `pyspark` wheel bundles the Spark jars, including the History Server scripts.

## Consequences
- `local/scripts/setup.sh` is the one-time setup; `source local/scripts/env.sh` per shell.
- Upgrading Spark means bumping both `pyproject.toml` and `RUNTIME_VERSION` in `gcp/env.sh` together.
- Java 21 would also work with Spark 3.5, but 17 is the version Dataproc 2.2 uses.

## Alternatives rejected
- Docker image with Spark: slower iteration, and hides the "what does PySpark actually need" lesson.
- System-wide Java via the Oracle installer: harder to remove; Homebrew keg-only is reversible.
