#!/usr/bin/env bash
# Run a stage at a scale on this machine, with console output tee'd into the run folder.
#   local/scripts/run.sh <generate|pipeline|all|report> <test|smoke|local> [extra shopco args]
set -euo pipefail
cd "$(dirname "$0")/../.."
source local/scripts/env.sh

STAGE="${1:?stage}"; SCALE="${2:?scale}"; shift 2
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${SCALE}-${STAGE}"
mkdir -p "runs/${RUN_ID}"
echo ">> run ${RUN_ID}  (Spark UI: http://localhost:4040 while running)"
python -m shopco "${STAGE}" --mode local --scale "${SCALE}" --data-root "data/${SCALE}" --runs-root runs --run-id "${RUN_ID}" "$@" \
  2> >(grep -v -E 'WARN|INFO' >&2) | tee "runs/${RUN_ID}/console.log"
echo ">> report: runs/${RUN_ID}/report.md"
