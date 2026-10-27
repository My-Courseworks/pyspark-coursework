#!/usr/bin/env bash
# Package the shopco code, submit one Dataproc Serverless batch, wait, print usage and the run report.
#   gcp/submit.sh <generate|pipeline|all> <smoke|local|cloud>
set -euo pipefail
cd "$(dirname "$0")/.."
source gcp/env.sh

STAGE="${1:-all}"; SCALE="${2:-smoke}"
BATCH_ID="shopco-${SCALE}-${STAGE}-$(date -u +%Y%m%dt%H%M%S)"
case "$SCALE" in
  cloud) SHUF=1024; MAXEX=32 ;;   # ~128 cores max; one ShopCo month in well under an hour
  local) SHUF=256;  MAXEX=8  ;;
  *)     SHUF=64;   MAXEX=4  ;;
esac

echo ">> packaging src/shopco -> shopco.zip and uploading to $BUCKET/code/"
rm -f /tmp/shopco.zip
(cd src && zip -qr /tmp/shopco.zip shopco -x '*__pycache__*')
gcloud storage cp /tmp/shopco.zip gcp/jobs/main.py "$BUCKET/code/" --quiet

echo ">> submitting $BATCH_ID  (scale=$SCALE stage=$STAGE shuffle=$SHUF maxExecutors=$MAXEX)"
echo ">> console: https://console.cloud.google.com/dataproc/batches/$REGION/$BATCH_ID?project=$PROJECT_ID"
set +e
gcloud dataproc batches submit pyspark "$BUCKET/code/main.py" \
  --project="$PROJECT_ID" --region="$REGION" --batch="$BATCH_ID" \
  --py-files="$BUCKET/code/shopco.zip" --version="$RUNTIME_VERSION" \
  --service-account="$SA" --subnet=default \
  --labels="scale=$SCALE,stage=$STAGE,trigger=manual" \
  --properties="spark.executor.cores=4,spark.sql.shuffle.partitions=$SHUF,spark.dynamicAllocation.maxExecutors=$MAXEX" \
  -- "$STAGE" --mode cloud --scale "$SCALE" \
     --data-root "$BUCKET/data/$SCALE" --runs-root "$BUCKET/runs" --run-id "$BATCH_ID" \
     --sink bigquery --bq-dataset "$BQ_DATASET" \
  2>&1 | grep -v -E "^\s*$|WARN |INFO " | grep -v -E "^SHOPCO_STEP \{|^SHOPCO_RUN \{"
RC=${PIPESTATUS[0]}
set -e

echo; echo ">> batch state and usage:"
gcloud dataproc batches describe "$BATCH_ID" --region="$REGION" --project="$PROJECT_ID" \
  --format='yaml(state,stateMessage,createTime,stateTime,runtimeInfo.approximateUsage)'

mkdir -p gcp/.state
if gcloud storage cp "$BUCKET/runs/$BATCH_ID/report.md" "gcp/.state/$BATCH_ID.report.md" --quiet 2>/dev/null; then
  echo; echo ">> report saved to gcp/.state/$BATCH_ID.report.md"; echo
  cat "gcp/.state/$BATCH_ID.report.md"
fi
exit "$RC"
