#!/usr/bin/env bash
# GCS bucket (data, code, run ledgers) and the BigQuery dataset. (gcp/adr/0004)
set -euo pipefail
source "$(dirname "$0")/../env.sh"

if ! gcloud storage buckets describe "$BUCKET" >/dev/null 2>&1; then
  echo ">> creating $BUCKET"
  gcloud storage buckets create "$BUCKET" --project="$PROJECT_ID" --location="$REGION" \
    --uniform-bucket-level-access --public-access-prevention
fi

# shuffle/tmp scratch is deleted after 7 days; data and runs are kept
LC="$(mktemp)"
cat > "$LC" <<'EOF'
{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7, "matchesPrefix": ["data/tmp/"]}}]}
EOF
gcloud storage buckets update "$BUCKET" --lifecycle-file="$LC" >/dev/null
rm -f "$LC"

if ! bq --project_id="$PROJECT_ID" show --dataset "$PROJECT_ID:$BQ_DATASET" >/dev/null 2>&1; then
  echo ">> creating BigQuery dataset $BQ_DATASET in $REGION"
  bq --project_id="$PROJECT_ID" --location="$REGION" mk --dataset \
    --description="ShopCo churn training set and run ledger (written by Dataproc Serverless)" \
    "$PROJECT_ID:$BQ_DATASET"
fi
echo ">> storage ready: $BUCKET, $PROJECT_ID:$BQ_DATASET"
