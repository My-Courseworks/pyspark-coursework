#!/usr/bin/env bash
# One service account runs the batches and the workflow; least privilege at project + bucket level. (gcp/adr/0005)
set -euo pipefail
source "$(dirname "$0")/../env.sh"

if ! gcloud iam service-accounts describe "$SA" --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo ">> creating service account $SA"
  gcloud iam service-accounts create "$SA_NAME" --project="$PROJECT_ID" \
    --display-name="ShopCo Dataproc Serverless batch runner"
fi

for role in \
  roles/dataproc.worker \
  roles/dataproc.editor \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/logging.logWriter \
  roles/monitoring.metricWriter \
  roles/workflows.invoker; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$SA" --role="$role" \
    --condition=None --quiet >/dev/null
  echo "   $role"
done

gcloud storage buckets add-iam-policy-binding "$BUCKET" --member="serviceAccount:$SA" \
  --role=roles/storage.objectAdmin >/dev/null
echo "   roles/storage.objectAdmin on $BUCKET"

# the workflow (running as SA) submits batches that also run as SA
gcloud iam service-accounts add-iam-policy-binding "$SA" --project="$PROJECT_ID" \
  --member="serviceAccount:$SA" --role=roles/iam.serviceAccountUser --quiet >/dev/null
echo "   roles/iam.serviceAccountUser on itself"
echo ">> iam ready"
