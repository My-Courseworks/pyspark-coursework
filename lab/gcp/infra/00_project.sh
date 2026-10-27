#!/usr/bin/env bash
# Project, billing link, APIs. Idempotent. (gcp/adr/0001)
set -euo pipefail
source "$(dirname "$0")/../env.sh"

if ! gcloud projects describe "$PROJECT_ID" >/dev/null 2>&1; then
  echo ">> creating project $PROJECT_ID"
  gcloud projects create "$PROJECT_ID" --name="ShopCo PySpark Lab"
fi

if [ -z "$(gcloud billing projects describe "$PROJECT_ID" --format='value(billingAccountName)')" ]; then
  BA="$(gcloud billing accounts list --filter='open=true' --format='value(name)' | head -1)"
  echo ">> linking billing account $BA"
  gcloud billing projects link "$PROJECT_ID" --billing-account="${BA#billingAccounts/}"
fi

echo ">> enabling APIs"
gcloud services enable --project="$PROJECT_ID" \
  compute.googleapis.com dataproc.googleapis.com storage.googleapis.com \
  bigquery.googleapis.com bigquerystorage.googleapis.com \
  logging.googleapis.com monitoring.googleapis.com \
  cloudresourcemanager.googleapis.com iam.googleapis.com serviceusage.googleapis.com \
  workflows.googleapis.com workflowexecutions.googleapis.com cloudscheduler.googleapis.com \
  billingbudgets.googleapis.com
echo ">> project ready: $PROJECT_ID (number $(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)'))"
