#!/usr/bin/env bash
# Deploy the Cloud Workflow and a nightly Cloud Scheduler trigger (created PAUSED: gcp/adr/0007).
set -euo pipefail
source "$(dirname "$0")/../env.sh"
ORCH="$(dirname "$0")/../orchestration"

echo ">> deploying workflow $WORKFLOW"
gcloud workflows deploy "$WORKFLOW" --project="$PROJECT_ID" --location="$REGION" \
  --source="$ORCH/workflow.yaml" --service-account="$SA" \
  --description="Submit one ShopCo batch to Dataproc Serverless and wait for it" >/dev/null

EXEC_URI="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/workflows/$WORKFLOW/executions"
BODY='{"argument": "{\"scale\": \"cloud\", \"stage\": \"pipeline\"}"}'
if gcloud scheduler jobs describe "$SCHEDULER_JOB" --project="$PROJECT_ID" --location="$REGION" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "$SCHEDULER_JOB" --project="$PROJECT_ID" --location="$REGION" \
    --schedule="0 3 * * *" --time-zone="UTC" --uri="$EXEC_URI" --http-method=POST \
    --message-body="$BODY" --oauth-service-account-email="$SA" >/dev/null
  echo ">> updated scheduler job $SCHEDULER_JOB"
else
  gcloud scheduler jobs create http "$SCHEDULER_JOB" --project="$PROJECT_ID" --location="$REGION" \
    --schedule="0 3 * * *" --time-zone="UTC" --uri="$EXEC_URI" --http-method=POST \
    --message-body="$BODY" --oauth-service-account-email="$SA" \
    --description="Nightly ShopCo churn pipeline at cloud scale (paused by default)" >/dev/null
  echo ">> created scheduler job $SCHEDULER_JOB"
fi
gcloud scheduler jobs pause "$SCHEDULER_JOB" --project="$PROJECT_ID" --location="$REGION" >/dev/null
echo ">> scheduler job is PAUSED. Resume with: gcloud scheduler jobs resume $SCHEDULER_JOB --location=$REGION --project=$PROJECT_ID"
echo ">> run the workflow once by hand: gcloud workflows run $WORKFLOW --location=$REGION --project=$PROJECT_ID --data='{\"scale\":\"smoke\",\"stage\":\"all\"}'"
