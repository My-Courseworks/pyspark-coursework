#!/usr/bin/env bash
# Budget alert on this project only, in the billing account's own currency. Budgets notify; they do not stop spend. (gcp/adr/0001)
# Uses the REST API: `gcloud billing budgets create` only reports "invalid argument" when the currency does not match the account.
set -euo pipefail
source "$(dirname "$0")/../env.sh"

BA="$(gcloud billing projects describe "$PROJECT_ID" --format='value(billingAccountName)')"   # billingAccounts/XXXX
PN="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
CUR="$(gcloud billing accounts describe "${BA#billingAccounts/}" --format='value(currencyCode)')"
NAME="shopco-pyspark-lab cap"
AMOUNT="${BUDGET_AMOUNT:-2000}"   # in $CUR; INR 2000 ≈ USD 24 at the time of writing
TOKEN="$(gcloud auth print-access-token)"
API="https://billingbudgets.googleapis.com/v1/${BA}/budgets"
HDR=(-H "Authorization: Bearer $TOKEN" -H "x-goog-user-project: $PROJECT_ID" -H "Content-Type: application/json")

if curl -s "$API" "${HDR[@]}" | grep -q "\"displayName\": \"$NAME\""; then
  echo ">> budget '$NAME' exists"
  exit 0
fi
echo ">> creating budget '$NAME' = $AMOUNT $CUR/month for project $PN"
curl -s -X POST "$API" "${HDR[@]}" -d "{
  \"displayName\": \"$NAME\",
  \"budgetFilter\": {\"projects\": [\"projects/$PN\"]},
  \"amount\": {\"specifiedAmount\": {\"currencyCode\": \"$CUR\", \"units\": \"$AMOUNT\"}},
  \"thresholdRules\": [{\"thresholdPercent\": 0.5}, {\"thresholdPercent\": 0.9}, {\"thresholdPercent\": 1.0}]
}" | grep -E '"name"|"units"|"currencyCode"|"error"|"message"'
