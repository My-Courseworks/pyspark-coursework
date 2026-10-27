#!/usr/bin/env bash
# Log-based metrics from the monitor's stdout lines, an email alert on failed runs, a dashboard. (gcp/adr/0008)
set -euo pipefail
source "$(dirname "$0")/../env.sh"
MON="$(dirname "$0")/../monitoring"

for f in metric_step_seconds metric_run_failures; do
  NAME="$(grep '^name:' "$MON/$f.yaml" | awk '{print $2}')"
  if gcloud logging metrics describe "$NAME" --project="$PROJECT_ID" >/dev/null 2>&1; then
    gcloud logging metrics update "$NAME" --project="$PROJECT_ID" --config-from-file="$MON/$f.yaml" >/dev/null
    echo ">> updated log metric $NAME"
  else
    gcloud logging metrics create "$NAME" --project="$PROJECT_ID" --config-from-file="$MON/$f.yaml" >/dev/null
    echo ">> created log metric $NAME"
  fi
done

CHANNEL="$(gcloud beta monitoring channels list --project="$PROJECT_ID" \
  --filter="type=\"email\" AND labels.email_address=\"$ALERT_EMAIL\"" --format='value(name)' | head -1)"
if [ -z "$CHANNEL" ]; then
  CHANNEL="$(gcloud beta monitoring channels create --project="$PROJECT_ID" --type=email \
    --display-name="ShopCo alerts ($ALERT_EMAIL)" --channel-labels="email_address=$ALERT_EMAIL" --format='value(name)')"
  echo ">> created email channel $CHANNEL"
fi

if ! gcloud alpha monitoring policies list --project="$PROJECT_ID" --format='value(displayName)' | grep -qx "ShopCo pipeline run failed"; then
  POL="$(mktemp)"; sed "s#__CHANNEL__#$CHANNEL#" "$MON/alert_run_failed.json.tmpl" > "$POL"
  gcloud alpha monitoring policies create --project="$PROJECT_ID" --policy-from-file="$POL" >/dev/null
  rm -f "$POL"; echo ">> created alert policy 'ShopCo pipeline run failed'"
fi

DASH="$(gcloud monitoring dashboards list --project="$PROJECT_ID" --filter='displayName="ShopCo PySpark pipeline"' --format='value(name)' | head -1)"
if [ -z "$DASH" ]; then
  gcloud monitoring dashboards create --project="$PROJECT_ID" --config-from-file="$MON/dashboard.json" >/dev/null
  echo ">> created dashboard 'ShopCo PySpark pipeline'"
else
  # update needs the current etag; the dashboard is pure config, so replace it instead
  gcloud monitoring dashboards delete "$DASH" --project="$PROJECT_ID" --quiet >/dev/null
  gcloud monitoring dashboards create --project="$PROJECT_ID" --config-from-file="$MON/dashboard.json" >/dev/null
  echo ">> replaced dashboard 'ShopCo PySpark pipeline'"
fi
echo ">> monitoring ready. Dashboards: https://console.cloud.google.com/monitoring/dashboards?project=$PROJECT_ID"
