#!/usr/bin/env bash
# Dataproc Serverless needs a subnet with Private Google Access and internal ingress allowed. (gcp/adr/0006)
set -euo pipefail
source "$(dirname "$0")/../env.sh"

echo ">> enabling Private Google Access on default/$REGION"
gcloud compute networks subnets update default --region="$REGION" --project="$PROJECT_ID" \
  --enable-private-ip-google-access

if ! gcloud compute firewall-rules describe default-allow-internal --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo ">> creating default-allow-internal"
  gcloud compute firewall-rules create default-allow-internal --project="$PROJECT_ID" \
    --network=default --direction=INGRESS --allow=tcp,udp,icmp --source-ranges=10.128.0.0/9
fi
gcloud compute networks subnets describe default --region="$REGION" --project="$PROJECT_ID" \
  --format='table(name,ipCidrRange,privateIpGoogleAccess)'
