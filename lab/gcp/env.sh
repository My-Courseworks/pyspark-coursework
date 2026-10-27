# Shared settings for every gcp/ script. Override any of them in the environment.
export PROJECT_ID="${PROJECT_ID:-shopco-pyspark-lab-2026}"
export CLOUDSDK_CORE_PROJECT="$PROJECT_ID"   # every gcloud call (and its API quota) goes to this project, not the user's default
export REGION="${REGION:-us-central1}"
export BUCKET="gs://${PROJECT_ID}-data"
export BQ_DATASET="${BQ_DATASET:-shopco}"
export SA_NAME="shopco-batch"
export SA="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export RUNTIME_VERSION="${RUNTIME_VERSION:-2.2}"   # Dataproc Serverless runtime 2.2 = Spark 3.5 (matches pyproject)
export BUDGET_USD="${BUDGET_USD:-20}"
export WORKFLOW="shopco-pipeline"
export SCHEDULER_JOB="shopco-nightly"
export ALERT_EMAIL="${ALERT_EMAIL:-$(gcloud config get-value account 2>/dev/null)}"
