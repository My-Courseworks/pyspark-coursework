#!/usr/bin/env bash
# Replay any past run's jobs/stages/tasks in the Spark History Server UI (http://localhost:18080).
set -euo pipefail
cd "$(dirname "$0")/../.."
source local/scripts/env.sh
SPARK_HOME="$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://$(pwd)/runs/spark-events"
echo ">> history server on http://localhost:18080 (Ctrl-C to stop)"
exec "$SPARK_HOME/sbin/start-history-server.sh" && tail -f "$SPARK_HOME"/logs/*history*
