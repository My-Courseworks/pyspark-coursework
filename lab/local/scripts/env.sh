# source this in each shell: Java for PySpark, venv on PATH
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
_lab_root="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/../.." && pwd)"
export PATH="$_lab_root/.venv/bin:$PATH"
export PYSPARK_PYTHON="$_lab_root/.venv/bin/python"
unset _lab_root
