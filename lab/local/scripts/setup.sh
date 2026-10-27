#!/usr/bin/env bash
# One-time local setup: Java 17 (Homebrew), a uv virtualenv, the shopco package in editable mode.
set -euo pipefail
cd "$(dirname "$0")/../.."

if ! /opt/homebrew/opt/openjdk@17/bin/java -version >/dev/null 2>&1; then
  echo ">> installing openjdk@17 via Homebrew"
  brew install openjdk@17
fi
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
echo ">> java: $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"

if [ ! -d .venv ]; then
  echo ">> creating .venv with uv"
  uv venv .venv --python 3.12
fi
echo ">> installing shopco (editable) + dev deps"
uv pip install --python .venv/bin/python -e ".[dev]"

echo
echo "Done. Each shell session needs:"
echo "  source local/scripts/env.sh"
echo "Then:"
echo "  local/scripts/run.sh all smoke      # ~1 min"
echo "  local/scripts/run.sh all local      # the 60M-event run"
echo "  .venv/bin/pytest                    # end-to-end test at tiny scale"
