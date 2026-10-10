#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$PROJECT_ROOT"
exec "$PROJECT_ROOT/.venv/bin/jupyter" lab 33602190_assessment2.ipynb --ip=127.0.0.1
