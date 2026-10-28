#!/usr/bin/env bash
# One-shot setup: checks prerequisites, creates .env and a local venv, starts PostgreSQL.
set -euo pipefail

cd "$(dirname "$0")/.."

command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
docker compose version >/dev/null || { echo "docker compose v2 is required" >&2; exit 1; }

PYTHON="${PYTHON:-python3}"
"$PYTHON" -c 'import sys; sys.exit(sys.version_info < (3, 11))' \
  || { echo "Python 3.11+ is required (set PYTHON=/path/to/python3.11)" >&2; exit 1; }

[ -f .env ] || { cp .env.example .env; echo "Created .env from .env.example"; }

if [ ! -d .venv ]; then
  "$PYTHON" -m venv .venv
fi
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -e ".[dev]"

docker compose up -d --wait db
make seed

echo
echo "Ready. Next: make generate-data && make ingest"
