#!/usr/bin/env bash
# One-shot setup: checks prerequisites, creates .env and a local venv, starts PostgreSQL.
set -euo pipefail

cd "$(dirname "$0")/.."

command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
docker compose version >/dev/null || { echo "docker compose v2 is required" >&2; exit 1; }

is_supported() { "$1" -c 'import sys; sys.exit(sys.version_info < (3, 11))' 2>/dev/null; }

# Use $PYTHON if set, otherwise the newest Python 3.11+ on the PATH. On macOS the
# default python3 is often Apple's 3.9, with a newer one installed alongside.
if [ -z "${PYTHON:-}" ]; then
  for candidate in python3.13 python3.12 python3.11 python3; do
    if command -v "$candidate" >/dev/null && is_supported "$candidate"; then
      PYTHON="$candidate"
      break
    fi
  done
fi
[ -n "${PYTHON:-}" ] && is_supported "$PYTHON" \
  || { echo "Python 3.11+ is required (set PYTHON=/path/to/python3.11)" >&2; exit 1; }
echo "Using $("$PYTHON" --version) ($PYTHON)"

[ -f .env ] || { cp .env.example .env; echo "Created .env from .env.example"; }

# Recreate a venv left behind by an older Python (e.g. an earlier failed setup).
if [ -d .venv ] && ! is_supported .venv/bin/python; then
  echo "Recreating .venv (it uses an unsupported Python)"
  rm -rf .venv
fi
if [ ! -d .venv ]; then
  "$PYTHON" -m venv .venv
fi
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -e ".[dev]"

docker compose up -d --wait db
make seed

echo
echo "Ready. Next: make generate-data && make ingest"
