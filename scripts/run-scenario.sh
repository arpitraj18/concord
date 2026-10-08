#!/usr/bin/env bash
# Usage: scripts/run-scenario.sh <scenario.json> [simulator-base-url]
# Starts a run and prints its id; then poll /sim/runs/<id> and read /dev/sink.
set -euo pipefail
scenario="${1:?scenario json file}"
base="${2:-http://localhost:8081}"
curl -sS -X POST "$base/sim/runs" -H 'Content-Type: application/json' --data-binary "@$scenario"
echo
