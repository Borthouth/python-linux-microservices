#!/bin/bash
# Starts, stops, or checks the ScanHub-API antivirus container used by ReportHub.
# ScanHub-API is its own compose project, so this script only manages that stack.
# Usage: scanhub-antivirus.sh {start|stop|status}
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCANHUB_DIR="${SCANHUB_DIR:-$(dirname "$SCRIPT_DIR")/ScanHub-API}"
SCANHUB_PORT="${SCANHUB_PORT:-9443}"
HEALTH_URL="https://localhost:${SCANHUB_PORT}/health"

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 {start|stop|status}" >&2
    exit 2
fi

if [ ! -f "$SCANHUB_DIR/docker-compose.yml" ]; then
    echo "ScanHub-API not found at $SCANHUB_DIR. Set SCANHUB_DIR to its location." >&2
    exit 1
fi

compose=(docker compose --project-directory "$SCANHUB_DIR" -f "$SCANHUB_DIR/docker-compose.yml")

is_healthy() {
    curl -fsk "$HEALTH_URL" >/dev/null 2>&1
}

wait_healthy() {
    for _ in $(seq 1 30); do
        if is_healthy; then
            echo "ScanHub-API is healthy at $HEALTH_URL"
            return 0
        fi
        sleep 2
    done
    echo "ScanHub-API did not become healthy. Recent logs:" >&2
    "${compose[@]}" logs --tail 50 scanhub-api >&2 || true
    return 1
}

case "$1" in
    start)
        "${compose[@]}" up -d
        wait_healthy
        ;;
    stop)
        "${compose[@]}" stop scanhub-api
        ;;
    status)
        "${compose[@]}" ps scanhub-api
        if is_healthy; then
            echo "health: ok ($HEALTH_URL)"
        else
            echo "health: unreachable ($HEALTH_URL)"
        fi
        ;;
esac
