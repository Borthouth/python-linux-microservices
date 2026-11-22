#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

SCANHUB_DIR="${SCANHUB_DIR:-$(dirname "$SCRIPT_DIR")/ScanHub-API}"

compose=(docker compose -f docker-compose.yml -f docker-compose.cdc.yml)
# ScanHub is optional. Its settings are only wired in when ScanHub-API is present.
if [ -f "$SCANHUB_DIR/docker-compose.yml" ]; then
    compose+=(-f docker-compose.scanhub.yml)
fi

wait_for_otel_collector() {
    for _ in {1..30}; do
        if curl -fsS http://127.0.0.1:13133/ >/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "OpenTelemetry Collector did not become healthy. Recent logs:" >&2
    "${compose[@]}" logs --tail 50 otel_collector >&2 || true
    return 1
}

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 {start|stop}" >&2
    exit 2
fi

case "$1" in
    start)
        "${compose[@]}" up -d otel_collector
        wait_for_otel_collector
        if [ -f "$SCANHUB_DIR/docker-compose.yml" ]; then
            "$SCRIPT_DIR/scanhub-antivirus.sh" start
        else
            echo "ScanHub-API not found at $SCANHUB_DIR; starting ReportHub without antivirus." >&2
        fi
        "${compose[@]}" up -d
        ;;
    stop)
        "${compose[@]}" stop --timeout 600 celery_beat app
        "${compose[@]}" stop --timeout 600 \
            celery_ux_worker celery_warm_worker celery_cdc_worker \
            celery_batch_worker celery_cold_worker celery_periodic_worker celery_transfer_worker
        "${compose[@]}" stop --timeout 600
        ;;
    *)
        echo "Usage: $0 {start|stop}" >&2
        exit 2
        ;;
esac
