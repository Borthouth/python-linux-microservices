#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

compose=(docker compose -f docker-compose.yml -f docker-compose.cdc.yml)

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 {start|stop}" >&2
    exit 2
fi

case "$1" in
    start)
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
