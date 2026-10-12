#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}" || exit 1

exec hypercorn \
    --config hypercorn-http.toml \
    --worker-class uvloop \
    scan_api:app