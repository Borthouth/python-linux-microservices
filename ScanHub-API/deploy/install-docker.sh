#!/usr/bin/env bash
# Prepares the host and starts ScanHub-API in Docker: host clamd instances,
# .env, data directories, /etc/scanhub/config.py, TLS certificate, then
# `docker compose up`. Run as root from anywhere: sudo ./deploy/install-docker.sh
# Safe to re-run. An existing config, key, or certificate is kept.
set -euo pipefail

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONFIG_PATH=/etc/scanhub/config.py
DATA_DIR=/var/lib/scanhub
SECRETS_DIR="$PROJECT_DIR/secrets"
APP_UID=10001
PORT=9443
OWNER=${SUDO_USER:-root}

if [[ $EUID -ne 0 ]]; then
    echo "Run as root: sudo $0" >&2
    exit 1
fi

for cmd in docker openssl curl; do
    command -v "$cmd" >/dev/null || { echo "Missing required command: $cmd" >&2; exit 1; }
done
docker compose version >/dev/null 2>&1 || {
    echo "The Docker Compose plugin is missing (docker compose version failed)." >&2
    exit 1
}

echo "==> Host clamd instances"
"$PROJECT_DIR/deploy/setup-clamd.sh"

CLAMD_GID=$(getent group clamav | cut -d: -f3 || true)
[[ -n $CLAMD_GID ]] || CLAMD_GID=$(getent group clamscan | cut -d: -f3 || true)
[[ -n $CLAMD_GID ]] || { echo "Could not find the clamav or clamscan group." >&2; exit 1; }

echo "==> .env (CLAMD_GID=$CLAMD_GID)"
cat > "$PROJECT_DIR/.env" <<EOF
# Written by deploy/install-docker.sh. Re-running the script overwrites this file.
CLAMD_GID=$CLAMD_GID
SCANHUB_CONFIG=$CONFIG_PATH
EOF
chown "$OWNER" "$PROJECT_DIR/.env"

echo "==> Data and log directories"
mkdir -p "$DATA_DIR/tmp" "$PROJECT_DIR/logs"
chown -R "$APP_UID:$APP_UID" "$DATA_DIR" "$PROJECT_DIR/logs"

echo "==> Application config"
mkdir -p "$(dirname "$CONFIG_PATH")"
if [[ -f $CONFIG_PATH ]]; then
    echo "    Keeping existing $CONFIG_PATH"
else
    api_key=$(openssl rand -hex 32)
    (umask 077 && cat > "$CONFIG_PATH" <<EOF
# Created by deploy/install-docker.sh. Mounted read-only into the container.
API_KEY = "$api_key"

MAX_FILE_SIZE = int(1.5 * 1024 * 1024 * 1024)  # 1.5 GB
CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB

# Compatibility settings used by the clamav_file scanner.
CLAMD_UNIX_SOCKET = "/run/clamd.scan/clamd.sock"
CLAMD_HOST = None
CLAMD_PORT = 3310
SCANNER_TYPE = "clamav_fildes"

SCANNER_POOL = "default"
SCANNER_POOLS = {
    "default": {
        "strategy": "least_connections",
        "instances": [
            {
                "instance_name": "clamav_scan_1",
                "type": "clamav_fildes",
                "socket": "/run/clamd.scan/clamd.sock",
            },
            {
                "instance_name": "clamav_scan_2",
                "type": "clamav_fildes",
                "socket": "/run/clamd.scan2/clamd.sock",
            },
        ],
    },
}
EOF
    )
    chown "$APP_UID:$APP_UID" "$CONFIG_PATH"
    chmod 600 "$CONFIG_PATH"
    echo "    Created $CONFIG_PATH with a new API key (read it with: sudo grep API_KEY $CONFIG_PATH)"
fi

echo "==> TLS certificate"
mkdir -p "$SECRETS_DIR"
# ReportHub reaches this API as host.docker.internal, so the certificate must list it.
cert_has_docker_name() {
    [[ -f $SECRETS_DIR/scanhubapi.crt ]] &&
        openssl x509 -in "$SECRETS_DIR/scanhubapi.crt" -noout -ext subjectAltName 2>/dev/null |
        grep -q 'host.docker.internal'
}
if [[ -f $SECRETS_DIR/scanhubapi.key ]] && cert_has_docker_name; then
    echo "    Keeping existing certificate in $SECRETS_DIR"
else
    host=$(hostname -f 2>/dev/null || hostname)
    openssl req -x509 -newkey rsa:4096 -sha256 -days 825 -nodes \
        -keyout "$SECRETS_DIR/scanhubapi.key" -out "$SECRETS_DIR/scanhubapi.crt" \
        -subj "/CN=$host" \
        -addext "subjectAltName=DNS:$host,DNS:localhost,DNS:host.docker.internal" 2>/dev/null
    echo "    Generated a self-signed certificate. Replace it with a trusted one for production."
fi
# The container runs as uid $APP_UID and must read the key through the bind mount.
chown "$APP_UID:$APP_UID" "$SECRETS_DIR/scanhubapi.key"
chmod 600 "$SECRETS_DIR/scanhubapi.key"
chmod 644 "$SECRETS_DIR/scanhubapi.crt"

echo "==> Building and starting the container"
cd "$PROJECT_DIR"
docker compose up -d --build

echo "==> Waiting for https://localhost:$PORT/health"
healthy=0
for _ in $(seq 1 30); do
    if curl -fsk "https://localhost:$PORT/health" >/dev/null 2>&1; then
        healthy=1
        break
    fi
    sleep 2
done
if [[ $healthy -ne 1 ]]; then
    echo "ScanHub did not become healthy. Recent container logs:" >&2
    docker compose logs --tail 50 scanhub-api >&2 || true
    exit 1
fi

echo
echo "ScanHub-API is running on https://$(hostname -f 2>/dev/null || hostname):$PORT"
echo "Test a scan (replace KEY with API_KEY from $CONFIG_PATH):"
echo "  curl -sk -H 'X-API-Key: KEY' -F file=@/path/to/file https://localhost:$PORT/scan"
