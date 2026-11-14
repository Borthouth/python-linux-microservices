#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.test.yml"

if [ "$#" -gt 1 ]; then
    echo "Error: supply at most one option." >&2
    exit 1
fi
case "${1:-}" in
    ""|--prepare-only) ;;
    --stop)
        docker compose -f "$COMPOSE_FILE" stop sftp-test
        exit 0
        ;;
    *)
        echo "Usage: $0 [--prepare-only|--stop]" >&2
        exit 1
        ;;
esac
KEY_DIR="$(
    docker compose -f "$COMPOSE_FILE" config --format json |
        python3 -c 'import json, os, sys; service = json.load(sys.stdin)["services"]["sftp-test"]; print(os.path.dirname(next(mount["source"] for mount in service["volumes"] if mount["target"] == "/home/reporthub/.ssh/keys/client_ed25519.pub")))'
)"
KEY_DIR="$(realpath -m "$KEY_DIR")"
REPO_ROOT="$(realpath "$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)")"
if [ "$KEY_DIR" = "/" ] || [ "$KEY_DIR" = "$(realpath "$HOME")" ]; then
    echo "Error: choose a dedicated SFTP key subdirectory, not the root or home directory." >&2
    exit 1
fi
case "$KEY_DIR/" in
    "$REPO_ROOT/"*)
        echo "Error: SFTP_TEST_KEY_DIR must be outside the Git repository." >&2
        exit 1
        ;;
esac

umask 077
mkdir -p "$KEY_DIR"
chmod 700 "$KEY_DIR"
if [ ! -f "$KEY_DIR/client_ed25519" ]; then
    if [ -e "$KEY_DIR/client_ed25519.pub" ]; then
        echo "Error: public key exists without its private key in $KEY_DIR; restore the private key rather than silently replacing it." >&2
        exit 1
    fi
    ssh-keygen -q -t ed25519 -N "" -C reporthub-sftp-test -f "$KEY_DIR/client_ed25519"
fi
ssh-keygen -y -P "" -f "$KEY_DIR/client_ed25519" > "$KEY_DIR/client_ed25519.pub"
chmod 600 "$KEY_DIR/client_ed25519"
chmod 644 "$KEY_DIR/client_ed25519.pub"
export SFTP_TEST_KEY_DIR="$KEY_DIR"
echo "Test SFTP client key: $KEY_DIR/client_ed25519 (keep private; do not commit)"

if [ "${1:-}" = "--prepare-only" ]; then
    exit 0
fi

if ! docker network inspect reporthub_shared_network >/dev/null 2>&1; then
    docker network create reporthub_shared_network >/dev/null
fi
docker compose -f "$COMPOSE_FILE" up -d --no-deps --wait sftp-test
echo "Test SFTP is ready: sftp-test:22 in Docker; host endpoint:"
docker compose -f "$COMPOSE_FILE" port sftp-test 22
echo "User: reporthub; remote directory: /upload; authentication: SSH private key."
docker compose -f "$COMPOSE_FILE" exec -T sftp-test \
    ssh-keygen -l -E sha256 -f /etc/ssh/ssh_host_ed25519_key.pub
