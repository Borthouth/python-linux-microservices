#!/usr/bin/env bash
# Installs ClamAV and configures one clamd instance per ScanHub scanner pool
# socket. Supports Ubuntu/Debian and RHEL-family systems. Run as root; safe to
# re-run. Instance names must match the socket paths in config.py
# (/run/clamd.<instance>/clamd.sock).
set -euo pipefail

INSTANCES=(scan scan2)
MAX_SIZE=1500M
MAX_THREADS=4

if [[ $EUID -ne 0 ]]; then
    echo "Run as root: sudo $0" >&2
    exit 1
fi

# shellcheck disable=SC1091
. /etc/os-release
case " ${ID:-} ${ID_LIKE:-} " in
    *" debian "*|*" ubuntu "*) FAMILY=debian ;;
    *" rhel "*|*" fedora "*)   FAMILY=rhel ;;
    *) echo "Unsupported OS: ${ID:-unknown}" >&2; exit 1 ;;
esac
echo "Detected $FAMILY family (${PRETTY_NAME:-$ID})"

write_conf() {
    # $1 instance, $2 clamd user, $3 log file, $4 config path
    local instance=$1 user=$2 log=$3 conf=$4
    cat > "$conf" <<EOF
# Managed by deploy/setup-clamd.sh. Re-running the script overwrites this file.
LocalSocket /run/clamd.${instance}/clamd.sock
LocalSocketGroup ${user}
LocalSocketMode 660
FixStaleSocket yes
User ${user}
PidFile /run/clamd.${instance}/clamd.pid

DatabaseDirectory /var/lib/clamav
LogFile ${log}
LogTime yes

# Must match the upload limit in config.py; ClamAV's 25M default would
# silently skip larger files.
MaxFileSize ${MAX_SIZE}
MaxScanSize 1600M
StreamMaxLength ${MAX_SIZE}
MaxThreads ${MAX_THREADS}
EOF
    chmod 644 "$conf"
}

wait_for_socket() {
    # $1 socket path. Returns 0 once clamd answers PING.
    local sock=$1 i
    for i in $(seq 1 120); do
        if [[ -S $sock ]] && python3 - "$sock" <<'PY' 2>/dev/null
import socket, sys
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.settimeout(5)
s.connect(sys.argv[1])
s.sendall(b"zPING\0")
sys.exit(0 if s.recv(64).startswith(b"PONG") else 1)
PY
        then
            return 0
        fi
        sleep 2
    done
    return 1
}

if [[ $FAMILY == debian ]]; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y clamav-daemon clamav-freshclam python3

    # The default clamav-daemon instance would be a third clamd; keep it off.
    systemctl disable --now clamav-daemon.service clamav-daemon.socket 2>/dev/null || true

    cat > /etc/systemd/system/clamd-scanhub@.service <<'EOF'
# Managed by deploy/setup-clamd.sh.
[Unit]
Description=ClamAV scan daemon for ScanHub (%i)
After=network.target clamav-freshclam.service
Wants=clamav-freshclam.service

[Service]
Type=simple
User=clamav
Group=clamav
RuntimeDirectory=clamd.%i
RuntimeDirectoryMode=0750
ExecStart=/usr/sbin/clamd --foreground=true --config-file=/etc/clamav/clamd-scanhub-%i.conf
Restart=on-failure
RestartSec=10
TimeoutStartSec=300
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

    CLAMD_GROUP=clamav
    for instance in "${INSTANCES[@]}"; do
        write_conf "$instance" clamav "/var/log/clamav/clamd-scanhub-${instance}.log" \
            "/etc/clamav/clamd-scanhub-${instance}.conf"
    done

    # Ubuntu's AppArmor profile for clamd only allows its default config and
    # /run/clamav, so the instance configs and sockets are denied without this.
    # Appends to the local override (the supported place for site rules).
    local_profile=/etc/apparmor.d/local/usr.sbin.clamd
    if [[ -f /etc/apparmor.d/usr.sbin.clamd ]] && command -v apparmor_parser >/dev/null; then
        mkdir -p /etc/apparmor.d/local
        touch "$local_profile"
        if ! grep -q "clamd-scanhub" "$local_profile"; then
            cat >> "$local_profile" <<'EOF'

# ScanHub clamd instances (deploy/setup-clamd.sh)
/etc/clamav/clamd-scanhub-*.conf r,
/run/clamd.*/ rw,
/run/clamd.*/** rwk,
/var/lib/scanhub/tmp/** rw,
EOF
        fi
        # Lets clamd receive file descriptors passed from the API container.
        # Without it, AppArmor denies those fds as disconnected paths.
        if ! grep -q 'attach_disconnected' /etc/apparmor.d/usr.sbin.clamd; then
            sed -i 's|^/usr/sbin/clamd {|/usr/sbin/clamd flags=(attach_disconnected) {|' /etc/apparmor.d/usr.sbin.clamd
        fi
        apparmor_parser -r /etc/apparmor.d/usr.sbin.clamd
    fi

    systemctl daemon-reload
    systemctl enable --now clamav-freshclam.service
    for instance in "${INSTANCES[@]}"; do
        systemctl enable --now "clamd-scanhub@${instance}.service"
    done
else
    dnf install -y epel-release || true
    dnf install -y clamd clamav-update python3

    # EPEL's clamd@.service reads /etc/clamd.d/<instance>.conf and creates
    # /run/clamd.<instance>.
    CLAMD_GROUP=clamscan
    for instance in "${INSTANCES[@]}"; do
        write_conf "$instance" clamscan "/var/log/clamd.${instance}" \
            "/etc/clamd.d/${instance}.conf"
    done

    systemctl daemon-reload
    if systemctl list-unit-files clamav-freshclam.service >/dev/null 2>&1; then
        systemctl enable --now clamav-freshclam.service
    else
        freshclam || true
    fi
    for instance in "${INSTANCES[@]}"; do
        systemctl enable --now "clamd@${instance}.service"
    done
fi

echo
echo "Waiting for clamd to answer on each socket (the first start can take a few minutes while signatures load)..."
status=0
for instance in "${INSTANCES[@]}"; do
    sock="/run/clamd.${instance}/clamd.sock"
    if wait_for_socket "$sock"; then
        echo "  OK   $sock"
    else
        echo "  FAIL $sock (check: journalctl -u clamd*${instance}* -u clamav-freshclam)" >&2
        status=1
    fi
done

echo
echo "Set these in ScanHub-API/.env:"
echo "  CLAMD_GID=$(getent group "$CLAMD_GROUP" | cut -d: -f3)"
exit "$status"
