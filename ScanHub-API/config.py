# config.py

# -----------------------------------------------------------------------------
# API security
# -----------------------------------------------------------------------------

API_KEY = "replace-with-a-real-secret"

# -----------------------------------------------------------------------------
# Upload settings
# -----------------------------------------------------------------------------

MAX_FILE_SIZE = int(1.5 * 1024 * 1024 * 1024)  # 1.5 GB

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB

# -----------------------------------------------------------------------------
# ClamAV connection - Compatability
# -----------------------------------------------------------------------------

CLAMD_UNIX_SOCKET = "/run/clamd.scan/clamd.sock"
CLAMD_HOST = None
CLAMD_PORT = 3310
SCANNER_TYPE = "clamav_fildes"

# -----------------------------------------------------------------------------
# Scanner pool selection
# -----------------------------------------------------------------------------

# You can setup multiple scanner pools and select which one to use here
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

    "round_robin": {
        "strategy": "round_robin",
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