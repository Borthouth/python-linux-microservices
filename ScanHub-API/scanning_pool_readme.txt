Scanner Pool Setup

ScanHub supports one or more antivirus scanner instances through a generic scanner pool.

Supported pool strategies:

- least_connections
- round_robin
- first

The pool is generic and is designed to support multiple antivirus engines in
the future (ClamAV, Sophos, ICAP, vendor APIs, etc.).

===============================================================================
CLAMAV MULTI-INSTANCE SETUP
===============================================================================

Assuming the primary ClamAV configuration is:

/etc/clamd.d/scan.conf

Example:

LocalSocket /run/clamd.scan/clamd.sock
PidFile /run/clamd.scan/clamd.pid

Create second configuration:

cp /etc/clamd.d/scan.conf /etc/clamd.d/scan2.conf

Edit:

/etc/clamd.d/scan2.conf

Update:

LocalSocket /run/clamd.scan2/clamd.sock
PidFile /run/clamd.scan2/clamd.pid
LogFile /var/log/clamd.scan2.log

Create runtime directory:

mkdir -p /run/clamd.scan2
chown clamav:virusgroup /run/clamd.scan2
chmod 710 /run/clamd.scan2

Start second instance:

systemctl enable clamd@scan2
systemctl start clamd@scan2

Verify:

systemctl status clamd@scan
systemctl status clamd@scan2

Verify sockets:

ls -l /run/clamd.scan/clamd.sock
ls -l /run/clamd.scan2/clamd.sock

===============================================================================
SCANHUB CONFIGURATION
===============================================================================

Example:

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

===============================================================================
POOL STRATEGIES
===============================================================================

least_connections

Routes scans to the scanner instance with the fewest active scans.

Recommended for production environments where file sizes and scan
durations vary significantly.

Example:

    clamav_scan_1 active scans = 3
    clamav_scan_2 active scans = 1

    Next scan routed to:
    clamav_scan_2


round_robin

Routes scans sequentially across scanner instances.

Example:

    Scan 1 -> clamav_scan_1
    Scan 2 -> clamav_scan_2
    Scan 3 -> clamav_scan_1
    Scan 4 -> clamav_scan_2

Useful when scan workloads are roughly equal and predictable
distribution is preferred.


first

Always routes scans to the first configured scanner instance.

Example:

    Scan 1 -> clamav_scan_1
    Scan 2 -> clamav_scan_1
    Scan 3 -> clamav_scan_1

Useful for troubleshooting, maintenance, benchmarking, or
temporarily disabling load balancing.


Selecting the Active Pool

The active pool is selected using:

    SCANNER_POOL = "least_connections"

Example:

    SCANNER_POOL = "least_connections"

    SCANNER_POOLS = {
        "least_connections": {
            "strategy": "least_connections",
            "instances": [...]
        }
    }

The currently active strategy can be verified using the /health endpoint.

active_connections values shown by the API represent the number of
in-progress scans currently assigned to each scanner instance.

When running multiple Uvicorn workers, active connection counts are
maintained per worker process.


===============================================================================
HEALTH CHECK
===============================================================================

curl -k https://localhost:9443/health

Example:

{
  "ok": true,
  "pool_strategy": "least_connections",
  "scanner_count": 2,
  "instances": [
    {
      "scanner_instance": "clamav_scan_1",
      "active_connections": 0
    },
    {
      "scanner_instance": "clamav_scan_2",
      "active_connections": 0
    }
  ]
}

===============================================================================
NOTES
===============================================================================

- Scanner pools support one or more scanner instances.
- Pools are generic and are not specific to ClamAV.
- Additional antivirus engines can be added without modifying ScanHub API logic.
- Each ClamAV instance loads its own virus signatures into memory.
- Running multiple ClamAV instances increases RAM usage.