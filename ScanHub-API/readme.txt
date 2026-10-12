===============================================================================
ScanHub API
===============================================================================

Overview
===============================================================================

The ScanHub API provides a REST interface to local antivirus scanning engines.
The recommended scanner is ClamAV FILDES (clamav_fildes), connected through one
or more local clamd Unix sockets via the ScanHub scanner pool.

Features:
- File upload scanning (/scan)
- Raw stream scanning (/scan/stream)
- API Key authentication
- HTTP/1.1 support
- HTTPS support
- HTTP/2 support (Hypercorn + TLS)
- HTTP/2-capable Python SDK (httpx)
- Persistent client connection pooling
- Automatic HTTP/2 to HTTP/1.1 fallback
- Logging to file
- Health monitoring endpoint (/health)
- Metrics endpoint (/metrics)
- Persistent SQLite metrics
- Version reporting
- EICAR antivirus test support
- ClamAV FILDES support
- Multi-worker support
- Scanner pool support
- Multiple antivirus instance support
- Least-connections routing
- Round-robin routing
- Generic scanner architecture

===============================================================================
INSTALLATION
===============================================================================

NOTE:
You may need to install ppip and svn
 - sudo dnf install python3-pip
 - sudo dnf install svn

1. Install Python Dependencies
-------------------------------------------------------------------------------

pip3 install fastapi hypercorn python-multipart pyclamd uvloop orjson httpx

-------------------------------------------------------------------------------
2. SVN Checkout Application Files
-------------------------------------------------------------------------------

SVN checkout:

svn co svn://svn.server.net/svn_bacs/ScanHub-API/trunk /home/ScanHub-API

Create the service account:

useradd --system \
    --home-dir /home/ScanHub-API \
    --shell /sbin/nologin \
    scanhub

If using ClamAV Unix sockets, grant access to the ClamAV group:

usermod -aG clamav scanhub

If this fails because 'clamav' does not exist use this command:

usermod -aG clamscan scanhub

Update permissions:

chown -R scanhub:scanhub /home/ScanHub-API
chmod 750 /home/ScanHub-API

NOTE:

Additional group memberships may be required depending on the
configured scanner implementation.

For ClamAV Unix socket scanning, the scanhub user must be able to
access the configured ClamAV socket files.

Also if you are using more than one ClamAV sockets/instances you
need to add the following so that folders for ClamAV in /run is
created at startup.

Create file and add
---------------------
vi /etc/tmpfiles.d/clamd.conf
d /run/clamd.scan  0750 clamav   virusgroup -
d /run/clamd.scan2 0750 clamav   virusgroup -

Run this to sort out permissions
----------------------------------
systemd-tmpfiles --create /etc/tmpfiles.d/clamd.conf

Restart services
-----------------
systemctl daemon-reload
systemctl restart clamd@scan
systemctl restart clamd@scan2

-------------------------------------------------------------------------------
3. Install the Service
-------------------------------------------------------------------------------

Copy the service file:

cp scanhub-api.service /etc/systemd/system/

-------------------------------------------------------------------------------
4. Set Permissions
-------------------------------------------------------------------------------

Make the startup script executable:

chmod 755 run_https.sh

Set service ownership:

chown root:root /etc/systemd/system/scanhub-api.service

Set service permissions:

chmod 644 /etc/systemd/system/scanhub-api.service

-------------------------------------------------------------------------------
5. Example Service Configuration
-------------------------------------------------------------------------------

The supplied systemd service file is:

    scanhub-api.service

Important settings:

    User=scanhub
    Group=scanhub
    SupplementaryGroups=clamav

    WorkingDirectory=/home/ScanHub-API

    ExecStart=/home/ScanHub-API/run.sh

Verify the service file:

    systemctl cat scanhub-api

Verify the service user:

    systemctl show scanhub-api | grep User

Verify the working directory:

    systemctl show scanhub-api | grep WorkingDirectory

-------------------------------------------------------------------------------
6. Enable the Service
-------------------------------------------------------------------------------

The systemd service file:

scanhub-api.service

Reload systemd:

systemctl daemon-reload

Enable the service:

systemctl enable scanhub-api.service

Start the service:

systemctl start scanhub-api.service

Check status:

systemctl status scanhub-api.service

===============================================================================
CLAMAV CONFIGURATION
===============================================================================

Determine the ClamAV socket location:

grep -E '^(LocalSocket|LocalSocketGroup|LocalSocketMode|TCPSocket|TCPAddr)' \
/etc/clamd.d/scan.conf

Example output:

LocalSocket /run/clamd.scan/clamd.sock
LocalSocketGroup clamav
LocalSocketMode 660

Update ClamAV MaxThreads:

2 vCPU  -> MaxThreads 4-8
4 vCPU  -> MaxThreads 8-16
8 vCPU  -> MaxThreads 16-32

Scanner pools are configured in config.py.

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
            }
        ]
    }
}

For multiple ClamAV instances see:

scanning_pool_readme.txt

Update the API key:

API_KEY = "replace-with-a-real-secret"

Replace this value with a strong secret before production use.

===============================================================================
CREATE UPLOAD DIRECTORY
===============================================================================

The API temporarily stores files before scanning.

Create the directory:

mkdir -p /var/lib/scanhub/tmp

Set ownership:

chown scanhub:scanhub /var/lib/scanhub/tmp

Set permissions:

chmod 770 /var/lib/scanhub/tmp

Enable SetGID:

chmod g+s /var/lib/scanhub/tmp

Verify:

ls -ld /var/lib/scanhub/tmp

Expected:

drwxrws--- scanhub scanhub

===============================================================================
CREATE LOG DIRECTORY
===============================================================================

ScanHub writes application logs to:

    /home/ScanHub-API/logs

The ScanHub service runs as:

    User=scanhub
    Group=scanhub

The scanhub user must have write access to the logs directory and any
existing log files.

Create the directory if it does not already exist:

    mkdir -p /home/ScanHub-API/logs

Set ownership:

    chown -R scanhub:scanhub /home/ScanHub-API/logs

Set permissions:

    chmod 750 /home/ScanHub-API/logs

If a log file already exists, ensure ownership is correct:

    chown scanhub:scanhub \
        /home/ScanHub-API/logs/scan_api.log

Verify write access:

    sudo -u scanhub touch \
        /home/ScanHub-API/logs/testfile

    rm -f \
        /home/ScanHub-API/logs/testfile

Verify ownership:

    ls -ld /home/ScanHub-API/logs
    ls -l /home/ScanHub-API/logs

Expected:

    drwxr-x--- scanhub scanhub logs

IMPORTANT:

If permissions are incorrect the service may fail to start with an error
similar to:

    PermissionError: [Errno 13] Permission denied:
    '/home/ScanHub-API/logs/scan_api.log'

NOTE:

If the application has previously been started as root, existing files such as:

    logs/scan_api.log
    /var/lib/scanhub/metrics.db

may need their ownership corrected manually before the service will start.


===============================================================================
CREATE METRICS DATABASE DIRECTORY
===============================================================================

ScanHub stores persistent metrics in SQLite.

The default database location is:

    /var/lib/scanhub/metrics.db

The ScanHub service runs as the following user:

    User=scanhub
    Group=scanhub

The scanhub user must have write access to both the database file and the
directory containing it.

Create the directory if it does not already exist:

    mkdir -p /var/lib/scanhub

Set ownership:

    chown scanhub:scanhub /var/lib/scanhub

Set permissions:

    chmod 775 /var/lib/scanhub

If a metrics database already exists, update ownership:

    chown scanhub:scanhub /var/lib/scanhub/metrics.db

Verify access:

    sudo -u scanhub touch /var/lib/scanhub/testfile

    rm -f /var/lib/scanhub/testfile

Verify ownership and permissions:

    ls -ld /var/lib/scanhub
    ls -l /var/lib/scanhub/metrics.db

Expected:

    drwxrwxr-x scanhub scanhub /var/lib/scanhub

    -rw-r--r-- scanhub scanhub /var/lib/scanhub/metrics.db

IMPORTANT:

SQLite WAL mode is enabled. The scanhub user must be able to create
additional files in the same directory, including:

    metrics.db-wal
    metrics.db-shm

If ownership or permissions are incorrect the service may fail to start
with an error similar to:

    sqlite3.OperationalError: attempt to write a readonly database

===============================================================================
APPLICATION CONFIGURATION
===============================================================================

Add the following values to your client application's configuration 
(for example, settings.py or system.py).


IMPORTANT:

Use the hostname rather than localhost, especially on single-machine
development environments.

SCANHUB_API_URL = "https://devlocal.localhost.lan:9433"
SCANHUB_API_KEY = "replace-with-a-real-secret"
SCANHUB_API_TIMEOUT = 300
SCANHUB_VERIFY_HTTPS_CERT = False

===============================================================================
RUNNING THE API
===============================================================================

ScanHub API runs using Hypercorn with TLS enabled.

Benefits:

- HTTP/2 support
- Persistent client connections
- Lower overhead under high concurrency
- Better performance when processing many small scan requests

Run manually:

./run_https.sh

Run as a service:

systemctl start scanhub-api.service

Restart:

systemctl restart scanhub-api.service

Stop:

systemctl stop scanhub-api.service

===============================================================================
LOG FILES
===============================================================================

Logs are stored under:

/home/ScanHub-API/logs

Example:

/home/ScanHub-API/logs/scan_api.log

===============================================================================
HTTPS CONFIGURATION
===============================================================================

ScanHub HTTPS deployments use Hypercorn.

Example startup:

    hypercorn --config hypercorn.toml scan_api:app

HTTP/2 is automatically enabled when:

    - TLS is configured
    - The client supports HTTP/2

No API code changes are required to use HTTP/2.

SCANHUB_API_URL = "https://devlocal.localhost.lan:9443"

Start the API manually using HTTPS:

./run_https.sh

===============================================================================
HTTP/2 SDK CONNECTION REUSE
===============================================================================

The ScanHub Python SDK uses httpx with connection pooling enabled.

A persistent HTTP client is maintained for the lifetime of the SDK instance.

Benefits:

- TCP connections are reused.
- TLS sessions can be reused.
- HTTP/2 multiplexing is supported.
- Connection establishment overhead is reduced.
- Automatic reconnection occurs when a server closes an idle connection.

When used from long-running processes such as Celery workers, subsequent
requests can reuse existing pooled connections rather than establishing a new
connection for every scan request.

Each worker process maintains its own independent connection pool.
Connections are not shared between worker processes.

To test HTTP/2 you can use this command:

curl -kv --http2 https://devlocal.localhost.lan:9443/health

===============================================================================
USING THE PYTHON SDK
===============================================================================

See sdk_readme.txt for:
- SDK installation
- Python examples
- Django integration
- Error handling
- scan_file()
- scan_bytes()
- scan_stream()

===============================================================================
HEALTH CHECK ENDPOINT
===============================================================================

HTTPS:

NOTE:

-k means insecure and is required when using a self-signed certificate.

curl -k https://devlocal.localhost.lan:9443/health

RESPONSE:

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
    ],
    "version": "1.0.0"
}

===============================================================================
METRICS ENDPOINT
===============================================================================

Metrics are stored in:

/var/lib/scanhub/metrics.db

Metrics survive service restarts and are shared between multiple Hypercorn workers.

HTTPS:

NOTE:

-k means insecure and is required when using a self-signed certificate.

curl -k https://devlocal.localhost.lan:9443/metrics

RESPONSE:

{
    "version": "1.0.0",
    "pid": 12345,
    "uptime_seconds": 3600,
    "scanner_pool": {
        "pool_strategy": "least_connections",
        "scanner_count": 2,
        "instances": [
            {
                "scanner_instance": "clamav_scan_1",
                "engine": "clamav",
                "scan_mode": "fildes",
                "active_connections": 0
            },
            {
                "scanner_instance": "clamav_scan_2",
                "engine": "clamav",
                "scan_mode": "fildes",
                "active_connections": 0
            }
        ]
    },
    "total_scans": 100,
    "clean_scans": 99,
    "infected_scans": 1,
    "scan_errors": 0,
    "total_bytes": 1048576,
    "largest_file": 524288,
    "average_scan_ms": 2.5,
    "average_upload_ms": 0.2
}

NOTE:

If the service fails during startup with:

    sqlite3.OperationalError: attempt to write a readonly database

verify ownership of:

    /var/lib/scanhub
    /var/lib/scanhub/metrics.db

Both must be writable by the scanhub service account.


===============================================================================
API KEY VALIDATION TESTING
===============================================================================

Invalid API keys should be rejected.

Example:

curl -k -i -X POST \
    -H "X-API-Key: incorrect-key" \
    -F "file=@testfile.txt" \
    https://devlocal.localhost.lan:9443/scan

Expected:

HTTP/1.1 401 Unauthorized

{
    "ok": false,
    "detail": "Unauthorized"
}


===============================================================================
FILE SCAN TESTING
===============================================================================

I recommend installing jk to improve what you get returned when using curl.
To use this install jk and add '| jq' at the end of your curl commands.

dnf install jq



HTTPS:

curl -k -X POST \
    -H "X-API-Key: replace-with-a-real-secret" \
    -F "file=@testfile.txt" \
    https://devlocal.localhost.lan:9443/scan

HTTPS EICAR Test:

curl -k -X POST \
    -H "X-API-Key: replace-with-a-real-secret" \
    -F "file=@eicar.txt" \
    https://devlocal.localhost.lan:9443/scan

Example Response:

{
    "filename": "testfile.txt",
    "size": 619,
    "upload_ms": 0.25,
    "scan_ms": 2.98,
    "ok": true,
    "engine": "clamav",
    "scan_mode": "fildes",
    "scanner_instance": "clamav_scan_1",
    "socket": "/run/clamd.scan/clamd.sock",
    "raw_result": "fdOK",
    "clean": true,
    "infected": false,
    "virus": null,
    "error": null,
    "pool_strategy": "least_connections"
}

===============================================================================
STREAM SCAN TESTING
===============================================================================

Update replace-with-a-real-secret to match the value configured
in config.py.

HTTP:

curl -X POST \
    -H "X-API-Key: replace-with-a-real-secret" \
    -H "Content-Type: application/octet-stream" \
    --data-binary @testfile.txt \
    http://devlocal.localhost.lan:8000/scan/stream

HTTPS:

curl -k -X POST \
    -H "X-API-Key: replace-with-a-real-secret" \
    -H "Content-Type: application/octet-stream" \
    --data-binary @testfile.txt \
    https://devlocal.localhost.lan:9443/scan/stream

HTTPS Stream Scan With Filename:

curl -k -X POST \
    -H "X-API-Key: replace-with-a-real-secret" \
    -H "X-Filename: testfile.txt" \
    -H "Content-Type: application/octet-stream" \
    --data-binary @testfile.txt \
    https://devlocal.localhost.lan:9443/scan/stream

===============================================================================
POSITIVE VIRUS TEST USING EICAR
===============================================================================

The EICAR file is a safe test file that ClamAV will detect as malware.

Create the file (single line):

echo 'X5O!P%@AP[4\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*' > eicar.txt

Create the file (multi-line):

cat > eicar.txt <<'EOF'
X5O!P%@AP[4\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*
EOF

Verify:

cat eicar.txt

===============================================================================
Test Using HTTP
===============================================================================

curl -X POST \
    -H "X-API-Key: replace-with-a-real-secret" \
    -F "file=@eicar.txt" \
    http://devlocal.localhost.lan:8000/scan

===============================================================================
Test Using HTTPS
===============================================================================

curl -k -X POST \
    -H "X-API-Key: replace-with-a-real-secret" \
    -F "file=@eicar.txt" \
    https://devlocal.localhost.lan:9443/scan

===============================================================================
Test Using HTTPS Stream Endpoint
===============================================================================

curl -k -X POST \
    -H "X-API-Key: replace-with-a-real-secret" \
    -H "X-Filename: eicar.txt" \
    -H "Content-Type: application/octet-stream" \
    --data-binary @eicar.txt \
    https://devlocal.localhost.lan:9443/scan/stream

===============================================================================
Expected Response
===============================================================================

{
    "ok": true,
    "filename": "eicar.txt",
    "size": 69,
    "engine": "clamav",
    "scan_mode": "fildes",
    "scanner_instance": "clamav_scan_1",
    "clean": false,
    "infected": true,
    "virus": "Eicar-Test-Signature",
    "error": null,
    "pool_strategy": "least_connections"
}

Depending on the ClamAV version, the virus name may also appear as:

Eicar-Test-Signature

===============================================================================
GENERATING HTTPS CERTIFICATES
===============================================================================

Generate a self-signed certificate:

openssl req -x509 -nodes -days 365 \
    -newkey rsa:2048 \
    -keyout scanhubapi.key \
    -out scanhubapi.crt

Example answers:

Country Name (2 letter code) []: GB
State or Province Name (full name) []: Wales
Locality Name (eg, city) []: Bangor
Organization Name (eg, company) []: localhost
Organizational Unit Name (eg, section) []: DEV
Common Name (eg, your name or your server's hostname) []: devlocal.localhost.lan

IMPORTANT:

The Common Name should match the hostname used to access the API.

Example:

devlocal.localhost.lan

===============================================================================
UNIT TEST
===============================================================================

Install:

pip3 install pytest pytest-mock httpx

Run all tests:

pytest -v tests/

Run SDK tests:

pytest -v tests/test_sdk.py

Run metrics tests:

pytest -v tests/test_metrics.py

===============================================================================
NOTES
===============================================================================

- Replace all occurrences of "replace-with-a-real-secret" with your actual API key.
- HTTPS examples use -k because the certificate is self-signed.
- The scanhub user must be a member of the clamav group.
- The API requires access to both the ClamAV Unix socket(s) and
  /var/lib/scanhub/tmp.
- For security, ClamAV TCP port 3310 should be disabled if the API is using
  Unix sockets only.
- The current recommended scanning engine is ClamAV FILDES (clamav_fildes).
- FILDES mode scans files using file descriptor passing (SCM_RIGHTS) and avoids
  transmitting file contents through the ClamAV socket, reducing overhead and
  improving scalability.
- Scanner pools support one or more scanner instances.
- Supported pool strategies are:
    - least_connections
    - round_robin
    - first
- The scanner_instance field in scan results identifies which scanner backend
  processed the request.
- Multiple ClamAV instances can be configured using separate Unix sockets.
- Scanner pools are generic and are designed to support additional antivirus
  engines in the future.
- Each ClamAV instance loads its own virus database into memory; running
  multiple instances will increase RAM consumption.
- Unit tests included.
- SDK included with sdk_readme.txt which details how to use in Python.
- Metrics are persisted to SQLite and survive service restarts.
- SQLite WAL mode is enabled to support multiple concurrent workers.
- Active scanner connection counts are maintained per Hypercorn worker process.
- Metrics statistics are shared across workers using SQLite.
- The Python SDK uses httpx and supports HTTP/2 when available.
- HTTP/2 requires HTTPS/TLS.
- SDK connection pools are maintained per process.
- HTTP/2 negotiation occurs automatically when supported by both client and server.
- The SDK automatically falls back to HTTP/1.1 when HTTP/2 is unavailable.
