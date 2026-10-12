================================================================================
ScanHub Python SDK
================================================================================

The ScanHub SDK provides a simple Python interface for submitting files to the
ScanHub Virus Scanning API.

The SDK can be used with:

- Django
- Flask
- FastAPI
- Celery
- Standalone Python scripts
- Any Python application

================================================================================
Requirements
================================================================================

Install the httpx module:

    pip install httpx

Copy the SDK file into your project:

    myproject/
    ├── app/
    ├── scanhub.py
    ├── manage.py
    └── settings.py

The SDK file is located at:

    sdk/scanhub.py

================================================================================
Importing the SDK
================================================================================

Import the client:

    from scanhub import ScanHubClient

Create a client instance:

    client = ScanHubClient(
        api_url="https://scanhub.company.com:9443",
        api_key="replace-with-a-real-secret",
        verify_ssl=True
    )

For development environments with self-signed certificates:

    client = ScanHubClient(
        api_url="https://localhost:9443",
        api_key="replace-with-a-real-secret",
        verify_ssl=False
    )

================================================================================
SDK CONFIGURATION
================================================================================

Add the following values to your application's config.py:

SCANHUB_API_URL = "https://localhost:9443"
SCANHUB_API_KEY = "replace-with-a-real-secret"

SCANHUB_API_TIMEOUT = 300

SCANHUB_API_RETRIES = 1
SCANHUB_API_RETRY_DELAY = 1

SCANHUB_VERIFY_HTTPS_CERT = False

Recommended values:

SCANHUB_API_TIMEOUT = 300
SCANHUB_API_RETRIES = 1
SCANHUB_API_RETRY_DELAY = 1

Retries are optional.

If SCANHUB_API_RETRIES or SCANHUB_API_RETRY_DELAY are not configured,
the SDK defaults to:

SCANHUB_API_RETRIES = 1
SCANHUB_API_RETRY_DELAY = 1

SCANHUB_VERIFY_HTTPS_CERT should normally be True.

For development environments using self-signed certificates it can be set to:

    SCANHUB_VERIFY_HTTPS_CERT = False

Production environments should use a trusted certificate and leave verification enabled.

================================================================================
CREATING A CLIENT
================================================================================

Manual:

    from scanhub import ScanHubClient

    client = ScanHubClient(
        api_url=SCANHUB_API_URL,
        api_key=SCANHUB_API_KEY,
        timeout=SCANHUB_API_TIMEOUT,
        verify_ssl=SCANHUB_VERIFY_HTTPS_CERT
    )   

Recommended:

    from scanhub import ScanHubClient

    client = ScanHubClient.from_config()
    result = client.scan_file("/tmp/test.txt")

    if result["clean"]:
        print("File is clean")
    else:
        print("File is not clean")

The SDK will automatically use the values configured in config.py.

================================================================================
RETRY BEHAVIOUR
================================================================================

Summary:

Retries are for temporary connection/API availability issues only.
Retries are not for virus detections, permission errors, invalid API keys,
or scan result errors.

The SDK supports automatic retry logic for temporary connection issues.

Retries are performed for:

- Connection failures
- Connection timeouts
- HTTP 502
- HTTP 503
- HTTP 504

Retries are NOT performed for:

- Invalid API keys (401)
- Permission errors
- Virus detections
- Scan failures returned by the API
- Invalid requests

Example:

    SCANHUB_API_RETRIES = 1

This means:

    Attempt 1

        If successful:
            Return response

        If temporary failure:
            Wait SCANHUB_API_RETRY_DELAY seconds

    Attempt 2

        Return response
        OR raise exception

================================================================================
USING THE SDK WITH CONFIG.PY
================================================================================

Example:

    from scanhub import ScanHubClient

    client = ScanHubClient.from_config()

    result = client.scan_file(
        "/tmp/test.txt"
    )

    print(result)

================================================================================
CLEAN FILE EXAMPLE
================================================================================

Response:

    {
        "filename": "test.txt",
        "size": 619,
        "upload_ms": 0.14,
        "scan_ms": 7.52,
        "ok": True,
        "engine": "clamav",
        "scan_mode": "fildes",
        "clean": True,
        "infected": False,
        "virus": None,
        "error": None
    }

Example:

    if result["clean"]:
        print("SAFE")
    else:
        print("UNSAFE")

Output:

    SAFE

================================================================================
INFECTED FILE EXAMPLE
================================================================================

Response:

    {
        "filename": "eicar.txt",
        "size": 69,
        "ok": True,
        "engine": "clamav",
        "scan_mode": "fildes",
        "clean": False,
        "infected": True,
        "virus": "Eicar-Test-Signature",
        "error": None
    }

Example:

    if result["infected"]:
        print(
            f"Virus found: "
            f"{result['virus']}"
        )

Output:

    Virus found: Eicar-Test-Signature

================================================================================
BEST PRACTICE
================================================================================

Create the client once:

    client = ScanHubClient.from_config()

Reuse the client throughout the application.

Recommended production settings:

    SCANHUB_API_TIMEOUT = 300
    SCANHUB_API_RETRIES = 1
    SCANHUB_API_RETRY_DELAY = 1

The SDK communicates only with the ScanHub API.

The SDK does not communicate directly with ClamAV.

The ScanHub server determines which scanning engine and scan mode
(clamav_fildes, clamav_file, etc.) is used.

================================================================================
Health Check
================================================================================

Example:

    status = client.health()

    print(status)

Typical Response:

    {
        "ok": True,
        "engine": "clamav",
        "scan_mode": "fildes",
        "clamd": "ClamAV 1.4.3"
    }

Validate Health:

    if status["ok"]:
        print("ScanHub is healthy")
    else:
        print("ScanHub is unavailable")

Output:

    ScanHub is healthy

================================================================================
Scanning a File
================================================================================

Example:

    result = client.scan_file("/tmp/test.txt")

    print(result)

Typical Clean Response:

    {
        "filename": "test.txt",
        "size": 619,
        "upload_ms": 0.14,
        "scan_ms": 7.52,
        "ok": True,
        "engine": "clamav",
        "scan_mode": "fildes",
        "clean": True,
        "infected": False,
        "virus": None,
        "error": None
    }

================================================================================
Determining if a File is Clean
================================================================================

Recommended Approach:

    result = client.scan_file("/tmp/test.txt")

    if result["clean"]:
        print("SAFE")
    else:
        print("UNSAFE")

Output:

    SAFE

You may also explicitly check:

    if result["clean"] is True:
        print("The file passed the virus scan")

Output:

    The file passed the virus scan

================================================================================
Simple Validation Example
================================================================================

Most applications only need the following:

    result = client.scan_file("/tmp/upload.txt")

    if not result["clean"]:
        raise Exception(
            f"Upload rejected. Virus detected: "
            f"{result.get('virus')}"
        )

    print("Upload accepted")

Output:

    Upload accepted

================================================================================
Scan Data Already Held in Memory
================================================================================

Example:

    data = b"Hello World"

    result = client.scan_bytes(
        data,
        filename="test.txt"
    )

    print(result["clean"])

Output:

    True

================================================================================
Scan a Raw Byte Stream
================================================================================

Example:

    data = b"Hello World"

    result = client.scan_stream(
        data,
        filename="stream.txt"
    )

    print(result)

================================================================================
Using the SDK in Django
================================================================================

Example settings.py:

    SCANHUB_API_URL = "https://localhost:9443"

    SCANHUB_API_KEY = "replace-with-a-real-secret"

Example Usage:

    from django.conf import settings

    from scanhub import ScanHubClient


    client = ScanHubClient(
        api_url=settings.SCANHUB_API_URL,
        api_key=settings.SCANHUB_API_KEY,
        verify_ssl=False
    )

================================================================================
Scanning Uploaded Django Files
================================================================================

Example:

    from tempfile import NamedTemporaryFile

    from django.conf import settings

    from scanhub import ScanHubClient


    client = ScanHubClient(
        api_url=settings.SCANHUB_API_URL,
        api_key=settings.SCANHUB_API_KEY,
        verify_ssl=False
    )


    def scan_uploaded_file(uploaded_file):

        with NamedTemporaryFile(delete=True) as tmp:

            for chunk in uploaded_file.chunks():
                tmp.write(chunk)

            tmp.flush()

            return client.scan_file(tmp.name)

Usage:

    result = scan_uploaded_file(
        request.FILES["file"]
    )

    if result["clean"]:
        print("File passed virus scan")
    else:
        print(
            f"Virus detected: "
            f"{result['virus']}"
        )

================================================================================
Exception Handling
================================================================================

The SDK raises the following exceptions:

    ScanHubError
    ScanHubAPIError
    ScanHubConnectionError

Example:

    try:

        result = client.scan_file("/tmp/test.txt")

    except ScanHubConnectionError:
        print("Unable to connect to ScanHub")

    except ScanHubAPIError as exc:
        print(f"API error: {exc}")

================================================================================
Recommended Production Check
================================================================================

This is the simplest and recommended implementation:

    result = client.scan_file(file_path)

    if result.get("clean"):
        print("SAFE")
    else:
        print("UNSAFE")

Typical Output:

    SAFE

or

    UNSAFE

A clean file will return:

    clean = True
    infected = False
    virus = None
    error = None

An infected file will return:

    clean = False
    infected = True
    virus = <Virus Name>

================================================================================
USING WITH CELERY
================================================================================

When used from Celery workers, create the ScanHub client once and reuse it.

Recommended:

    client = ScanHubClient.from_config()

    @shared_task
    def scan_document(path):
        return client.scan_file(path)

This allows the SDK connection pool to be reused for subsequent tasks handled
by the same worker process.

Each Celery worker process maintains its own independent connection pool.
Connections are not shared between worker processes.

================================================================================
SCANHUB API COMPATIBILITY
================================================================================

The SDK works with:

- Hypercorn
- FastAPI
- HTTP/1.1
- HTTP/2
- HTTPS
- Self-signed certificates
- Trusted certificates

For best performance, HTTPS with Hypercorn and HTTP/2 is recommended.


