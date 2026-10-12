import os
import time
import logging
import tempfile
import sqlite3
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Header, HTTPException, Request
from fastapi.responses import ORJSONResponse
from starlette.concurrency import run_in_threadpool

from config import (
    API_KEY,
    MAX_FILE_SIZE,
    CHUNK_SIZE,
)

from scanners.factory import get_scanner_pool

app = FastAPI(
    title="Internal Virus Scan API",
    default_response_class=ORJSONResponse,
)

# -----------------------------------------------------------------------------
# Version
# -----------------------------------------------------------------------------

VERSION = "1.0.0"

# -----------------------------------------------------------------------------
# Paths / local settings
# -----------------------------------------------------------------------------

TEMP_DIR = "/var/lib/scanhub/tmp"

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "scan_api.log")

METRICS_DB = os.getenv(
    "SCANHUB_METRICS_DB",
    "/var/lib/scanhub/metrics.db"
)

def init_metrics_db():
    conn = sqlite3.connect(METRICS_DB)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY,
            total_scans INTEGER NOT NULL DEFAULT 0,
            clean_scans INTEGER NOT NULL DEFAULT 0,
            infected_scans INTEGER NOT NULL DEFAULT 0,
            scan_errors INTEGER NOT NULL DEFAULT 0,
            total_bytes INTEGER NOT NULL DEFAULT 0,
            largest_file INTEGER NOT NULL DEFAULT 0,
            total_scan_ms REAL NOT NULL DEFAULT 0,
            total_upload_ms REAL NOT NULL DEFAULT 0
        )
    """)

    conn.execute("""
        INSERT OR IGNORE INTO metrics (
            id,
            total_scans,
            clean_scans,
            infected_scans,
            scan_errors,
            total_bytes,
            largest_file,
            total_scan_ms,
            total_upload_ms
        )
        VALUES (1,0,0,0,0,0,0,0,0)
    """)

    conn.commit()
    conn.close()

os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

init_metrics_db()


# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------

logger = logging.getLogger("scan_api")
logger.setLevel(logging.INFO)
logger.propagate = False

if not logger.handlers:
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s"
    )

    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# -----------------------------------------------------------------------------
# Scanner setup
# -----------------------------------------------------------------------------

START_TIME = time.time()

scanner_pool = get_scanner_pool()

logger.info(
    "ScanHub version=%s scanner_pool_initialised=true",
    VERSION,
)


# -----------------------------------------------------------------------------
# Middleware
# -----------------------------------------------------------------------------

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    client_ip = request.client.host if request.client else "unknown"

    try:
        response = await call_next(request)
        duration = round((time.time() - start_time) * 1000, 2)

        logger.info(
            "REQUEST method=%s path=%s status=%s http_version=%s client_ip=%s duration_ms=%s",
            request.method,
            request.url.path,
            response.status_code,
            request.scope.get("http_version"),
            client_ip,
            duration
        )

        return response

    except Exception:
        duration = round((time.time() - start_time) * 1000, 2)

        logger.exception(
            "REQUEST_FAILED method=%s path=%s http_version=%s client_ip=%s duration_ms=%s",
            request.method,
            request.url.path,
            request.scope.get("http_version"),
            client_ip,
            duration
        )

        raise


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def check_api_key(x_api_key: Optional[str], request: Request) -> None:
    if not x_api_key or x_api_key != API_KEY:
        client_ip = request.client.host if request.client else "unknown"

        logger.warning("Unauthorized request from %s", client_ip)

        raise HTTPException(status_code=401, detail="Unauthorized")


def log_scan_result(
    *,
    upload_type: str,
    filename: Optional[str],
    size: int,
    temp_path: Optional[str],
    result: dict,
    scan_ms: float,
    upload_ms: float
) -> None:
    if result.get("infected"):
        logger.warning(
            "Virus detected upload_type=%s filename=%s size=%s virus=%s engine=%s scan_mode=%s upload_ms=%s scan_ms=%s",
            upload_type,
            filename,
            size,
            result.get("virus"),
            result.get("engine"),
            result.get("scan_mode"),
            upload_ms,
            scan_ms
        )

    elif result.get("error"):
        logger.error(
            "Scan error upload_type=%s filename=%s size=%s error=%s path=%s raw_result=%s engine=%s scan_mode=%s upload_ms=%s scan_ms=%s",
            upload_type,
            filename,
            size,
            result.get("error"),
            temp_path,
            result.get("raw_result"),
            result.get("engine"),
            result.get("scan_mode"),
            upload_ms,
            scan_ms
        )

    else:
        logger.info(
            "Scan clean upload_type=%s filename=%s size=%s engine=%s scan_mode=%s upload_ms=%s scan_ms=%s",
            upload_type,
            filename,
            size,
            result.get("engine"),
            result.get("scan_mode"),
            upload_ms,
            scan_ms
        )

def update_metrics(
    *,
    size: int,
    result: dict,
    scan_ms: float,
    upload_ms: float
):

    conn = sqlite3.connect(METRICS_DB)

    conn.execute("""
        UPDATE metrics
        SET
            total_scans = total_scans + 1,
            total_bytes = total_bytes + ?,
            total_scan_ms = total_scan_ms + ?,
            total_upload_ms = total_upload_ms + ?,
            largest_file = CASE
                WHEN ? > largest_file THEN ?
                ELSE largest_file
            END
        WHERE id = 1
    """, (
        size,
        scan_ms,
        upload_ms,
        size,
        size
    ))

    if result.get("infected"):

        conn.execute("""
            UPDATE metrics
            SET infected_scans = infected_scans + 1
            WHERE id = 1
        """)

    elif result.get("error"):

        conn.execute("""
            UPDATE metrics
            SET scan_errors = scan_errors + 1
            WHERE id = 1
        """)

    else:

        conn.execute("""
            UPDATE metrics
            SET clean_scans = clean_scans + 1
            WHERE id = 1
        """)

    conn.commit()
    conn.close()


# -----------------------------------------------------------------------------
# Exception handlers
# -----------------------------------------------------------------------------

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.warning(
        "HTTP error %s on %s: %s",
        exc.status_code,
        request.url.path,
        exc.detail
    )

    return ORJSONResponse(
        status_code=exc.status_code,
        content={"ok": False, "detail": exc.detail}
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception on %s", request.url.path)

    return ORJSONResponse(
        status_code=500,
        content={"ok": False, "detail": "Internal server error"}
    )


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

@app.get("/health")
def health():
    result = scanner_pool.health()
    result["version"] = VERSION

    logger.info(
        "Health check successful strategy=%s",
        result.get("pool_strategy")
    )

    return result


@app.get("/metrics")
def metrics():

    conn = sqlite3.connect(METRICS_DB)
    conn.row_factory = sqlite3.Row

    row = conn.execute(
        "SELECT * FROM metrics WHERE id = 1"
    ).fetchone()

    conn.close()

    total_scans = row["total_scans"]

    avg_scan_ms = (
        round(row["total_scan_ms"] / total_scans, 2)
        if total_scans else 0
    )

    avg_upload_ms = (
        round(row["total_upload_ms"] / total_scans, 2)
        if total_scans else 0
    )

    return {
        "version": VERSION,
        "pid": os.getpid(),
        "uptime_seconds": int(time.time() - START_TIME),

        "scanner_pool": scanner_pool.status(),

        "total_scans": row["total_scans"],
        "clean_scans": row["clean_scans"],
        "infected_scans": row["infected_scans"],
        "scan_errors": row["scan_errors"],

        "total_bytes": row["total_bytes"],
        "largest_file": row["largest_file"],

        "average_scan_ms": avg_scan_ms,
        "average_upload_ms": avg_upload_ms
    }

@app.post("/scan")
async def scan(
    request: Request,
    file: UploadFile = File(...),
    x_api_key: Optional[str] = Header(default=None)
):
    """
    Scan multipart/form-data upload.

    The upload is streamed to a temporary file in chunks.
    Python does not hold the full file in memory.

    The configured scanner decides whether to scan by file descriptor,
    file path, socket, ICAP, or another vendor-specific method.
    """

    check_api_key(x_api_key, request)

    temp_path = None
    total_size = 0

    upload_start = time.time()

    try:
        suffix = os.path.splitext(file.filename or "")[1]

        with tempfile.NamedTemporaryFile(
            mode="w+b",
            delete=False,
            suffix=suffix,
            dir=TEMP_DIR
        ) as tmp:
            temp_path = tmp.name

            while True:
                chunk = await file.read(CHUNK_SIZE)
                if not chunk:
                    break

                total_size += len(chunk)

                if total_size > MAX_FILE_SIZE:
                    logger.warning(
                        "Upload too large: filename=%s size=%s max=%s",
                        file.filename,
                        total_size,
                        MAX_FILE_SIZE
                    )

                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large. Max allowed is {MAX_FILE_SIZE} bytes."
                    )

                tmp.write(chunk)

            if total_size == 0:
                raise HTTPException(
                    status_code=400,
                    detail="Empty uploaded file"
                )

            tmp.flush()
            tmp.seek(0)

            # Update file permission
            os.chmod(temp_path, 0o644)

            upload_ms = round((time.time() - upload_start) * 1000, 2)

            logger.info(
                "Scanning file filename=%s size=%s path=%s",
                file.filename,
                total_size,
                temp_path
            )

            scan_start = time.time()

            result = await run_in_threadpool(
                scanner_pool.scan,
                temp_path,
                tmp.fileno(),
                file.filename,
                total_size
            )

            logger.info("Scanner selected=%s", result.get("scanner_instance"))

            scan_ms = round((time.time() - scan_start) * 1000, 2)

        log_scan_result(
            upload_type="multipart",
            filename=file.filename,
            size=total_size,
            temp_path=temp_path,
            result=result,
            upload_ms=upload_ms,
            scan_ms=scan_ms
        )

        update_metrics(
            size=total_size,
            result=result,
            scan_ms=scan_ms,
            upload_ms=upload_ms
        )

        return {
            "filename": file.filename,
            "size": total_size,
            "upload_ms": upload_ms,
            "scan_ms": scan_ms,
            **result
        }

    finally:
        await file.close()

        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                logger.warning(
                    "Failed to remove temp file %s",
                    temp_path,
                    exc_info=True
                )


@app.post("/scan/stream")
async def scan_stream(
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
    x_filename: Optional[str] = Header(default=None, alias="X-Filename")
):
    """
    Scan raw request-body bytes.

    Optional header:
        X-Filename: original file name used only for logging/response/suffix.

    This is useful for clients that do not want multipart/form-data.
    """

    check_api_key(x_api_key, request)

    temp_path = None
    total_size = 0

    upload_start = time.time()

    try:
        suffix = os.path.splitext(x_filename or "")[1]

        with tempfile.NamedTemporaryFile(
            mode="w+b",
            delete=False,
            suffix=suffix,
            dir=TEMP_DIR
        ) as tmp:
            temp_path = tmp.name

            async for chunk in request.stream():
                if not chunk:
                    continue

                total_size += len(chunk)

                if total_size > MAX_FILE_SIZE:
                    logger.warning(
                        "Stream too large: filename=%s size=%s max=%s",
                        x_filename,
                        total_size,
                        MAX_FILE_SIZE
                    )

                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large. Max allowed is {MAX_FILE_SIZE} bytes."
                    )

                tmp.write(chunk)

            if total_size == 0:
                raise HTTPException(
                    status_code=400,
                    detail="Empty request body"
                )

            tmp.flush()
            tmp.seek(0)

            # Update file permission
            os.chmod(temp_path, 0o644)

            upload_ms = round((time.time() - upload_start) * 1000, 2)

            logger.info(
                "Scanning stream filename=%s size=%s path=%s",
                x_filename,
                total_size,
                temp_path
            )

            scan_start = time.time()

            result = await run_in_threadpool(
                scanner_pool.scan,
                temp_path,
                tmp.fileno(),
                x_filename,
                total_size
            )

            scan_ms = round((time.time() - scan_start) * 1000, 2)

        log_scan_result(
            upload_type="stream",
            filename=x_filename,
            size=total_size,
            temp_path=temp_path,
            result=result,
            upload_ms=upload_ms,
            scan_ms=scan_ms
        )

        update_metrics(
            size=total_size,
            result=result,
            scan_ms=scan_ms,
            upload_ms=upload_ms
        )

        return {
            "filename": x_filename,
            "size": total_size,
            "upload_ms": upload_ms,
            "scan_ms": scan_ms,
            **result
        }

    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                logger.warning(
                    "Failed to remove temp file %s",
                    temp_path,
                    exc_info=True
                )