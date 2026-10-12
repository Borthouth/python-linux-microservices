#!/usr/bin/env python3

"""
Run using:
nohup python3 -u redis_check.py > redis_check.log 2>&1 &
"""

import os
import socket
import shutil
import subprocess
import time

from datetime import datetime

import smtplib
from email.mime.text import MIMEText


# =============================================================================
# CONFIGURATION
# =============================================================================

REDIS_PORT = 6379
REDIS_SERVICE = "redis"
RDB_FILE = "/var/lib/redis/dump.rdb"
REDIS_PASSWORD = ""

CREATE_NEW_RDB_IF_MISSING = True
CREATE_NEW_RDB_AFTER_RECOVERY = True

CHECK_INTERVAL = 60

EMAIL_FROM = "redis-monitor@test.com"

EMAIL_TO = [
    "email1@test.com",
    "email2@test.com",
]

SMTP_SERVER = "127.0.0.1"
SMTP_PORT = 25


# =============================================================================
# FUNCTIONS
# =============================================================================


def log(msg):

    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} - {msg}"

    try:
        with open(
            "/home/RedisCheck/redis_check.log",
            "a"
        ) as fh:
            fh.write(line + "\n")
    except Exception as e:
        print(f"LOGGING ERROR: {e}", flush=True)

def is_port_open(host="127.0.0.1", port=6379, timeout=5):
    try:
        with socket.create_connection((host, port), timeout):
            return True
    except OSError:
        return False


def is_redis_active():
    result = subprocess.run(
        ["systemctl", "is-active", "--quiet", REDIS_SERVICE],
        check=False
    )

    return result.returncode == 0


def check_rdb():

    result = subprocess.run(
        ["redis-check-rdb", RDB_FILE],
        capture_output=True,
        text=True
    )

    output = (
        (result.stdout or "") +
        (result.stderr or "")
    )

    valid = result.returncode == 0

    return valid, output


def get_redis_logs(lines=100):

    result = subprocess.run(
        [
            "journalctl",
            "-u",
            REDIS_SERVICE,
            "-n",
            str(lines),
            "--no-pager"
        ],
        capture_output=True,
        text=True
    )

    return result.stdout


def stop_redis():

    log("Stopping Redis service")

    subprocess.run(
        ["systemctl", "stop", REDIS_SERVICE],
        check=False
    )


def start_redis():

    log("Starting Redis service")

    subprocess.run(
        ["systemctl", "start", REDIS_SERVICE],
        check=False
    )


def rename_rdb():

    timestamp = datetime.now().strftime(
        "%Y%m%d-%H%M%S"
    )

    new_name = f"{RDB_FILE}-{timestamp}"

    shutil.move(
        RDB_FILE,
        new_name
    )

    return new_name


def create_new_rdb():

    cmd = ["redis-cli"]

    if REDIS_PASSWORD not in ["", None]:
        cmd.extend([
            "-a",
            REDIS_PASSWORD
        ])

    cmd.append("BGSAVE")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )

    return (
        result.returncode == 0,
        result.stdout + result.stderr
    )


def send_email(subject, body):

    try:

        msg = MIMEText(body)

        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(EMAIL_TO)

        smtp = smtplib.SMTP(
            SMTP_SERVER,
            SMTP_PORT,
            timeout=30
        )

        smtp.sendmail(
            EMAIL_FROM,
            EMAIL_TO,
            msg.as_string()
        )

        smtp.quit()

        log(
            f"Email sent successfully: {subject}"
        )

    except Exception as exc:

        log(
            f"Failed to send email: {str(exc)}"
        )


# =============================================================================
# MAIN RECOVERY LOGIC
# =============================================================================


def recover_redis(rdb_valid=None, rdb_output=None):

    hostname = socket.gethostname()

    if not os.path.exists(RDB_FILE):

        send_email(
            f"[CRITICAL] Redis Down - No RDB ({hostname})",
            f"""
Server: {hostname}

Redis is down but the RDB file does not exist.

Expected file:

{RDB_FILE}

Manual investigation required.
"""
        )

        return

    if rdb_valid is None:
        log("Running redis-check-rdb")
        rdb_valid, rdb_output = check_rdb()

    redis_logs_before = get_redis_logs()

    if rdb_valid:
        return

    stop_redis()

    time.sleep(5)

    renamed_file = rename_rdb()

    log(f"Renamed {RDB_FILE} to {renamed_file}")

    start_redis()

    time.sleep(10)

    bgsave_ok = False
    bgsave_output = "BGSAVE disabled"

    if CREATE_NEW_RDB_AFTER_RECOVERY:

        bgsave_ok, bgsave_output = create_new_rdb()

        log(f"BGSAVE success: {bgsave_ok}")
        log(f"BGSAVE output: {bgsave_output.strip()}")

        time.sleep(10)

    new_rdb_exists = os.path.exists(RDB_FILE)

    if new_rdb_exists:

        log(
            f"New dump.rdb created "
            f"({os.path.getsize(RDB_FILE)} bytes)"
        )

    else:

        log("New dump.rdb not found")

    redis_logs_after = get_redis_logs()

    service_active = is_redis_active()

    port_open = is_port_open(
        "127.0.0.1",
        REDIS_PORT
    )

    restart_ok = (
        service_active and
        port_open
    )

    send_email(
        f"[CRITICAL] Redis RDB Corruption Detected ({hostname})",
        f"""
Redis corruption recovery was triggered.

Server:
{hostname}

Redis Service:
{REDIS_SERVICE}

Redis Port:
{REDIS_PORT}

Original RDB:
{RDB_FILE}

Renamed RDB:
{renamed_file}

Redis Restart Successful:
{'YES' if restart_ok else 'NO'}

New dump.rdb created:
{'YES' if new_rdb_exists else 'NO'}

======================================================================
redis-check-rdb Output
======================================================================

{rdb_output}

======================================================================
Redis Logs Before Recovery
======================================================================

{redis_logs_before}

======================================================================
Redis Logs After Recovery
======================================================================

{redis_logs_after}

======================================================================
BGSAVE Output
======================================================================

{bgsave_output}

======================================================================

Automatic Actions Performed:

1. Redis service stopped
2. Corrupt RDB renamed
3. Redis service restarted

Please investigate the cause of the corruption.
"""
    )

    if restart_ok:
        log("Redis successfully restarted")
    else:
        log("Redis failed to restart")


# =============================================================================
# MAIN LOOP
# =============================================================================

def main():

    log("Redis monitor started")

    while True:

        try:

            #
            # Always validate the RDB if it exists
            #
            if os.path.exists(RDB_FILE):

                rdb_valid, rdb_output = check_rdb()

                if not rdb_valid:

                    log("CORRUPT RDB DETECTED")

                    recover_redis(
                        rdb_valid=rdb_valid,
                        rdb_output=rdb_output
                    )

                    time.sleep(CHECK_INTERVAL)
                    continue

                else:

                    log("Redis RDB file healthy")

            else:

                log(
                    f"RDB file not found: {RDB_FILE}"
                )

                if (
                    CREATE_NEW_RDB_IF_MISSING
                    and is_redis_active()
                    and is_port_open("127.0.0.1", REDIS_PORT)
                ):

                    log("Attempting to create new dump.rdb")

                    bgsave_ok, bgsave_output = create_new_rdb()

                    log(f"BGSAVE success: {bgsave_ok}")
                    log(f"BGSAVE output: {bgsave_output.strip()}")

                    time.sleep(10)

                    if os.path.exists(RDB_FILE):

                        log(
                            f"New dump.rdb created "
                            f"({os.path.getsize(RDB_FILE)} bytes)"
                        )

                    else:

                        log(
                            "BGSAVE completed but dump.rdb still does not exist"
                        )


            #
            # Normal Redis health checks
            #
            port_open = is_port_open(
                "127.0.0.1",
                REDIS_PORT
            )

            service_active = is_redis_active()

            if port_open and service_active:

                log("Redis healthy")

            else:

                log(
                    f"Redis appears down "
                    f"(port_open={port_open}, "
                    f"service_active={service_active})"
                )

                log("Attempting Redis restart")

                start_redis()

                time.sleep(10)

                service_active = is_redis_active()

                port_open = is_port_open(
                    "127.0.0.1",
                    REDIS_PORT
                )

                if port_open and service_active:

                    log("Redis restart successful")

                else:

                    log(
                        "Redis restart failed, "
                        "running corruption recovery"
                    )

                    recover_redis()

        except Exception as exc:

            error = str(exc)

            log(f"ERROR: {error}")

            try:

                send_email(
                    "[CRITICAL] Redis Monitor Failure",
                    f"""
The Redis monitoring script failed.

Server:
{socket.gethostname()}

Error:
{error}
"""
                )

            except Exception:
                pass

        time.sleep(CHECK_INTERVAL)



if __name__ == "__main__":
    main()
