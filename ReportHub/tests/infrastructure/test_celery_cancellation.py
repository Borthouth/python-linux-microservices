import base64
import os
from pathlib import Path
import secrets
import subprocess
import unittest
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(
    os.environ.get("REPORTHUB_CELERY_CANCELLATION_TEST") == "1",
    "Set REPORTHUB_CELERY_CANCELLATION_TEST=1 to run disposable Celery cancellation tests.",
)
class CeleryCancellationIntegrationTests(unittest.TestCase):
    def test_worker_termination_stops_active_clickhouse_query(self):
        environment = os.environ.copy()
        environment.update({
            "CANCELLATION_TEST_PASSWORD": secrets.token_urlsafe(32),
            "CANCELLATION_TEST_FIELD_KEY": base64.urlsafe_b64encode(
                os.urandom(32)
            ).decode(),
        })
        command = [
            "docker", "compose", "--env-file", "/dev/null",
            "-p", f"reporthub-cancellation-{uuid4().hex}",
            "-f", "tests/docker-compose.celery-cancellation.yml",
        ]
        try:
            result = subprocess.run(
                [
                    *command, "up", "--pull", "never",
                    "--abort-on-container-exit", "--exit-code-from", "probe",
                    "--attach", "probe",
                ],
                cwd=ROOT, env=environment, capture_output=True, text=True,
                timeout=240,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            print(result.stdout, end="")
        finally:
            subprocess.run(
                [*command, "down", "--volumes", "--remove-orphans", "--timeout", "10"],
                cwd=ROOT, env=environment, check=True, timeout=60,
            )
