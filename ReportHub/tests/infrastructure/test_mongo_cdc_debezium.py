import base64
import os
from pathlib import Path
import secrets
import subprocess
import unittest
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(
    os.environ.get("REPORTHUB_MONGO_CDC_DEBEZIUM_TEST") == "1",
    "Set REPORTHUB_MONGO_CDC_DEBEZIUM_TEST=1 for the isolated Debezium probe.",
)
class MongoCdcDebeziumTests(unittest.TestCase):
    def test_snapshot_and_change_event_shapes(self):
        environment = os.environ.copy()
        environment.update({
            "MONGO_CDC_TEST_PASSWORD": secrets.token_urlsafe(32),
            "MONGO_CDC_TEST_FIELD_KEY": base64.urlsafe_b64encode(os.urandom(32)).decode(),
        })
        command = [
            "docker", "compose", "--env-file", "/dev/null",
            "-p", f"reporthub-mongo-cdc-{uuid4().hex}",
            "-f", "tests/docker-compose.mongo-cdc.yml",
        ]
        try:
            result = subprocess.run(
                [
                    *command, "up", "--pull", "never",
                    "--abort-on-container-exit", "--exit-code-from", "probe",
                    "--attach", "probe",
                ],
                cwd=ROOT, env=environment, capture_output=True, text=True, timeout=600,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("MONGO_CDC_DEBEZIUM_PROBE_PASSED", result.stdout)
            print(result.stdout, end="")
        finally:
            subprocess.run(
                [*command, "down", "--remove-orphans", "--timeout", "10"],
                cwd=ROOT, env=environment, check=True, timeout=90,
            )
