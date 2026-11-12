import base64
import os
from pathlib import Path
import secrets
import subprocess
import unittest
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(
    os.environ.get("REPORTHUB_IDENTITY_TEST") == "1",
    "Set REPORTHUB_IDENTITY_TEST=1 to run disposable database identity tests.",
)
class ServiceIdentityTests(unittest.TestCase):
    def test_isolated_database_permissions(self):
        environment = os.environ.copy()
        environment.update({
            "IDENTITY_ADMIN_PASSWORD": secrets.token_urlsafe(32),
            "IDENTITY_RUNTIME_PASSWORD": secrets.token_urlsafe(32),
            "IDENTITY_MIGRATION_PASSWORD": secrets.token_urlsafe(32),
            "IDENTITY_FIELD_KEY": base64.urlsafe_b64encode(os.urandom(32)).decode(),
        })
        command = [
            "docker", "compose", "--env-file", "/dev/null",
            "-p", f"reporthub-identities-{uuid4().hex}",
            "-f", "tests/docker-compose.identities.yml",
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
            self.assertIn("IDENTITY_PERMISSION_TESTS_PASSED", result.stdout)
            print(result.stdout, end="")
        finally:
            subprocess.run(
                [*command, "down", "--volumes", "--remove-orphans", "--timeout", "10"],
                cwd=ROOT, env=environment, check=True, timeout=60,
            )
