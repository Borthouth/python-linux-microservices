import base64
import os
from pathlib import Path
import secrets
import subprocess
import unittest
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(
    os.environ.get("REPORTHUB_CDC_CLEANUP_INVENTORY_TEST") == "1",
    "Set REPORTHUB_CDC_CLEANUP_INVENTORY_TEST=1 for disposable inventory tests.",
)
class CdcCleanupInventoryTests(unittest.TestCase):
    def test_isolated_inventory(self):
        self.run_probe(scenario="inventory")

    def test_confirmed_cleanup(self):
        self.run_probe(scenario="delete")

    def test_active_group_stops_cleanup(self):
        self.run_probe(scenario="active-group")

    def test_postgres_permission_failure_stops_cleanup(self):
        self.run_probe(scenario="postgres-permission")

    def test_worker_interruption_blocks_fresh_inventory(self):
        self.run_probe(scenario="worker-interrupted")

    def test_delayed_delete_after_worker_interruption(self):
        self.run_probe(scenario="worker-delete-interrupted")

    def test_clickhouse_permission_failure_stops_cleanup(self):
        self.run_probe(scenario="clickhouse-permission")

    def run_probe(self, scenario):
        environment = os.environ.copy()
        environment.update({
            "CLEANUP_TEST_PASSWORD": secrets.token_urlsafe(32),
            "CLEANUP_TEST_CLICKHOUSE_PASSWORD": secrets.token_urlsafe(32),
            "CLEANUP_TEST_FIELD_KEY": base64.urlsafe_b64encode(os.urandom(32)).decode(),
            "CLEANUP_TEST_DELETE": scenario,
        })
        command = [
            "docker", "compose", "--env-file", "/dev/null",
            "-p", f"reporthub-cleanup-inventory-{uuid4().hex}",
            "-f", "tests/docker-compose.cdc-cleanup.yml",
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
            self.assertIn("CDC_CLEANUP_INVENTORY_TESTS_PASSED", result.stdout)
            if scenario == "delete":
                self.assertIn("CDC_CLEANUP_DELETION_TESTS_PASSED", result.stdout)
            elif scenario == "active-group":
                self.assertIn("CDC_CLEANUP_ACTIVE_GROUP_TESTS_PASSED", result.stdout)
            elif scenario == "postgres-permission":
                self.assertIn("CDC_CLEANUP_PERMISSION_TESTS_PASSED", result.stdout)
            elif scenario == "worker-interrupted":
                self.assertIn("CDC_CLEANUP_WORKER_INTERRUPTION_TESTS_PASSED", result.stdout)
            elif scenario == "worker-delete-interrupted":
                self.assertIn("CDC_CLEANUP_DELAYED_DELETE_TESTS_PASSED", result.stdout)
            elif scenario == "clickhouse-permission":
                self.assertIn("CDC_CLEANUP_CLICKHOUSE_PERMISSION_TESTS_PASSED", result.stdout)
            print(result.stdout, end="")
        finally:
            subprocess.run(
                [*command, "down", "--remove-orphans", "--timeout", "10"],
                cwd=ROOT, env=environment, check=True, timeout=90,
            )
