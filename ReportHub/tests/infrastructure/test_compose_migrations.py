import json
import os
from pathlib import Path
import shutil
import subprocess
import unittest

from tests.infrastructure.test_compose_credentials import CREDENTIALS, SERVICES


ROOT = Path(__file__).resolve().parents[2]
MIGRATION_CREDENTIALS = {
    "MIGRATION_POSTGRES_USER": "test_migrator",
    "MIGRATION_POSTGRES_PASSWORD": "test_migrator_password",
}


@unittest.skipUnless(shutil.which("docker"), "Docker Compose is not available")
class ComposeMigrationsTests(unittest.TestCase):
    def compose_config(self, migration_credentials, *, separated=True, cdc=True):
        environment = os.environ.copy()
        for key in (*MIGRATION_CREDENTIALS, *CREDENTIALS):
            environment.pop(key, None)
        environment.update(CREDENTIALS)
        environment.update(migration_credentials)
        command = [
            "docker", "compose", "--env-file", ".env.example",
            "-f", "docker-compose.yml",
        ]
        if cdc:
            command.extend(["-f", "docker-compose.cdc.yml"])
        command.extend(["-f", "docker-compose.application-credentials.yml"])
        if separated:
            command.extend(["-f", "docker-compose.migrations.yml"])
        return subprocess.run(
            [*command, "config", "--format", "json"],
            cwd=ROOT, env=environment, capture_output=True, text=True, timeout=30,
        )

    def test_all_runtime_services_wait_for_success_without_running_migrations(self):
        for cdc in (False, True):
            with self.subTest(cdc=cdc):
                result = self.compose_config(MIGRATION_CREDENTIALS, cdc=cdc)
                self.assertEqual(result.returncode, 0, result.stderr)
                services = json.loads(result.stdout)["services"]
                for name in SERVICES:
                    service = services[name]
                    self.assertEqual(service["environment"]["MIGRATIONS_ON_STARTUP"], "0")
                    self.assertEqual(
                        service["depends_on"]["migrations"]["condition"],
                        "service_completed_successfully",
                    )
                    self.assertIn("postgres", service["depends_on"])
                    for key, value in CREDENTIALS.items():
                        self.assertEqual(service["environment"][key], value)
                    for value in MIGRATION_CREDENTIALS.values():
                        self.assertNotIn(value, service["environment"].values())
                if cdc:
                    self.assertIn("debezium-connect", services["app"]["depends_on"])
                    self.assertIn("debezium-connect", services["celery_cdc_worker"]["depends_on"])

    def test_migrations_use_dedicated_identity_and_run_once(self):
        result = self.compose_config(MIGRATION_CREDENTIALS)
        self.assertEqual(result.returncode, 0, result.stderr)
        service = json.loads(result.stdout)["services"]["migrations"]
        self.assertEqual(service["command"], ["python", "manage.py", "migrate", "--noinput"])
        self.assertEqual(service["restart"], "no")
        self.assertEqual(service["environment"]["MIGRATIONS_ON_STARTUP"], "0")
        self.assertEqual(service["environment"]["APP_POSTGRES_USER"], "test_migrator")
        self.assertEqual(
            service["environment"]["APP_POSTGRES_PASSWORD"], "test_migrator_password",
        )

    def test_missing_or_empty_migration_credentials_are_rejected(self):
        for credentials in (
            {},
            {"MIGRATION_POSTGRES_USER": "test_migrator"},
            {"MIGRATION_POSTGRES_PASSWORD": "test_migrator_password"},
            {**MIGRATION_CREDENTIALS, "MIGRATION_POSTGRES_USER": ""},
            {**MIGRATION_CREDENTIALS, "MIGRATION_POSTGRES_PASSWORD": ""},
        ):
            with self.subTest(credentials=tuple(credentials)):
                result = self.compose_config(credentials)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("MIGRATION_POSTGRES_", result.stderr)

    def test_default_stack_keeps_legacy_startup(self):
        result = self.compose_config({}, separated=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        services = json.loads(result.stdout)["services"]
        self.assertNotIn("migrations", services)
        for name in SERVICES:
            self.assertNotIn("MIGRATIONS_ON_STARTUP", services[name]["environment"])
