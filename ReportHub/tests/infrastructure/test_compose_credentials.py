import json
import os
from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[2]
SERVICES = (
    "app",
    "celery_ux_worker",
    "celery_warm_worker",
    "celery_cdc_worker",
    "celery_batch_worker",
    "celery_cold_worker",
    "celery_periodic_worker",
    "celery_beat",
)
CREDENTIALS = {
    "APP_POSTGRES_USER": "test_pg_user",
    "APP_POSTGRES_PASSWORD": "test_pg_password",
    "APP_CLICKHOUSE_USER": "test_ch_user",
    "APP_CLICKHOUSE_PASSWORD": "test_ch_password",
}


@unittest.skipUnless(shutil.which("docker"), "Docker Compose is not available")
class ComposeCredentialsTests(unittest.TestCase):
    def compose_config(self, credentials):
        environment = os.environ.copy()
        for key in CREDENTIALS:
            environment.pop(key, None)
        environment.update(credentials)
        result = subprocess.run(
            [
                "docker", "compose", "--env-file", ".env.example",
                "-f", "docker-compose.yml", "-f", "docker-compose.cdc.yml",
                "-f", "docker-compose.application-credentials.yml",
                "config", "--format", "json",
            ],
            cwd=ROOT, env=environment, check=True, capture_output=True, text=True,
        )
        return json.loads(result.stdout)["services"]

    def test_explicit_credentials_reach_every_application_service(self):
        services = self.compose_config(CREDENTIALS)
        for service in SERVICES:
            with self.subTest(service=service):
                environment = services[service]["environment"]
                for key, value in CREDENTIALS.items():
                    self.assertEqual(environment[key], value)
        for service in ("postgres", "clickhouse"):
            self.assertTrue(CREDENTIALS.keys().isdisjoint(services[service]["environment"]))

    def test_unset_credentials_are_not_populated_with_empty_strings(self):
        services = self.compose_config({})
        for service in SERVICES:
            with self.subTest(service=service):
                environment = services[service]["environment"]
                for key in CREDENTIALS:
                    self.assertIsNone(environment.get(key))
