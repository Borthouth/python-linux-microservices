import base64
import os
from pathlib import Path
import secrets
import subprocess
import time
import unittest
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = "0041_alter_sourceprofile_last_sync_status"


@unittest.skipUnless(
    os.environ.get("REPORTHUB_MIGRATION_GATE_TEST") == "1",
    "Set REPORTHUB_MIGRATION_GATE_TEST=1 to run disposable migration-gate tests.",
)
class MigrationGateIntegrationTests(unittest.TestCase):
    def test_failed_and_fresh_migrations_gate_restricted_runtime(self):
        environment = os.environ.copy()
        environment.update({
            "POSTGRES_DB": "reporthub",
            "MIGRATION_POSTGRES_USER": "gate_migrator",
            "GATE_ADMIN_PASSWORD": secrets.token_urlsafe(32),
            "GATE_RUNTIME_PASSWORD": secrets.token_urlsafe(32),
            "GATE_MIGRATION_PASSWORD": secrets.token_urlsafe(32),
            "GATE_FIELD_KEY": base64.urlsafe_b64encode(os.urandom(32)).decode(),
        })
        environment["MIGRATION_POSTGRES_PASSWORD"] = "intentionally-invalid"
        command = [
            "docker", "compose", "--env-file", "/dev/null",
            "-p", f"reporthub-migration-gate-{uuid4().hex}",
            "-f", "docker-compose.migrations.yml",
            "-f", "tests/docker-compose.migration-gate.yml",
        ]

        def compose(*args, check=True, timeout=180):
            result = subprocess.run(
                [*command, *args], cwd=ROOT, env=environment,
                capture_output=True, text=True, timeout=timeout,
            )
            if check:
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return result

        def service_id(service):
            result = compose("ps", "--all", "--quiet", service)
            return result.stdout.strip()

        def inspect(container, field):
            result = subprocess.run(
                ["docker", "inspect", "--format", field, container],
                capture_output=True, text=True, timeout=15, check=True,
            )
            return result.stdout.strip()

        def wait_for_migration(success=True):
            container = service_id("migrations")
            self.assertTrue(container)
            result = subprocess.run(
                ["docker", "wait", container], capture_output=True, text=True,
                timeout=180, check=True,
            )
            if success:
                self.assertEqual(
                    result.stdout.strip(), "0", compose("logs", "migrations").stdout,
                )
            else:
                self.assertNotEqual(result.stdout.strip(), "0")
            return container

        def check_runtime():
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                result = compose(
                    "exec", "-T", "app", "python",
                    "tests/integration/migration_gate_probe.py", "check",
                    check=False, timeout=15,
                )
                if result.returncode == 0:
                    self.assertIn("GATE_RUNTIME_VERIFIED", result.stdout)
                    return
                time.sleep(0.5)
            self.fail(
                f"Restricted runtime did not become ready: {result.stdout}{result.stderr}\n"
                f"{compose('logs', 'app').stdout}"
            )

        def migration_count():
            result = compose(
                "exec", "-T", "postgres", "psql", "-U", "gate_admin",
                "-d", "reporthub", "-At", "-c",
                f"SELECT count(*) FROM django_migrations WHERE app = 'control_plane' "
                f"AND name = '{MIGRATION}'",
            )
            return result.stdout.strip()

        try:
            failed = compose("up", "-d", "--no-build", "app", check=False)
            self.assertNotEqual(failed.returncode, 0, failed.stdout + failed.stderr)
            migration = service_id("migrations")
            self.assertTrue(migration)
            self.assertNotEqual(inspect(migration, "{{.State.ExitCode}}"), "0")
            logs = compose("logs", "migrations").stdout
            self.assertIn("password authentication failed", logs)
            app = service_id("app")
            if app:
                self.assertEqual(inspect(app, "{{.State.StartedAt}}"), "0001-01-01T00:00:00Z")

            environment["MIGRATION_POSTGRES_PASSWORD"] = environment["GATE_MIGRATION_PASSWORD"]
            compose("up", "-d", "--no-build", "--force-recreate", "migrations")
            wait_for_migration(success=False)
            self.assertIn("gate blocks migration DDL", compose("logs", "migrations").stdout)
            failed = compose("up", "-d", "--no-build", "app", check=False)
            self.assertNotEqual(failed.returncode, 0, failed.stdout + failed.stderr)
            app = service_id("app")
            if app:
                self.assertEqual(inspect(app, "{{.State.StartedAt}}"), "0001-01-01T00:00:00Z")
            compose(
                "exec", "-T", "postgres", "psql", "-U", "gate_admin",
                "-d", "reporthub", "-v", "ON_ERROR_STOP=1", "-c",
                "ALTER EVENT TRIGGER gate_block_migration_ddl DISABLE",
            )
            compose("up", "-d", "--no-build", "--force-recreate", "migrations")
            first = wait_for_migration()
            compose("up", "-d", "--no-build", "app")
            self.assertEqual(service_id("migrations"), first)
            check_runtime()
            self.assertEqual(migration_count(), "1")

            compose("stop", "app")
            compose(
                "exec", "-T", "postgres", "psql", "-U", "gate_admin",
                "-d", "reporthub", "-v", "ON_ERROR_STOP=1", "-c",
                f"DELETE FROM django_migrations WHERE app = 'control_plane' "
                f"AND name = '{MIGRATION}'",
            )
            self.assertEqual(migration_count(), "0")
            compose("up", "-d", "--no-build", "--force-recreate", "migrations")
            second = wait_for_migration()
            self.assertNotEqual(first, second)
            self.assertEqual(migration_count(), "1")
            compose("up", "-d", "--no-build", "app")
            self.assertEqual(service_id("migrations"), second)
            check_runtime()
        finally:
            compose("down", "--volumes", "--remove-orphans", "--timeout", "10", timeout=60)
