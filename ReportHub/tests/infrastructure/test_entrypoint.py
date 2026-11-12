import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class EntrypointTests(unittest.TestCase):
    def run_entrypoint(self, migration_status=0, command_status=0, startup_migrations=None):
        with tempfile.TemporaryDirectory(prefix="reporthub-entrypoint-") as directory:
            path = Path(directory)
            commands = {
                "nc": "#!/bin/sh\nexit 0\n",
                "sleep": "#!/bin/sh\nexit 0\n",
                "python": (
                    "#!/bin/sh\n"
                    'printf "migration:%s\\n" "$@" >> "$EVENTS"\n'
                    'exit "$MIGRATION_STATUS"\n'
                ),
                "main": (
                    "#!/bin/sh\n"
                    'printf "main:%s\\n" "$@" >> "$EVENTS"\n'
                    'exit "$COMMAND_STATUS"\n'
                ),
            }
            for name, content in commands.items():
                command = path / name
                command.write_text(content)
                command.chmod(0o700)

            events = path / "events"
            environment = os.environ.copy()
            environment.pop("MIGRATIONS_ON_STARTUP", None)
            environment.update({
                "PATH": str(path),
                "EVENTS": str(events),
                "MIGRATION_STATUS": str(migration_status),
                "COMMAND_STATUS": str(command_status),
            })
            if startup_migrations is not None:
                environment["MIGRATIONS_ON_STARTUP"] = startup_migrations
            result = subprocess.run(
                ["/bin/sh", str(ROOT / "entrypoint.sh"), "main", "two words", "--flag"],
                cwd=path, env=environment, capture_output=True, text=True, timeout=5,
            )
            return result, events.read_text().splitlines() if events.exists() else []

    def test_successful_migrations_start_main_command_with_original_arguments(self):
        result, events = self.run_entrypoint()

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(events, [
            "migration:manage.py", "migration:migrate",
            "main:two words", "main:--flag",
        ])
        self.assertEqual(result.stderr, "")

    def test_failed_migrations_prevent_main_command_and_preserve_exit_status(self):
        for status in (1, 23):
            with self.subTest(status=status):
                result, events = self.run_entrypoint(migration_status=status)

                self.assertEqual(result.returncode, status)
                self.assertEqual(events, ["migration:manage.py", "migration:migrate"])
                self.assertIn("Django migrations failed", result.stderr)
                self.assertIn("refusing to start", result.stderr)

    def test_main_command_failure_preserves_exit_status(self):
        result, events = self.run_entrypoint(command_status=17)

        self.assertEqual(result.returncode, 17)
        self.assertEqual(events[-2:], ["main:two words", "main:--flag"])
        self.assertNotIn("Django migrations failed", result.stderr)

    def test_explicitly_enabled_migrations_run_before_main_command(self):
        result, events = self.run_entrypoint(startup_migrations="1")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(events[:2], ["migration:manage.py", "migration:migrate"])
        self.assertEqual(events[2:], ["main:two words", "main:--flag"])

    def test_disabled_startup_migrations_only_run_main_command(self):
        result, events = self.run_entrypoint(
            migration_status=23, startup_migrations="0",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(events, ["main:two words", "main:--flag"])

    def test_invalid_startup_migration_setting_prevents_startup(self):
        for value in ("", "false", "2"):
            with self.subTest(value=value):
                result, events = self.run_entrypoint(startup_migrations=value)

                self.assertEqual(result.returncode, 2)
                self.assertEqual(events, [])
                self.assertIn("MIGRATIONS_ON_STARTUP must be 0 or 1", result.stderr)
