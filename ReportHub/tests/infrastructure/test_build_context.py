import base64
import os
from pathlib import Path
import subprocess
import unittest
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(
    os.environ.get("REPORTHUB_BUILD_CONTEXT_TEST") == "1",
    "Set REPORTHUB_BUILD_CONTEXT_TEST=1 to build and inspect the application image.",
)
class BuildContextTests(unittest.TestCase):
    def test_standalone_image_contains_application_and_excludes_local_data(self):
        image = f"reporthub-build-context-test:{uuid4().hex}"
        built = False
        try:
            build = subprocess.run(
                ["docker", "build", "-t", image, "."],
                cwd=ROOT, capture_output=True, text=True, timeout=600,
            )
            self.assertEqual(build.returncode, 0, build.stderr)
            built = True
            script = """
import os
from pathlib import Path
from django.core.management import execute_from_command_line
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings"
for path in (
    "reports/models.py",
    "reports/migrations/0016_report_recipient_access.py",
    "reports/templates/reports/components/report_list_content.html",
    "warehouse/models.py",
):
    assert Path(path).is_file(), path
for path in (".env", ".env.example", "docker", "logs", "media", "staticfiles"):
    assert not Path(path).exists(), path
execute_from_command_line(["manage.py", "check"])
"""
            run = subprocess.run(
                [
                    "docker", "run", "--rm", "--network", "none",
                    "--env", "SECRET_KEY=isolated-test-only",
                    "--env", f"FIELD_ENCRYPTION_KEY={base64.urlsafe_b64encode(b'a' * 32).decode()}",
                    "--entrypoint", "python", image, "-c", script,
                ],
                cwd=ROOT, capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertIn("System check identified no issues", run.stdout)
        finally:
            if built:
                subprocess.run(
                    ["docker", "image", "rm", image],
                    cwd=ROOT, capture_output=True, text=True, check=True,
                )
