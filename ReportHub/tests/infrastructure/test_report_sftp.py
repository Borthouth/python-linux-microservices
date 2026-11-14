import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import unittest


@unittest.skipUnless(
    os.environ.get("REPORTHUB_REPORT_SFTP_TEST") == "1",
    "Set REPORTHUB_REPORT_SFTP_TEST=1 for disposable report/Celery/SFTP infrastructure.",
)
class ReportSftpIntegrationTests(unittest.TestCase):
    def run_command(self, command, *, env=None):
        result = subprocess.run(
            command, env=env, text=True, capture_output=True, timeout=300,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def test_real_report_fanout_retry_and_host_key_rejection(self):
        tests = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="reporthub-report-sftp-") as directory:
            path = Path(directory)
            self.run_command([
                "ssh-keygen", "-q", "-t", "ed25519", "-N", "",
                "-f", str(path / "client_ed25519"),
            ])
            env = os.environ.copy()
            env.update(
                CANCELLATION_TEST_PASSWORD=secrets.token_urlsafe(24),
                CANCELLATION_TEST_FIELD_KEY=base64.urlsafe_b64encode(os.urandom(32)).decode(),
                REPORTHUB_CANCELLATION_APP_IMAGE=os.environ.get(
                    "REPORTHUB_REPORT_SFTP_APP_IMAGE", "reporthub-celery_transfer_worker"
                ),
                SFTP_TEST_KEY_DIR=directory,
            )
            services = json.loads(self.run_command([
                "docker", "compose", "-f", str(tests / "docker-compose.celery-cancellation.yml"),
                "config", "--format", "json",
            ], env=env))["services"]
            sftp = json.loads(self.run_command([
                "docker", "compose", "-f", str(tests / "docker-compose.test.yml"),
                "config", "--format", "json",
            ], env=env))["services"]["sftp-test"]
            sftp.pop("container_name", None)
            sftp.pop("ports", None)
            sftp["networks"] = {"default": None}
            sftp["restart"] = "no"
            selected = {
                name: services[name] for name in ("postgres", "redis", "clickhouse", "minio", "probe")
            }
            selected["sftp-test"] = sftp
            probe = selected["probe"]
            probe["entrypoint"] = ["python", "tests/integration/report_sftp_probe.py"]
            probe["environment"]["CLICKHOUSE_HOST"] = "clickhouse"
            probe["depends_on"].pop("clickhouse-proxy", None)
            probe["volumes"].append({
                "type": "bind", "source": str(path / "client_ed25519"),
                "target": "/probe/client_ed25519", "read_only": True,
            })
            config = {
                "services": selected, "networks": {"default": {"internal": True}},
                "volumes": {"sftp_test_data": {}, "sftp_test_host_keys": {}},
            }
            fixture = path / "compose.json"
            fixture.write_text(json.dumps(config))
            compose = [
                "docker", "compose", "-p", "reporthub-report-sftp-" + secrets.token_hex(4),
                "-f", str(fixture),
            ]
            try:
                self.run_command(compose + [
                    "up", "-d", "--wait", "postgres", "redis", "clickhouse", "minio", "sftp-test",
                ])
                public_key = self.run_command(compose + [
                    "exec", "-T", "sftp-test", "cat", "/etc/ssh/ssh_host_ed25519_key.pub",
                ]).split()[1]
                fingerprint = "SHA256:" + base64.b64encode(
                    hashlib.sha256(base64.b64decode(public_key)).digest()
                ).decode().rstrip("=")
                output = self.run_command(compose + [
                    "run", "--rm", "--no-deps", "-e", f"SFTP_TEST_HOST_FINGERPRINT={fingerprint}", "probe",
                ])
                self.assertIn("REPORT_SFTP_FANOUT_RETRY_PASSED", output)
            finally:
                self.run_command(compose + ["down", "--volumes", "--remove-orphans"])


if __name__ == "__main__":
    unittest.main()
