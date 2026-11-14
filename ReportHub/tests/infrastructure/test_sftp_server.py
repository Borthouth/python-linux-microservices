import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from uuid import uuid4


@unittest.skipUnless(
    os.environ.get("REPORTHUB_SFTP_TEST") == "1",
    "Set REPORTHUB_SFTP_TEST=1 to run disposable Docker SFTP tests.",
)
class SftpServerTests(unittest.TestCase):
    def run_command(self, command, *, input=None, check=True):
        result = subprocess.run(
            command, input=input, text=True, capture_output=True, timeout=180,
        )
        if check and result.returncode:
            self.fail(f"Command failed: {command[0]}\n{result.stdout}\n{result.stderr}")
        return result

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="reporthub-sftp-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.project = "reporthub-sftp-probe-" + uuid4().hex[:10]
        self.run_command([
            "ssh-keygen", "-q", "-t", "ed25519", "-N", "",
            "-f", str(self.directory / "client_ed25519"),
        ])
        compose_file = Path(__file__).resolve().parents[1] / "docker-compose.test.yml"
        environment = os.environ.copy()
        environment["SFTP_TEST_KEY_DIR"] = str(self.directory)
        result = subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "config", "--format", "json"],
            env=environment, text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        source = json.loads(result.stdout)
        service = source["services"]["sftp-test"]
        service.pop("container_name", None)
        service.pop("ports", None)
        service["networks"] = {"default": None}
        service["restart"] = "no"
        self.image = service["image"]
        config = {
            "services": {"sftp-test": service},
            "volumes": {"sftp_test_data": {}, "sftp_test_host_keys": {}},
            "networks": {"default": {"internal": True}},
        }
        fixture = self.directory / "compose.json"
        fixture.write_text(json.dumps(config))
        self.compose = ["docker", "compose", "-p", self.project, "-f", str(fixture)]
        self.addCleanup(
            self.run_command, self.compose + ["down", "--volumes", "--remove-orphans"]
        )
        self.run_command(self.compose + ["up", "-d", "--wait", "sftp-test"])
        self.payload = "id,total\n1,12.50\n2,7.25\n"
        (self.directory / "payload.csv").write_text(self.payload)
        self.server_key = self.host_key()
        (self.directory / "known_hosts").write_text(f"sftp-test {self.server_key}\n")

    def host_key(self):
        public_key = self.run_command(
            self.compose + ["exec", "-T", "sftp-test", "cat", "/etc/ssh/ssh_host_ed25519_key.pub"]
        ).stdout
        return " ".join(public_key.split()[:2])

    def transfer(self, commands, *, check=True):
        return self.run_command([
            "docker", "run", "--rm", "-i", "--network", f"{self.project}_default",
            "--mount", f"type=bind,source={self.directory},target=/probe,readonly",
            "--entrypoint", "bash", self.image, "-c",
            "sftp -b - -i /probe/client_ed25519 -o BatchMode=yes "
            "-o ConnectTimeout=5 -o StrictHostKeyChecking=yes "
            "-o UserKnownHostsFile=/probe/known_hosts reporthub@sftp-test "
            "&& cmp /probe/payload.csv /tmp/downloaded.csv",
        ], input=commands, check=check)

    def test_upload_survives_recreation_with_pinned_host_key(self):
        self.transfer(
            "put /probe/payload.csv /upload/persisted.csv\n"
            "get /upload/persisted.csv /tmp/downloaded.csv\n"
        )
        self.run_command(
            self.compose + ["up", "-d", "--no-deps", "--force-recreate", "--wait", "sftp-test"]
        )
        self.assertEqual(self.host_key(), self.server_key)
        self.transfer("get /upload/persisted.csv /tmp/downloaded.csv\n")
        result = self.transfer("put /probe/payload.csv /outside-upload.csv\n", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Permission denied", result.stderr)
        wrong_key = " ".join((self.directory / "client_ed25519.pub").read_text().split()[:2])
        (self.directory / "known_hosts").write_text(f"sftp-test {wrong_key}\n")
        result = self.transfer("get /upload/persisted.csv /tmp/downloaded.csv\n", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Host key verification failed", result.stderr)


if __name__ == "__main__":
    unittest.main()
