import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]
PROBE_IMAGE = 'reporthub-java-logging-probe'


@unittest.skipUnless(shutil.which('docker'), 'Docker is not available')
class JavaLoggingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.directory.cleanup)
        name = f'reporthub-java-probe-{uuid4().hex}'
        subprocess.run(
            ['docker', 'create', '--name', name, PROBE_IMAGE],
            check=True, capture_output=True, timeout=30,
        )
        try:
            subprocess.run(
                ['docker', 'cp', f'{name}:/opt/kafka/libs/logging-probe.jar', cls.directory.name],
                check=True, capture_output=True, timeout=30,
            )
        finally:
            subprocess.run(
                ['docker', 'rm', '--volumes', name],
                check=True, capture_output=True, timeout=30,
            )

    def run_probe(self, service, size=0):
        marker = f'java-logging-{uuid4().hex}'
        command = [
            'docker', 'run', '--rm', '--network', 'none',
            '--hostname', 'localhost', '--log-driver', 'none',
        ]
        if service == 'kafka':
            command += [PROBE_IMAGE]
        else:
            config = ROOT / 'otel/java/debezium-log4j2.xml'
            probe = Path(self.directory.name) / 'logging-probe.jar'
            command += [
                '-v', f'{config}:/probe/log4j2.xml:ro',
                '-v', f'{probe}:/probe/logging-probe.jar:ro',
                '--entrypoint', 'java', 'quay.io/debezium/connect:3.5.2.Final',
                '-Dlog4j.configuration=file:/kafka/config/log4j.properties',
                '-Dlog4j2.configurationFile=/probe/log4j2.xml',
                '-Dkafka.logs.dir=/tmp',
                '-cp', '/probe/logging-probe.jar:/kafka/libs/*', 'LoggingProbe',
            ]
        result = subprocess.run(
            [*command, marker, str(size)],
            capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, '')
        lines = result.stdout.splitlines()
        self.assertEqual(len(lines), 2)
        error, warning = [json.loads(line) for line in lines]
        for event in (error, warning):
            self.assertEqual(event['schema'], 'reporthub.log.v1')
            self.assertEqual(event['logger'], 'reporthub.logging.probe')
        self.assertEqual(error['severity'], 'ERROR')
        self.assertIn(f'{marker} ERROR start\nquoted "value" and \\path\tcontrol \u2603', error['message'])
        self.assertIn(f'{"x" * size}{marker} END', error['message'])
        self.assertIn('java.lang.IllegalStateException: outer failure', error['message'])
        self.assertIn('Caused by: java.lang.IllegalArgumentException: inner cause', error['message'])
        self.assertIn('Suppressed: java.lang.UnsupportedOperationException: suppressed failure', error['message'])
        self.assertEqual(warning['severity'], 'WARN')
        self.assertIn(f'{marker} WARN message without an exception', warning['message'])
        self.assertNotIn('Exception', warning['message'])
        if service == 'debezium':
            self.assertIn('probe|probe-connector|streaming', error['message'])

    def test_java_exceptions_are_single_json_lines(self):
        for service in ('kafka', 'debezium'):
            with self.subTest(service=service):
                self.run_probe(service)

    def test_large_java_exceptions_remain_valid_json_at_the_source(self):
        for service in ('kafka', 'debezium'):
            with self.subTest(service=service):
                self.run_probe(service, size=128 * 1024)
