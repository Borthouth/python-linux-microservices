import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]
COLLECTOR_IMAGE = 'reporthub-otel_collector'


@unittest.skipUnless(shutil.which('docker'), 'Docker is not available')
class CollectorLoggingTests(unittest.TestCase):
    def collect(self, records, disabled=('redis',)):
        with tempfile.TemporaryDirectory() as directory:
            config = subprocess.run(
                [
                    'docker', 'run', '--rm', '--entrypoint', 'python3',
                    '-v', f'{ROOT}/otel/collector.yaml:/etc/otelcol-contrib/config.yaml:ro',
                    '-v', f'{ROOT}/otel:/opt/reporthub-otel:ro',
                    '-e', f'PROBE_DISABLED_SOURCES={json.dumps(list(disabled))}',
                    COLLECTOR_IMAGE, '-c',
                    '''
import json
import os
import sys
import yaml
sys.path.insert(0, '/opt/reporthub-otel')
from supervise import render_config
from source_policy import default_sources
disabled = json.loads(os.environ['PROBE_DISABLED_SOURCES'])
config = yaml.safe_load(render_config([s for s in default_sources() if s not in disabled]))
config['receivers'] = {'otlp': {'protocols': {'http': {'endpoint': '0.0.0.0:4318'}}}}
config['extensions'] = {}
config['exporters'] = {'file': {'path': '/probe/output.json'}}
config['service']['extensions'] = []
config['service']['pipelines']['logs']['receivers'] = ['otlp']
config['service']['pipelines']['logs']['exporters'] = ['file']
print(yaml.safe_dump(config))
''',
                ],
                check=True, capture_output=True, text=True, timeout=30,
            )
            (Path(directory) / 'config.yaml').write_text(config.stdout)
            container = f'reporthub-collector-probe-{uuid4().hex}'
            subprocess.run(
                [
                    'docker', 'run', '--detach', '--name', container,
                    '--log-driver', 'json-file', '--log-opt', 'max-size=1m',
                    '--log-opt', 'max-file=1',
                    '-p', '127.0.0.1::4318', '-v', f'{directory}:/probe',
                    '--entrypoint', '/usr/local/bin/otelcol-contrib', COLLECTOR_IMAGE,
                    '--config=/probe/config.yaml',
                ],
                check=True, capture_output=True, timeout=30,
            )
            try:
                address = subprocess.run(
                    ['docker', 'port', container, '4318/tcp'],
                    check=True, capture_output=True, text=True, timeout=10,
                ).stdout.strip()
                endpoint = f'http://{address}/v1/logs'
                for attempt in range(30):
                    try:
                        with urlopen(Request(
                            endpoint, data=b'{}', headers={'Content-Type': 'application/json'},
                        ), timeout=1):
                            break
                    except (URLError, ConnectionError, TimeoutError) as exc:
                        last_error = exc
                        time.sleep(0.1)
                else:
                    logs = subprocess.run(
                        ['docker', 'logs', '--tail', '30', container],
                        check=True, capture_output=True, text=True, timeout=10,
                    )
                    self.fail(f'Probe Collector did not start: {last_error}\n{logs.stdout}{logs.stderr}')
                payload = json.dumps({'resourceLogs': [{
                    'resource': {'attributes': [{
                        'key': 'service.name', 'value': {'stringValue': 'original-source'},
                    }]},
                    'scopeLogs': [{'logRecords': records}],
                }]}).encode()
                with urlopen(Request(
                    endpoint, data=payload, headers={'Content-Type': 'application/json'},
                ), timeout=5) as response:
                    self.assertEqual(response.status, 200)

                subprocess.run(
                    ['docker', 'stop', '--time', '10', container],
                    check=True, capture_output=True, timeout=20,
                )
                output = Path(directory) / 'output.json'
                self.assertTrue(output.exists(), 'Collector did not export the probe records')
                events = []
                for line in output.read_text().splitlines():
                    for resource in json.loads(line)['resourceLogs']:
                        attributes = {a['key']: a['value']['stringValue'] for a in resource['resource']['attributes']}
                        for scope in resource['scopeLogs']:
                            for record in scope['logRecords']:
                                events.append((attributes['service.name'], record))
                return events
            finally:
                subprocess.run(
                    ['docker', 'rm', '--force', container],
                    check=True, capture_output=True, timeout=30,
                )

    def test_mixed_sources_keep_their_own_service_severity_and_redacted_message(self):
        cases = [
            ('kafka', 'fluent.tag', 'reporthub.reporthub-kafka', 'WARN', 'WARNING', 'container'),
            ('debezium-connect', 'fluent.tag', 'reporthub.reporthub-debezium-connect', 'ERROR', 'ERROR', 'container'),
            ('celery_warm_worker', 'fluent.tag', 'reporthub.reporthub_celery_warm_worker', 'INFO', 'INFO', 'container'),
            ('celery_batch_worker', 'fluent.tag', 'reporthub.reporthub_celery_batch_worker', 'FATAL', 'CRITICAL', 'container'),
            ('clamav', 'journald._COMM', 'clamd', 'WARNING', 'WARNING', 'journald'),
            ('example.service', 'journald._SYSTEMD_UNIT', 'example.service', 'DEBUG', 'DEBUG', 'journald'),
            ('original-source', None, None, 'ERROR', 'ERROR', None),
        ]
        records = []
        message = 'quoted "value"\npassword=probe-secret WARNING DEBUG'
        for index, (service, key, value, severity, _, _) in enumerate(cases):
            attributes = {key: value} if key else {}
            if service == 'clamav':
                attributes.update({
                    'reporthub.input': 'journal',
                    'journald._SYSTEMD_UNIT': 'clamd-scanhub@scan.service',
                })
            records.append({
                'body': {'stringValue': json.dumps({
                    'schema': 'reporthub.log.v1', 'severity': severity,
                    'logger': 'reporthub_etl' if service == 'celery_batch_worker' else 'probe',
                    'message': f'{service}: {message}',
                }, separators=(',', ':') if index % 2 else None)},
                'attributes': [
                    {'key': name, 'value': {'stringValue': value}}
                    for name, value in attributes.items()
                ],
            })
        for tag in ('redis', 'clickhouse', 'otel-collector'):
            records.append({
                'body': {'stringValue': f'excluded {tag}'},
                'attributes': [{
                    'key': 'fluent.tag',
                    'value': {'stringValue': f'reporthub.reporthub-{tag}'},
                }],
            })
        collected = self.collect(records)
        events = dict(collected)
        self.assertEqual(len(events), len(collected), 'Unexpected duplicate or misattributed record')
        self.assertEqual(set(events), {case[0] for case in cases})
        for service, key, value, _, severity, source in cases:
            event = events[service]
            self.assertEqual(event['severityText'], severity)
            self.assertEqual(event['body']['stringValue'], f'{service}: {message}'.replace(
                'probe-secret', '[REDACTED]',
            ))
            attributes = {a['key']: a['value']['stringValue'] for a in event['attributes']}
            self.assertEqual(
                attributes['logger.name'],
                'reporthub_etl' if service == 'celery_batch_worker' else 'probe',
            )
            if key:
                self.assertEqual(attributes[key], value)
                self.assertEqual(attributes['source'], source)
            if service.startswith('celery'):
                self.assertEqual(attributes['worker'], service)

    def test_logical_sources_are_independent_and_file_mirrors_are_always_dropped(self):
        def record(message, attributes, logger='probe'):
            return {
                'body': {'stringValue': json.dumps({
                    'schema': 'reporthub.log.v1', 'severity': 'INFO',
                    'logger': logger, 'message': message,
                })},
                'attributes': [
                    {'key': name, 'value': {'stringValue': value}}
                    for name, value in attributes.items()
                ],
            }

        worker = {'fluent.tag': 'reporthub.reporthub_celery_batch_worker'}
        app = {'fluent.tag': 'reporthub.reporthub-app'}
        records = [
            record('etl-worker', worker, 'reporthub_etl'),
            record('etl-web', app, 'reporthub_etl'),
            record('celery-runtime', worker),
            record('django-runtime', app),
            record('host-event', {
                'reporthub.input': 'journal', 'journald._SYSTEMD_UNIT': 'example.service',
                'journald._COMM': 'example',
            }),
            record('direct-otlp', {}),
            record('etl-worker', {'log.file.path': '/var/log/reporthub/etl_execution.log'}, 'reporthub_etl'),
            record('clamav-scan', {'log.file.path': '/hostfs/var/log/clamav/clamd-scanhub-scan.log'}),
            record('clamav-scan2', {'log.file.path': '/hostfs/var/log/clamav/clamd-scanhub-scan2.log'}),
            record('clamav-update', {'log.file.path': '/hostfs/var/log/clamav/freshclam.log'}),
        ]
        for message, command, unit in (
            ('clamav-scan', 'clamd', 'clamd-scanhub@scan.service'),
            ('clamav-scan2', 'clamd', 'clamd-scanhub@scan2.service'),
            ('clamav-update', 'freshclam', 'clamav-freshclam.service'),
        ):
            records.append(record(message, {
                'reporthub.input': 'journal', 'journald._COMM': command,
                'journald._SYSTEMD_UNIT': unit,
            }))

        required = {
            'etl-worker': {'fluent', 'etl'},
            'etl-web': {'fluent', 'etl'},
            'celery-runtime': {'fluent', 'celery'},
            'django-runtime': {'fluent', 'django'},
            'host-event': {'journal'},
            'clamav-scan': {'clamav'},
            'clamav-scan2': {'clamav'},
            'clamav-update': {'clamav'},
            'direct-otlp': {'otlp'},
        }
        for disabled in (
            (), ('etl',), ('celery', 'django'), ('journal',), ('clamav',),
            ('etl', 'celery', 'django'), ('clamav', 'journal'), ('fluent',),
            ('fluent', 'journal', 'clamav', 'otlp'),
        ):
            with self.subTest(disabled=disabled):
                events = self.collect(records, disabled=disabled)
                self.assertCountEqual(
                    [event['body']['stringValue'] for _, event in events],
                    [message for message, sources in required.items() if not sources.intersection(disabled)],
                )
                for service, event in events:
                    if event['body']['stringValue'].startswith('clamav-'):
                        self.assertEqual(service, 'clamav')
