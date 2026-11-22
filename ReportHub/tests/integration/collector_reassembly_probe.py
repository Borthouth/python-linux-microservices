"""Opt-in acceptance probe; exits nonzero when upstream reassembly is unsafe.

Build otel/experimental/Dockerfile first. Uses disposable containers and local
file output only; never connects to the shared Collector or ClickHouse.
Requires PyYAML, also used by the Collector supervisor.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import uuid4

import yaml


ROOT = Path(__file__).resolve().parents[2]
IMAGE = 'reporthub-otel-reassembly-probe:0.165.0'
SOURCE_IMAGE = 'reporthub-otel_collector'
MESSAGE = 'start ' + 'x' * (128 * 1024) + ' password=probe-secret\ntraceback END'


def docker(*args, **kwargs):
    return subprocess.run(
        ['docker', *args], check=True, capture_output=True, text=True,
        timeout=kwargs.pop('timeout', 30), **kwargs,
    ).stdout.strip()


def structured(message):
    return json.dumps({
        'schema': 'reporthub.log.v1', 'severity': 'ERROR',
        'logger': 'reassembly.probe', 'message': message,
    }, ensure_ascii=False)


def record(body, ordinal=None, last=False, partial_id='same-id',
           container='container-a', stream='stdout', tag='reporthub.reporthub-kafka'):
    attributes = {'fluent.tag': tag, 'container_id': container, 'source': stream}
    if ordinal is not None:
        attributes.update({
            'partial_message': 'true', 'partial_id': partial_id,
            'partial_ordinal': str(ordinal), 'partial_last': str(last).lower(),
        })
    return {
        'body': {'stringValue': body},
        'attributes': [
            {'key': key, 'value': {'stringValue': value}}
            for key, value in attributes.items()
        ],
    }


def configuration():
    config = yaml.safe_load((ROOT / 'otel/collector.yaml').read_text())
    config['receivers'] = {
        'otlp': {'protocols': {'http': {'endpoint': '0.0.0.0:4318'}}},
        'fluent_forward': {'endpoint': '0.0.0.0:24224'},
    }
    config['extensions'] = {}
    config['exporters'] = {'file': {'path': '/probe/output.json', 'flush_interval': '100ms'}}
    config['processors']['logstransform/reassembly'] = yaml.safe_load(
        (ROOT / 'otel/experimental/reassembly.yaml').read_text(),
    )
    config['processors']['memory_limiter'].update(limit_mib=128, spike_limit_mib=32)
    config['service']['extensions'] = []
    config['service']['pipelines']['logs'] = {
        'receivers': ['otlp', 'fluent_forward'],
        'processors': [
            'memory_limiter', 'logstransform/reassembly',
            'groupbyattrs/source', 'transform/logs',
        ],
        'exporters': ['file'],
    }
    return config


def send(endpoint, records):
    payload = json.dumps({'resourceLogs': [{
        'scopeLogs': [{'logRecords': records}],
    }]}).encode()
    with urlopen(Request(
        endpoint, data=payload, headers={'Content-Type': 'application/json'},
    ), timeout=5) as response:
        result = json.load(response)
        if response.status != 200 or result.get('partialSuccess'):
            raise RuntimeError(f'OTLP input rejected: {result}')


def events_from(path):
    events = []
    if path.exists():
        for line in path.read_text().splitlines():
            for resource in json.loads(line).get('resourceLogs', []):
                attributes = {
                    item['key']: next(iter(item['value'].values()))
                    for item in resource.get('resource', {}).get('attributes', [])
                }
                for scope in resource.get('scopeLogs', []):
                    for event in scope.get('logRecords', []):
                        events.append({**event, 'probe_resource': attributes})
    return events


def run_case(records, *, real_docker=False, shutdown=False, separate_batches=False):
    with tempfile.TemporaryDirectory(prefix='reporthub-reassembly-') as directory:
        path = Path(directory)
        (path / 'config.yaml').write_text(yaml.safe_dump(configuration()))
        name = f'reporthub-reassembly-{uuid4().hex}'
        docker(
            'create', '--name', name, '--log-driver', 'json-file',
            '--log-opt', 'max-size=1m', '--log-opt', 'max-file=1',
            '--cpus', '2', '--memory', '256m',
            '--user', f'{os.getuid()}:{os.getgid()}',
            '-p', '127.0.0.1::4318', '-p', '127.0.0.1::24224',
            '-v', f'{path}:/probe', IMAGE, '--config=/probe/config.yaml',
        )
        try:
            docker('start', name)
            endpoint = f"http://{docker('port', name, '4318/tcp')}/v1/logs"
            for _ in range(50):
                try:
                    send(endpoint, [])
                    break
                except (URLError, ConnectionError, TimeoutError):
                    if docker('inspect', '--format', '{{.State.Running}}', name) != 'true':
                        raise RuntimeError(docker('logs', name))
                    time.sleep(0.1)
            else:
                raise RuntimeError('Isolated Collector did not become responsive.')
            if real_docker:
                docker(
                    'run', '--rm', '--network', 'none', '-i',
                    '--log-driver', 'fluentd', '--log-opt',
                    f"fluentd-address={docker('port', name, '24224/tcp')}",
                    '--log-opt', 'tag=reporthub.reporthub-kafka',
                    '--entrypoint', 'python3', SOURCE_IMAGE, '-c',
                    'import sys; sys.stdout.write(sys.stdin.read())',
                    input=structured(MESSAGE) + '\n',
                )
            else:
                if separate_batches:
                    for item in records:
                        send(endpoint, [item])
                else:
                    send(endpoint, records)
            if not shutdown:
                time.sleep(2)
            docker('stop', '--time', '10', name)
            logs = subprocess.run(
                ['docker', 'logs', name], check=True, capture_output=True,
                text=True, timeout=10,
            )
            return events_from(path / 'output.json'), logs.stdout + logs.stderr
        except subprocess.CalledProcessError as exc:
            logs = subprocess.run(
                ['docker', 'logs', name], check=True, capture_output=True,
                text=True, timeout=10,
            )
            raise RuntimeError(f'{exc.stderr}\n{logs.stdout}\n{logs.stderr}') from exc
        finally:
            docker('rm', '--force', name)


def body(event):
    return event.get('body', {}).get('stringValue', '')


def complete(events, expected):
    actual = sorted(
        (event['probe_resource'].get('service.name'), body(event), event.get('severityText'))
        for event in events
    )
    wanted = sorted(
        (service, message.replace('probe-secret', '[REDACTED]'), 'ERROR')
        for service, message in expected
    )
    return actual == wanted and all(
        any(
            attribute['key'] == 'logger.name'
            and attribute['value'].get('stringValue') == 'reassembly.probe'
            for attribute in event.get('attributes', [])
        )
        for event in events
    )


def explicitly_incomplete(events):
    # Partial metadata is retained on healthy reconstructions too. It is not
    # proof that an incomplete message was detected or rejected.
    return bool(events) and all(
        any(
            attribute['key'] == 'reporthub.reassembly.error'
            and attribute['value'].get('stringValue')
            for attribute in event.get('attributes', [])
        )
        for event in events
    )


def probe_cases():
    encoded = structured('prefix MIDDLE suffix password=probe-secret')
    offset = encoded.index('MIDDLE')
    pieces = [encoded[:offset], 'MIDDLE', encoded[offset + len('MIDDLE'):]]
    fragments = [record(piece, index + 1, index == 2) for index, piece in enumerate(pieces)]
    small = structured('small password=probe-secret')
    large = structured(MESSAGE)
    large_pieces = [large[index:index + 16384] for index in range(0, len(large), 16384)]
    large_fragments = [
        record(piece, index + 1, index == len(large_pieces) - 1)
        for index, piece in enumerate(large_pieces)
    ]
    streams = []
    for container, stream, tag, service in (
        ('a', 'stdout', 'reporthub.reporthub-kafka', 'kafka'),
        ('b', 'stdout', 'reporthub.reporthub-debezium-connect', 'debezium-connect'),
        ('a', 'stderr', 'reporthub.reporthub-kafka', 'kafka'),
    ):
        message = f'{container}/{stream} password=probe-secret'
        text = structured(message)
        midpoint = len(text) // 2
        streams.append((container, stream, tag, service, message, [text[:midpoint], text[midpoint:]]))
    interleaved = [
        record(parts[index], index + 1, index == 1, container=container, stream=stream, tag=tag)
        for index in range(2)
        for container, stream, tag, _, _, parts in streams
    ]
    secret_offset = encoded.index('probe-secret')
    secret_parts = [encoded[:secret_offset + 3], encoded[secret_offset + 3:]]
    return [
        ('normal structured record', [record(small)], [('kafka', 'small password=probe-secret')], {}),
        ('128 KiB synthetic fragments', large_fragments, [('kafka', MESSAGE)], {}),
        ('fragments across separate HTTP batches', fragments, [
            ('kafka', 'prefix MIDDLE suffix password=probe-secret'),
        ], {'separate_batches': True}),
        ('128 KiB real Docker Fluent Forward', [], [('kafka', MESSAGE)], {'real_docker': True}),
        ('container, service and stdout/stderr isolation', interleaved, [
            (service, message) for _, _, _, service, message, _ in streams
        ], {}),
        ('secret split across fragments', [
            record(piece, index + 1, index == 1) for index, piece in enumerate(secret_parts)
        ], [('kafka', 'prefix MIDDLE suffix password=probe-secret')], {}),
        ('missing middle fragment', [fragments[0], fragments[2]], None, {}),
        ('duplicate middle fragment', [fragments[0], fragments[1], fragments[1], fragments[2]], None, {}),
        ('out-of-order fragments', [fragments[1], fragments[0], fragments[2]], None, {}),
        ('missing final fragment timeout', fragments[:2], None, {}),
        ('incomplete message on shutdown', fragments[:2], None, {'shutdown': True}),
        ('size limit', [record('x' * 16384, index + 1) for index in range(18)], None, {}),
        ('source limit', [record('unfinished', 1, partial_id=str(index)) for index in range(9)], None, {}),
    ]


def main():
    cases = probe_cases()
    results = []
    for label, records, expected, options in cases:
        events, logs = run_case(records, **options)
        original = expected
        if original is None and label not in ('size limit', 'source limit'):
            original = [('kafka', 'prefix MIDDLE suffix password=probe-secret')]
        exact = complete(events, original) if original is not None else None
        accepted = bool(exact) or (expected is None and explicitly_incomplete(events))
        result = {
            'case': label, 'accepted': accepted, 'exported_records': len(events),
            'matches_original_message': exact,
            'exported_body_bytes': sum(len(body(event).encode()) for event in events),
            'error_log_lines': sum(
                'error' in line.lower() and 'error_mode' not in line
                for line in logs.splitlines()
            ),
        }
        results.append(result)
        print(json.dumps(result), flush=True)
    ready = all(result['accepted'] for result in results)
    print(json.dumps({'ready_for_deployment': ready, 'cases': len(results)}))
    return 0 if ready else 1


if __name__ == '__main__':
    sys.exit(main())
