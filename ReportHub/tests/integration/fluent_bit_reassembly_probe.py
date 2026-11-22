"""Opt-in raw Fluent Bit reassembly gate; no production pipeline is changed.

Run alongside collector_reassembly_probe.py using Python with PyYAML available.
Uses the same input cases but checks raw reassembly before parsing/redaction.
Exits nonzero if the candidate is not safe to advance to end-to-end testing.
"""

import json
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from collector_reassembly_probe import MESSAGE, ROOT, SOURCE_IMAGE, docker, probe_cases, structured


IMAGE = 'cr.fluentbit.io/fluent/fluent-bit:5.2.0'


def send(endpoint, records):
    payload = []
    for event in records:
        attributes = {
            item['key']: item['value']['stringValue']
            for item in event['attributes']
        }
        payload.append({**attributes, 'log': event['body']['stringValue']})
    with urlopen(Request(
        endpoint, data=json.dumps(payload).encode(),
        headers={'Content-Type': 'application/json'},
    ), timeout=5) as response:
        if response.status != 201:
            raise RuntimeError(f'Fluent Bit HTTP input returned {response.status}')


def run_case(records, *, real_docker=False, shutdown=False, separate_batches=False):
    name = f'reporthub-fluent-bit-probe-{uuid4().hex}'
    docker(
        'create', '--name', name, '--log-driver', 'json-file',
        '--log-opt', 'max-size=2m', '--log-opt', 'max-file=1',
        '--cpus', '2', '--memory', '256m',
        '-p', '127.0.0.1::9880', '-p', '127.0.0.1::24224',
        '-p', '127.0.0.1::2020',
        '-v', f'{ROOT}/otel/experimental/fluent-bit.conf:/probe.conf:ro',
        IMAGE, '-c', '/probe.conf',
    )
    try:
        docker('start', name)
        health = f"http://{docker('port', name, '2020/tcp')}/api/v1/health"
        for _ in range(50):
            try:
                with urlopen(health, timeout=1) as response:
                    if response.status == 200:
                        break
            except (URLError, ConnectionError, TimeoutError):
                if docker('inspect', '--format', '{{.State.Running}}', name) != 'true':
                    raise RuntimeError('Isolated Fluent Bit exited during startup.')
                time.sleep(0.1)
        else:
            raise RuntimeError('Isolated Fluent Bit did not become responsive.')
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
            endpoint = f"http://{docker('port', name, '9880/tcp')}/probe"
            for batch in ([item] for item in records) if separate_batches else [records]:
                send(endpoint, batch)
        if not shutdown:
            time.sleep(3)
        docker('stop', '--time', '10', name)
        logs = subprocess.run(
            ['docker', 'logs', name], check=True, capture_output=True,
            text=True, timeout=10,
        )
        events = [
            json.loads(line) for line in logs.stdout.splitlines()
            if line.startswith('{')
        ]
        return events, logs.stdout + logs.stderr
    except (subprocess.CalledProcessError, RuntimeError) as exc:
        logs = subprocess.run(
            ['docker', 'logs', name], check=True, capture_output=True,
            text=True, timeout=10,
        )
        raise RuntimeError(f'{exc}\n{logs.stdout}\n{logs.stderr}') from exc
    finally:
        docker('rm', '--force', name)


def exact_reconstruction(events, expected):
    return sorted(event.get('log', '').removesuffix('\n') for event in events) == sorted(
        structured(message) for _, message in expected
    )


def main():
    results = []
    for label, records, expected, options in probe_cases():
        events, logs = run_case(records, **options)
        original = expected
        if original is None and label not in ('size limit', 'source limit'):
            original = [('kafka', 'prefix MIDDLE suffix password=probe-secret')]
        exact = exact_reconstruction(events, original) if original is not None else None
        incomplete = bool(events) and all(
            event.get('reporthub.reassembly.error') or event.get('multiline_truncated')
            for event in events
        )
        result = {
            'case': label,
            'accepted': bool(exact) or (expected is None and incomplete),
            'exported_records': len(events),
            'matches_original_message': exact,
            'exported_body_bytes': sum(len(event.get('log', '').encode()) for event in events),
            'error_log_lines': sum('[error]' in line.lower() for line in logs.splitlines()),
        }
        results.append(result)
        print(json.dumps(result), flush=True)
    ready = all(result['accepted'] for result in results)
    print(json.dumps({'ready_for_end_to_end_testing': ready, 'cases': len(results)}))
    return 0 if ready else 1


if __name__ == '__main__':
    sys.exit(main())
