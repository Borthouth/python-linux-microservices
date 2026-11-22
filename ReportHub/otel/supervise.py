import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen

import yaml

from source_policy import default_sources, filter_conditions


CONTROL = Path('/var/lib/reporthub-logging')
RUNTIME = Path('/var/lib/otelcol/runtime')
BINARY = '/usr/local/bin/otelcol-contrib'
BASE = Path('/etc/otelcol-contrib/config.yaml')
logger = logging.getLogger(__name__)
stopping = False


def write_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value))
    os.replace(temporary, path)


def render_config(enabled):
    config = yaml.safe_load(BASE.read_text())
    config['processors']['filter/sources'] = {
        'error_mode': 'propagate',
        'logs': {'log_record': filter_conditions(enabled)},
    }
    processors = config['service']['pipelines']['logs']['processors']
    processors.insert(processors.index('transform/logs') + 1, 'filter/sources')
    return yaml.safe_dump(config)


def start(path):
    return subprocess.Popen([BINARY, f'--config={path}'])


def stop(child):
    child.terminate()
    try:
        child.wait(timeout=20)
    except subprocess.TimeoutExpired:
        logger.error('Collector did not stop gracefully; terminating it.')
        child.kill()
        child.wait()


def healthy(child):
    for _ in range(40):
        if stopping or child.poll() is not None:
            return False
        try:
            with urlopen('http://127.0.0.1:13133/', timeout=1) as response:
                if response.status == 200:
                    return True
        except (URLError, OSError):
            pass
        time.sleep(0.25)
    return False


def request_stop(signum, frame):
    global stopping
    stopping = True


def main():
    CONTROL.mkdir(parents=True, exist_ok=True)
    RUNTIME.mkdir(parents=True, exist_ok=True)
    active = RUNTIME / 'active.yaml'
    candidate = RUNTIME / 'candidate.yaml'
    last_good = RUNTIME / 'selection.json'
    previous = json.loads(last_good.read_text()) if last_good.exists() else {
        'enabled_sources': default_sources(), 'revision': None,
    }
    active.write_text(render_config(previous['enabled_sources']))
    child = start(active)
    applied_revision = previous['revision']
    attempted = None
    status = {'revision': applied_revision, 'error': ''}
    try:
        if not healthy(child):
            raise RuntimeError('Collector failed initial health check.')
        while not stopping:
            if child.poll() is not None:
                raise RuntimeError(f'Collector exited with code {child.returncode}.')
            desired = CONTROL / 'desired.json'
            if desired.exists():
                content = desired.read_text()
                if content != attempted:
                    attempted = content
                    revision = None
                    try:
                        selection = json.loads(content)
                        revision = selection['revision']
                        if not isinstance(revision, str):
                            raise ValueError('Invalid source revision.')
                        candidate.write_text(render_config(selection['enabled_sources']))
                        validation = subprocess.run(
                            [BINARY, 'validate', f'--config={candidate}'],
                            capture_output=True, text=True, timeout=30,
                        )
                        if validation.returncode:
                            logger.error('Source configuration validation failed: %s', validation.stderr)
                            raise ValueError('Collector rejected the source configuration.')
                        stop(child)
                        child = start(candidate)
                        if not healthy(child):
                            stop(child)
                            child = start(active)
                            if not healthy(child):
                                raise RuntimeError('Collector rollback failed.')
                            raise ValueError('Collector could not apply the selection; previous configuration restored.')
                        os.replace(candidate, active)
                        write_json(last_good, selection)
                        applied_revision = revision
                        status = {
                            'revision': applied_revision, 'error': '',
                        }
                    except (ValueError, KeyError, TypeError, OSError, yaml.YAMLError, subprocess.TimeoutExpired) as exc:
                        logger.exception('Could not apply log source selection.')
                        status = {
                            'revision': applied_revision,
                            'failed_revision': revision,
                            'error': str(exc),
                        }
            write_json(CONTROL / 'status.json', {**status, 'heartbeat': time.time()})
            time.sleep(1)
    finally:
        stop(child)


if __name__ == '__main__':
    if len(sys.argv) > 1 and not sys.argv[1].startswith('--config='):
        os.execv(BINARY, [BINARY, *sys.argv[1:]])
    logging.basicConfig(level=logging.INFO)
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    main()
