import json
import logging
from pathlib import Path
import time

from django.conf import settings

from operations_plane.models import OperationsLogSources
from otel.source_policy import CONTAINER_SOURCE_PATTERNS, SOURCE_CATEGORIES, default_sources


logger = logging.getLogger(__name__)


def control_directory():
    return Path(getattr(settings, 'OPERATIONS_LOGGING_CONTROL_DIR', '/var/lib/reporthub-logging'))


def collection_status(selection=None):
    if selection is None:
        selection = OperationsLogSources.objects.filter(pk=1).first()
    status = {}
    error = ''
    try:
        status = json.loads((control_directory() / 'status.json').read_text())
        if not isinstance(status, dict):
            raise ValueError('Collector status must be an object.')
    except FileNotFoundError:
        error = 'Collector confirmation is not available yet.'
    except (OSError, ValueError):
        logger.exception('Could not read Collector source status.')
        error = 'Could not read Collector confirmation.'
    heartbeat = status.get('heartbeat')
    alive = isinstance(heartbeat, (float, int)) and 0 <= time.time() - heartbeat < 15
    if not alive and not error:
        error = 'Collector confirmation is stale or unavailable. Saved choices may not be active.'
    applied = bool(alive and selection and status.get('revision') == str(selection.revision))
    failed = bool(selection and status.get('failed_revision') == str(selection.revision))
    if failed:
        error = status.get('error') or 'Collector could not apply the saved choices.'
    enabled = selection.enabled_sources if selection else default_sources()
    categories = []
    for key, label in SOURCE_CATEGORIES:
        container = key in CONTAINER_SOURCE_PATTERNS or key in ('other', 'etl')
        effective = key in enabled and (not container or 'fluent' in enabled)
        categories.append({
            'key': key, 'label': label, 'enabled': effective,
            'state': ('Enabled' if effective else 'Ingestion disabled') if applied else 'Unconfirmed',
        })
    return {
        'applied': applied,
        'pending': bool(selection and not applied and not failed),
        'status_error': error,
        'categories': categories,
        'fluent_enabled': 'fluent' in enabled,
        'enabled_count': sum(category['enabled'] for category in categories),
    }
