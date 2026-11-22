import os
import logging

from celery import Celery
from celery.signals import after_setup_logger, after_setup_task_logger, task_postrun, task_prerun

from config.logging import JsonLogFormatter

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

celery_app = Celery('reporthub_worker')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# Namespace 'CELERY' means all celery-related configuration keys in settings.py should start with CELERY_
celery_app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django apps.
celery_app.autodiscover_tasks()

logger = logging.getLogger(__name__)


@after_setup_logger.connect(weak=False)
@after_setup_task_logger.connect(weak=False)
def format_celery_logs(logger=None, **kwargs):
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setFormatter(JsonLogFormatter())


def _log_task_event(task_id, task, state):
    if task is None:
        return
    delivery_info = getattr(task.request, 'delivery_info', None) or {}
    queue = delivery_info.get('routing_key') or ''
    logger.info(
        'REPORTHUB_TASK_EVENT task_id=%s task_name=%s queue=%s state=%s',
        task_id or '',
        getattr(task, 'name', ''),
        queue,
        state,
    )


@task_prerun.connect(weak=False)
def log_task_started(task_id=None, task=None, **kwargs):
    _log_task_event(task_id, task, 'started')


@task_postrun.connect(weak=False)
def log_task_finished(task_id=None, task=None, state=None, **kwargs):
    _log_task_event(task_id, task, (state or 'unknown').lower())