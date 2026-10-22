import logging
import time

from celery.signals import task_prerun, task_postrun

logger = logging.getLogger("performance_watchdog")

_task_start_times = {}


@task_prerun.connect
def task_started(task_id=None, task=None, **kwargs):
    if not task_id:
        return

    # Do not monitor the watchdog's own reporting task.
    if task and task.name.startswith("performance_watchdog."):
        return

    _task_start_times[task_id] = time.perf_counter()


@task_postrun.connect
def task_finished(
    task_id=None,
    task=None,
    state=None,
    **kwargs,
):
    if not task_id:
        return

    start_time = _task_start_times.pop(task_id, None)

    if start_time is None:
        return

    duration = time.perf_counter() - start_time

    report = f"""
======================================================================
CELERY PERFORMANCE WATCHDOG
======================================================================

Task: {task.name if task else "Unknown"}
Task ID: {task_id}
Execution Duration: {duration:.4f} seconds
Status: {state}

======================================================================
"""

    logger.warning(report)