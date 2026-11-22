import logging
from uuid import UUID

from celery.signals import task_postrun, task_prerun
from django.db import DatabaseError, transaction
from kombu.exceptions import OperationalError
from redis.exceptions import RedisError

from control_plane.models import DeadLetterAttempt, DeadLetterTask, SourceProfile, SyncStatus
from control_plane.utils import get_task_progress


logger = logging.getLogger(__name__)

REPORT_TASK = "reports.tasks.execute_report_task"
SOURCE_TASKS = {
    "etl.tasks.sync_data_source_task",
    "etl.tasks.test_source_connection_task",
    "etl.tasks.introspect_source_schema_task",
}
SUPPORTED_TASKS = SOURCE_TASKS | {REPORT_TASK}


def _arguments(task_name, args, kwargs):
    args = args or ()
    kwargs = kwargs or {}
    key = "report_id" if task_name == REPORT_TASK else "source_pk"
    target = args[0] if args else kwargs.get(key)
    execution = (args[1] if len(args) > 1 else kwargs.get("execution_id")) if task_name == REPORT_TASK else None
    try:
        if task_name == REPORT_TASK:
            target = str(UUID(str(target)))
            execution = str(UUID(str(execution))) if execution else ""
        else:
            target = str(int(target))
            if not 0 < int(target) <= 2**63 - 1:
                raise ValueError
    except (TypeError, ValueError, OverflowError):
        return "", ""
    return target, execution or ""


def _attempt(task_id):
    try:
        identifier = UUID(str(task_id))
    except (ValueError, TypeError):
        return None
    return DeadLetterAttempt.objects.select_for_update().select_related("failure").filter(task_id=identifier).first()


@task_prerun.connect(weak=False)
def track_requeue_start(sender=None, task_id=None, **kwargs):
    if not sender or sender.name not in SUPPORTED_TASKS or not task_id:
        return
    try:
        with transaction.atomic():
            attempt = _attempt(task_id)
            if attempt and attempt.status in (
                DeadLetterTask.Status.DISPATCHING, DeadLetterTask.Status.QUEUED, DeadLetterTask.Status.UNCERTAIN,
            ):
                attempt.status = DeadLetterTask.Status.RUNNING
                attempt.save(update_fields=["status", "updated_at"])
                attempt.failure.status = DeadLetterTask.Status.RUNNING
                attempt.failure.save(update_fields=["status", "updated_at"])
    except DatabaseError:
        logger.exception("Unable to record DLQ task start for %s", task_id)


@task_postrun.connect(weak=False)
def track_task_completion(sender=None, task_id=None, args=None, kwargs=None, retval=None, state=None, **unused):
    if not sender or sender.name not in SUPPORTED_TASKS or not task_id or state == "RETRY":
        return
    try:
        progress = get_task_progress(task_id)
    except RedisError:
        logger.exception("Unable to read progress for DLQ task %s", task_id)
        progress = {"status": "UNKNOWN"}

    outcome = progress.get("status", "UNKNOWN")
    if outcome == "CANCELLED":
        status = DeadLetterTask.Status.CANCELLED
    elif state == "FAILURE" or outcome == "FAILED":
        status = DeadLetterTask.Status.FAILED
    elif state == "SUCCESS" and outcome == "SUCCESS":
        status = DeadLetterTask.Status.SUCCEEDED
    else:
        status = DeadLetterTask.Status.UNCERTAIN
    error = str(progress.get("error") or (retval if state == "FAILURE" else ""))[:10000]

    try:
        with transaction.atomic():
            attempt = _attempt(task_id)
            if attempt:
                if attempt.status not in (
                    DeadLetterTask.Status.DISPATCHING, DeadLetterTask.Status.QUEUED,
                    DeadLetterTask.Status.RUNNING, DeadLetterTask.Status.UNCERTAIN,
                ):
                    return
                if status == DeadLetterTask.Status.FAILED:
                    status = DeadLetterTask.Status.FAILED_AGAIN
                attempt.status = status
                attempt.error_message = error
                attempt.save(update_fields=["status", "error_message", "updated_at"])
                attempt.failure.status = status
                attempt.failure.save(update_fields=["status", "updated_at"])
            elif status == DeadLetterTask.Status.FAILED:
                target, execution = _arguments(sender.name, args, kwargs)
                if sender.name == REPORT_TASK and target and not execution:
                    _, execution = _arguments(sender.name, [target, progress.get("execution_id")], {})
                DeadLetterTask.objects.get_or_create(
                    original_task_id=task_id,
                    defaults={
                        "task_name": sender.name,
                        "target_id": target,
                        "original_execution_id": execution,
                        "error_message": error,
                    },
                )
    except DatabaseError:
        logger.exception("Unable to record DLQ task outcome for %s", task_id)


class RetryRejected(Exception):
    pass


class DispatchUncertain(Exception):
    pass


def requeue_failure(failure_id, user):
    from etl.tasks import introspect_source_schema_task, sync_data_source_task, test_source_connection_task
    from reports.models import ReportDefinition, ReportExecution
    from reports.tasks import execute_report_task

    tasks = {
        REPORT_TASK: execute_report_task,
        "etl.tasks.sync_data_source_task": sync_data_source_task,
        "etl.tasks.test_source_connection_task": test_source_connection_task,
        "etl.tasks.introspect_source_schema_task": introspect_source_schema_task,
    }
    with transaction.atomic():
        failure = DeadLetterTask.objects.select_for_update().get(pk=failure_id)
        if failure.status not in (DeadLetterTask.Status.FAILED, DeadLetterTask.Status.FAILED_AGAIN):
            raise RetryRejected("Only failed tasks can be requeued.")
        if failure.task_name not in tasks or not failure.target_id:
            raise RetryRejected("This entry does not contain supported retry details.")
        task = tasks[failure.task_name]
        execution = None
        if failure.task_name == REPORT_TASK:
            report = ReportDefinition.objects.select_for_update().filter(pk=failure.target_id, is_active=True).first()
            if not report or not report.source.is_active:
                raise RetryRejected("The report or its data source is unavailable or inactive.")
            previous = failure.attempts.first()
            execution_id = previous.execution_id if previous else failure.original_execution_id
            old_execution = ReportExecution.objects.filter(pk=execution_id, report=report).first() if execution_id else None
            if not old_execution or old_execution.status != "failed" or old_execution.artifact_reconciliation_available:
                raise RetryRejected("The previous report run must be failed and have no unresolved artifact upload.")
            if ReportExecution.objects.filter(report=report, status__in=["pending", "running"]).exists():
                raise RetryRejected("The report already has a pending or running execution.")
            execution = ReportExecution.objects.create(report=report, status="pending", created_by=user)
            task_args = [str(report.pk), str(execution.pk)]
            queue = "queue_warm"
        else:
            source = SourceProfile.objects.select_for_update().filter(pk=failure.target_id, is_active=True).first()
            if not source:
                raise RetryRejected("The data source is unavailable or inactive.")
            if source.last_sync_status == SyncStatus.RUNNING:
                raise RetryRejected("The data source already has a running operation.")
            source.last_sync_status = SyncStatus.RUNNING
            source.save(update_fields=["last_sync_status"])
            task_args = [source.pk]
            queue = "queue_hot" if failure.task_name == "etl.tasks.sync_data_source_task" else "queue_ux"
        attempt = DeadLetterAttempt.objects.create(
            failure=failure, requested_by=user, status=DeadLetterTask.Status.DISPATCHING,
            execution_id=str(execution.pk) if execution else "",
        )
        failure.status = DeadLetterTask.Status.DISPATCHING
        failure.save(update_fields=["status", "updated_at"])

    try:
        task.apply_async(args=task_args, task_id=str(attempt.task_id), queue=queue)
    except (OperationalError, OSError, RedisError) as exc:
        logger.exception("DLQ task publication outcome is uncertain for %s", attempt.task_id)
        with transaction.atomic():
            attempt = DeadLetterAttempt.objects.select_for_update().select_related("failure").get(pk=attempt.pk)
            if attempt.status == DeadLetterTask.Status.DISPATCHING:
                attempt.status = DeadLetterTask.Status.UNCERTAIN
                attempt.save(update_fields=["status", "updated_at"])
                attempt.failure.status = DeadLetterTask.Status.UNCERTAIN
                attempt.failure.save(update_fields=["status", "updated_at"])
        raise DispatchUncertain("The queue outcome is uncertain. Check the task before retrying.") from exc

    with transaction.atomic():
        attempt = DeadLetterAttempt.objects.select_for_update().select_related("failure").get(pk=attempt.pk)
        if attempt.status == DeadLetterTask.Status.DISPATCHING:
            attempt.status = DeadLetterTask.Status.QUEUED
            attempt.save(update_fields=["status", "updated_at"])
            attempt.failure.status = DeadLetterTask.Status.QUEUED
            attempt.failure.save(update_fields=["status", "updated_at"])
