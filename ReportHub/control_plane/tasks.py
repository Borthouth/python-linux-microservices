import logging
import stat
from datetime import timedelta
from uuid import uuid4

from billiard.exceptions import SoftTimeLimitExceeded
from celery import shared_task
from django.db import transaction
from django.utils import timezone
from kombu.exceptions import OperationalError
import paramiko

from control_plane.models import CdcResourceCleanup, TransferDestination
from reports.sftp import DeliveryError, sftp_connection


logger = logging.getLogger(__name__)
TEST_IN_PROGRESS = (
    TransferDestination.TestStatus.QUEUED, TransferDestination.TestStatus.RUNNING,
)


def expire_destination_test(destination):
    if (
        destination.last_test_status in TEST_IN_PROGRESS
        and destination.last_tested_at
        and destination.last_tested_at < timezone.now() - timedelta(minutes=5)
    ):
        message = "The connection test timed out or the transfer worker did not respond. Test again."
        updated = TransferDestination.objects.filter(
            pk=destination.pk, last_test_task_id=destination.last_test_task_id,
            last_test_status__in=TEST_IN_PROGRESS,
            last_tested_at=destination.last_tested_at,
        ).update(last_test_status=TransferDestination.TestStatus.FAILED, last_test_error=message)
        if updated:
            destination.last_test_status = TransferDestination.TestStatus.FAILED
            destination.last_test_error = message
            logger.warning("SFTP connection test expired for destination %s", destination.pk)


def _publish_destination_test(destination_id, task_id):
    try:
        test_transfer_destination_task.apply_async(
            args=[destination_id], task_id=task_id, queue="queue_transfers", expires=120,
        )
    except OperationalError:
        message = "Unable to queue the connection test. Check the broker and try again."
        TransferDestination.objects.filter(
            pk=destination_id, last_test_task_id=task_id,
            last_test_status=TransferDestination.TestStatus.QUEUED,
        ).update(last_test_status=TransferDestination.TestStatus.FAILED, last_test_error=message)
        logger.error("Unable to publish SFTP connection test for destination %s", destination_id)
        raise DeliveryError(message) from None


def queue_destination_test(destination):
    """Queue a test while the caller holds the destination's row lock."""
    expire_destination_test(destination)
    if destination.last_test_status in TEST_IN_PROGRESS:
        raise DeliveryError("A connection test is already queued or running for this destination.")
    task_id = str(uuid4())
    TransferDestination.objects.filter(pk=destination.pk).update(
        last_test_status=TransferDestination.TestStatus.QUEUED,
        last_test_error="", last_test_task_id=task_id, last_tested_at=timezone.now(),
    )
    transaction.on_commit(lambda: _publish_destination_test(destination.pk, task_id))


@shared_task(bind=True, soft_time_limit=60, time_limit=90)
def test_transfer_destination_task(self, destination_id):
    with transaction.atomic():
        destination = TransferDestination.objects.select_for_update().filter(pk=destination_id).first()
        if (
            destination is None
            or destination.last_test_task_id != self.request.id
            or destination.last_test_status != TransferDestination.TestStatus.QUEUED
        ):
            logger.info("Ignoring deleted, duplicate or superseded SFTP connection test %s", self.request.id)
            return "Connection test no longer pending."
        TransferDestination.objects.filter(pk=destination_id).update(
            last_test_status=TransferDestination.TestStatus.RUNNING,
        )
    try:
        with sftp_connection(destination) as sftp:
            attributes = sftp.stat(destination.remote_directory)
            if attributes.st_mode is None or not stat.S_ISDIR(attributes.st_mode):
                raise DeliveryError("The configured remote directory is not a directory.")
    except (DeliveryError, OSError, EOFError, paramiko.SSHException, SoftTimeLimitExceeded) as error:
        if isinstance(error, DeliveryError):
            message = str(error)
        elif isinstance(error, paramiko.AuthenticationException):
            message = "SFTP authentication failed. Review the destination credentials."
        elif isinstance(error, SoftTimeLimitExceeded):
            message = "The SFTP connection test exceeded its time limit."
        else:
            message = "Unable to connect or access the remote directory. Check connectivity, SSH settings and directory permissions."
        TransferDestination.objects.filter(
            pk=destination_id, last_test_task_id=self.request.id,
            last_test_status=TransferDestination.TestStatus.RUNNING,
        ).update(
            last_test_status=TransferDestination.TestStatus.FAILED,
            last_test_error=message, last_tested_at=timezone.now(),
        )
        logger.error("SFTP connection test for destination %s failed (%s): %s", destination_id, type(error).__name__, message)
        raise DeliveryError(message) from None
    TransferDestination.objects.filter(
        pk=destination_id, last_test_task_id=self.request.id,
        last_test_status=TransferDestination.TestStatus.RUNNING,
    ).update(
        last_test_status=TransferDestination.TestStatus.SUCCESS,
        last_test_error="", last_tested_at=timezone.now(),
    )
    return "Connection, pinned host key, authentication and remote directory verified. No files were written."


def _claim_cleanup_task(cleanup_id, task_id, expected_status):
    with transaction.atomic():
        cleanup = CdcResourceCleanup.objects.select_for_update().filter(
            pk=cleanup_id,
            status=expected_status,
        ).first()
        if (
            cleanup is None
            or cleanup.started_at is not None
            or (cleanup.task_id and cleanup.task_id != task_id)
        ):
            return None
        cleanup.task_id = task_id or ""
        cleanup.started_at = timezone.now()
        cleanup.updated_by = cleanup.created_by
        cleanup._history_user = cleanup.created_by
        cleanup.save(update_fields=[
            "task_id", "started_at", "updated_by", "updated_at",
        ])
        return cleanup


def _record_cleanup_task_failure(cleanup_id, error):
    cleanup = CdcResourceCleanup.objects.filter(pk=cleanup_id).first()
    if cleanup is None:
        logger.error("CDC resource cleanup %s disappeared after task failure.", cleanup_id)
        return
    safe_message = (
        f"{type(error).__name__}: cleanup task failed; see maintenance worker logs."
    )
    logger.error("CDC resource cleanup %s failed: %s", cleanup_id, safe_message)
    cleanup.status = CdcResourceCleanup.Status.FAILED
    cleanup.last_error = safe_message
    cleanup.completed_at = timezone.now()
    cleanup.updated_by = cleanup.created_by
    cleanup._history_user = cleanup.created_by
    cleanup.save(update_fields=[
        "status", "last_error", "completed_at", "updated_by", "updated_at",
    ])


@shared_task(
    bind=True,
    queue="queue_maintenance",
    soft_time_limit=240,
    time_limit=300,
)
def scan_cdc_resources_task(self, cleanup_id):
    cleanup = _claim_cleanup_task(
        cleanup_id,
        self.request.id,
        CdcResourceCleanup.Status.SCANNING,
    )
    if cleanup is None:
        return "CDC resource inventory is no longer pending."
    try:
        from etl.cdc_cleanup import scan_cleanup_resources

        scan_cleanup_resources(cleanup_id)
    except Exception as error:
        _record_cleanup_task_failure(cleanup_id, error)
        raise
    return f"CDC resource inventory completed with status {CdcResourceCleanup.objects.get(pk=cleanup_id).status}."


@shared_task(
    bind=True,
    queue="queue_maintenance",
    soft_time_limit=240,
    time_limit=300,
)
def cleanup_cdc_resources_task(self, cleanup_id):
    cleanup = _claim_cleanup_task(
        cleanup_id,
        self.request.id,
        CdcResourceCleanup.Status.QUEUED,
    )
    if cleanup is None:
        return "CDC resource cleanup is no longer queued."
    try:
        from etl.cdc_cleanup import execute_cleanup

        execute_cleanup(cleanup_id)
    except Exception as error:
        _record_cleanup_task_failure(cleanup_id, error)
        raise
    return f"CDC resource cleanup completed with status {CdcResourceCleanup.objects.get(pk=cleanup_id).status}."
