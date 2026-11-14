import hashlib
import json
import logging
import posixpath
import shutil
import tempfile
from uuid import uuid4

from billiard.exceptions import SoftTimeLimitExceeded
from botocore.exceptions import BotoCoreError, ClientError
from celery import shared_task
from django.core.files.storage import default_storage
from django.db import transaction
from django.utils import timezone
from kombu.exceptions import OperationalError
import paramiko

from reports.models import ReportFileDelivery
from reports.sftp import DeliveryError, publish_file, sftp_connection


logger = logging.getLogger(__name__)


def destination_signature(destination):
    config = {
        field: getattr(destination, field)
        for field in (
            "protocol", "host", "port", "username", "authentication",
            "host_key_sha256", "remote_directory",
        )
    }
    return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()


def validate_delivery(delivery):
    destination = delivery.destination
    if destination is None:
        raise DeliveryError("The transfer destination was deleted.")
    if not destination.is_active:
        raise DeliveryError("The transfer destination is inactive.")
    if destination_signature(destination) != delivery.destination_signature:
        raise DeliveryError("The destination connection settings changed. Run a new report instead of redirecting an old delivery.")
    if (
        delivery.report is None
        or not delivery.report.is_file_delivery_active
        or not delivery.report.transfer_destinations.filter(pk=destination.pk).exists()
    ):
        raise DeliveryError("File delivery is disabled or this destination was removed from the report.")
    execution = delivery.execution
    if (
        execution is None or execution.status != "completed" or execution.is_cancelled
        or not delivery.file_path or execution.file_path != delivery.file_path
        or execution.report_id != delivery.report_id
    ):
        raise DeliveryError("The completed report artifact is no longer available for this delivery.")
    return destination


def _publish_task(delivery_id, task_id):
    try:
        deliver_report_file_task.apply_async(
            args=[str(delivery_id)], task_id=task_id, queue="queue_transfers"
        )
    except OperationalError:
        message = "Unable to queue the SFTP delivery. Check the broker, then resend this delivery."
        ReportFileDelivery.objects.filter(
            pk=delivery_id, task_id=task_id, status=ReportFileDelivery.Status.QUEUED
        ).update(status=ReportFileDelivery.Status.FAILED, error_message=message, updated_at=timezone.now())
        logger.error("Unable to publish SFTP delivery %s", delivery_id)
        raise DeliveryError(message) from None


def queue_delivery(delivery_id, retry=False):
    with transaction.atomic():
        delivery = ReportFileDelivery.objects.select_for_update().get(pk=delivery_id)
        expected = ReportFileDelivery.Status.FAILED if retry else ReportFileDelivery.Status.QUEUED
        if delivery.status != expected or (not retry and delivery.task_id):
            raise DeliveryError("Only a failed delivery can be resent; queued, running and delivered records are not resent.")
        if retry:
            validate_delivery(delivery)
            delivery.retry_count += 1
        delivery.status = ReportFileDelivery.Status.QUEUED
        delivery.error_message = ""
        delivery.completed_at = None
        delivery.task_id = str(uuid4())
        delivery.save(update_fields=[
            "status", "error_message", "completed_at", "task_id", "retry_count", "updated_at",
        ])
        transaction.on_commit(
            lambda: _publish_task(delivery.pk, delivery.task_id)
        )
    return delivery


def queue_report_deliveries(execution, destinations):
    extension = posixpath.splitext(execution.file_path)[1].lower()
    if extension not in {".csv", ".xlsx", ".json", ".pdf"}:
        raise DeliveryError("The report artifact has an unsupported file extension.")
    for destination in destinations:
        delivery_id = uuid4()
        delivery, created = ReportFileDelivery.objects.get_or_create(
            execution=execution, destination=destination,
            defaults={
                "id": delivery_id, "report": execution.report,
                "destination_name": destination.name,
                "destination_signature": destination_signature(destination),
                "file_path": execution.file_path,
                "remote_path": posixpath.join(
                    destination.remote_directory,
                    f"report-{execution.pk}-{delivery_id}{extension}",
                ),
            },
        )
        if created:
            try:
                queue_delivery(delivery.pk)
            except DeliveryError:
                logger.error("Report %s has an unqueued file delivery %s", execution.pk, delivery.pk)


def _error_message(error):
    if isinstance(error, DeliveryError):
        return str(error)
    if isinstance(error, paramiko.AuthenticationException):
        return "SFTP authentication failed. Review the destination credentials."
    if isinstance(error, SoftTimeLimitExceeded):
        return "SFTP delivery exceeded its time limit. Check the remote file before resending."
    if isinstance(error, (BotoCoreError, ClientError)):
        return "Unable to retrieve the report artifact from storage."
    if isinstance(error, paramiko.SSHException):
        return "The SSH/SFTP connection failed. Review server access and authentication settings."
    return "SFTP or artifact I/O failed. Check connectivity, directory permissions and available space."


@shared_task(
    bind=True, name="reports.tasks.deliver_report_file_task",
    soft_time_limit=300, time_limit=330, acks_late=True,
)
def deliver_report_file_task(self, delivery_id):
    with transaction.atomic():
        delivery = ReportFileDelivery.objects.select_for_update().get(pk=delivery_id)
        if delivery.status != ReportFileDelivery.Status.QUEUED or delivery.task_id != self.request.id:
            logger.info("Ignoring duplicate or stale SFTP task for delivery %s", delivery_id)
            return "Delivery already claimed or task superseded."
        delivery.status = ReportFileDelivery.Status.RUNNING
        delivery.attempt_count += 1
        delivery.save(update_fields=["status", "attempt_count", "updated_at"])
    try:
        destination = validate_delivery(delivery)
        with tempfile.TemporaryFile() as spool:
            with default_storage.open(delivery.file_path, "rb") as artifact:
                shutil.copyfileobj(artifact, spool, length=65536)
            spool.seek(0)
            with sftp_connection(destination) as sftp:
                publish_file(sftp, spool, delivery.remote_path)
    except (
        DeliveryError, OSError, EOFError, paramiko.SSHException,
        BotoCoreError, ClientError, SoftTimeLimitExceeded,
    ) as error:
        message = _error_message(error)
        ReportFileDelivery.objects.filter(pk=delivery_id, task_id=self.request.id).update(
            status=ReportFileDelivery.Status.FAILED, error_message=message,
            completed_at=timezone.now(), updated_at=timezone.now(),
        )
        logger.error("SFTP delivery %s failed (%s): %s", delivery_id, type(error).__name__, message)
        raise DeliveryError(message) from None
    ReportFileDelivery.objects.filter(pk=delivery_id, task_id=self.request.id).update(
        status=ReportFileDelivery.Status.SUCCEEDED, error_message="",
        completed_at=timezone.now(), updated_at=timezone.now(),
    )
    return "File delivered successfully."
