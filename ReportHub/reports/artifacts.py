from django.db import transaction
from django.utils import timezone
from minio.error import S3Error

from .models import ReportArtifactAttempt, ReportExecution


MISSING_OBJECT_CODES = {"NoSuchBucket", "NoSuchKey", "NoSuchObject"}


def report_artifact_matches(client, bucket_name, attempt):
    if not attempt.file_path or attempt.size is None or not attempt.sha256:
        return False

    try:
        stored = client.stat_object(bucket_name, attempt.file_path)
    except S3Error as exc:
        if exc.code in MISSING_OBJECT_CODES:
            return False
        raise

    metadata = {
        str(key).lower(): str(value)
        for key, value in (stored.metadata or {}).items()
    }
    stored_checksum = metadata.get("reporthub-sha256") or metadata.get(
        "x-amz-meta-reporthub-sha256"
    )
    return (
        stored.size == attempt.size
        and stored_checksum == attempt.sha256
    )


def mark_report_artifact_verified(attempt_id):
    return ReportArtifactAttempt.objects.filter(
        pk=attempt_id, status=ReportArtifactAttempt.Status.UPLOADING
    ).update(
        status=ReportArtifactAttempt.Status.VERIFIED,
        error_message="",
        updated_at=timezone.now(),
    ) == 1


@transaction.atomic
def finalize_report_artifact(attempt_id):
    execution_id = ReportArtifactAttempt.objects.values_list(
        "execution_id", flat=True
    ).get(pk=attempt_id)
    execution = ReportExecution.objects.select_for_update().get(pk=execution_id)
    attempt = ReportArtifactAttempt.objects.select_for_update().get(pk=attempt_id)

    if (
        attempt.status == ReportArtifactAttempt.Status.COMPLETED
        and execution.status == "completed"
        and execution.file_path == attempt.file_path
    ):
        return "completed"

    if attempt.status not in {
        ReportArtifactAttempt.Status.UPLOADING,
        ReportArtifactAttempt.Status.VERIFIED,
    }:
        return "not_recoverable"

    if execution.current_artifact_attempt_id != attempt.pk:
        attempt.status = ReportArtifactAttempt.Status.SUPERSEDED
        attempt.save(update_fields=["status", "updated_at"])
        return "superseded"

    if execution.is_cancelled or execution.status == "cancelled":
        return "cancelled"

    if execution.status not in {"running", "failed"}:
        return "not_recoverable"

    attempt.status = ReportArtifactAttempt.Status.COMPLETED
    attempt.error_message = ""
    attempt.save(update_fields=["status", "error_message", "updated_at"])

    execution.status = "completed"
    execution.file_path = attempt.file_path
    execution.row_count = attempt.row_count
    execution.completed_at = timezone.now()
    execution.progress_percentage = 100
    execution.error_message = ""
    execution.save(update_fields=[
        "status", "file_path", "row_count", "completed_at",
        "progress_percentage", "error_message", "updated_at",
    ])
    return "completed"
