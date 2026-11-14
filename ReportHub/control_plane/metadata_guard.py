from contextlib import contextmanager

from django.db import connection, transaction

from django.db.models import Q
from django.shortcuts import get_object_or_404

from control_plane.models import CdcResourceCleanup, CdcToBatchTransition, SourceProfile


class SourceMetadataCutoverPending(RuntimeError):
    pass


class SourceMetadataCleanupPending(RuntimeError):
    pass


def guard_source_cleanup_deletion(source_id):
    if not connection.in_atomic_block:
        raise RuntimeError("Source cleanup coordination requires a transaction.")
    get_object_or_404(SourceProfile.objects.select_for_update(), pk=source_id)
    if CdcResourceCleanup.objects.filter(
        rebuild__source_table__source_id=source_id,
    ).filter(
        Q(status__in=[
            CdcResourceCleanup.Status.QUEUED,
            CdcResourceCleanup.Status.RUNNING,
        ]) | Q(external_operation_uncertain=True, uncertainty_override_at__isnull=True)
    ).exists():
        raise SourceMetadataCleanupPending(
            "Source metadata cannot be deleted while a cleanup is queued, running, "
            "or has an unresolved external operation. Review CDC Resource Cleanup first."
        )


def acquire_source_metadata_lock(source_id):
    if not connection.in_atomic_block:
        raise RuntimeError("Source metadata coordination requires a transaction.")
    if connection.vendor == "postgresql":
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                [f"reporthub:source-metadata:{source_id}"],
            )


@contextmanager
def source_metadata_write_guard(source_id):
    with transaction.atomic():
        acquire_source_metadata_lock(source_id)
        if CdcToBatchTransition.objects.filter(
            source_id=source_id,
            status=CdcToBatchTransition.Status.CUTOVER_PENDING,
        ).exists():
            raise SourceMetadataCutoverPending(
                "Source metadata cannot be changed while CDC-to-batch cutover is pending."
            )
        yield
