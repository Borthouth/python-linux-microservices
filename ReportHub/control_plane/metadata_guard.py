from contextlib import contextmanager

from django.db import connection, transaction

from control_plane.models import CdcToBatchTransition


class SourceMetadataCutoverPending(RuntimeError):
    pass


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
