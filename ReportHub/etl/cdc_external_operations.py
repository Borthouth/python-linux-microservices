from contextlib import contextmanager

from django.conf import settings
from django.db import connection, transaction

from control_plane.models import (
    CdcSourceExternalOperation,
    CdcToBatchTransition,
    DbChoice,
    IngestionMode,
    SourceProfile,
)


class CdcExternalOperationCutoverPending(RuntimeError):
    pass


def _require_source_row_locks():
    if not connection.features.has_select_for_update:
        raise RuntimeError("CDC external operation coordination requires row locks.")


def _begin_cdc_external_operation(source_pk, kind, task_id=""):
    """Commit a source-scoped record before starting any external work."""
    if kind not in CdcSourceExternalOperation.Kind.values:
        raise ValueError("Invalid CDC external operation kind.")
    if not isinstance(task_id, str) or len(task_id) > 255:
        raise ValueError("Invalid CDC external operation task ID.")
    _require_source_row_locks()
    with transaction.atomic(durable=True):
        source = SourceProfile.objects.select_for_update().get(pk=source_pk)
        if kind != CdcSourceExternalOperation.Kind.REPORT and (
            not source.is_active
            or source.ingestion_mode != IngestionMode.CDC
            or source.db_type == DbChoice.MONGODB
        ):
            raise RuntimeError("The source is not eligible for CDC external work.")
        if source.cdc_to_batch_transitions.filter(
            status=CdcToBatchTransition.Status.CUTOVER_PENDING
        ).exists():
            raise CdcExternalOperationCutoverPending(
                "CDC-to-batch cutover is pending for this source."
            )
        return CdcSourceExternalOperation.objects.create(
            source=source, kind=kind, task_id=task_id
        )


def _complete_cdc_external_operation(source_pk, operation_pk):
    """Remove a record only after the caller has proved all external work ended."""
    _require_source_row_locks()
    with transaction.atomic(durable=True):
        source = SourceProfile.objects.select_for_update().get(pk=source_pk)
        operation = CdcSourceExternalOperation.objects.select_for_update().get(
            pk=operation_pk, source=source
        )
        operation.delete()


@contextmanager
def _track_cdc_external_operation(source_pk, kind, task_id=""):
    """Keep the record if execution fails or its completion cannot be saved."""
    if not getattr(settings, "CDC_EXTERNAL_OPERATION_TRACKING", False):
        yield
        return
    operation = _begin_cdc_external_operation(source_pk, kind, task_id)
    yield
    _complete_cdc_external_operation(source_pk, operation.pk)


@contextmanager
def _cdc_external_operation_barrier(source_pk):
    """Hold the source lock after checking records; the caller must commit pending inside."""
    _require_source_row_locks()
    with transaction.atomic(durable=True):
        source = SourceProfile.objects.select_for_update().get(pk=source_pk)
        _assert_no_cdc_external_operations(source)
        yield source


def _assert_no_cdc_external_operations(source):
    """Call only while holding the source row lock."""
    if source.cdc_external_operations.exists():
        raise RuntimeError(
            "CDC external work is active or has an uncertain outcome for this source."
        )
