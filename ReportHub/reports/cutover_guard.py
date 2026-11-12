from contextlib import contextmanager

from django.db import connection, transaction

from control_plane.models import CdcSourceExternalOperation, CdcToBatchTransition
from etl.cdc_external_operations import _track_cdc_external_operation
from reports.models import ReportDefinition


@contextmanager
def report_query_guard(report, task_id=""):
    """Keep cooperating cutovers out while a report reads a source."""
    with _track_cdc_external_operation(
        report.source_id, CdcSourceExternalOperation.Kind.REPORT, task_id
    ):
        with _report_query_guard(report) as current:
            yield current


@contextmanager
def _report_query_guard(report):
    with transaction.atomic():
        if connection.vendor != "postgresql":
            raise RuntimeError("Report cutover coordination requires PostgreSQL.")
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock_shared(hashtextextended(%s, 0))",
                [f"reporthub:report-cutover:{report.source_id}"],
            )
        current = ReportDefinition.objects.select_related("base_table").get(pk=report.pk)
        if current.source_id != report.source_id:
            raise RuntimeError("The report source changed while waiting to run.")
        if CdcToBatchTransition.objects.filter(
            source_id=current.source_id,
            status=CdcToBatchTransition.Status.CUTOVER_PENDING,
        ).exists():
            raise RuntimeError(
                "Report cannot run while CDC-to-batch cutover is pending for its source."
            )
        yield current


def acquire_report_cutover_lock(source_id):
    """Require an atomic block held through a future cutover's pending-state commit."""
    if connection.vendor != "postgresql" or not connection.in_atomic_block:
        raise RuntimeError("Report cutover lock requires a PostgreSQL transaction.")
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            [f"reporthub:report-cutover:{source_id}"],
        )
