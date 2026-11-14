import csv
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from uuid import uuid4

import clickhouse_connect
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from app.core.celery_app import celery_app
from control_plane.models import SourceField, SourceProfile, SourceTable, TransferDestination
from django.conf import settings
from django.core.files.storage import default_storage
from django.core.management import call_command
from reports.delivery import queue_delivery
from reports.models import ReportDefinition, ReportExecution, ReportFileDelivery
from reports.query_builder import source_table_clickhouse_name
from reports.sftp import sftp_connection
from reports.tasks import execute_report_task


def wait_for(predicate, description, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for {description}")


def main():
    call_command("migrate", interactive=False, verbosity=0)
    source = SourceProfile.objects.create(
        name="Synthetic report SFTP source", host="unused.test", port=5432,
        database_name="synthetic", username="reader",
    )
    table = SourceTable.objects.create(source=source, table_name="items")
    SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
    client = clickhouse_connect.get_client(
        host=settings.CLICKHOUSE_HOST, port=settings.CLICKHOUSE_PORT,
        username=settings.CLICKHOUSE_USER, password=settings.CLICKHOUSE_PASSWORD,
    )
    try:
        physical = source_table_clickhouse_name(table)
        client.command(f"CREATE TABLE `{physical}` (id Int32) ENGINE=MergeTree ORDER BY tuple()")
        client.insert(physical, [[1], [2], [3]], column_names=["id"])
    finally:
        client.close()
    fingerprint = os.environ["SFTP_TEST_HOST_FINGERPRINT"]
    common = {
        "host": "sftp-test", "port": 22, "username": "reporthub",
        "authentication": "private_key", "private_key": Path("/probe/client_ed25519").read_text(),
        "host_key_sha256": fingerprint, "remote_directory": "/upload", "is_active": True,
    }
    good = TransferDestination.objects.create(name="Working SFTP", **common)
    other = TransferDestination.objects.create(name="Second recipient", **common)
    bad = TransferDestination.objects.create(
        name="Bad authentication", **{**common, "username": "not-a-user"},
    )
    untrusted = TransferDestination.objects.create(
        name="Wrong fingerprint", **{**common, "host_key_sha256": "SHA256:" + "A" * 43},
    )
    report = ReportDefinition.objects.create(
        name="Synthetic multi-destination report", source=source, base_table=table,
        selected_fields=["id"], config={"showTotals": False}, is_file_delivery_active=True,
    )
    report.transfer_destinations.set([good, other, bad, untrusted])
    hostname = "report-sftp-probe-" + uuid4().hex[:8] + "@probe"
    with tempfile.TemporaryFile(mode="w+t") as output:
        worker = subprocess.Popen([
            sys.executable, "-m", "celery", "-A", "config", "worker",
            "--pool=prefork", "--concurrency=2", "--queues=queue_warm,queue_transfers",
            "--without-gossip", "--without-mingle", "--without-heartbeat",
            "--loglevel=INFO", f"--hostname={hostname}",
        ], stdout=output, stderr=subprocess.STDOUT)
        passed = False
        try:
            wait_for(
                lambda: celery_app.control.ping(destination=[hostname], timeout=1),
                "real prefork Celery worker",
            )
            task = execute_report_task.delay(str(report.pk))
            task.get(timeout=90, propagate=True)
            execution = ReportExecution.objects.get(report=report)
            assert execution.status == "completed", execution.status
            assert execution.row_count == 3, execution.row_count
            wait_for(
                lambda: execution.file_deliveries.filter(status__in=["succeeded", "failed"]).count() == 4,
                "independent deliveries",
            )
            deliveries = {delivery.destination_id: delivery for delivery in execution.file_deliveries.all()}
            assert deliveries[good.pk].status == deliveries[other.pk].status == "succeeded"
            assert deliveries[bad.pk].status == deliveries[untrusted.pk].status == "failed"
            assert "fingerprint mismatch" in deliveries[untrusted.pk].error_message
            with default_storage.open(execution.file_path, "rb") as artifact:
                expected = artifact.read()
            rows = list(csv.reader(io.StringIO(expected.decode("utf-8-sig"))))
            assert rows[0] == ["id"] and sorted(rows[1:]) == [["1"], ["2"], ["3"]], rows
            with sftp_connection(good) as sftp:
                for destination in (good, other):
                    with sftp.open(deliveries[destination.pk].remote_path, "rb") as remote:
                        assert remote.read() == expected
                assert len(sftp.listdir("/upload")) == 2
                sftp.remove(deliveries[good.pk].remote_path)
            # Simulate lost success bookkeeping after a completed remote publication.
            retry = deliveries[other.pk]
            ReportFileDelivery.objects.filter(pk=retry.pk).update(status="failed", error_message="Synthetic lost acknowledgement")
            queue_delivery(retry.pk, retry=True)
            wait_for(
                lambda: ReportFileDelivery.objects.filter(pk=retry.pk, status="succeeded", retry_count=1).exists(),
                "idempotent per-destination retry",
            )
            with sftp_connection(good) as sftp:
                names = sftp.listdir("/upload")
                assert names == [retry.remote_path.rsplit("/", 1)[-1]], names
            execution.refresh_from_db()
            assert execution.status == "completed"
            assert ReportFileDelivery.objects.get(pk=deliveries[good.pk].pk).attempt_count == 1
            assert ReportFileDelivery.objects.get(pk=retry.pk).attempt_count == 2
            print("REPORT_SFTP_FANOUT_RETRY_PASSED", flush=True)
            passed = True
        finally:
            if not passed:
                output.seek(0)
                print(output.read(), file=sys.stderr)
            worker.terminate()
            try:
                worker.wait(timeout=20)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=10)


if __name__ == "__main__":
    main()
