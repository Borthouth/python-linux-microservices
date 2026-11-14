import errno
import io
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase, TransactionTestCase
from django.urls import reverse
from kombu.exceptions import OperationalError
import paramiko

from control_plane.models import SourceField, SourceProfile, SourceTable, TransferDestination
from reports.delivery import (
    DeliveryError, deliver_report_file_task, destination_signature,
    queue_delivery, queue_report_deliveries,
)
from reports.models import ReportDefinition, ReportExecution, ReportFileDelivery
from reports.sftp import PinnedHostKeyPolicy, publish_file
from reports.tasks import execute_report_task


class ReportDeliveryTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="delivery-admin", is_staff=True)
        self.client.force_login(self.user)
        source = SourceProfile.objects.create(
            name="Delivery source", host="localhost", port=5432,
            database_name="synthetic", username="reader",
        )
        table = SourceTable.objects.create(source=source, table_name="items")
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        self.report = ReportDefinition.objects.create(
            name="Delivery report", source=source, base_table=table,
            selected_fields=["id"], is_file_delivery_active=True,
        )
        self.destination = TransferDestination.objects.create(
            name="Synthetic SFTP", host="sftp.example.test", username="writer",
            password="synthetic-password", is_active=True,
            host_key_sha256="SHA256:" + "A" * 43, remote_directory="/upload",
        )
        self.report.transfer_destinations.add(self.destination)
        self.execution = ReportExecution.objects.create(
            report=self.report, status="completed", file_path="reports/synthetic.csv",
        )

    def delivery(self, **overrides):
        data = {
            "report": self.report, "execution": self.execution,
            "destination": self.destination, "destination_name": self.destination.name,
            "destination_signature": destination_signature(self.destination),
            "file_path": self.execution.file_path, "remote_path": "/upload/synthetic.csv",
            "task_id": str(uuid4()),
        }
        data.update(overrides)
        return ReportFileDelivery.objects.create(**data)

    def test_edit_saves_multiple_destinations_independently_of_email(self):
        other = TransferDestination.objects.create(
            name="Second SFTP", host="other.test", username="writer", password="synthetic",
            is_active=True, host_key_sha256=self.destination.host_key_sha256,
        )
        response = self.client.post(reverse("reports:report_edit", args=[self.report.pk]), {
            "name": self.report.name, "source": self.report.source_id,
            "base_table": self.report.base_table_id, "export_format": "csv",
            "selected_fields_json": '["id"]', "is_file_delivery_active": "on",
            "transfer_destinations": [self.destination.pk, other.pk],
            "is_email_active": "on", "email_recipients": "synthetic@example.test",
        })
        self.assertRedirects(response, reverse("reports:report_list"))
        self.report.refresh_from_db()
        self.assertTrue(self.report.is_file_delivery_active)
        self.assertTrue(self.report.is_email_active)
        self.assertSetEqual(
            set(self.report.transfer_destinations.values_list("pk", flat=True)),
            {self.destination.pk, other.pk},
        )
        response = self.client.get(reverse("reports:report_edit", args=[self.report.pk]))
        self.assertContains(response, "Automated File Delivery")
        self.assertContains(response, "Search transfer destinations")
        self.assertContains(response, other.name)

    def test_enabled_file_delivery_requires_enabled_destinations(self):
        url = reverse("reports:report_edit", args=[self.report.pk])
        payload = {
            "name": self.report.name, "source": self.report.source_id,
            "base_table": self.report.base_table_id, "export_format": "csv",
            "is_file_delivery_active": "on",
        }
        response = self.client.post(url, payload)
        self.assertContains(response, "Select at least one active transfer destination.")
        self.destination.is_active = False
        self.destination.save()
        payload["transfer_destinations"] = [self.destination.pk]
        response = self.client.post(url, payload)
        self.assertContains(response, "Remove inactive destinations")

    @patch("reports.delivery.deliver_report_file_task.apply_async")
    def test_fanout_is_unique_and_does_not_resend_existing_records(self, publish):
        with self.captureOnCommitCallbacks(execute=True):
            queue_report_deliveries(self.execution, [self.destination])
            queue_report_deliveries(self.execution, [self.destination])
        self.assertEqual(self.execution.file_deliveries.count(), 1)
        publish.assert_called_once()
        self.assertEqual(publish.call_args.kwargs["queue"], "queue_transfers")

    @patch("reports.delivery.publish_file")
    @patch("reports.delivery.sftp_connection")
    @patch("reports.delivery.default_storage")
    def test_task_uses_stored_artifact_and_duplicate_task_is_noop(self, storage, connection, publish):
        storage.open.return_value = io.BytesIO(b"id\n1\n")
        delivery = self.delivery()
        result = deliver_report_file_task.apply(
            args=[str(delivery.pk)], task_id=delivery.task_id, throw=True,
        ).get()
        self.assertIn("successfully", result)
        delivery.refresh_from_db()
        self.assertEqual(delivery.status, "succeeded")
        self.assertEqual(delivery.attempt_count, 1)
        self.assertEqual(delivery.retry_count, 0)
        storage.open.assert_called_once_with(self.execution.file_path, "rb")
        publish.assert_called_once()
        deliver_report_file_task.apply(args=[str(delivery.pk)], task_id=delivery.task_id, throw=True)
        publish.assert_called_once()
        self.execution.refresh_from_db()
        self.assertEqual(self.execution.status, "completed")

    @patch("reports.delivery.sftp_connection", side_effect=paramiko.AuthenticationException("secret-marker"))
    @patch("reports.delivery.default_storage")
    def test_failure_is_sanitized_and_manual_retry_is_scoped(self, storage, connection):
        storage.open.return_value = io.BytesIO(b"id\n1\n")
        delivery = self.delivery()
        with self.assertRaises(DeliveryError):
            deliver_report_file_task.apply(args=[str(delivery.pk)], task_id=delivery.task_id, throw=True)
        delivery.refresh_from_db()
        self.assertEqual(delivery.status, "failed")
        self.assertNotIn("secret-marker", delivery.error_message)
        old_task = delivery.task_id
        with patch("reports.delivery.deliver_report_file_task.apply_async") as publish:
            with self.captureOnCommitCallbacks(execute=True):
                queue_delivery(delivery.pk, retry=True)
        delivery.refresh_from_db()
        self.assertEqual(delivery.retry_count, 1)
        self.assertEqual(delivery.attempt_count, 1)
        self.assertNotEqual(delivery.task_id, old_task)
        publish.assert_called_once()
        with self.assertRaises(DeliveryError):
            queue_delivery(delivery.pk, retry=True)

    def test_retry_rejects_changed_destination_and_successful_delivery(self):
        delivery = self.delivery(status="failed")
        self.destination.host = "different.example.test"
        self.destination.save()
        with self.assertRaisesRegex(DeliveryError, "connection settings changed"):
            queue_delivery(delivery.pk, retry=True)
        delivery.status = "succeeded"
        delivery.save()
        with self.assertRaisesRegex(DeliveryError, "Only a failed delivery"):
            queue_delivery(delivery.pk, retry=True)

    @patch("reports.delivery.default_storage")
    def test_disabled_destination_and_stale_task_do_not_transfer(self, storage):
        delivery = self.delivery()
        deliver_report_file_task.apply(args=[str(delivery.pk)], task_id=str(uuid4()), throw=True)
        storage.open.assert_not_called()
        self.destination.is_active = False
        self.destination.save()
        with self.assertRaisesRegex(DeliveryError, "inactive"):
            deliver_report_file_task.apply(args=[str(delivery.pk)], task_id=delivery.task_id, throw=True)
        storage.open.assert_not_called()
        delivery.refresh_from_db()
        self.assertEqual(delivery.status, "failed")

    @patch("reports.tasks.report_artifact_matches", return_value=True)
    @patch("reports.tasks.send_report_email_task.delay")
    @patch("reports.tasks.queue_report_deliveries")
    @patch("reports.tasks.Minio")
    @patch("reports.tasks.clickhouse_connect.get_client")
    @patch("reports.tasks.set_task_progress")
    def test_generation_dispatches_email_and_file_delivery_only_after_completion(
        self, progress, clickhouse, minio, queue_files, email, artifact_matches,
    ):
        clickhouse.return_value.query.return_value.result_rows = [(1,)]
        self.report.is_email_active = True
        self.report.email_recipients = "synthetic@example.test"
        self.report.save()
        execute_report_task.apply(args=[str(self.report.pk)], throw=True)
        generated = ReportExecution.objects.filter(report=self.report).exclude(pk=self.execution.pk).get()
        self.assertEqual(generated.status, "completed")
        queued_execution, destinations = queue_files.call_args.args
        self.assertEqual(queued_execution.pk, generated.pk)
        self.assertEqual([destination.pk for destination in destinations], [self.destination.pk])
        email.assert_called_once_with(self.report.pk, generated.pk)


class DeliveryPublishFailureTests(TransactionTestCase):
    def test_broker_failure_does_not_block_other_destinations_or_fail_generation(self):
        source = SourceProfile.objects.create(name="Publish source", host="localhost", port=5432, database_name="synthetic", username="reader")
        table = SourceTable.objects.create(source=source, table_name="items")
        report = ReportDefinition.objects.create(name="Publish report", source=source, base_table=table)
        execution = ReportExecution.objects.create(report=report, status="completed", file_path="reports/test.csv")
        destinations = [
            TransferDestination.objects.create(
                name=f"Target {index}", host="sftp.test", username="writer",
                password="synthetic", host_key_sha256="SHA256:" + "A" * 43,
                remote_directory="/upload", is_active=True,
            ) for index in range(2)
        ]
        with patch(
            "reports.delivery.deliver_report_file_task.apply_async",
            side_effect=[OperationalError("broker unavailable"), Mock()],
        ) as publish:
            queue_report_deliveries(execution, destinations)
        self.assertEqual(publish.call_count, 2)
        self.assertSetEqual(set(execution.file_deliveries.values_list("status", flat=True)), {"failed", "queued"})
        execution.refresh_from_db()
        self.assertEqual(execution.status, "completed")


class SftpPublicationTests(SimpleTestCase):
    def fake_sftp(self, files):
        sftp = Mock()
        def stat(path):
            if path not in files:
                raise FileNotFoundError(errno.ENOENT, "Missing")
            return SimpleNamespace(st_size=len(files[path]))
        def put(source, path, **kwargs):
            files[path] = source.read()
        def rename(source, target):
            if target in files:
                raise FileExistsError(target)
            files[target] = files.pop(source)
        sftp.stat.side_effect = stat
        sftp.open.side_effect = lambda path, mode: io.BytesIO(files[path])
        sftp.putfo.side_effect = put
        sftp.rename.side_effect = rename
        return sftp

    def test_publish_uses_staging_then_no_overwrite_rename_and_retry_recognizes_same_file(self):
        files = {}
        sftp = self.fake_sftp(files)
        publish_file(sftp, io.BytesIO(b"synthetic"), "/upload/report.csv")
        self.assertEqual(files, {"/upload/report.csv": b"synthetic"})
        publish_file(sftp, io.BytesIO(b"synthetic"), "/upload/report.csv")
        self.assertEqual(sftp.putfo.call_count, 1)
        with self.assertRaisesRegex(DeliveryError, "different contents"):
            publish_file(sftp, io.BytesIO(b"other"), "/upload/report.csv")
        self.assertEqual(files["/upload/report.csv"], b"synthetic")

    def test_lost_rename_response_retry_does_not_duplicate_upload(self):
        files = {}
        sftp = self.fake_sftp(files)
        original = sftp.rename.side_effect
        def lost_response(source, target):
            original(source, target)
            raise OSError("Lost response")
        sftp.rename.side_effect = lost_response
        with self.assertRaises(OSError):
            publish_file(sftp, io.BytesIO(b"synthetic"), "/upload/report.csv")
        publish_file(sftp, io.BytesIO(b"synthetic"), "/upload/report.csv")
        self.assertEqual(sftp.putfo.call_count, 1)

    def test_unknown_host_key_is_rejected_without_trusting_it(self):
        key = Mock()
        key.asbytes.return_value = b"synthetic server key"
        with self.assertRaisesRegex(DeliveryError, "fingerprint mismatch"):
            PinnedHostKeyPolicy("SHA256:" + "A" * 43).missing_host_key(Mock(), "synthetic", key)
