import hashlib
from types import SimpleNamespace
from unittest.mock import patch

from django.conf import settings
from django.db import OperationalError
from django.test import TestCase

from control_plane.models import SourceField, SourceProfile, SourceTable
from reports.models import ReportArtifactAttempt, ReportDefinition, ReportExecution
from reports.tasks import execute_report_task, reconcile_report_artifact_task


class ReportArtifactRecoveryTests(TestCase):
    def setUp(self):
        source = SourceProfile.objects.create(
            name="Artifact source",
            host="localhost",
            port=5432,
            database_name="synthetic",
            username="reader",
        )
        table = SourceTable.objects.create(
            source=source, schema_name="public", table_name="items"
        )
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        self.report = ReportDefinition.objects.create(
            name="Artifact report",
            source=source,
            base_table=table,
            selected_fields=["id"],
        )

    def mock_storage(self, *, fail_after_upload=False, mismatch=False):
        objects = {}
        client = SimpleNamespace(
            bucket_exists=lambda bucket: True,
            make_bucket=lambda bucket: None,
        )

        def put_object(bucket, path, stream, size, content_type, metadata):
            payload = stream.read()
            objects[(bucket, path)] = {
                "payload": payload,
                "size": len(payload) + (1 if mismatch else 0),
                "metadata": metadata,
            }
            if fail_after_upload:
                raise RuntimeError("upload response lost")

        def stat_object(bucket, path):
            stored = objects.get((bucket, path))
            if stored is None:
                return SimpleNamespace(size=0, metadata={})
            return SimpleNamespace(size=stored["size"], metadata=stored["metadata"])

        client.put_object = put_object
        client.stat_object = stat_object
        return client, objects

    def run_report(self, execution, storage):
        with patch("reports.tasks.Minio", return_value=storage), patch(
            "reports.tasks.clickhouse_connect.get_client"
        ) as clickhouse, patch("reports.tasks.set_task_progress"):
            clickhouse.return_value.query.return_value.result_rows = [(7,)]
            return execute_report_task.apply(
                args=[str(self.report.pk), str(execution.pk)], throw=True
            ).get()

    def test_completed_artifact_is_verified_and_uses_unique_attempt_path(self):
        execution = ReportExecution.objects.create(report=self.report)
        storage, objects = self.mock_storage()

        self.run_report(execution, storage)

        execution.refresh_from_db()
        attempt = ReportArtifactAttempt.objects.get(execution=execution)
        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.file_path, attempt.file_path)
        self.assertEqual(attempt.status, ReportArtifactAttempt.Status.COMPLETED)
        self.assertIn(f"/attempts/{attempt.pk}/", attempt.file_path)
        stored = next(value for (_, path), value in objects.items() if path == attempt.file_path)
        self.assertEqual(
            attempt.sha256,
            hashlib.sha256(stored["payload"]).hexdigest(),
        )

    def test_reconciliation_recovers_upload_with_lost_response(self):
        execution = ReportExecution.objects.create(report=self.report)
        storage, objects = self.mock_storage(fail_after_upload=True)

        with patch("reports.tasks.Minio", return_value=storage), patch(
            "reports.tasks.clickhouse_connect.get_client"
        ) as clickhouse, patch("reports.tasks.set_task_progress"):
            clickhouse.return_value.query.return_value.result_rows = [(7,)]
            with self.assertRaisesRegex(RuntimeError, "response lost"):
                execute_report_task.apply(
                    args=[str(self.report.pk), str(execution.pk)], throw=True
                )

        execution.refresh_from_db()
        attempt = ReportArtifactAttempt.objects.get(execution=execution)
        self.assertEqual(execution.status, "failed")
        self.assertEqual(attempt.status, ReportArtifactAttempt.Status.UPLOADING)
        self.assertTrue(objects)

        with patch("reports.tasks.Minio", return_value=storage):
            result = reconcile_report_artifact_task.apply(
                args=[str(execution.pk), str(attempt.pk)], throw=True
            ).get()

        execution.refresh_from_db()
        attempt.refresh_from_db()
        self.assertIn("marked complete", result)
        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.file_path, attempt.file_path)
        self.assertEqual(attempt.status, ReportArtifactAttempt.Status.COMPLETED)

    def test_reconciliation_recovers_when_database_commit_fails_after_upload(self):
        execution = ReportExecution.objects.create(report=self.report)
        storage, objects = self.mock_storage()

        with patch("reports.tasks.Minio", return_value=storage), patch(
            "reports.tasks.clickhouse_connect.get_client"
        ) as clickhouse, patch(
            "reports.tasks.finalize_report_artifact",
            side_effect=OperationalError("database commit failed"),
        ), patch("reports.tasks.set_task_progress"):
            clickhouse.return_value.query.return_value.result_rows = [(7,)]
            with self.assertRaisesRegex(OperationalError, "database commit failed"):
                execute_report_task.apply(
                    args=[str(self.report.pk), str(execution.pk)], throw=True
                )

        execution.refresh_from_db()
        attempt = ReportArtifactAttempt.objects.get(execution=execution)
        self.assertEqual(execution.status, "failed")
        self.assertEqual(attempt.status, ReportArtifactAttempt.Status.VERIFIED)
        self.assertTrue(objects)

        with patch("reports.tasks.Minio", return_value=storage):
            reconcile_report_artifact_task.apply(
                args=[str(execution.pk), str(attempt.pk)], throw=True
            )

        execution.refresh_from_db()
        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.file_path, attempt.file_path)

    def test_mismatched_object_is_not_published_or_deleted(self):
        execution = ReportExecution.objects.create(report=self.report)
        storage, objects = self.mock_storage(mismatch=True)

        with patch("reports.tasks.Minio", return_value=storage), patch(
            "reports.tasks.clickhouse_connect.get_client"
        ) as clickhouse, patch("reports.tasks.set_task_progress"):
            clickhouse.return_value.query.return_value.result_rows = [(7,)]
            with self.assertRaisesRegex(RuntimeError, "did not match"):
                execute_report_task.apply(
                    args=[str(self.report.pk), str(execution.pk)], throw=True
                )

        execution.refresh_from_db()
        attempt = ReportArtifactAttempt.objects.get(execution=execution)
        with patch("reports.tasks.Minio", return_value=storage):
            result = reconcile_report_artifact_task.apply(
                args=[str(execution.pk), str(attempt.pk)], throw=True
            ).get()

        execution.refresh_from_db()
        attempt.refresh_from_db()
        self.assertIn("retained for review", result)
        self.assertEqual(execution.status, "failed")
        self.assertIsNone(execution.file_path)
        self.assertEqual(attempt.status, ReportArtifactAttempt.Status.UPLOADING)
        self.assertTrue(attempt.error_message)
        self.assertTrue(objects)

    def test_reexecuting_same_record_never_overwrites_previous_attempt(self):
        execution = ReportExecution.objects.create(report=self.report)
        storage, objects = self.mock_storage()

        self.run_report(execution, storage)
        first_path = ReportExecution.objects.get(pk=execution.pk).file_path
        self.run_report(execution, storage)

        execution.refresh_from_db()
        attempts = list(ReportArtifactAttempt.objects.filter(execution=execution))
        self.assertEqual(len(attempts), 2)
        self.assertNotEqual(first_path, execution.file_path)
        bucket_name = settings.AWS_STORAGE_BUCKET_NAME
        self.assertIn((bucket_name, first_path), objects)
        self.assertIn((bucket_name, execution.file_path), objects)
