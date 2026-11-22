import json
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse
from kombu.exceptions import OperationalError

from control_plane.dlq import (
    REPORT_TASK,
    track_requeue_start,
    track_task_completion,
)
from control_plane.models import (
    DeadLetterTask,
    DbChoice,
    SourceProfile,
    SourceTable,
    SyncStatus,
)
from reports.models import ReportArtifactAttempt, ReportDefinition, ReportExecution
from reports.tasks import execute_report_task
from etl.tasks import test_source_connection_task as source_connection_task


SOURCE_TASK = "etl.tasks.introspect_source_schema_task"


class DeadLetterQueueTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_user(username="dlq-admin", is_staff=True)
        self.client.force_login(self.admin)
        self.source = SourceProfile.objects.create(
            name="DLQ source", db_type=DbChoice.POSTGRESQL, host="localhost",
            database_name="source", username="reader", last_sync_status=SyncStatus.FAILED,
        )
        self.sender = SimpleNamespace(name=SOURCE_TASK)

    def record_failure(self):
        task_id = str(uuid4())
        with patch("control_plane.dlq.get_task_progress", return_value={"status": "FAILED", "error": "Source failed"}):
            track_task_completion(
                sender=self.sender, task_id=task_id, args=[self.source.pk],
                state="SUCCESS", retval="Failure reported by task",
            )
        return DeadLetterTask.objects.get(original_task_id=task_id)

    def test_connection_task_records_business_failure_with_its_real_task_id(self):
        self.source.db_type = "unsupported"
        self.source.save(update_fields=["db_type"])
        progress = {}

        def remember_progress(task_id, percentage, status="RUNNING", **kwargs):
            progress[str(task_id)] = {"status": status, **kwargs}

        task_id = str(uuid4())
        with patch("etl.tasks.set_task_progress", side_effect=remember_progress), \
             patch("etl.tasks.get_etl_logger"), \
             patch("control_plane.dlq.get_task_progress", side_effect=lambda identifier: progress[str(identifier)]):
            source_connection_task.apply(args=[self.source.pk], task_id=task_id)
        failure = DeadLetterTask.objects.get(original_task_id=task_id)
        self.assertEqual(failure.task_name, "etl.tasks.test_source_connection_task")
        self.assertEqual(failure.target_id, str(self.source.pk))
        self.assertIn("Unsupported database type", failure.error_message)

    def test_cancelled_task_is_not_recorded_as_a_failure(self):
        with patch("control_plane.dlq.get_task_progress", return_value={"status": "CANCELLED"}):
            track_task_completion(sender=self.sender, task_id=str(uuid4()), state="FAILURE")
        self.assertFalse(DeadLetterTask.objects.exists())

    def test_scheduled_report_failure_records_created_execution(self):
        table = SourceTable.objects.create(source=self.source, table_name="records")
        report = ReportDefinition.objects.create(name="Scheduled report", source=self.source, base_table=table)
        progress = {}

        def remember_progress(task_id, percentage, status="RUNNING", **kwargs):
            progress.setdefault(str(task_id), {}).update({"status": status, **kwargs})

        task_id = str(uuid4())
        with patch("reports.tasks.set_task_progress", side_effect=remember_progress), \
             patch("reports.tasks.report_query_guard", side_effect=RuntimeError("query failed")), \
             patch("control_plane.dlq.get_task_progress", side_effect=lambda identifier: progress[str(identifier)]):
            execute_report_task.apply(args=[str(report.pk)], task_id=task_id)
        failure = DeadLetterTask.objects.get(original_task_id=task_id)
        execution = ReportExecution.objects.get(pk=failure.original_execution_id)
        self.assertEqual(execution.report, report)
        self.assertEqual(execution.status, "failed")

    @patch("etl.tasks.introspect_source_schema_task.apply_async")
    def test_source_retry_tracks_outcomes_and_retains_attempt_history(self, publish):
        failure = self.record_failure()
        url = reverse("control_plane:dlq_retry", args=[failure.pk])
        self.assertRedirects(self.client.post(url), reverse("control_plane:dlq_list"), fetch_redirect_response=False)
        failure.refresh_from_db()
        self.assertEqual(failure.status, DeadLetterTask.Status.QUEUED)
        attempt = failure.attempts.get()
        publish.assert_called_once_with(args=[self.source.pk], task_id=str(attempt.task_id), queue="queue_ux")
        self.assertEqual(attempt.requested_by, self.admin)

        self.client.post(url)
        self.assertEqual(publish.call_count, 1)
        track_requeue_start(sender=self.sender, task_id=str(attempt.task_id))
        failure.refresh_from_db()
        self.assertEqual(failure.status, DeadLetterTask.Status.RUNNING)
        with patch("control_plane.dlq.get_task_progress", return_value={"status": "FAILED", "error": "Failed again"}):
            track_task_completion(sender=self.sender, task_id=str(attempt.task_id), state="SUCCESS")
        failure.refresh_from_db()
        attempt.refresh_from_db()
        self.assertEqual(failure.status, DeadLetterTask.Status.FAILED_AGAIN)
        self.assertEqual(attempt.error_message, "Failed again")
        self.assertEqual(failure.error_message, "Source failed")

        self.source.last_sync_status = SyncStatus.FAILED
        self.source.save(update_fields=["last_sync_status"])
        self.client.post(url)
        newest = failure.attempts.first()
        with patch("control_plane.dlq.get_task_progress", return_value={"status": "SUCCESS"}):
            track_task_completion(sender=self.sender, task_id=str(newest.task_id), state="SUCCESS")
        failure.refresh_from_db()
        self.assertEqual(failure.status, DeadLetterTask.Status.SUCCEEDED)
        self.assertEqual(failure.attempts.count(), 2)

    @patch("etl.tasks.introspect_source_schema_task.apply_async", side_effect=OperationalError("broker detail"))
    def test_uncertain_publication_is_not_retried_or_reported_as_success(self, publish):
        failure = self.record_failure()
        url = reverse("control_plane:dlq_retry", args=[failure.pk])
        with self.assertLogs("control_plane.dlq", level="ERROR"):
            self.client.post(url)
        failure.refresh_from_db()
        self.assertEqual(failure.status, DeadLetterTask.Status.UNCERTAIN)
        self.assertEqual(failure.attempts.get().status, DeadLetterTask.Status.UNCERTAIN)
        self.client.post(url)
        publish.assert_called_once()

    @patch("etl.tasks.introspect_source_schema_task.apply_async")
    def test_fast_unknown_outcome_is_not_overwritten_as_queued(self, publish):
        failure = self.record_failure()

        def finish_before_publish_returns(**options):
            with patch("control_plane.dlq.get_task_progress", return_value={"status": "UNKNOWN"}):
                track_task_completion(sender=self.sender, task_id=options["task_id"], state="SUCCESS")

        publish.side_effect = finish_before_publish_returns
        self.client.post(reverse("control_plane:dlq_retry", args=[failure.pk]))
        failure.refresh_from_db()
        self.assertEqual(failure.status, DeadLetterTask.Status.UNCERTAIN)

    @patch("etl.tasks.introspect_source_schema_task.apply_async")
    def test_cancelled_retry_is_not_marked_failed(self, publish):
        failure = self.record_failure()
        self.client.post(reverse("control_plane:dlq_retry", args=[failure.pk]))
        attempt = failure.attempts.get()
        with patch("control_plane.dlq.get_task_progress", return_value={"status": "CANCELLED"}):
            track_task_completion(sender=self.sender, task_id=str(attempt.task_id), state="FAILURE")
        failure.refresh_from_db()
        self.assertEqual(failure.status, DeadLetterTask.Status.CANCELLED)

    @patch("control_plane.views.dashboard.get_redis_client")
    def test_dashboard_hides_new_failure_details_from_non_admins(self, redis_client):
        redis_client.return_value.keys.return_value = []
        self.record_failure()
        url = reverse("control_plane:dashboard")
        self.assertContains(self.client.get(url), "Source failed")
        self.client.force_login(get_user_model().objects.create_user(username="dlq-viewer"))
        response = self.client.get(url)
        self.assertNotContains(response, "Source failed")
        self.assertContains(response, "Details available to administrators.")

    @patch("control_plane.views.dlq.get_redis_client")
    def test_report_without_execution_does_not_offer_retry(self, redis_client):
        redis_client.return_value.keys.return_value = []
        DeadLetterTask.objects.create(
            original_task_id=str(uuid4()), task_name=REPORT_TASK, target_id=str(uuid4()),
        )
        response = self.client.get(reverse("control_plane:dlq_list"))
        self.assertContains(response, "Retry details unavailable")
        self.assertNotContains(response, ">Re-queue</button>")

    @patch("control_plane.views.dlq.get_redis_client")
    def test_list_filters_legacy_entries_and_formats_date(self, redis_client):
        client = redis_client.return_value
        client.keys.return_value = [b"dlq:task:old"]
        client.get.return_value = json.dumps({
            "id": "old", "task_name": "old.task", "failed_at": "2024-01-02T03:04:05Z",
            "traceback": "old failure",
        }).encode()
        failure = self.record_failure()
        failure.status = DeadLetterTask.Status.SUCCEEDED
        failure.save(update_fields=["status"])
        url = reverse("control_plane:dlq_list")
        response = self.client.get(url, {"status": "failed"})
        self.assertContains(response, "old.task")
        self.assertContains(response, "Jan 2, 2024 03:04:05")
        self.assertContains(response, "Retry details unavailable")
        self.assertNotContains(response, SOURCE_TASK)
        self.client.post(reverse("control_plane:dlq_requeue", args=["old"]))
        self.client.post(reverse("control_plane:dlq_requeue", args=[failure.pk]))
        client.delete.assert_not_called()
        failure.refresh_from_db()
        self.assertEqual(failure.status, DeadLetterTask.Status.SUCCEEDED)
        response = self.client.get(url, {"status": "succeeded"})
        self.assertContains(response, SOURCE_TASK)
        self.assertNotContains(response, "old.task")

    @patch("etl.tasks.introspect_source_schema_task.apply_async")
    def test_retry_requires_admin_post_and_active_idle_source(self, publish):
        failure = self.record_failure()
        url = reverse("control_plane:dlq_retry", args=[failure.pk])
        self.assertEqual(self.client.get(url).status_code, 405)
        csrf_client = Client(enforce_csrf_checks=True)
        csrf_client.force_login(self.admin)
        self.assertEqual(csrf_client.post(url).status_code, 403)
        normal_user = get_user_model().objects.create_user(username="dlq-user")
        self.client.force_login(normal_user)
        self.assertEqual(self.client.post(url).status_code, 302)
        self.client.force_login(self.admin)
        for active, status in ((False, SyncStatus.FAILED), (True, SyncStatus.RUNNING)):
            self.source.is_active = active
            self.source.last_sync_status = status
            self.source.save(update_fields=["is_active", "last_sync_status"])
            self.client.post(url)
        failure.refresh_from_db()
        self.assertEqual(failure.status, DeadLetterTask.Status.FAILED)
        self.assertFalse(failure.attempts.exists())
        publish.assert_not_called()

    @patch("reports.tasks.execute_report_task.apply_async")
    def test_report_retry_creates_new_run_and_rejects_uncertain_artifacts(self, publish):
        table = SourceTable.objects.create(source=self.source, table_name="records")
        report = ReportDefinition.objects.create(name="DLQ report", source=self.source, base_table=table)
        old = ReportExecution.objects.create(report=report, status="failed")
        failure = DeadLetterTask.objects.create(
            original_task_id=str(uuid4()), task_name=REPORT_TASK, target_id=str(report.pk),
            original_execution_id=str(old.pk),
        )
        artifact = ReportArtifactAttempt.objects.create(execution=old, status=ReportArtifactAttempt.Status.UPLOADING)
        old.current_artifact_attempt_id = artifact.pk
        old.save(update_fields=["current_artifact_attempt_id"])
        url = reverse("control_plane:dlq_retry", args=[failure.pk])
        self.client.post(url)
        publish.assert_not_called()
        artifact.status = ReportArtifactAttempt.Status.FAILED
        artifact.save(update_fields=["status"])
        self.client.post(url)
        attempt = failure.attempts.get()
        new_run = ReportExecution.objects.get(pk=attempt.execution_id)
        self.assertEqual(new_run.report, report)
        self.assertEqual(new_run.status, "pending")
        publish.assert_called_once_with(
            args=[str(report.pk), str(new_run.pk)], task_id=str(attempt.task_id), queue="queue_warm"
        )
