from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from control_plane.models import SourceProfile, SourceTable
from reports.models import ReportArtifactAttempt, ReportDefinition, ReportExecution


class ReportArtifactReconciliationViewTests(TestCase):
    def setUp(self):
        source = SourceProfile.objects.create(
            name="Artifact view source",
            host="localhost",
            port=5432,
            database_name="synthetic",
            username="reader",
        )
        table = SourceTable.objects.create(
            source=source, schema_name="public", table_name="items"
        )
        report = ReportDefinition.objects.create(
            name="Artifact view report", source=source, base_table=table
        )
        self.execution = ReportExecution.objects.create(
            report=report, status="failed"
        )
        self.attempt = ReportArtifactAttempt.objects.create(
            execution=self.execution,
            file_path="reports/attempts/candidate.csv",
            sha256="a" * 64,
            size=12,
            status=ReportArtifactAttempt.Status.UPLOADING,
        )
        self.execution.current_artifact_attempt_id = self.attempt.pk
        self.execution.save(update_fields=["current_artifact_attempt_id", "updated_at"])
        self.user = get_user_model().objects.create_user(
            username="artifact-admin", password="test-password", is_staff=True
        )
        self.client.force_login(self.user)
        self.url = reverse(
            "control_plane:report_artifact_reconcile",
            args=[self.execution.pk, self.attempt.pk],
        )

    @patch("control_plane.views.dashboard.reconcile_report_artifact_task.delay")
    def test_staff_can_queue_reconciliation_for_current_uncertain_attempt(self, enqueue):
        response = self.client.post(self.url)

        self.assertRedirects(response, reverse("control_plane:report_history"))
        enqueue.assert_called_once_with(
            str(self.execution.pk), str(self.attempt.pk)
        )
        self.attempt.error_message = "The upload response was lost."
        self.attempt.save(update_fields=["error_message", "updated_at"])
        history = self.client.get(reverse("control_plane:report_history"))
        self.assertContains(history, "Reconcile artifact")
        self.assertContains(history, "The stored object was retained and not deleted.")

    @patch("control_plane.views.dashboard.reconcile_report_artifact_task.delay")
    def test_superseded_attempt_cannot_be_reconciled(self, enqueue):
        self.execution.current_artifact_attempt_id = None
        self.execution.save(update_fields=["current_artifact_attempt_id", "updated_at"])

        response = self.client.post(self.url)

        self.assertRedirects(response, reverse("control_plane:report_history"))
        enqueue.assert_not_called()

    @patch("control_plane.views.dashboard.reconcile_report_artifact_task.delay")
    def test_cancelled_execution_cannot_be_reconciled(self, enqueue):
        self.execution.status = "cancelled"
        self.execution.is_cancelled = True
        self.execution.save(update_fields=["status", "is_cancelled", "updated_at"])

        response = self.client.post(self.url)

        self.assertRedirects(response, reverse("control_plane:report_history"))
        enqueue.assert_not_called()

    def test_completed_artifact_download_redirects_to_minio_download_view(self):
        self.execution.status = "completed"
        self.execution.file_path = "reports/attempts/candidate.csv"
        self.execution.save(update_fields=["status", "file_path", "updated_at"])

        response = self.client.get(
            reverse("control_plane:download_report", args=[self.execution.pk])
        )

        self.assertRedirects(
            response,
            reverse("reports:report_download", args=[self.execution.pk]),
            fetch_redirect_response=False,
        )
