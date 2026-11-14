import io
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import close_old_connections, connection, transaction
from django.test import RequestFactory, TestCase, TransactionTestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from openpyxl import load_workbook
from reportlab.platypus import SimpleDocTemplate

from control_plane.models import (
    CdcSourceExternalOperation,
    CdcToBatchTransition,
    IngestionMode,
    SourceField,
    SourceProfile,
    SourceRelationship,
    SourceTable,
)
from etl.cdc_external_operations import (
    _cdc_external_operation_barrier,
    _complete_cdc_external_operation,
)
from reports.ai_design import validate_report_design
from reports.ai_summary import build_summary_metrics, generate_report_summary
from reports.charting import build_chart_data, build_pdf_chart
from reports.cutover_guard import acquire_report_cutover_lock, report_query_guard
from reports.exporters import build_json_report, build_xlsx_report
from reports.models import EmailAuditLog, ReportDefinition, ReportExecution
from reports.ollama import OllamaUnavailable
from reports.pdf_export import build_pdf_report
from reports.query_builder import build_clickhouse_report_query
from reports.tasks import execute_report_task
from reports.views import email_webhook_receiver


class ReportCutoverGuardTests(TransactionTestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Report cutover source", host="localhost", port=5432,
            database_name="test_db", username="test_user", ingestion_mode=IngestionMode.CDC,
        )
        table = SourceTable.objects.create(
            source=self.source, schema_name="public", table_name="events"
        )
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        self.report = ReportDefinition.objects.create(
            name="Cutover report", source=self.source, base_table=table,
            selected_fields=["id"],
        )
        self.transition = CdcToBatchTransition.objects.create(source=self.source)

    @patch("reports.tasks.clickhouse_connect.get_client")
    @patch("reports.tasks.set_task_progress")
    def test_pending_cutover_fails_report_without_query(self, progress, client):
        self.transition.status = CdcToBatchTransition.Status.CUTOVER_PENDING
        self.transition.save(update_fields=["status"])
        with self.assertRaisesRegex(RuntimeError, "cutover is pending"):
            execute_report_task.apply(args=[self.report.pk], throw=True)
        client.assert_not_called()
        execution = ReportExecution.objects.get(report=self.report)
        self.assertEqual(execution.status, "failed")
        self.assertIn("cutover is pending", execution.error_message)

    def test_preparing_transition_allows_report(self):
        with report_query_guard(self.report) as report:
            self.assertEqual(report.pk, self.report.pk)

    @patch("reports.tasks.Minio")
    @patch("reports.tasks.report_artifact_matches", return_value=True)
    @patch("reports.tasks.clickhouse_connect.get_client")
    @patch("reports.tasks.set_task_progress")
    def test_preparing_transition_report_completes(
        self, progress, client, artifact_matches, minio
    ):
        client.return_value.query.return_value.result_rows = [(42,)]
        result = execute_report_task.apply(args=[self.report.pk], throw=True).get()
        self.assertIn("successfully executed", result)
        execution = ReportExecution.objects.get(report=self.report)
        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.row_count, 1)
        client.return_value.close.assert_called_once()
        self.assertEqual(
            client.return_value.query.call_args.kwargs["settings"],
            {"join_use_nulls": 1, "query_id": f"report_{execution.pk}"},
        )
        self.assertNotIn("transport_settings", client.return_value.query.call_args.kwargs)
        minio.return_value.put_object.assert_called_once()

    def test_report_guards_are_shared_and_exclusive_cutover_waits(self):
        first_entered, release = Event(), Event()
        second_entered, exclusive_entered = Event(), Event()

        def guarded(first):
            close_old_connections()
            try:
                with report_query_guard(self.report):
                    (first_entered if first else second_entered).set()
                    if first and not release.wait(10):
                        raise TimeoutError("Report guard test timed out.")
            finally:
                close_old_connections()

        def cutover():
            close_old_connections()
            try:
                with transaction.atomic():
                    acquire_report_cutover_lock(self.source.pk)
                    exclusive_entered.set()
                    CdcToBatchTransition.objects.filter(pk=self.transition.pk).update(
                        status=CdcToBatchTransition.Status.CUTOVER_PENDING
                    )
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=3) as pool:
            first = pool.submit(guarded, True)
            try:
                self.assertTrue(first_entered.wait(10))
                second = pool.submit(guarded, False)
                self.assertTrue(second_entered.wait(10))
                second.result(timeout=10)
                pending = pool.submit(cutover)
                self.assertFalse(exclusive_entered.wait(0.5))
            finally:
                release.set()
            first.result(timeout=10)
            pending.result(timeout=10)
        self.assertTrue(exclusive_entered.is_set())
        with self.assertRaisesRegex(RuntimeError, "cutover is pending"):
            with report_query_guard(self.report):
                self.fail("A report must not run while cutover is pending.")

    def test_exclusive_guard_requires_transaction(self):
        with self.assertRaisesRegex(RuntimeError, "requires a PostgreSQL transaction"):
            acquire_report_cutover_lock(self.source.pk)

    def test_waiting_report_reloads_table_metadata_after_cutover(self):
        SourceTable.objects.filter(pk=self.report.base_table_id).update(cdc_managed=True)
        waiting = Event()

        def watch_queries(execute, sql, params, many, context):
            if "pg_advisory_xact_lock_shared" in sql:
                waiting.set()
            return execute(sql, params, many, context)

        def run_report():
            close_old_connections()
            try:
                with connection.execute_wrapper(watch_queries):
                    with report_query_guard(self.report) as report:
                        return build_clickhouse_report_query(report, ["id"])[0]
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                acquire_report_cutover_lock(self.source.pk)
                running = pool.submit(run_report)
                self.assertTrue(waiting.wait(10))
                SourceTable.objects.filter(pk=self.report.base_table_id).update(cdc_managed=False)
                SourceProfile.objects.filter(pk=self.source.pk).update(ingestion_mode=IngestionMode.BATCH)
                CdcToBatchTransition.objects.filter(pk=self.transition.pk).update(
                    status=CdcToBatchTransition.Status.SUCCEEDED
                )
            query = running.result(timeout=10)
        self.assertNotIn("_cdc_deleted", query)

    def test_source_scoped_exclusive_lock_does_not_block_other_source(self):
        other = SourceProfile.objects.create(
            name="Other source", host="localhost", port=5432,
            database_name="test_db", username="test_user",
        )
        table = SourceTable.objects.create(source=other, schema_name="public", table_name="items")
        report = ReportDefinition.objects.create(name="Other report", source=other, base_table=table)
        entered = Event()

        def run_other():
            close_old_connections()
            try:
                with report_query_guard(report):
                    entered.set()
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                acquire_report_cutover_lock(self.source.pk)
                running = pool.submit(run_other)
                self.assertTrue(entered.wait(10))
                running.result(timeout=10)


@override_settings(CDC_EXTERNAL_OPERATION_TRACKING=True)
class TrackedReportCutoverGuardTests(TransactionTestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Tracked report source", host="localhost", port=5432,
            database_name="test_db", username="test_user", ingestion_mode=IngestionMode.CDC,
        )
        table = SourceTable.objects.create(
            source=self.source, schema_name="public", table_name="events"
        )
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        self.report = ReportDefinition.objects.create(
            name="Tracked report", source=self.source, base_table=table,
            selected_fields=["id"],
        )
        self.transition = CdcToBatchTransition.objects.create(source=self.source)

    def test_record_visible_before_query_and_cleared_on_success(self):
        def verify():
            close_old_connections()
            try:
                record = CdcSourceExternalOperation.objects.get(source=self.source)
                self.assertEqual(record.kind, CdcSourceExternalOperation.Kind.REPORT)
                self.assertEqual(record.task_id, "report-task")
            finally:
                close_old_connections()

        with report_query_guard(self.report, task_id="report-task"):
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(verify).result(timeout=10)
        self.assertFalse(CdcSourceExternalOperation.objects.exists())

    def test_failure_retains_record_and_blocks_cutover(self):
        with self.assertRaisesRegex(RuntimeError, "query response lost"):
            with report_query_guard(self.report):
                raise RuntimeError("query response lost")
        record = CdcSourceExternalOperation.objects.get(source=self.source)
        self.assertEqual(record.kind, CdcSourceExternalOperation.Kind.REPORT)
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            with _cdc_external_operation_barrier(self.source.pk):
                pass
        _complete_cdc_external_operation(self.source.pk, record.pk)

    def test_connection_loss_leaves_record_after_report_lock_is_released(self):
        disconnected, release = Event(), Event()

        def query():
            close_old_connections()
            try:
                with report_query_guard(self.report):
                    connection.close()
                    disconnected.set()
                    if not release.wait(timeout=10):
                        raise TimeoutError("Report release timed out.")
                    raise RuntimeError("query response lost")
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=1) as pool:
            worker = pool.submit(query)
            try:
                self.assertTrue(disconnected.wait(timeout=10))
                with transaction.atomic():
                    acquire_report_cutover_lock(self.source.pk)
                with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
                    with _cdc_external_operation_barrier(self.source.pk):
                        pass
            finally:
                release.set()
            with self.assertRaisesRegex(RuntimeError, "query response lost"):
                worker.result(timeout=10)
        self.assertTrue(CdcSourceExternalOperation.objects.filter(source=self.source).exists())

    def test_pending_cutover_rejects_report_without_creating_record(self):
        self.transition.status = CdcToBatchTransition.Status.CUTOVER_PENDING
        self.transition.save(update_fields=["status"])
        with self.assertRaisesRegex(RuntimeError, "cutover is pending"):
            with report_query_guard(self.report):
                self.fail("A pending cutover must block reports.")
        self.assertFalse(CdcSourceExternalOperation.objects.exists())

    def test_inactive_and_batch_sources_still_allow_reports(self):
        self.source.is_active = False
        self.source.ingestion_mode = IngestionMode.BATCH
        self.source.save(update_fields=["is_active", "ingestion_mode"])
        with report_query_guard(self.report):
            self.assertTrue(CdcSourceExternalOperation.objects.filter(
                source=self.source, kind=CdcSourceExternalOperation.Kind.REPORT
            ).exists())
        self.assertFalse(CdcSourceExternalOperation.objects.exists())

    @patch("reports.tasks.Minio")
    @patch("reports.tasks.clickhouse_connect.get_client")
    @patch("reports.tasks.set_task_progress")
    def test_report_task_keeps_record_when_clickhouse_response_is_lost(
        self, progress, client, minio
    ):
        client.return_value.query.side_effect = RuntimeError("query response lost")
        with self.assertRaisesRegex(RuntimeError, "query response lost"):
            execute_report_task.apply(args=[self.report.pk], throw=True)
        record = CdcSourceExternalOperation.objects.get(source=self.source)
        self.assertEqual(record.kind, CdcSourceExternalOperation.Kind.REPORT)
        self.assertTrue(record.task_id)
        self.assertEqual(ReportExecution.objects.get(report=self.report).status, "failed")
        client.return_value.close.assert_called_once()
        minio.assert_not_called()

    @patch("reports.tasks.Minio")
    @patch("reports.tasks.report_artifact_matches", return_value=True)
    @patch("reports.tasks.clickhouse_connect.get_client")
    @patch("reports.tasks.set_task_progress")
    def test_report_task_clears_record_after_successful_query(
        self, progress, client, artifact_matches, minio
    ):
        client.return_value.query.return_value.result_rows = [(42,)]
        execute_report_task.apply(args=[self.report.pk], throw=True).get()
        self.assertFalse(CdcSourceExternalOperation.objects.filter(source=self.source).exists())
        self.assertEqual(ReportExecution.objects.get(report=self.report).status, "completed")


class EmailWebhookTests(TestCase):
    def setUp(self):
        self.log = EmailAuditLog.objects.create(
            recipients="test@example.com", message_id="message-123", status="sent"
        )
        self.url = reverse("control_plane:email_webhook_receiver")
        self.factory = RequestFactory()

    def test_routed_webhook_accepts_single_provider_event(self):
        response = self.client.post(
            self.url,
            data=json.dumps({"MessageID": "message-123", "Type": "Bounce", "reason": "Mailbox full"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "success"})
        self.log.refresh_from_db()
        self.assertEqual((self.log.status, self.log.error_message), ("bounced", "Mailbox full"))

    def test_legacy_webhook_accepts_event_list(self):
        request = self.factory.post(
            "/",
            data=json.dumps([{"sg_message_id": "message-123", "event": "delivered"}]),
            content_type="application/json",
        )
        response = email_webhook_receiver(request)

        self.assertEqual(response.status_code, 200)
        self.log.refresh_from_db()
        self.assertEqual(self.log.status, "delivered")

    def test_invalid_payloads_return_safe_bad_request(self):
        for payload in ("not json", '"not an event"', '[{"event": "delivered"}, 1]'):
            with self.subTest(payload=payload):
                response = self.client.post(self.url, data=payload, content_type="application/json")
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.json(), {"error": "Invalid webhook payload."})

    @patch("reports.email_webhooks.EmailAuditLog.objects.filter")
    def test_processing_failure_is_logged_without_exposing_exception(self, filter_logs):
        filter_logs.side_effect = RuntimeError("sensitive internal details")

        with self.assertLogs("reports.email_webhooks", level="ERROR") as logs:
            response = self.client.post(
                self.url,
                data=json.dumps([{"message-id": "message-123", "event": "delivered"}]),
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json(), {"error": "Failed to process webhook."})
        self.assertNotContains(response, "sensitive internal details", status_code=500)
        self.assertIn("Failed to process email delivery webhook", logs.output[0])

    def test_non_post_is_rejected(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 405)


@override_settings(CACHES={
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}
})
class ReportManagementDownloadTests(TestCase):
    def setUp(self):
        admin = get_user_model().objects.create_user(
            username="report-admin",
            password="test-password",
            is_staff=True,
        )
        self.client.force_login(admin)
        source = SourceProfile.objects.create(
            name="Download test source",
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
        )
        table = SourceTable.objects.create(
            source=source,
            schema_name="public",
            table_name="items",
        )
        self.report = ReportDefinition.objects.create(
            name="Download test report",
            source=source,
            base_table=table,
        )

    def test_pending_cutover_blocks_report_metadata_writes(self):
        CdcToBatchTransition.objects.create(
            source=self.report.source, status=CdcToBatchTransition.Status.CUTOVER_PENDING
        )
        cases = [
            (self.client.post, reverse("reports:report_create"), {"source": self.report.source_id}),
            (self.client.post, reverse("reports:report_edit", args=[self.report.pk]), {}),
            (self.client.get, reverse("reports:report_toggle", args=[self.report.pk]), {}),
            (self.client.post, reverse("reports:report_delete", args=[self.report.pk]), {}),
        ]
        for method, url, data in cases:
            with self.subTest(url=url):
                response = method(url, data, follow=True)
                self.assertContains(
                    response,
                    "Source metadata cannot be changed while CDC-to-batch cutover is pending.",
                )
                self.report.refresh_from_db()
                self.assertEqual(self.report.name, "Download test report")
                self.assertTrue(self.report.is_active)

    def test_report_move_to_pending_source_is_rejected(self):
        other = SourceProfile.objects.create(
            name="Pending report destination", host="localhost", port=5432,
            database_name="test_db", username="test_user",
        )
        CdcToBatchTransition.objects.create(
            source=other, status=CdcToBatchTransition.Status.CUTOVER_PENDING
        )
        response = self.client.post(
            reverse("reports:report_edit", args=[self.report.pk]),
            {"source": other.pk}, follow=True,
        )
        self.assertContains(response, "cutover is pending")
        self.report.refresh_from_db()
        self.assertNotEqual(self.report.source_id, other.pk)

    def post_ai_design(self):
        return self.client.post(
            reverse("reports:ai_report_design"),
            data=json.dumps({
                "source_id": self.report.source_id,
                "table_id": self.report.base_table_id,
                "description": "Show the items",
            }),
            content_type="application/json",
        )

    @patch("reports.views.generate_report_design")
    def test_ai_design_value_error_does_not_expose_exception_details(self, generate):
        generate.side_effect = ValueError("sensitive internal details")

        with self.assertLogs("reports.views", level="ERROR") as logs:
            response = self.post_ai_design()

        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.json(),
            {"error": "The AI returned an invalid report configuration. Please try again."},
        )
        self.assertNotContains(response, "sensitive internal details", status_code=422)
        self.assertIn("AI report design generation or validation failed", logs.output[0])

    @patch("reports.views.generate_report_design", return_value={"reportType": "tabular"})
    def test_ai_design_success_returns_configuration(self, generate):
        response = self.post_ai_design()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"config": {"reportType": "tabular"}})

    @patch("reports.views.generate_report_design", side_effect=OllamaUnavailable)
    def test_ai_design_unavailable_keeps_service_error(self, generate):
        response = self.post_ai_design()

        self.assertEqual(response.status_code, 503)
        self.assertIn("local AI service is unavailable", response.json()["error"])

    def test_download_is_hidden_until_a_completed_run_exists(self):
        response = self.client.get(reverse("reports:report_list"))
        self.assertNotContains(response, "/reports/execution/")

        execution = ReportExecution.objects.create(
            report=self.report,
            status="completed",
            file_path="reports/test.csv",
            completed_at=timezone.now(),
        )
        response = self.client.get(reverse("reports:report_list"))
        self.assertContains(
            response,
            reverse("reports:report_download", args=[execution.pk]),
        )

    def test_report_status_polls_once_until_execution_finishes(self):
        status_url = reverse("reports:report_status", args=[self.report.pk])
        list_url = reverse("reports:report_list")
        for url in (list_url, status_url):
            with self.subTest(status="never run", url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.content.decode().count(f'hx-get="{status_url}"'), 0)

        with patch("reports.views.execute_report_task.delay") as dispatch:
            response = self.client.post(reverse("reports:report_run", args=[self.report.pk]))
        self.assertRedirects(response, list_url)
        execution = self.report.executions.get()
        self.assertEqual(execution.status, "pending")
        dispatch.assert_called_once_with(str(self.report.pk), str(execution.pk))
        for status in ("pending", "running", "completed", "failed", "cancelled"):
            ReportExecution.objects.filter(pk=execution.pk).update(status=status)
            for url in (list_url, status_url):
                with self.subTest(status=status, url=url):
                    response = self.client.get(url)
                    self.assertEqual(response.status_code, 200)
                    content = response.content.decode()
                    self.assertEqual(content.count(f'id="report-status-{self.report.pk}"'), 1)
                    active = status in ("pending", "running")
                    self.assertEqual(content.count(f'hx-get="{status_url}"'), int(active))
                    self.assertEqual(content.count('hx-trigger="every 2s"'), int(active))
                    if status == "pending":
                        self.assertIn("Queued...", content)

    def test_download_is_hidden_again_after_report_is_edited(self):
        execution = ReportExecution.objects.create(
            report=self.report,
            status="completed",
            file_path="reports/test.csv",
            completed_at=timezone.now(),
        )
        response = self.client.get(reverse("reports:report_list"))
        self.assertContains(
            response,
            reverse("reports:report_download", args=[execution.pk]),
        )

        self.report.description = "Updated after the run"
        self.report.save()

        response = self.client.get(reverse("reports:report_list"))
        self.assertNotContains(
            response,
            reverse("reports:report_download", args=[execution.pk]),
        )

    def test_editing_one_report_does_not_hide_another_reports_download(self):
        edited_execution = ReportExecution.objects.create(
            report=self.report,
            status="completed",
            file_path="reports/test.csv",
            completed_at=timezone.now(),
        )
        other_report = ReportDefinition.objects.create(
            name="Unaffected report",
            source=self.report.source,
            base_table=self.report.base_table,
        )
        other_execution = ReportExecution.objects.create(
            report=other_report,
            status="completed",
            file_path="reports/other.csv",
            completed_at=timezone.now(),
        )

        self.report.description = "Updated after the run"
        self.report.save()

        response = self.client.get(reverse("reports:report_list"))
        self.assertNotContains(
            response,
            reverse("reports:report_download", args=[edited_execution.pk]),
        )
        self.assertContains(
            response,
            reverse("reports:report_download", args=[other_execution.pk]),
        )

    def test_edit_form_hides_legacy_filter_config_without_deleting_it(self):
        self.report.filter_config = {"status": "active"}
        self.report.save()

        response = self.client.get(
            reverse("reports:report_edit", args=[self.report.pk])
        )

        self.assertNotContains(response, "Filter Rules (JSON)")
        self.assertContains(response, "Export Chart")
        self.assertContains(response, "Bar chart")
        self.assertContains(response, "Numeric value")
        self.assertContains(response, 'id="id_export_format"')
        self.report.refresh_from_db()
        self.assertEqual(self.report.filter_config, {"status": "active"})

    def test_pdf_ai_summary_option_is_saved_in_report_configuration(self):
        response = self.client.post(
            reverse("reports:report_edit", args=[self.report.pk]),
            {
                "name": self.report.name,
                "description": "",
                "source": self.report.source_id,
                "base_table": self.report.base_table_id,
                "selected_fields_json": "[]",
                "export_format": "pdf",
                "include_ai_summary": "on",
                "email_recipients": "",
                "is_active": "true",
                "config_json": (
                    '{"reportType":"tabular","selectedColumns":[],"'
                    'selectedRelationships":[],"showTotals":true,'
                    '"summaryPosition":"bottom","chart":{"type":""}}'
                ),
            },
        )

        self.assertRedirects(response, reverse("reports:report_list"))
        self.report.refresh_from_db()
        self.assertTrue(self.report.config["includeAiSummary"])

    def test_ai_summary_option_is_restored_for_report_editing(self):
        self.report.config = {"includeAiSummary": True}
        self.report.save()

        response = self.client.get(
            reverse("reports:report_edit", args=[self.report.pk])
        )

        self.assertContains(response, "Include AI summary")
        self.assertContains(response, 'name="include_ai_summary"')
        self.assertContains(response, 'id="id_include_ai_summary" checked')

    def test_changing_from_pdf_to_csv_disables_ai_summary(self):
        self.report.export_format = "pdf"
        self.report.config = {"includeAiSummary": True}
        self.report.save()

        response = self.client.post(
            reverse("reports:report_edit", args=[self.report.pk]),
            {
                "name": self.report.name,
                "description": "",
                "source": self.report.source_id,
                "base_table": self.report.base_table_id,
                "selected_fields_json": "[]",
                "export_format": "csv",
                "email_recipients": "",
                "is_active": "true",
                "config_json": '{"reportType":"tabular","selectedColumns":[]}',
            },
        )

        self.assertRedirects(response, reverse("reports:report_list"))
        self.report.refresh_from_db()
        self.assertFalse(self.report.config["includeAiSummary"])

    def test_ai_summary_option_is_saved_for_xlsx_and_json(self):
        for export_format in ("xlsx", "json"):
            with self.subTest(export_format=export_format):
                response = self.client.post(
                    reverse("reports:report_edit", args=[self.report.pk]),
                    {
                        "name": self.report.name,
                        "description": "",
                        "source": self.report.source_id,
                        "base_table": self.report.base_table_id,
                        "selected_fields_json": "[]",
                        "export_format": export_format,
                        "include_ai_summary": "on",
                        "email_recipients": "",
                        "is_active": "true",
                        "config_json": '{"reportType":"tabular","selectedColumns":[]}',
                    },
                )

                self.assertRedirects(response, reverse("reports:report_list"))
                self.report.refresh_from_db()
                self.assertTrue(self.report.config["includeAiSummary"])


class ExcelReportExportTests(TestCase):
    def test_builds_grouped_chart_data_and_renders_pdf_chart_types(self):
        rows = [["Artist A", 2], ["Artist B", 3], ["Artist A", 4]]
        for chart_type in ("bar", "line", "pie"):
            with self.subTest(chart_type=chart_type):
                chart_data = build_chart_data(
                    {
                        "type": chart_type,
                        "categoryIndex": 0,
                        "valueIndex": 1,
                        "aggregation": "SUM",
                    },
                    ["Artist", "Albums"],
                    rows,
                )
                self.assertEqual(chart_data["categories"], ["Artist A", "Artist B"])
                self.assertEqual(chart_data["values"], [6, 3])
                pdf_buffer = io.BytesIO()
                SimpleDocTemplate(pdf_buffer).build([build_pdf_chart(chart_data)])
                self.assertTrue(pdf_buffer.getvalue().startswith(b"%PDF"))

    def test_rejects_invalid_chart_fields_and_negative_pie_values(self):
        with self.assertRaisesMessage(ValueError, "different valid category and value"):
            build_chart_data(
                {
                    "type": "bar",
                    "categoryIndex": 0,
                    "valueIndex": 0,
                    "aggregation": "SUM",
                },
                ["Value"],
                [[1]],
            )

        with self.assertRaisesMessage(ValueError, "non-negative values"):
            build_chart_data(
                {
                    "type": "pie",
                    "categoryIndex": 0,
                    "valueIndex": 1,
                    "aggregation": "SUM",
                },
                ["Category", "Value"],
                [["A", -1]],
            )

    def test_builds_formatted_workbook_with_data_and_bottom_summaries(self):
        artifact = build_xlsx_report(
            ["Album", "Count"],
            [["Blue Train", 4], ["Giant Steps", 6]],
            ["Count (count): 10"],
            True,
            "bottom",
        )

        workbook = load_workbook(io.BytesIO(artifact), data_only=True)
        worksheet = workbook["Report"]
        self.assertEqual(
            tuple(tuple(row) for row in worksheet.values),
            (
                ("Album", "Count"),
                ("Blue Train", 4),
                ("Giant Steps", 6),
                (None, None),
                ("Summary Aggregates", None),
                ("Count (count): 10", None),
            ),
        )
        self.assertEqual(worksheet.freeze_panes, "A2")
        self.assertEqual(worksheet.auto_filter.ref, "A1:B3")

    def test_places_ai_summary_in_separated_top_row(self):
        artifact = build_xlsx_report(
            ["Album", "Count"],
            [["Blue Train", 4]],
            [],
            False,
            "bottom",
            ai_summary="- One album was found.",
        )

        worksheet = load_workbook(io.BytesIO(artifact), data_only=True)["Report"]
        self.assertIsNone(worksheet["A1"].value)
        self.assertEqual(worksheet["A2"].value, "AI Summary: - One album was found.")
        self.assertIsNone(worksheet["A3"].value)
        self.assertEqual(worksheet["A4"].value, "Album")
        self.assertEqual(worksheet["A5"].value, "Blue Train")
        self.assertEqual(worksheet.freeze_panes, "A5")
        self.assertEqual(worksheet.auto_filter.ref, "A4:B5")

    def test_adds_ai_summary_as_top_level_json_property(self):
        artifact = build_json_report(
            ["Album"],
            [["Blue Train"]],
            ["Album (count): 1"],
            True,
            "One album was found.",
        )

        payload = json.loads(artifact)
        self.assertEqual(payload["ai_summary"], "One album was found.")
        self.assertEqual(payload["data"], [{"Album": "Blue Train"}])
        self.assertEqual(payload["summaries"], ["Album (count): 1"])

    def test_embeds_configured_chart(self):
        chart_data = build_chart_data(
            {
                "type": "bar",
                "categoryIndex": 0,
                "valueIndex": 1,
                "aggregation": "SUM",
            },
            ["Artist", "Albums"],
            [["Artist A", 2], ["Artist B", 3], ["Artist A", 4]],
        )
        artifact = build_xlsx_report(
            ["Artist", "Albums"],
            [["Artist A", 2], ["Artist B", 3], ["Artist A", 4]],
            [],
            False,
            "bottom",
            chart_data,
        )

        workbook = load_workbook(io.BytesIO(artifact))
        self.assertEqual(len(workbook["Report"]._charts), 1)
        self.assertEqual(workbook["_ChartData"].sheet_state, "hidden")
        self.assertEqual(workbook["_ChartData"]["A2"].value, "Artist A")
        self.assertEqual(workbook["_ChartData"]["B2"].value, 6)

    def test_places_summary_before_data_when_requested(self):
        artifact = build_xlsx_report(
            ["Album"],
            [["Blue Train"]],
            ["Album (count): 1"],
            True,
            "top",
        )

        worksheet = load_workbook(io.BytesIO(artifact), data_only=True)["Report"]
        self.assertEqual(worksheet["A1"].value, "Summary Aggregates")
        self.assertEqual(worksheet["A4"].value, "Album")
        self.assertEqual(worksheet.freeze_panes, "A5")


class AiReportSummaryTests(TestCase):
    def test_summary_metrics_include_statistics_but_not_row_values(self):
        rows = [["Private customer", 10], ["Confidential client", 20]]

        metrics = build_summary_metrics(["Customer", "Amount"], rows)

        self.assertEqual(metrics["included_row_count"], 2)
        self.assertEqual(metrics["fields"][0]["distinct_values"], 2)
        self.assertNotIn("Private customer", str(metrics))
        self.assertNotIn("Confidential client", str(metrics))
        self.assertEqual(
            metrics["fields"][1]["numeric_range"],
            {"min": "10", "max": "20", "average": "15"},
        )

    @patch("reports.ai_summary.generate_text", return_value="- Values range from 10 to 20.")
    def test_generates_summary_from_aggregate_metrics_only(self, generate_text):
        summary = generate_report_summary(
            ["Customer", "Amount"],
            [["Private customer", 10], ["Confidential client", 20]],
        )

        self.assertEqual(summary, "- Values range from 10 to 20.")
        prompt = generate_text.call_args.args[0]
        self.assertIn('"included_row_count":2', prompt)
        self.assertNotIn("Private customer", prompt)
        self.assertNotIn("Confidential client", prompt)

    def test_pdf_layout_wraps_wide_data_and_includes_ai_summary(self):
        artifact = build_pdf_report(
            report_name="Wide report",
            generated_at="2026-11-06 12:00:00",
            headers=["ID", "Title", "Artist", "Composer", "Genre", "Description"],
            rows=[[
                1,
                "A title that wraps",
                "An artist",
                "A composer",
                "Rock",
                "A private value with <markup> & special characters",
            ]],
            summary_lines=[],
            show_totals=False,
            summary_position="bottom",
            ai_summary="• The report contains one record.",
            chart_data=build_chart_data(
                {
                    "type": "bar",
                    "categoryIndex": 0,
                    "valueIndex": 1,
                    "aggregation": "SUM",
                },
                ["Artist", "Albums"],
                [["Artist A", 2], ["Artist B", 3]],
            ),
        )

        self.assertTrue(artifact.startswith(b"%PDF"))
        self.assertRegex(
            artifact.decode("latin-1"),
            r"/MediaBox\s+\[\s+0 0 792 612\s+\]",
        )


class ReportDesignValidationTests(TestCase):
    def setUp(self):
        self.schema = {
            "base_table": {
                "table_id": 1,
                "label": "public.orders",
                "fields": [{"name": "id", "type": "integer"}],
            },
            "related_tables": [
                {
                    "key": "7",
                    "table_id": 2,
                    "label": "public.customers",
                    "fields": [{"name": "name", "type": "text"}],
                },
            ],
        }

    def valid_design(self):
        return {
            "reportType": "tabular",
            "showTotals": True,
            "summaryPosition": "bottom",
            "selectedRelationships": ["7"],
            "selectedColumns": [
                {
                    "table_key": "base",
                    "field_name": "id",
                    "label": "Order ID",
                    "aggregate": "",
                },
                {
                    "table_key": "7",
                    "field_name": "name",
                    "label": "Customer",
                    "aggregate": "COUNT",
                },
            ],
        }

    def test_validates_design_and_maps_selected_fields_for_builder(self):
        result = validate_report_design(self.valid_design(), self.schema)

        self.assertEqual(result["selectedColumns"][0]["field"], "base:id")
        self.assertEqual(result["selectedColumns"][1]["table_id"], 2)
        self.assertEqual(result["selectedRelationships"], ["7"])

    def test_rejects_relationship_outside_available_schema(self):
        design = self.valid_design()
        design["selectedRelationships"] = ["999"]

        with self.assertRaisesMessage(ValueError, "unavailable table relationship"):
            validate_report_design(design, self.schema)

    def test_rejects_field_from_relationship_not_selected(self):
        design = self.valid_design()
        design["selectedRelationships"] = []

        with self.assertRaisesMessage(ValueError, "outside the available schema"):
            validate_report_design(design, self.schema)

    def test_rejects_unsupported_aggregate(self):
        design = self.valid_design()
        design["selectedColumns"][0]["aggregate"] = "DROP"

        with self.assertRaisesMessage(ValueError, "unsupported aggregate"):
            validate_report_design(design, self.schema)

    def test_normalizes_model_no_aggregate_variants(self):
        design = self.valid_design()
        design["selectedColumns"][0]["aggregate"] = "none"
        design["selectedColumns"][1]["aggregate"] = None

        result = validate_report_design(design, self.schema)

        self.assertEqual(
            [column["aggregate"] for column in result["selectedColumns"]],
            ["", ""],
        )

    def test_rejects_incomplete_layout_configuration(self):
        design = self.valid_design()
        design.pop("showTotals")

        with self.assertRaisesMessage(ValueError, "invalid totals setting"):
            validate_report_design(design, self.schema)


class ClickHouseReportQueryTests(TestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Join query test source",
            host="localhost",
            port=3306,
            database_name="test_db",
            username="test_user",
        )
        self.orders = SourceTable.objects.create(
            source=self.source,
            schema_name="public",
            table_name="orders",
        )
        self.customers = SourceTable.objects.create(
            source=self.source,
            schema_name="public",
            table_name="customers",
        )
        for table, field_name in (
            (self.orders, "id"),
            (self.orders, "customer_id"),
            (self.customers, "id"),
            (self.customers, "display`name"),
        ):
            SourceField.objects.create(
                table=table,
                field_name=field_name,
                data_type="integer",
            )
        self.customer_relationship = SourceRelationship.objects.create(
            source=self.source,
            from_schema="public",
            from_table="orders",
            from_field="customer_id",
            to_schema="public",
            to_table="customers",
            to_field="id",
            constraint_name="fk_orders_customers",
        )

    def make_report(self, base_table, join_config):
        return ReportDefinition(
            name="Join query report",
            source=self.source,
            base_table=base_table,
            join_config=join_config,
        )

    def test_builds_join_and_qualifies_columns(self):
        report = self.make_report(
            self.orders,
            [str(self.customer_relationship.pk)],
        )

        query, headers, aggregates = build_clickhouse_report_query(
            report,
            [
                {
                    "table_key": "base",
                    "field_name": "id",
                    "label": "Order ID",
                },
                {
                    "table_key": str(self.customer_relationship.pk),
                    "field_name": "display`name",
                    "label": "Customer",
                    "aggregate": "COUNT",
                },
            ],
        )

        self.assertIn("FROM `source_", query)
        self.assertIn("LEFT JOIN `source_", query)
        self.assertIn("t0.`customer_id` = t1.`id`", query)
        self.assertIn("t1.`display``name` AS `column_1`", query)
        self.assertEqual(headers, ["Order ID", "Customer"])
        self.assertEqual(aggregates, ["", "COUNT"])

    def test_uses_physical_generations_for_base_and_joined_tables(self):
        self.orders.clickhouse_physical_table = "rh_generation_orders"
        self.orders.save(update_fields=["clickhouse_physical_table"])
        self.customers.clickhouse_physical_table = "rh_generation_customers"
        self.customers.save(update_fields=["clickhouse_physical_table"])
        report = self.make_report(self.orders, [str(self.customer_relationship.pk)])
        query, _, _ = build_clickhouse_report_query(report, [
            {"table_key": "base", "field_name": "id"},
            {"table_key": str(self.customer_relationship.pk), "field_name": "id"},
        ])
        self.assertIn("FROM `rh_generation_orders` AS t0", query)
        self.assertIn("LEFT JOIN `rh_generation_customers` AS t1", query)

    def test_invalid_physical_generation_is_rejected(self):
        self.orders.clickhouse_physical_table = "invalid-name"
        report = self.make_report(self.orders, [])
        with self.assertRaisesRegex(ValueError, "Invalid ClickHouse physical table name"):
            build_clickhouse_report_query(report, ["id"])

    def test_incremental_tables_use_final_for_deduplicated_rows(self):
        self.orders.clickhouse_uses_replacing_merge_tree = True
        self.orders.save(update_fields=["clickhouse_uses_replacing_merge_tree"])
        self.customers.clickhouse_uses_replacing_merge_tree = True
        self.customers.save(update_fields=["clickhouse_uses_replacing_merge_tree"])
        report = self.make_report(
            self.orders,
            [str(self.customer_relationship.pk)],
        )

        query, _, _ = build_clickhouse_report_query(
            report,
            [
                {"table_key": "base", "field_name": "id"},
                {
                    "table_key": str(self.customer_relationship.pk),
                    "field_name": "id",
                },
            ],
        )

        self.assertIn("FROM `source_", query)
        self.assertIn("` AS t0 FINAL", query)
        self.assertIn("LEFT JOIN `source_", query)
        self.assertIn("` AS t1 FINAL", query)

    def test_cdc_tables_filter_versioned_delete_tombstones(self):
        self.orders.cdc_managed = True
        self.orders.save(update_fields=["cdc_managed"])
        self.customers.cdc_managed = True
        self.customers.save(update_fields=["cdc_managed"])
        report = self.make_report(
            self.orders,
            [str(self.customer_relationship.pk)],
        )

        query, _, _ = build_clickhouse_report_query(
            report,
            [
                {"table_key": "base", "field_name": "id"},
                {
                    "table_key": str(self.customer_relationship.pk),
                    "field_name": "id",
                },
            ],
        )

        self.assertIn(
            "FROM (SELECT * FROM `source_",
            query,
        )
        self.assertEqual(query.count("`_cdc_deleted` = 0"), 2)
        self.assertEqual(query.count(" FINAL WHERE "), 2)

    def test_tabular_detail_keeps_selected_columns_as_source_rows(self):
        report = self.make_report(self.orders, [])
        report.config = {"reportType": "tabular"}

        query, headers, aggregates = build_clickhouse_report_query(
            report,
            [
                {"table_key": "base", "field_name": "id", "aggregate": "COUNT"},
                {"table_key": "base", "field_name": "customer_id"},
            ],
        )

        self.assertNotIn("GROUP BY", query)
        self.assertIn("t0.`id` AS `column_0`", query)
        self.assertEqual(headers, ["id", "customer_id"])
        self.assertEqual(aggregates, ["COUNT", ""])

    def test_grouped_summary_aggregates_fields_and_groups_by_dimensions(self):
        report = self.make_report(self.orders, [])
        report.config = {"reportType": "grouped_summary"}

        query, headers, aggregates = build_clickhouse_report_query(
            report,
            [
                {"table_key": "base", "field_name": "customer_id", "label": "Customer"},
                {"table_key": "base", "field_name": "id", "label": "Order count", "aggregate": "COUNT"},
            ],
        )

        self.assertIn(
            "COUNT(t0.`id`) AS `column_1`",
            query,
        )
        self.assertIn("GROUP BY t0.`customer_id`", query)
        self.assertEqual(headers, ["Customer", "COUNT(Order count)"])
        self.assertEqual(aggregates, ["", "COUNT"])

    def test_grouped_summary_without_dimensions_returns_one_overall_row(self):
        report = self.make_report(self.orders, [])
        report.config = {"reportType": "grouped_summary"}

        query, _, _ = build_clickhouse_report_query(
            report,
            [{"table_key": "base", "field_name": "id", "aggregate": "SUM"}],
        )

        self.assertIn("SUM(t0.`id`) AS `column_0`", query)
        self.assertNotIn("GROUP BY", query)

    def test_rejects_unsupported_saved_report_type(self):
        report = self.make_report(self.orders, [])
        report.config = {"reportType": "unexpected"}

        with self.assertRaisesMessage(ValueError, "saved report type"):
            build_clickhouse_report_query(
                report,
                [{"table_key": "base", "field_name": "id"}],
            )

    def test_builds_join_from_referenced_table_toward_foreign_key(self):
        report = self.make_report(
            self.customers,
            [str(self.customer_relationship.pk)],
        )

        query, _, _ = build_clickhouse_report_query(
            report,
            [{"table_key": "base", "field_name": "id"}],
        )

        self.assertIn("t0.`id` = t1.`customer_id`", query)

    def test_builds_composite_foreign_key_as_one_join(self):
        self.orders.table_name = "order_lines"
        self.orders.save(update_fields=["table_name"])
        self.customers.table_name = "orders"
        self.customers.save(update_fields=["table_name"])
        self.customer_relationship.from_table = "order_lines"
        self.customer_relationship.from_field = "order_id"
        self.customer_relationship.to_table = "orders"
        self.customer_relationship.to_field = "order_id"
        self.customer_relationship.constraint_name = "fk_order_lines_orders"
        self.customer_relationship.save()
        SourceField.objects.create(
            table=self.orders,
            field_name="order_id",
            data_type="integer",
        )
        SourceField.objects.create(
            table=self.customers,
            field_name="order_id",
            data_type="integer",
        )
        second_relationship = SourceRelationship.objects.create(
            source=self.source,
            from_schema="public",
            from_table="order_lines",
            from_field="line_number",
            to_schema="public",
            to_table="orders",
            to_field="line_number",
            constraint_name="fk_order_lines_orders",
        )
        SourceField.objects.create(
            table=self.orders,
            field_name="line_number",
            data_type="integer",
        )
        SourceField.objects.create(
            table=self.customers,
            field_name="line_number",
            data_type="integer",
        )
        key = ",".join(
            str(pk)
            for pk in sorted(
                [self.customer_relationship.pk, second_relationship.pk]
            )
        )
        report = self.make_report(self.customers, [key])

        query, _, _ = build_clickhouse_report_query(
            report,
            [{"table_key": "base", "field_name": "order_id"}],
        )

        self.assertIn(
            "t0.`order_id` = t1.`order_id` AND t0.`line_number` = t1.`line_number`",
            query,
        )
        self.assertEqual(query.count("LEFT JOIN"), 1)

    def test_rejects_relationship_not_attached_to_base_table(self):
        unrelated_table = SourceTable.objects.create(
            source=self.source,
            schema_name="public",
            table_name="regions",
        )
        unrelated_relationship = SourceRelationship.objects.create(
            source=self.source,
            from_schema="public",
            from_table="customers",
            from_field="region_id",
            to_schema="public",
            to_table="regions",
            to_field="id",
            constraint_name="fk_customers_regions",
        )
        SourceField.objects.create(
            table=unrelated_table,
            field_name="id",
            data_type="integer",
        )
        report = self.make_report(
            self.orders,
            [str(unrelated_relationship.pk)],
        )

        with self.assertRaisesMessage(ValueError, "directly to the base table"):
            build_clickhouse_report_query(
                report,
                [{"table_key": "base", "field_name": "id"}],
            )
