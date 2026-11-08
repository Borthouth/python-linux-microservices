import json
from datetime import timedelta
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.db import IntegrityError, transaction
from django.test import RequestFactory, SimpleTestCase, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from control_plane.models import (
    CdcExecutionFailure,
    CdcTableRebuild,
    CdcToBatchTableCheckpoint,
    CdcToBatchTransition,
    DbChoice,
    IngestionMode,
    SourceField,
    SourceProfile,
    SourceTable,
    TrackedUser,
    WarehouseDatasetMapping,
    UserWidgetPreference,
)
from control_plane.views.dlq import dlq_requeue_view
from control_plane.views.dashboard import update_widget_preferences
from etl.progress import PIPELINE_STEPS, PREPARING_INGESTION
from reports.models import EmailAuditLog, ReportDefinition, ReportExecution
from reports.progress import REPORT_RUNNING_STEPS, ReportProgress


class CdcToBatchCheckpointTests(TestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Transition checkpoint source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
            ingestion_mode=IngestionMode.CDC,
        )
        self.table = SourceTable.objects.create(
            source=self.source,
            schema_name="public",
            table_name="events",
            primary_key_fields=["id"],
            cdc_managed=True,
        )

    def test_only_one_active_transition_per_source(self):
        transition = CdcToBatchTransition.objects.create(source=self.source)
        with self.assertRaises(IntegrityError), transaction.atomic():
            CdcToBatchTransition.objects.create(source=self.source)

        transition.status = CdcToBatchTransition.Status.CUTOVER_PENDING
        transition.save(update_fields=["status"])
        with self.assertRaises(IntegrityError), transaction.atomic():
            CdcToBatchTransition.objects.create(source=self.source)

        transition.status = CdcToBatchTransition.Status.SUCCEEDED
        transition.save(update_fields=["status"])
        self.assertIsNotNone(CdcToBatchTransition.objects.create(source=self.source).pk)

    def test_one_durable_checkpoint_per_transition_and_table(self):
        transition = CdcToBatchTransition.objects.create(source=self.source)
        checkpoint = CdcToBatchTableCheckpoint.objects.create(
            transition=transition,
            source_table=self.table,
            staging_table="source_events_batch_stage",
            active_uuid_before="old-uuid",
            staging_uuid="stage-uuid",
            status=CdcToBatchTableCheckpoint.Status.STAGED,
        )
        with self.assertRaises(IntegrityError), transaction.atomic():
            CdcToBatchTableCheckpoint.objects.create(
                transition=transition,
                source_table=self.table,
                staging_table="duplicate_stage",
            )
        checkpoint.refresh_from_db()
        self.assertEqual(checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.assertEqual(checkpoint.active_uuid_before, "old-uuid")
        self.assertEqual(checkpoint.staging_uuid, "stage-uuid")
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)


class CdcTableRebuildAdminTests(TestCase):
    def setUp(self):
        user = get_user_model().objects.create_user(
            username="cdc-admin", is_staff=True, is_superuser=True,
        )
        self.client.force_login(user)
        self.source = SourceProfile.objects.create(
            name="CDC admin source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
            ingestion_mode=IngestionMode.CDC,
        )
        self.table = SourceTable.objects.create(
            source=self.source,
            schema_name="public",
            table_name="events",
            primary_key_fields=["id"],
            cdc_managed=True,
        )

    @patch("control_plane.admin.rebuild_cdc_table_task.delay")
    def test_action_queues_eligible_and_skips_ineligible_tables(self, delay):
        keyless = SourceTable.objects.create(
            source=self.source,
            schema_name="public",
            table_name="keyless",
            cdc_managed=True,
        )
        response = self.client.post(
            reverse("admin:control_plane_sourcetable_changelist"),
            {
                "action": "rebuild_selected_cdc_tables",
                "_selected_action": [self.table.pk, keyless.pk],
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Queued 1 CDC table rebuild(s).")
        self.assertContains(response, "Skipped 1 ineligible table(s).")
        delay.assert_called_once_with(self.table.pk)

    def test_rebuild_checkpoint_is_visible_but_cannot_be_added_or_deleted(self):
        recovery = CdcTableRebuild.objects.create(
            source_table=self.table,
            connector_name="admin-test-generation",
            topic_prefix="admin-test-generation",
            notification_topic="admin-test-generation.notifications",
            consumer_group="admin-test-generation-group",
            staging_table="source_events_admin_test",
            status=CdcTableRebuild.Status.CUTOVER_PENDING,
            last_error="Cutover needs attention",
        )
        response = self.client.get(
            reverse("admin:control_plane_cdctablerebuild_changelist")
        )
        self.assertContains(response, "Cutover pending")
        response = self.client.get(
            reverse("admin:control_plane_cdctablerebuild_change", args=[recovery.pk])
        )
        self.assertContains(response, "Cutover needs attention")
        self.assertNotContains(response, 'class="deletelink"')
        self.assertEqual(
            self.client.post(reverse("admin:control_plane_cdctablerebuild_add")).status_code,
            403,
        )
        self.assertEqual(
            self.client.post(
                reverse("admin:control_plane_cdctablerebuild_change", args=[recovery.pk]),
                {"status": CdcTableRebuild.Status.SUCCEEDED},
            ).status_code,
            403,
        )
        self.assertEqual(
            self.client.post(
                reverse("admin:control_plane_cdctablerebuild_delete", args=[recovery.pk]),
                {"post": "yes"},
            ).status_code,
            403,
        )
        recovery.refresh_from_db()
        self.assertEqual(recovery.status, CdcTableRebuild.Status.CUTOVER_PENDING)


class DlqRequeueViewTests(SimpleTestCase):
    def test_htmx_response_escapes_task_id(self):
        task_id = '"><script>alert(1)</script>'
        request = RequestFactory().post(
            "/dlq/task/requeue/", HTTP_HX_REQUEST="true"
        )
        request.user = get_user_model()(is_staff=True)

        with patch("control_plane.views.dlq.get_redis_client") as get_redis_client:
            get_redis_client.return_value = Mock()
            response = dlq_requeue_view(request, task_id)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.content.decode(),
            '<div id="dlq-row-&quot;&gt;&lt;script&gt;alert(1)&lt;/script&gt;" '
            'class="hidden"></div>',
        )

    def test_requeue_error_does_not_expose_exception_details(self):
        request = RequestFactory().post("/dlq/task/requeue/")
        request.user = get_user_model()(is_staff=True)

        with patch("control_plane.views.dlq.get_redis_client") as get_redis_client:
            get_redis_client.side_effect = RuntimeError("sensitive backend details")
            with self.assertLogs("control_plane.views.dlq", level="ERROR"):
                response = dlq_requeue_view(request, "task-id")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            json.loads(response.content),
            {"status": "error", "message": "Unable to re-queue task."},
        )


class WidgetPreferenceViewTests(SimpleTestCase):
    def test_update_error_does_not_expose_exception_details(self):
        request = RequestFactory().post(
            "/api/widgets/update/",
            data=json.dumps({"widget": "stats", "visible": False}),
            content_type="application/json",
        )
        request.user = get_user_model()(is_staff=True)

        with patch(
            "control_plane.views.dashboard.UserWidgetPreference.objects.get_or_create",
            side_effect=RuntimeError("sensitive database details"),
        ):
            with self.assertLogs("control_plane.views.dashboard", level="ERROR"):
                response = update_widget_preferences(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            json.loads(response.content),
            {
                "status": "error",
                "message": "Unable to update widget preferences.",
            },
        )


class PipelineAutomationMessageTests(TestCase):
    def test_manual_run_message_is_rendered_once_below_navbar(self):
        from django_celery_beat.models import IntervalSchedule, PeriodicTask

        admin = get_user_model().objects.create_user(
            username="pipeline-admin", is_staff=True, is_superuser=True
        )
        self.client.force_login(admin)
        task = PeriodicTask.objects.create(
            name="Manual pipeline",
            task="etl.tasks.trigger_all_active_clickhouse_ingestion_task",
            interval=IntervalSchedule.objects.create(every=1, period="hours"),
        )
        with patch("celery.current_app.send_task"):
            response = self.client.get(
                reverse("control_plane:pipeline_automation_run", args=[task.pk]),
                follow=True,
            )

        message = "Pipeline automation &#x27;Manual pipeline&#x27; triggered manually."
        self.assertContains(response, message, count=1, html=False)
        content = response.content.decode()
        self.assertLess(content.index(message), content.index("Pipeline Automations</h1>"))
        message_start = content.index(message)
        container_start = content.rfind('<div class=', 0, message_start)
        self.assertIn("text-blue-300", content[container_start:message_start])
        self.assertNotIn("text-emerald-400", content[container_start:message_start])

    def test_completed_action_remains_green(self):
        from django.contrib.messages import SUCCESS
        from django.contrib.messages.storage.base import Message
        from django.template.loader import render_to_string

        html = render_to_string(
            "control_plane/components/messages.html",
            {"messages": [Message(SUCCESS, "Saved", extra_tags="custom")]},
        )
        self.assertIn("text-emerald-400", html)
        self.assertNotIn("text-blue-300", html)


class CdcFailureDashboardTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_user(
            username="cdc-admin", is_staff=True
        )
        self.source = SourceProfile.objects.create(
            name="CDC warehouse source",
            db_type=DbChoice.MYSQL,
            host="localhost",
            port=3306,
            database_name="test",
            username="test",
        )
        CdcExecutionFailure.objects.create(
            source=self.source,
            task_id="cdc-task-123",
            error_message="RuntimeError: CDC execution failed.",
        )
        self.client.force_login(self.admin)

    def test_pipeline_history_does_not_display_cdc_failures(self):
        response = self.client.get(reverse("control_plane:pipeline_history"))
        self.assertContains(response, "CDC warehouse source")
        self.assertNotContains(response, "cdc-task-123")
        self.assertNotContains(response, "Recent CDC Execution Failures")
        self.assertEqual(response.context["breadcrumbs"], [
            {"name": "Dashboard", "url": reverse("control_plane:dashboard")},
            {"name": "Administration", "url": ""},
            {"name": "Pipeline History", "url": ""},
        ])

    def test_cdc_history_displays_persisted_failures(self):
        response = self.client.get(reverse("control_plane:cdc_failure_history"))
        self.assertContains(response, "CDC warehouse source")
        self.assertContains(response, "cdc-task-123")
        self.assertContains(response, "RuntimeError: CDC execution failed.")
        self.assertEqual(response.context["breadcrumbs"], [
            {"name": "Dashboard", "url": reverse("control_plane:dashboard")},
            {"name": "Administration", "url": ""},
            {"name": "CDC Execution Failures", "url": ""},
        ])

    def test_cdc_history_searches_source_database_task_and_error(self):
        url = reverse("control_plane:cdc_failure_history")
        for query in ("warehouse", "MYSQL", "TASK-123", "runtimeerror"):
            with self.subTest(query=query):
                response = self.client.get(url, {"q": query})
                self.assertContains(response, "cdc-task-123")
        response = self.client.get(url, {"q": "not found"})
        self.assertNotContains(response, "cdc-task-123")
        self.assertContains(response, "No CDC execution failures match your search.")

    def test_cdc_history_paginates_all_results_and_preserves_search(self):
        for number in range(25):
            CdcExecutionFailure.objects.create(
                source=self.source, task_id=f"matching-{number:02}",
                error_message="search & test",
            )
        url = reverse("control_plane:cdc_failure_history")
        response = self.client.get(url, {"q": "search & test"})
        self.assertEqual(response.context["page_obj"].paginator.count, 25)
        self.assertEqual(len(response.context["page_obj"]), 10)
        self.assertContains(response, "matching-24")
        self.assertNotContains(response, "matching-00")
        self.assertContains(response, "page=2&q=search%20%26%20test")
        response = self.client.get(url, {"q": "search & test", "page": 3})
        self.assertEqual(len(response.context["page_obj"]), 5)
        self.assertContains(response, "matching-00")
        self.assertContains(response, "page=2&q=search%20%26%20test")
        response = self.client.get(url, {"page": "not-a-page"})
        self.assertEqual(response.context["page_obj"].number, 1)

    def test_cdc_history_requires_admin(self):
        url = reverse("control_plane:cdc_failure_history")
        self.client.logout()
        self.assertEqual(self.client.get(url).status_code, 302)
        user = get_user_model().objects.create_user(username="cdc-history-viewer")
        self.client.force_login(user)
        response = self.client.get(url)
        self.assertEqual(response.status_code, 302)
        self.assertNotContains(response, "cdc-task-123", status_code=302)

    def test_admin_navigation_includes_cdc_failures(self):
        self.admin.is_superuser = True
        self.admin.save(update_fields=["is_superuser"])
        response = self.client.get(reverse("control_plane:pipeline_history"))
        self.assertContains(
            response,
            f'href="{reverse("control_plane:cdc_failure_history")}"',
        )
        response = self.client.get(reverse("control_plane:cdc_failure_history"))
        self.assertContains(
            response,
            'border-indigo-500 text-white">\n                            <span>Administration</span>',
        )

    @patch("control_plane.views.dashboard.get_redis_client")
    def test_dashboard_and_widget_preference(self, redis_client):
        redis_client.return_value.keys.return_value = []
        response = self.client.get(reverse("control_plane:dashboard"))
        self.assertContains(response, "cdc-task-123")
        self.assertContains(response, "CDC Failures")
        self.assertContains(response, reverse("control_plane:cdc_failure_history"))
        self.assertContains(response, reverse("control_plane:pipeline_history"))
        response = self.client.post(
            reverse("control_plane:update_widget_preferences"),
            data=json.dumps({"widget": "cdc_failures", "visible": False}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(
            UserWidgetPreference.objects.get(user=self.admin).show_cdc_failures
        )

    @patch("control_plane.views.dashboard.get_redis_client")
    def test_dashboard_hides_cdc_failure_details_from_non_admins(self, redis_client):
        redis_client.return_value.keys.return_value = []
        user = get_user_model().objects.create_user(username="cdc-viewer")
        self.client.force_login(user)
        response = self.client.get(reverse("control_plane:dashboard"))
        self.assertNotContains(response, "cdc-task-123")
        self.assertNotContains(response, "CDC Failures")


class EmailAuditDashboardTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_user(
            username="email-widget-admin", is_staff=True
        )
        self.client.force_login(self.admin)

    @patch("control_plane.views.dashboard.get_redis_client")
    def test_latest_five_records_and_link(self, redis_client):
        redis_client.return_value.keys.return_value = []
        logs = [
            EmailAuditLog.objects.create(
                recipients=f"recipient-{number}@example.com",
                status="bounced" if number == 5 else "sent",
            )
            for number in range(6)
        ]
        timestamp = timezone.now()
        EmailAuditLog.objects.update(created_at=timestamp)

        response = self.client.get(reverse("control_plane:dashboard"))
        self.assertEqual(list(response.context["recent_email_audits"]), logs[::-1][:5])
        self.assertContains(response, "Recent Email Deliveries")
        self.assertContains(response, "recipient-5@example.com")
        self.assertNotContains(response, "recipient-0@example.com")
        self.assertContains(response, "Bounced")
        self.assertContains(response, "Standalone")
        self.assertContains(response, reverse("control_plane:email_audit_logs"))
        self.assertContains(response, "toggleEmailAuditWidget(")

    @patch("control_plane.views.dashboard.get_redis_client")
    def test_empty_and_persisted_visibility_per_user(self, redis_client):
        redis_client.return_value.keys.return_value = []
        url = reverse("control_plane:dashboard")
        response = self.client.get(url)
        self.assertContains(response, "No email delivery audit records found.")
        self.assertContains(response, 'id="widget-email_audit-wrapper"')
        preference_url = reverse("control_plane:update_widget_preferences")
        for visible in (False, True):
            response = self.client.post(
                preference_url,
                data=json.dumps({"widget": "email_audit", "visible": visible}),
                content_type="application/json",
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                UserWidgetPreference.objects.get(user=self.admin).show_email_audit,
                visible,
            )
            response = self.client.get(url)
            expected_class = " mb-6" if visible else "hidden mb-6"
            self.assertContains(
                response, f'class="{expected_class}" id="widget-email_audit-wrapper"'
            )
            if not visible:
                other_admin = get_user_model().objects.create_user(
                    username="other-email-widget-admin", is_staff=True
                )
                self.client.force_login(other_admin)
                self.assertTrue(self.client.get(url).context["widget_prefs"].show_email_audit)
                self.client.force_login(self.admin)

    @patch("control_plane.views.dashboard.get_redis_client")
    def test_audit_details_and_control_require_staff(self, redis_client):
        redis_client.return_value.keys.return_value = []
        EmailAuditLog.objects.create(recipients="private@example.com")
        for is_superuser in (False, True):
            with self.subTest(is_superuser=is_superuser):
                user = get_user_model().objects.create_user(
                    username=f"email-nonstaff-{is_superuser}",
                    is_staff=False,
                    is_superuser=is_superuser,
                )
                self.client.force_login(user)
                response = self.client.get(reverse("control_plane:dashboard"))
                self.assertEqual(list(response.context["recent_email_audits"]), [])
                self.assertNotContains(response, "private@example.com")
                self.assertNotContains(response, "Recent Email Deliveries")
                self.assertNotContains(response, "toggleEmailAuditWidget(")
                self.assertEqual(
                    self.client.get(reverse("control_plane:email_audit_logs")).status_code,
                    302,
                )


class EmailAuditBreadcrumbTests(TestCase):
    def test_email_audit_breadcrumb_includes_dashboard(self):
        admin = get_user_model().objects.create_user(
            username="email-audit-admin", is_staff=True
        )
        self.client.force_login(admin)
        response = self.client.get(reverse("control_plane:email_audit_logs"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["breadcrumbs"], [
            {"name": "Dashboard", "url": reverse("control_plane:dashboard")},
            {"name": "Administration", "url": ""},
            {"name": "Email Delivery Audit", "url": ""},
        ])

    def test_audit_controls_use_escaped_data_without_missing_script(self):
        admin = get_user_model().objects.create_user(
            username="email-control-admin", is_staff=True
        )
        self.client.force_login(admin)
        log = EmailAuditLog.objects.create(
            recipients='test\'"${danger}<tag>',
            status="failed",
            error_message='Error: `${danger}` <script>alert("x")</script>',
        )
        response = self.client.get(reverse("control_plane:email_audit_logs"))
        self.assertNotContains(response, "control_plane/js/email_audit.js")
        self.assertContains(response, 'data-recipients="test&#x27;&quot;${danger}&lt;tag&gt;"')
        self.assertContains(
            response,
            'data-error-message="Error: `${danger}` &lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;"',
        )
        self.assertContains(
            response, f'data-delete-url="{reverse("control_plane:email_audit_delete", args=[log.pk])}"'
        )
        self.assertContains(response, "$el.dataset.errorMessage")
        self.assertContains(response, "$el.dataset.recipients")


class ActivePipelineExecutionsTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="pipeline-viewer", is_superuser=True, is_staff=True
        )
        self.client.force_login(self.user)
        self.data = {
            f"task-{number:02}": {
                "status": "RUNNING",
                "source_name": f"Source {number:02}",
                "current_table": "Reading rows",
                "started_at": f"2026-01-01T00:{number:02}:00+00:00",
            }
            for number in range(12)
        }
        self.data["report"] = {
            "status": "RUNNING",
            "current_step": "Generating report",
        }
        self.data["finished"] = {"status": "SUCCESS"}
        self.redis_patch = patch("control_plane.views.dashboard.get_redis_client")
        self.redis = self.redis_patch.start().return_value
        self.redis.keys.side_effect = lambda pattern: (
            [f"task:progress:{task_id}".encode() for task_id in self.data]
            if pattern == "task:progress:*" else []
        )
        self.progress_patch = patch(
            "control_plane.views.dashboard.get_task_progress",
            side_effect=lambda task_id: self.data[task_id],
        )
        self.progress_patch.start()
        self.addCleanup(self.redis_patch.stop)
        self.addCleanup(self.progress_patch.stop)

    def test_dashboard_and_polled_fragment_show_only_five_most_recent(self):
        response = self.client.get(reverse("control_plane:dashboard"))
        self.assertEqual(response.context["stats"]["active_pipelines"], 12)
        self.assertEqual(len(response.context["active_running_sources"]), 5)
        self.assertContains(response, "Source 11")
        self.assertNotContains(response, "Source 06")
        self.assertContains(response, reverse("control_plane:active_pipeline_executions"))
        self.assertContains(response, "View All Active Pipeline Executions")
        response = self.client.get(reverse("control_plane:active_pipelines_fragment"))
        self.assertEqual(len(response.context["active_running_sources"]), 5)
        self.assertContains(response, "Source 11")
        self.assertNotContains(response, "Source 06")
        content = response.content.decode()
        self.assertGreater(
            content.index("View All Active Pipeline Executions"),
            content.index("Source 07"),
        )

    def test_search_and_pagination_preserve_query(self):
        url = reverse("control_plane:active_pipeline_executions")
        response = self.client.get(url, {"q": "source"})
        self.assertEqual(response.context["page_obj"].paginator.count, 12)
        self.assertEqual(len(response.context["page_obj"]), 10)
        self.assertContains(response, "?page=2&q=source")
        response = self.client.get(url, {"q": "SOURCE", "page": 2})
        self.assertEqual(len(response.context["page_obj"]), 2)
        self.assertContains(response, "Source 00")
        response = self.client.get(url, {"q": "task-03"})
        self.assertEqual(response.context["page_obj"].paginator.count, 1)
        self.assertContains(response, "Source 03")
        response = self.client.get(url, {"q": "missing"})
        self.assertContains(response, "No active pipeline executions match")

    def test_breadcrumbs_include_administration(self):
        response = self.client.get(reverse("control_plane:active_pipeline_executions"))
        self.assertEqual(response.context["breadcrumbs"], [
            {"name": "Dashboard", "url": reverse("control_plane:dashboard")},
            {"name": "Administration", "url": ""},
            {"name": "Active Pipeline Executions", "url": ""},
        ])

    def test_filter_by_current_step_and_preserve_it_across_pages(self):
        for number in range(11):
            self.data[f"task-{number:02}"]["current_table"] = PREPARING_INGESTION
        self.data["task-11"]["current_table"] = "db.customers"
        url = reverse("control_plane:active_pipeline_executions")
        response = self.client.get(url, {"q": "source", "step": PREPARING_INGESTION})
        self.assertEqual(response.context["page_obj"].paginator.count, 11)
        self.assertContains(response, 'value="Preparing ingestion..." selected')
        self.assertContains(response, "?page=2&q=source&step=Preparing%20ingestion...")
        response = self.client.get(url, {"q": "source", "step": PREPARING_INGESTION, "page": 2})
        self.assertEqual(len(response.context["page_obj"]), 1)
        self.assertContains(response, "Source 00")
        self.assertNotContains(response, "Source 11")
        self.assertContains(response, "?page=1&q=source&step=Preparing%20ingestion...")
        response = self.client.get(url, {"q": "db.customers"})
        self.assertEqual(response.context["page_obj"].paginator.count, 1)
        self.assertContains(response, "Source 11")

    def test_step_options_are_fixed_even_without_running_executions(self):
        self.data.clear()
        response = self.client.get(reverse("control_plane:active_pipeline_executions"))
        self.assertEqual(
            response.context["filter_options"],
            [{"value": step, "label": step} for step in PIPELINE_STEPS],
        )
        for step in PIPELINE_STEPS:
            response = self.client.get(
                reverse("control_plane:active_pipeline_executions"), {"step": step}
            )
            self.assertContains(response, f'value="{step}" selected')
            self.assertEqual(response.context["page_obj"].paginator.count, 0)

    def test_each_fixed_step_filters_matching_executions(self):
        for number, step in enumerate(PIPELINE_STEPS):
            self.data[f"task-{number:02}"]["current_table"] = step
        url = reverse("control_plane:active_pipeline_executions")
        for number, step in enumerate(PIPELINE_STEPS):
            with self.subTest(step=step):
                response = self.client.get(url, {"step": step})
                self.assertEqual(response.context["page_obj"].paginator.count, 1)
                self.assertEqual(response.context["page_obj"][0]["task_id"], f"task-{number:02}")
                self.assertNotContains(response, '<option value="Reading rows"')

    def test_previous_unlisted_step_filter_remains_visible(self):
        response = self.client.get(
            reverse("control_plane:active_pipeline_executions"),
            {"step": "No longer running"},
        )
        self.assertEqual(response.context["page_obj"].paginator.count, 0)
        self.assertContains(response, "The selected step is not in the fixed list: No longer running.")
        self.assertNotContains(response, '<option value="No longer running"')
        self.assertContains(response, "No active pipeline executions match")

    def test_page_requires_login(self):
        self.client.logout()
        response = self.client.get(reverse("control_plane:active_pipeline_executions"))
        self.assertEqual(response.status_code, 302)


class ActiveReportGenerationsTests(TestCase):
    def setUp(self):
        user = get_user_model().objects.create_user(
            username="report-viewer", is_staff=True, is_superuser=True
        )
        self.client.force_login(user)
        source = SourceProfile.objects.create(
            name="Report source", db_type=DbChoice.MYSQL,
            host="localhost", port=3306, database_name="test", username="test",
        )
        table = SourceTable.objects.create(
            source=source, schema_name="test", table_name="items"
        )
        self.executions = []
        created_at = timezone.now()
        for number in range(12):
            report = ReportDefinition.objects.create(
                name=f"Report {number:02}", source=source, base_table=table
            )
            self.executions.append(
                ReportExecution.objects.create(
                    report=report, status="running", progress_percentage=number
                )
            )
            ReportExecution.objects.filter(pk=self.executions[-1].pk).update(
                created_at=created_at + timedelta(seconds=number)
            )
        ReportExecution.objects.create(
            report=self.executions[0].report, status="completed"
        )

    @patch("control_plane.views.dashboard.get_redis_client")
    def test_dashboard_and_fragment_limit_and_link(self, redis_client):
        redis_client.return_value.keys.return_value = []
        response = self.client.get(reverse("control_plane:dashboard"))
        self.assertEqual(len(response.context["active_running_reports"]), 5)
        self.assertEqual(
            [execution.pk for execution in response.context["active_running_reports"]],
            [execution.pk for execution in reversed(self.executions[-5:])],
        )
        self.assertContains(response, "Report 11")
        self.assertNotContains(response, "Report 06")
        self.assertContains(response, "View All Active Report Generations")
        self.assertContains(response, reverse("control_plane:active_report_generations"))
        response = self.client.get(reverse("control_plane:active_reports_fragment"))
        self.assertEqual(len(response.context["active_running_reports"]), 5)
        self.assertEqual(
            [execution.pk for execution in response.context["active_running_reports"]],
            [execution.pk for execution in reversed(self.executions[-5:])],
        )
        self.assertContains(response, "Report 11")
        self.assertNotContains(response, "Report 06")
        self.assertGreater(
            response.content.decode().index("View All Active Report Generations"),
            response.content.decode().index("Report 07"),
        )

    def test_page_search_and_pagination(self):
        url = reverse("control_plane:active_report_generations")
        response = self.client.get(url, {"q": "report"})
        self.assertEqual(response.context["page_obj"].paginator.count, 12)
        self.assertEqual(len(response.context["page_obj"]), 10)
        self.assertContains(response, "?page=2&q=report")
        response = self.client.get(url, {"q": "REPORT", "page": 2})
        self.assertEqual(len(response.context["page_obj"]), 2)
        self.assertContains(response, "Report 00")
        response = self.client.get(url, {"q": "report 03"})
        self.assertEqual(response.context["page_obj"].paginator.count, 1)
        self.assertContains(response, "Report 03")
        response = self.client.get(url, {"q": "missing"})
        self.assertContains(response, "No active report generations match")

    def test_step_labels_and_each_filter(self):
        url = reverse("control_plane:active_report_generations")
        ReportExecution.objects.filter(status="running").update(progress_percentage=0)
        for execution, (_, _, progress) in zip(self.executions, REPORT_RUNNING_STEPS):
            ReportExecution.objects.filter(pk=execution.pk).update(progress_percentage=progress)
        response = self.client.get(url)
        self.assertContains(response, "Current step")
        self.assertEqual(len(response.context["filter_options"]), 5)
        for index, (key, label, _) in enumerate(REPORT_RUNNING_STEPS):
            with self.subTest(step=key):
                response = self.client.get(url, {"step": key})
                self.assertEqual(response.context["page_obj"].paginator.count, 1)
                self.assertEqual(response.context["page_obj"][0].pk, self.executions[index].pk)
                self.assertContains(response, label)
                self.assertContains(response, f'value="{key}" selected')
        response = self.client.get(url, {"step": "unknown"})
        self.assertEqual(response.context["page_obj"].paginator.count, 8)
        self.assertContains(response, "Unknown")
        self.assertEqual(self.client.get(url, {"step": "invalid"}).status_code, 400)

    def test_step_filter_search_pagination_and_clear(self):
        ReportExecution.objects.filter(status="running").update(
            progress_percentage=ReportProgress.QUERYING
        )
        url = reverse("control_plane:active_report_generations")
        response = self.client.get(url, {"q": "report", "step": "querying"})
        self.assertEqual(response.context["page_obj"].paginator.count, 12)
        self.assertContains(response, "?page=2&q=report&step=querying")
        self.assertContains(response, "Clear")
        response = self.client.get(url, {"q": "report", "step": "querying", "page": 2})
        self.assertContains(response, "?page=1&q=report&step=querying")
        self.assertEqual(len(response.context["page_obj"]), 2)
        response = self.client.get(url, {"q": "report 03", "step": "querying"})
        self.assertEqual(response.context["page_obj"].paginator.count, 1)
        self.assertContains(response, "Report 03")

    def test_missing_creation_time_sorts_last(self):
        ReportExecution.objects.filter(pk=self.executions[-1].pk).update(created_at=None)
        response = self.client.get(reverse("control_plane:active_report_generations"))
        self.assertNotIn(self.executions[-1].pk, [item.pk for item in response.context["page_obj"]])

    def test_empty_widget_and_page(self):
        ReportExecution.objects.filter(status="running").update(status="completed")
        response = self.client.get(reverse("control_plane:active_reports_fragment"))
        self.assertContains(response, "No active report generations running.")
        self.assertContains(response, "View All Active Report Generations")
        response = self.client.get(reverse("control_plane:active_report_generations"))
        self.assertEqual(response.context["page_obj"].paginator.count, 0)
        self.assertContains(response, "No active report generations match")
        for _, label, _ in REPORT_RUNNING_STEPS:
            self.assertContains(response, label)

    def test_breadcrumbs_include_administration(self):
        response = self.client.get(reverse("control_plane:active_report_generations"))
        self.assertEqual(response.context["breadcrumbs"], [
            {"name": "Dashboard", "url": reverse("control_plane:dashboard")},
            {"name": "Administration", "url": ""},
            {"name": "Active Report Generations", "url": ""},
        ])

    def test_page_requires_login(self):
        self.client.logout()
        response = self.client.get(reverse("control_plane:active_report_generations"))
        self.assertEqual(response.status_code, 302)


class UserStatusViewTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.admin = self.user_model.objects.create_user(
            username="admin",
            password="test-password",
            is_staff=True,
            is_superuser=True,
        )
        self.target = self.user_model.objects.create_user(
            username="target",
            password="test-password",
        )
        self.client.force_login(self.admin)

    def test_toggle_deactivates_user_and_records_audit_history(self):
        response = self.client.post(
            reverse("control_plane:user_toggle_active", args=[self.target.pk])
        )

        self.assertRedirects(response, reverse("control_plane:user_management"))
        self.target.refresh_from_db()
        self.assertFalse(self.target.is_active)

        history = TrackedUser.history.filter(id=self.target.pk).latest()
        self.assertFalse(history.is_active)
        self.assertEqual(history.history_user_id, self.admin.pk)
        self.assertEqual(history.history_change_reason, "Account deactivated by admin")

    def test_user_status_filter_shows_only_inactive_accounts(self):
        self.target.is_active = False
        self.target.save(update_fields=["is_active"])
        response = self.client.get(
            reverse("control_plane:user_management"),
            {"status": "inactive"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.target.username)
        self.assertEqual(
            [user.pk for user in response.context["page_obj"].object_list],
            [self.target.pk],
        )

    def test_admin_cannot_deactivate_their_own_account(self):
        response = self.client.post(
            reverse("control_plane:user_toggle_active", args=[self.admin.pk])
        )

        self.assertRedirects(response, reverse("control_plane:user_management"))
        self.admin.refresh_from_db()
        self.assertTrue(self.admin.is_active)

    def test_toggle_reactivates_user(self):
        self.target.is_active = False
        self.target.save(update_fields=["is_active"])

        response = self.client.post(
            reverse("control_plane:user_toggle_active", args=[self.target.pk])
        )

        self.assertRedirects(response, reverse("control_plane:user_management"))
        self.target.refresh_from_db()
        self.assertTrue(self.target.is_active)
        history = TrackedUser.history.filter(id=self.target.pk).latest()
        self.assertTrue(history.is_active)
        self.assertEqual(history.history_change_reason, "Account reactivated by admin")


@override_settings(CACHES={
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}
})
class WarehouseFieldSelectionTests(TestCase):
    def setUp(self):
        admin = get_user_model().objects.create_user(
            username="warehouse-admin",
            password="test-password",
            is_staff=True,
            is_superuser=True,
        )
        self.client.force_login(admin)
        self.source = SourceProfile.objects.create(
            name="Warehouse selection source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
        )
        self.table = SourceTable.objects.create(
            source=self.source,
            schema_name="public",
            table_name="events",
            primary_key_fields=["event_id"],
        )
        SourceField.objects.create(
            table=self.table,
            field_name="event_id",
            data_type="INTEGER",
        )
        SourceField.objects.create(
            table=self.table,
            field_name="modified_at",
            data_type="TIMESTAMP WITH TIME ZONE",
        )

    def test_saves_warehouse_fields_without_a_modified_at_requirement(self):
        response = self.client.post(
            reverse(
                "control_plane:warehouse_field_selection",
                args=[self.source.pk],
            ),
            {
                "selected_fields": ["events.event_id", "events.modified_at"],
            },
        )

        self.assertRedirects(response, reverse("control_plane:warehouse_list"))
        self.assertEqual(
            set(
                self.source.warehouse_mappings.filter(is_enabled=True)
                .values_list("field_name", flat=True)
            ),
            {"event_id", "modified_at"},
        )
        self.table.refresh_from_db()
        self.assertIsNone(self.table.incremental_field_name)

    def test_field_page_explains_cdc_requirements(self):
        response = self.client.get(
            reverse(
                "control_plane:warehouse_field_selection",
                args=[self.source.pk],
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No modified-at column is required")
        self.assertContains(response, "Primary Key")
        self.assertContains(response, "event_id")

    def test_field_page_prompts_sync_when_primary_key_metadata_is_missing(self):
        self.table.primary_key_fields = []
        self.table.save(update_fields=["primary_key_fields"])

        response = self.client.get(
            reverse(
                "control_plane:warehouse_field_selection",
                args=[self.source.pk],
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Primary-key metadata is missing or stale")
        self.assertContains(response, reverse("control_plane:source_list"))
        self.assertContains(response, "bounded full-refresh batches")
        self.assertNotContains(response, "Primary Key")

    def test_ignores_legacy_modified_at_form_values(self):
        response = self.client.post(
            reverse(
                "control_plane:warehouse_field_selection",
                args=[self.source.pk],
            ),
            {
                "selected_fields": ["events.event_id"],
                f"incremental_field_{self.table.pk}": "event_id",
            },
        )

        self.assertRedirects(response, reverse("control_plane:warehouse_list"))
        self.assertTrue(
            WarehouseDatasetMapping.objects.filter(
                source_profile=self.source,
                field_name="event_id",
                is_enabled=True,
            ).exists()
        )
