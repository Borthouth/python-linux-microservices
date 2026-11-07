import json
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.test import RequestFactory, SimpleTestCase, TestCase, override_settings
from django.urls import reverse

from control_plane.models import (
    DbChoice,
    SourceField,
    SourceProfile,
    SourceTable,
    TrackedUser,
    WarehouseDatasetMapping,
)
from control_plane.views.dlq import dlq_requeue_view
from control_plane.views.dashboard import update_widget_preferences


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

    def test_saves_warehouse_fields_and_incremental_field(self):
        response = self.client.post(
            reverse(
                "control_plane:warehouse_field_selection",
                args=[self.source.pk],
            ),
            {
                "selected_fields": ["events.event_id", "events.modified_at"],
                f"incremental_field_{self.table.pk}": "modified_at",
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
        self.assertEqual(self.table.incremental_field_name, "modified_at")
        self.assertFalse(self.table.incremental_ready)

    def test_field_page_offers_supported_modified_at_fields(self):
        response = self.client.get(
            reverse(
                "control_plane:warehouse_field_selection",
                args=[self.source.pk],
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Modified-at field (optional)")
        self.assertContains(response, "modified_at (TIMESTAMP WITH TIME ZONE)")

    def test_rejects_non_datetime_incremental_field(self):
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

        self.assertRedirects(
            response,
            reverse(
                "control_plane:warehouse_field_selection",
                args=[self.source.pk],
            ),
        )
        self.assertFalse(
            WarehouseDatasetMapping.objects.filter(
                source_profile=self.source
            ).exists()
        )
