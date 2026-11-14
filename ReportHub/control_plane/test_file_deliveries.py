import json
from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.contrib.messages import get_messages
from django.test import Client, RequestFactory, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from control_plane.models import (
    SourceProfile, SourceTable, TransferDestination, UserWidgetPreference,
)
from control_plane.views.file_deliveries import file_delivery_resend_view
from reports.delivery import DeliveryError
from reports.models import ReportDefinition, ReportExecution, ReportFileDelivery


@override_settings(CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}})
class FileDeliveryAuditTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.admin = get_user_model().objects.create_user(
            username="file-audit-admin", is_staff=True
        )
        source = SourceProfile.objects.create(
            name="Delivery source", host="example.test", database_name="delivery",
            username="reader",
        )
        table = SourceTable.objects.create(source=source, table_name="orders")
        cls.report = ReportDefinition.objects.create(
            name="Monthly deliveries", source=source, base_table=table
        )
        cls.destination = TransferDestination.objects.create(
            name="Current archive", host="sftp.example.test", username="writer",
        )

    def setUp(self):
        self.client.force_login(self.admin)
        self.url = reverse("control_plane:file_delivery_audit")

    def create_delivery(self, **overrides):
        values = {
            "report": self.report,
            "destination": self.destination, "destination_name": "Archived destination",
            "status": "failed", "error_message": "Transfer refused <script>alert(1)</script>",
            "remote_path": "/archive/monthly.csv", "retry_count": 2, "attempt_count": 3,
        }
        if "execution" not in overrides:
            values["execution"] = ReportExecution.objects.create(
                report=self.report, status="completed"
            )
        values.update(overrides)
        return ReportFileDelivery.objects.create(**values)

    def resend_url(self, delivery):
        return reverse("control_plane:file_delivery_resend", args=[delivery.pk])

    def test_audit_shows_details_escaped_errors_and_only_failed_resend(self):
        failed = self.create_delivery()
        others = [self.create_delivery(status=status) for status in ("queued", "running", "succeeded")]
        response = self.client.get(self.url)
        for value in (
            self.report.name, str(failed.execution_id), "Archived destination",
            "/archive/monthly.csv", "Retries", "Attempts: 3",
            "Transfer refused &lt;script&gt;alert(1)&lt;/script&gt;",
        ):
            self.assertContains(response, value)
        self.assertNotContains(response, "<script>alert(1)</script>")
        self.assertContains(response, self.resend_url(failed))
        self.assertContains(response, "data-confirm-resend")
        self.assertContains(response, "csrfmiddlewaretoken")
        for delivery in others:
            self.assertNotContains(response, self.resend_url(delivery))

    def test_search_status_filters_and_pagination_preserve_filters(self):
        for number in range(22):
            self.create_delivery(destination_name=f"Archive match {number}")
        self.create_delivery(status="succeeded", destination_name="Archive match other")
        self.create_delivery(destination_name="Unrelated")
        response = self.client.get(self.url, {"q": " Archive match ", "status": "failed"})
        self.assertEqual(response.context["page_obj"].paginator.count, 22)
        self.assertEqual(len(response.context["page_obj"]), 20)
        self.assertContains(response, "page=2")
        self.assertContains(response, "q=Archive%20match")
        self.assertContains(response, "status=failed")
        response = self.client.get(self.url, {"q": "Archive match", "status": "failed", "page": 2})
        self.assertEqual(len(response.context["page_obj"]), 2)
        for query in (self.report.name, self.destination.name, "monthly.csv", "Transfer refused"):
            with self.subTest(query=query):
                response = self.client.get(self.url, {"q": query})
                self.assertEqual(response.context["page_obj"].paginator.count, 24)

    def test_full_execution_uuid_search_returns_only_that_executions_deliveries(self):
        failed = self.create_delivery()
        succeeded = self.create_delivery(
            execution=failed.execution, destination=None, status="succeeded"
        )
        self.create_delivery()
        for query in (str(failed.execution_id), str(failed.execution_id).upper()):
            with self.subTest(query=query):
                response = self.client.get(self.url, {"q": query})
                self.assertEqual(
                    set(response.context["page_obj"]), {failed, succeeded}
                )
        response = self.client.get(
            self.url, {"q": str(failed.execution_id), "status": "failed"}
        )
        self.assertEqual(list(response.context["page_obj"]), [failed])
        response = self.client.get(self.url, {"q": str(uuid4())})
        self.assertEqual(response.context["page_obj"].paginator.count, 0)

    def test_deleted_relations_and_empty_results_render(self):
        self.create_delivery(report=None, execution=None, destination=None)
        self.create_delivery(
            report=None, execution=None, destination=None, destination_name=""
        )
        response = self.client.get(self.url)
        self.assertContains(response, "Archived destination")
        self.assertContains(response, "Deleted destination")
        self.assertContains(response, "Deleted report")
        self.assertContains(response, "No execution available")
        response = self.client.get(self.url, {"q": "no-such-delivery"})
        self.assertContains(response, "No file delivery audit records found.")

    @patch("control_plane.views.file_deliveries.queue_delivery")
    def test_staff_only_including_inactive_and_nonstaff_superuser(self, queue):
        delivery = self.create_delivery()
        users = [
            None,
            get_user_model().objects.create_user(username="file-reader"),
            get_user_model().objects.create_user(
                username="file-inactive", is_staff=True, is_active=False
            ),
            get_user_model().objects.create_user(
                username="file-nonstaff-root", is_superuser=True
            ),
        ]
        for user in users:
            self.client.logout()
            if user:
                self.client.force_login(user)
            for url in (self.url, self.resend_url(delivery)):
                for method in ("get", "post"):
                    with self.subTest(user=user, url=url, method=method):
                        response = getattr(self.client, method)(url)
                        self.assertEqual(response.status_code, 302)
                        self.assertTrue(response.url.startswith(reverse("control_plane:login")))
        queue.assert_not_called()

    @patch("control_plane.views.file_deliveries.queue_delivery")
    def test_resend_is_post_only_csrf_protected_and_requires_existing_record(self, queue):
        delivery = self.create_delivery()
        url = self.resend_url(delivery)
        self.assertEqual(self.client.get(url).status_code, 405)
        csrf_client = Client(enforce_csrf_checks=True)
        csrf_client.force_login(self.admin)
        self.assertEqual(csrf_client.post(url).status_code, 403)
        csrf_client.get(self.url)
        response = csrf_client.post(
            url, {"csrfmiddlewaretoken": csrf_client.cookies["csrftoken"].value}
        )
        self.assertRedirects(response, self.url, fetch_redirect_response=False)
        queue.assert_called_once_with(delivery.pk, retry=True)
        queue.reset_mock()
        missing_url = reverse("control_plane:file_delivery_resend", args=[uuid4()])
        self.assertEqual(self.client.post(missing_url).status_code, 404)
        queue.assert_not_called()

    @patch("control_plane.views.file_deliveries.queue_delivery")
    def test_resend_queues_failed_deliveries_and_surfaces_errors(self, queue):
        delivery = self.create_delivery()
        response = self.client.post(self.resend_url(delivery), follow=True)
        queue.assert_called_once_with(delivery.pk, retry=True)
        self.assertEqual(
            [str(message) for message in get_messages(response.wsgi_request)],
            ["File delivery queued for resend."],
        )
        for error, expected in (
            (DeliveryError("Delivery is no longer failed."), "Delivery is no longer failed."),
            (DeliveryError("Delivery queue unavailable."), "Delivery queue unavailable."),
        ):
            with self.subTest(error=error):
                queue.side_effect = error
                with self.assertLogs("control_plane.views.file_deliveries", level="WARNING") as logs:
                    response = self.client.post(self.resend_url(delivery), follow=True)
                self.assertIn(str(delivery.pk), logs.output[0])
                self.assertIn("error_type=DeliveryError", logs.output[0])
                self.assertNotIn(str(error), logs.output[0])
                self.assertIsNone(logs.records[0].exc_info)
                self.assertRedirects(response, self.url, fetch_redirect_response=False)
                self.assertEqual(
                    [str(message) for message in get_messages(response.wsgi_request)],
                    [expected],
                )
                delivery.refresh_from_db()
                self.assertEqual(delivery.status, "failed")
                self.assertEqual(delivery.retry_count, 2)

    @patch("control_plane.views.file_deliveries.queue_delivery")
    def test_unexpected_resend_errors_propagate_without_view_logging(self, queue):
        delivery = self.create_delivery()
        queue.side_effect = RuntimeError("private connection details")
        request = RequestFactory().post(self.resend_url(delivery))
        request.user = self.admin
        with self.assertNoLogs("control_plane.views.file_deliveries", level="DEBUG"):
            with self.assertRaises(RuntimeError):
                file_delivery_resend_view(request, delivery.pk)
        delivery.refresh_from_db()
        self.assertEqual(delivery.status, "failed")
        self.assertEqual(delivery.retry_count, 2)

    @patch("control_plane.views.file_deliveries.queue_delivery")
    def test_nonfailed_deliveries_cannot_be_resent(self, queue):
        for status in ("succeeded", "running", "queued"):
            with self.subTest(status=status):
                delivery = self.create_delivery(status=status)
                response = self.client.post(self.resend_url(delivery), follow=True)
                self.assertRedirects(response, self.url, fetch_redirect_response=False)
                self.assertEqual(
                    [str(message) for message in get_messages(response.wsgi_request)],
                    ["Only failed file deliveries can be resent."],
                )
                delivery.refresh_from_db()
                self.assertEqual(delivery.status, status)
                self.assertEqual(delivery.retry_count, 2)
        queue.assert_not_called()


@override_settings(CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}})
class FileDeliveryDashboardTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_user(
            username="file-widget-admin", is_staff=True
        )
        self.client.force_login(self.admin)
        redis_patch = patch("control_plane.views.dashboard.get_redis_client")
        self.redis = redis_patch.start().return_value
        self.redis.keys.return_value = []
        self.addCleanup(redis_patch.stop)

    def test_latest_five_deliveries_direct_link_and_empty_state(self):
        url = reverse("control_plane:dashboard")
        response = self.client.get(url)
        self.assertContains(response, "No file delivery audit records found.")
        deliveries = [
            ReportFileDelivery.objects.create(
                destination_name=f"Widget destination {number}", status="failed",
                retry_count=number,
            ) for number in range(6)
        ]
        ReportFileDelivery.objects.update(created_at=timezone.now())
        response = self.client.get(url)
        expected = sorted(deliveries, key=lambda delivery: delivery.pk, reverse=True)[:5]
        self.assertEqual(list(response.context["recent_file_deliveries"]), expected)
        self.assertContains(response, "Recent File Deliveries")
        self.assertContains(response, "toggleFileDeliveriesWidget(")
        self.assertContains(response, reverse("control_plane:file_delivery_audit"))
        for delivery in expected:
            self.assertContains(response, delivery.destination_name)
        omitted = next(delivery for delivery in deliveries if delivery not in expected)
        self.assertNotContains(response, omitted.destination_name)
        self.assertNotContains(response, "Filter file deliveries")

    def test_preference_persists_per_user_and_rejects_invalid_payloads(self):
        url = reverse("control_plane:update_widget_preferences")
        self.assertTrue(UserWidgetPreference.objects.create(user=self.admin).show_file_deliveries)
        for visible in (False, True):
            response = self.client.post(
                url, json.dumps({"widget": "file_deliveries", "visible": visible}),
                content_type="application/json",
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                UserWidgetPreference.objects.get(user=self.admin).show_file_deliveries,
                visible,
            )
            response = self.client.get(reverse("control_plane:dashboard"))
            expected_class = " mb-6" if visible else "hidden mb-6"
            self.assertContains(
                response, f'class="{expected_class}" id="widget-file_deliveries-wrapper"'
            )
        for data in (
            {"widget": "file_deliveries", "visible": "false"},
            {"widget": "file_deliveries", "visible": None},
            {"widget": "file_deliveries", "visible": 0},
            {"widget": "file_deliveries"},
            {"widget": "unknown", "visible": False},
            [],
        ):
            with self.subTest(data=data):
                self.assertEqual(self.client.post(
                    url, json.dumps(data), content_type="application/json"
                ).status_code, 400)
                self.assertTrue(
                    UserWidgetPreference.objects.get(user=self.admin).show_file_deliveries
                )
        self.client.post(
            url, json.dumps({"widget": "file_deliveries", "visible": False}),
            content_type="application/json",
        )
        other = get_user_model().objects.create_user(username="other-file-admin", is_staff=True)
        self.client.force_login(other)
        self.assertTrue(
            self.client.get(reverse("control_plane:dashboard")).context["widget_prefs"].show_file_deliveries
        )

    def test_nonstaff_cannot_see_deliveries_or_change_preference(self):
        ReportFileDelivery.objects.create(destination_name="Private file destination")
        for is_superuser in (False, True):
            with self.subTest(is_superuser=is_superuser):
                user = get_user_model().objects.create_user(
                    username=f"file-widget-reader-{is_superuser}", is_superuser=is_superuser
                )
                self.client.force_login(user)
                response = self.client.get(reverse("control_plane:dashboard"))
                self.assertEqual(response.context["recent_file_deliveries"], [])
                self.assertNotContains(response, "Private file destination")
                self.assertNotContains(response, "Recent File Deliveries")
                self.assertNotContains(response, "toggleFileDeliveriesWidget(")
                self.assertNotContains(response, reverse("control_plane:file_delivery_audit"))
                response = self.client.post(
                    reverse("control_plane:update_widget_preferences"),
                    json.dumps({"widget": "file_deliveries", "visible": False}),
                    content_type="application/json",
                )
                self.assertIn(response.status_code, (302, 403))
                self.assertTrue(UserWidgetPreference.objects.get(user=user).show_file_deliveries)
