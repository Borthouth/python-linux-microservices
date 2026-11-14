import base64
import json
import stat
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from uuid import uuid4
from unittest.mock import Mock, patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db import connection
from django.db import IntegrityError, close_old_connections, transaction
from django.test import (
    Client, RequestFactory, SimpleTestCase, TestCase, TransactionTestCase, override_settings,
    skipUnlessDBFeature,
)
from django.urls import reverse
from django.utils import timezone
from kombu.exceptions import OperationalError
import paramiko

from control_plane.models import (
    CdcExecutionFailure,
    CdcSourceExternalOperation,
    CdcTableRebuild,
    CdcToBatchTableCheckpoint,
    CdcToBatchTransition,
    DbChoice,
    IngestionMode,
    Role,
    SourceField,
    SourceProfile,
    SourceTable,
    SyncStatus,
    TrackedUser,
    TransferDestination,
    UserSourceAccess,
    WarehouseDatasetMapping,
    UserWidgetPreference,
)
from control_plane.forms.transfer_destinations import TransferDestinationForm
from control_plane.tasks import test_transfer_destination_task
from control_plane.views.dlq import dlq_requeue_view
from control_plane.views.dashboard import update_widget_preferences
from etl.progress import PIPELINE_STEPS, PREPARING_INGESTION
from etl.tasks import stage_cdc_to_batch_transition_task
from reports.models import EmailAuditLog, ReportDefinition, ReportExecution
from reports.progress import REPORT_RUNNING_STEPS, ReportProgress
from reports.sftp import DeliveryError


class BreadcrumbRouteTests(SimpleTestCase):
    def test_registered_breadcrumb_links_resolve(self):
        from control_plane.breadcrumbs import BREADCRUMB_REGISTRY

        for page_name, crumbs in BREADCRUMB_REGISTRY.items():
            for crumb in crumbs:
                target = crumb["url"]
                if not target:
                    continue
                if ":" not in target:
                    target = f"control_plane:{target}"
                with self.subTest(page_name=page_name, target=target):
                    self.assertTrue(reverse(target).startswith("/"))


@override_settings(CACHES={
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}
})
class NavbarNavigationTests(TestCase):
    def setUp(self):
        admin = get_user_model().objects.create_user(
            username="navigation-admin",
            is_staff=True,
        )
        self.client.force_login(admin)

    def test_admin_navigation_is_split_into_linked_groups(self):
        response = self.client.get(
            reverse("control_plane:transfer_destination_list")
        )

        for label in (
            "Data &amp; Pipelines",
            "Operations",
            "History",
            "Governance",
            "Integrations",
        ):
            self.assertContains(response, label)
        self.assertContains(
            response,
            f'href="{reverse("control_plane:active_pipeline_executions")}"',
        )
        self.assertContains(
            response,
            f'href="{reverse("control_plane:cdc_rebuild_list")}"',
        )
        self.assertNotContains(response, "Administration")

    def test_history_route_uses_its_active_menu_and_working_breadcrumb(self):
        response = self.client.get(reverse("control_plane:cdc_rebuild_list"))

        self.assertContains(
            response,
            'border-indigo-500 text-white">\n                            <span>History</span>',
        )
        self.assertEqual(response.context["breadcrumbs"], [
            {"name": "Dashboard", "url": reverse("control_plane:dashboard")},
            {"name": "CDC Table Rebuilds", "url": ""},
        ])


@override_settings(CACHES={
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}
})
class SourceConnectionPollingTests(TestCase):
    def setUp(self):
        admin = get_user_model().objects.create_user(
            username="source-polling-admin", is_staff=True,
        )
        self.client.force_login(admin)
        self.source = SourceProfile.objects.create(
            name="Polling test source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
        )

    def test_list_and_status_poll_once_only_while_running(self):
        status_url = reverse("control_plane:source_status", args=[self.source.pk])
        for status in (SyncStatus.RUNNING, SyncStatus.SUCCESS, SyncStatus.FAILED):
            with self.subTest(status=status):
                SourceProfile.objects.filter(pk=self.source.pk).update(
                    last_sync_status=status,
                )
                for url in (reverse("control_plane:source_list"), status_url):
                    with self.subTest(url=url):
                        response = self.client.get(url)
                        self.assertContains(
                            response, f'id="source-status-{self.source.pk}"', count=1,
                        )
                        if status == SyncStatus.RUNNING:
                            self.assertContains(response, f'hx-get="{status_url}"', count=1)
                            self.assertContains(response, 'hx-trigger="every 2s"', count=1)
                        else:
                            self.assertNotContains(response, f'hx-get="{status_url}"')


class TransferDestinationTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.admin = get_user_model().objects.create_user(
            username="transfer-admin", is_staff=True
        )
        key = ed25519.Ed25519PrivateKey.generate()
        cls.key_text = key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode()
        cls.encrypted_key_text = key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.BestAvailableEncryption(b"synthetic-passphrase"),
        ).decode()
        cls.openssh_key_text = key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH,
            serialization.NoEncryption(),
        ).decode()

    def setUp(self):
        self.client.force_login(self.admin)
        self.list_url = reverse("control_plane:transfer_destination_list")
        self.create_url = reverse("control_plane:transfer_destination_create")

    def destination_data(self, **overrides):
        data = {
            "name": "Synthetic SFTP target",
            "host": "sftp.example.test",
            "port": "22",
            "username": "report_writer",
            "authentication": "password",
            "password": " synthetic-password ",
            "private_key": "",
            "private_key_passphrase": "",
            "host_key_sha256": "SHA256:" + base64.b64encode(bytes(range(32))).decode().rstrip("="),
            "remote_directory": "/incoming/reports",
        }
        data.update(overrides)
        return data

    def create_destination(self, **overrides):
        form = TransferDestinationForm(self.destination_data(**overrides))
        self.assertTrue(form.is_valid(), form.errors)
        return form.save()

    def test_staff_only_for_all_destination_endpoints(self):
        destination = self.create_destination()
        urls = [
            self.list_url, self.create_url,
            reverse("control_plane:transfer_destination_edit", args=[destination.pk]),
            reverse("control_plane:transfer_destination_delete", args=[destination.pk]),
            reverse("control_plane:transfer_destination_toggle", args=[destination.pk]),
            reverse("control_plane:transfer_destination_test", args=[destination.pk]),
            reverse("control_plane:transfer_destination_test_status", args=[destination.pk]),
        ]
        users = [
            None,
            get_user_model().objects.create_user(username="transfer-reader"),
            get_user_model().objects.create_user(username="transfer-inactive", is_staff=True, is_active=False),
            get_user_model().objects.create_user(username="transfer-nonstaff-superuser", is_superuser=True),
        ]
        for user in users:
            self.client.logout()
            if user:
                self.client.force_login(user)
            for url in urls:
                for method in ("get", "post"):
                    with self.subTest(user=user, url=url, method=method):
                        response = getattr(self.client, method)(url, self.destination_data(name="Forbidden"))
                        self.assertEqual(response.status_code, 302)
                        self.assertTrue(response.url.startswith(reverse("control_plane:login")))
        self.assertEqual(TransferDestination.objects.count(), 1)
        destination.refresh_from_db()
        self.assertEqual(destination.name, "Synthetic SFTP target")

    def test_create_encrypts_password_and_excludes_all_secrets_from_history(self):
        response = self.client.post(self.create_url, self.destination_data())
        self.assertRedirects(response, self.list_url)
        destination = TransferDestination.objects.get()
        self.assertEqual(destination.password, " synthetic-password ")
        self.assertFalse(destination.is_active)
        self.assertEqual(destination.created_by, self.admin)
        self.assertEqual(destination.history.get().history_user, self.admin)
        with connection.cursor() as cursor:
            cursor.execute("SELECT password FROM control_plane_transferdestination WHERE id = %s", [destination.pk])
            encrypted = cursor.fetchone()[0]
        self.assertNotIn("synthetic-password", encrypted)
        self.assertNotEqual(encrypted, destination.password)
        historical_fields = {field.name for field in destination.history.model._meta.fields}
        self.assertTrue({"password", "private_key", "private_key_passphrase"}.isdisjoint(historical_fields))

    def test_key_and_passphrase_are_encrypted_and_blank_edit_keeps_them(self):
        destination = self.create_destination(
            authentication="private_key", password="",
            private_key=self.encrypted_key_text, private_key_passphrase="synthetic-passphrase",
        )
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT private_key, private_key_passphrase FROM control_plane_transferdestination WHERE id = %s",
                [destination.pk],
            )
            encrypted_key, encrypted_passphrase = cursor.fetchone()
        self.assertNotIn("PRIVATE KEY", encrypted_key)
        self.assertNotIn("synthetic-passphrase", encrypted_passphrase)
        url = reverse("control_plane:transfer_destination_edit", args=[destination.pk])
        response = self.client.post(url, self.destination_data(
            name="Renamed target", authentication="private_key", password="",
        ))
        self.assertRedirects(response, self.list_url)
        destination.refresh_from_db()
        self.assertEqual(destination.private_key, self.encrypted_key_text)
        self.assertEqual(destination.private_key_passphrase, "synthetic-passphrase")
        self.assertEqual(destination.updated_by, self.admin)

    def test_blank_password_edit_preserves_it_and_rotation_replaces_it(self):
        destination = self.create_destination()
        url = reverse("control_plane:transfer_destination_edit", args=[destination.pk])
        for submitted, expected in [("", " synthetic-password "), ("replacement-password", "replacement-password")]:
            response = self.client.post(url, self.destination_data(password=submitted))
            self.assertRedirects(response, self.list_url)
            destination.refresh_from_db()
            self.assertEqual(destination.password, expected)

    def test_secrets_are_never_rendered_even_after_invalid_submission(self):
        destination = self.create_destination(
            authentication="private_key", password="",
            private_key=self.encrypted_key_text, private_key_passphrase="synthetic-passphrase",
        )
        url = reverse("control_plane:transfer_destination_edit", args=[destination.pk])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIn("no-store", response.headers["Cache-Control"])
        self.assertNotContains(response, self.encrypted_key_text)
        self.assertNotContains(response, "synthetic-passphrase")
        response = self.client.post(url, self.destination_data(
            authentication="private_key", password="", private_key="invalid-private-key-marker",
            private_key_passphrase="invalid-passphrase-marker",
        ))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Enter a valid SSH private key")
        self.assertNotContains(response, "invalid-private-key-marker")
        self.assertNotContains(response, "invalid-passphrase-marker")
        response = self.client.post(self.create_url, self.destination_data(
            name="", password="invalid-password-marker",
        ))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "invalid-password-marker")

    def test_switching_authentication_removes_obsolete_credentials(self):
        destination = self.create_destination()
        url = reverse("control_plane:transfer_destination_edit", args=[destination.pk])
        response = self.client.post(url, self.destination_data(
            authentication="private_key", password="", private_key=self.openssh_key_text,
        ))
        self.assertRedirects(response, self.list_url)
        destination.refresh_from_db()
        self.assertEqual(destination.password, "")
        self.assertEqual(destination.private_key, self.openssh_key_text)
        response = self.client.post(url, self.destination_data(password="new-password"))
        self.assertRedirects(response, self.list_url)
        destination.refresh_from_db()
        self.assertEqual(destination.password, "new-password")
        self.assertEqual(destination.private_key, "")
        self.assertEqual(destination.private_key_passphrase, "")

    def test_replacing_encrypted_key_can_explicitly_clear_passphrase(self):
        destination = self.create_destination(
            authentication="private_key", password="",
            private_key=self.encrypted_key_text, private_key_passphrase="synthetic-passphrase",
        )
        data = self.destination_data(
            authentication="private_key", password="", private_key=self.key_text,
        )
        form = TransferDestinationForm(data, instance=destination)
        self.assertFalse(form.is_valid())
        self.assertIn("private_key", form.errors)
        destination.refresh_from_db()
        data["clear_private_key_passphrase"] = "on"
        form = TransferDestinationForm(data, instance=destination)
        self.assertTrue(form.is_valid(), form.errors)
        form.save()
        destination.refresh_from_db()
        self.assertEqual(destination.private_key_passphrase, "")
        destination.full_clean()

    def test_invalid_destinations_and_authentication_are_rejected(self):
        cases = [
            ("host", "sftp://server.test/path"), ("host", "host.test:22"),
            ("host", "host name"), ("host", "::::"),
            ("port", "0"), ("port", "-1"), ("port", "65536"), ("port", "abc"),
            ("remote_directory", "relative/path"), ("remote_directory", "/safe/../other"),
            ("remote_directory", "/safe\nother"), ("remote_directory", "//other"),
            ("host_key_sha256", "SHA256:not-a-valid-fingerprint"),
            ("host_key_sha256", "SHA256:" + "B" * 43),
            ("authentication", "ftp"), ("password", ""),
        ]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                form = TransferDestinationForm(self.destination_data(**{field: value}))
                self.assertFalse(form.is_valid())
                self.assertIn(field, form.errors)
        for host in ["localhost", "192.0.2.10", "2001:db8::1", "sftp.internal.test."]:
            form = TransferDestinationForm(self.destination_data(host=host))
            self.assertTrue(form.is_valid(), form.errors)

    def test_missing_invalid_or_conflicting_key_credentials_are_rejected(self):
        for key, passphrase in [("", ""), ("not a key", ""), (self.encrypted_key_text, "wrong-passphrase")]:
            form = TransferDestinationForm(self.destination_data(
                authentication="private_key", password="", private_key=key,
                private_key_passphrase=passphrase,
            ))
            self.assertFalse(form.is_valid())
            self.assertIn("private_key", form.errors)
        form = TransferDestinationForm(self.destination_data(private_key=self.key_text))
        self.assertFalse(form.is_valid())
        self.assertIn("private_key", form.errors)
        destination = TransferDestination(**self.destination_data(private_key=self.key_text))
        with self.assertRaises(ValidationError):
            destination.full_clean()

    def test_list_search_breadcrumbs_and_metadata_history(self):
        destination = self.create_destination()
        self.create_destination(name="Other destination", host="other.example.test")
        response = self.client.get(self.list_url, {"q": "SFTP"})
        self.assertContains(response, destination.name)
        self.assertNotContains(response, "Other destination")
        self.assertNotContains(response, "synthetic-password")
        self.assertEqual(
            response.context["breadcrumbs"][1],
            {"name": "Transfer Destinations", "url": ""},
        )
        self.assertContains(response, "Configure SFTP destinations for automated report file delivery.")
        response = self.client.get(self.create_url)
        self.assertContains(response, "active destinations can be selected for report file delivery.")
        self.assertContains(response, "Allow reports with file delivery enabled to transfer files to this destination.")
        self.assertNotContains(response, "Configuration only")
        url = reverse("control_plane:transfer_destination_edit", args=[destination.pk])
        response = self.client.post(url, self.destination_data(name="Renamed target", password=""))
        self.assertRedirects(response, self.list_url)
        response = self.client.get(reverse("control_plane:audit_log"))
        self.assertContains(response, "Renamed target")
        self.assertNotContains(response, "synthetic-password")

    def test_destination_breadcrumbs_link_back_to_the_list(self):
        destination = self.create_destination()
        pages = [
            (self.list_url, "Transfer Destinations"),
            (self.create_url, "Add SFTP Destination"),
            (reverse("control_plane:transfer_destination_edit", args=[destination.pk]), "Edit SFTP Destination"),
            (reverse("control_plane:transfer_destination_delete", args=[destination.pk]), "Delete SFTP Destination"),
        ]
        for url, title in pages:
            with self.subTest(url=url):
                response = self.client.get(url)
                expected = [
                    {"name": "Dashboard", "url": reverse("control_plane:dashboard")},
                ]
                if url == self.list_url:
                    expected.append({"name": title, "url": ""})
                else:
                    expected.extend([
                        {"name": "Transfer Destinations", "url": self.list_url},
                        {"name": title, "url": ""},
                    ])
                self.assertEqual(response.context["breadcrumbs"], expected)

    def test_status_filter_combines_with_search_and_pagination(self):
        for number in range(16):
            self.create_destination(name=f"Target {number:02}", is_active=True)
        disabled = self.create_destination(name="Target disabled")
        self.create_destination(name="Other destination", is_active=True)
        response = self.client.get(self.list_url, {"q": "target", "status": "active"})
        self.assertEqual(response.context["page_obj"].paginator.count, 16)
        self.assertContains(response, "?page=2&q=target&status=active")
        self.assertNotContains(response, disabled.name)
        self.assertContains(response, 'value="active" selected')
        self.assertContains(response, "Clear")
        response = self.client.get(self.list_url, {"q": "target", "status": "active", "page": 2})
        self.assertEqual([item.name for item in response.context["page_obj"]], ["Target 15"])
        self.assertContains(response, "?page=1&q=target&status=active")
        response = self.client.get(self.list_url, {"q": "target", "status": "inactive"})
        self.assertEqual([item.pk for item in response.context["page_obj"]], [disabled.pk])
        self.assertContains(response, 'value="inactive" selected')

    def test_deletion_requires_post_and_retains_only_nonsecret_history(self):
        destination = self.create_destination()
        url = reverse("control_plane:transfer_destination_delete", args=[destination.pk])
        self.assertEqual(self.client.get(url).status_code, 200)
        self.assertTrue(TransferDestination.objects.filter(pk=destination.pk).exists())
        self.assertEqual(self.client.put(url).status_code, 405)
        self.assertRedirects(self.client.post(url), self.list_url)
        self.assertFalse(TransferDestination.objects.filter(pk=destination.pk).exists())
        history = TransferDestination.history.filter(id=destination.pk).latest()
        self.assertEqual(history.history_type, "-")
        self.assertEqual(history.history_user, self.admin)

    def test_mutations_require_csrf_token(self):
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.admin)
        response = client.get(self.create_url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(client.post(self.create_url, self.destination_data()).status_code, 403)
        token = client.cookies["csrftoken"].value
        response = client.post(self.create_url, self.destination_data(), HTTP_X_CSRFTOKEN=token)
        self.assertRedirects(response, self.list_url)

    def test_toggle_requires_post_csrf_and_records_audit_without_changing_credentials(self):
        destination = self.create_destination()
        url = reverse("control_plane:transfer_destination_toggle", args=[destination.pk])
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.admin)
        client.get(self.list_url)
        self.assertEqual(client.get(url).status_code, 405)
        self.assertEqual(client.post(url).status_code, 403)
        token = client.cookies["csrftoken"].value
        for expected in [True, False]:
            response = client.post(url, HTTP_X_CSRFTOKEN=token)
            self.assertRedirects(response, self.list_url)
            destination.refresh_from_db()
            self.assertEqual(destination.is_active, expected)
            self.assertEqual(destination.password, " synthetic-password ")
            self.assertEqual(destination.history.latest().is_active, expected)
            self.assertEqual(destination.history.latest().history_user, self.admin)
            page = client.get(self.list_url)
            self.assertContains(page, ">Deactivate</button>" if expected else ">Activate</button>")
            self.assertContains(page, ">Active</span>" if expected else ">Inactive</span>")

    @patch("control_plane.tasks.test_transfer_destination_task.apply_async")
    def test_connection_test_is_post_csrf_and_queued_once_even_when_disabled(self, publish):
        destination = self.create_destination()
        url = reverse("control_plane:transfer_destination_test", args=[destination.pk])
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.admin)
        client.get(self.list_url)
        self.assertEqual(client.get(url).status_code, 405)
        self.assertEqual(client.post(url).status_code, 403)
        with self.captureOnCommitCallbacks(execute=True):
            response = client.post(url, HTTP_X_CSRFTOKEN=client.cookies["csrftoken"].value)
        self.assertRedirects(response, self.list_url)
        destination.refresh_from_db()
        self.assertFalse(destination.is_active)
        self.assertEqual(destination.last_test_status, TransferDestination.TestStatus.QUEUED)
        publish.assert_called_once_with(
            args=[destination.pk], task_id=destination.last_test_task_id,
            queue="queue_transfers", expires=120,
        )
        response = self.client.post(url, follow=True)
        self.assertContains(response, "already queued or running")
        self.assertEqual(publish.call_count, 1)

    @patch("control_plane.tasks.sftp_connection")
    def test_connection_task_checks_directory_without_writing_and_ignores_duplicates(self, connection):
        destination = self.create_destination()
        task_id = str(uuid4())
        TransferDestination.objects.filter(pk=destination.pk).update(
            last_test_status=TransferDestination.TestStatus.QUEUED, last_test_task_id=task_id,
        )
        sftp = connection.return_value.__enter__.return_value
        sftp.stat.return_value.st_mode = stat.S_IFDIR | 0o755
        for _ in range(2):
            test_transfer_destination_task.apply(args=[destination.pk], task_id=task_id, throw=True)
        destination.refresh_from_db()
        self.assertEqual(destination.last_test_status, TransferDestination.TestStatus.SUCCESS)
        self.assertIsNotNone(destination.last_tested_at)
        connection.assert_called_once()
        sftp.stat.assert_called_once_with(destination.remote_directory)
        sftp.open.assert_not_called()
        sftp.putfo.assert_not_called()
        response = self.client.get(reverse("control_plane:transfer_destination_test_status", args=[destination.pk]))
        self.assertContains(response, "Passed")
        self.assertNotContains(response, "hx-trigger")
        self.assertIn("no-store", response.headers["Cache-Control"])

    def test_connection_status_polls_only_while_queued_or_running(self):
        destination = self.create_destination()
        status_url = reverse(
            "control_plane:transfer_destination_test_status", args=[destination.pk],
        )
        for status in (
            TransferDestination.TestStatus.QUEUED,
            TransferDestination.TestStatus.RUNNING,
            TransferDestination.TestStatus.SUCCESS,
            TransferDestination.TestStatus.FAILED,
        ):
            with self.subTest(status=status):
                TransferDestination.objects.filter(pk=destination.pk).update(
                    last_test_status=status, last_tested_at=timezone.now(),
                    last_test_error="Connection failed" if status == TransferDestination.TestStatus.FAILED else "",
                )
                for url in (self.list_url, status_url):
                    with self.subTest(url=url):
                        response = self.client.get(url)
                        self.assertContains(
                            response, f'id="destination-test-{destination.pk}"', count=1,
                        )
                        if status in (
                            TransferDestination.TestStatus.QUEUED,
                            TransferDestination.TestStatus.RUNNING,
                        ):
                            self.assertContains(response, f'hx-get="{status_url}"', count=1)
                            self.assertContains(response, 'hx-trigger="every 2s"', count=1)
                        else:
                            self.assertNotContains(response, f'hx-get="{status_url}"')

    @patch("control_plane.tasks.sftp_connection")
    def test_connection_failures_are_safe_and_non_directory_is_rejected(self, connection):
        destination = self.create_destination()
        cases = [
            (paramiko.AuthenticationException("synthetic-secret-marker"), "authentication failed"),
            (OSError("synthetic-secret-marker"), "Unable to connect"),
            (DeliveryError("Server host-key fingerprint mismatch."), "fingerprint mismatch"),
            (None, "not a directory"),
        ]
        for error, expected in cases:
            with self.subTest(error=type(error).__name__):
                task_id = str(uuid4())
                TransferDestination.objects.filter(pk=destination.pk).update(
                    last_test_status=TransferDestination.TestStatus.QUEUED, last_test_task_id=task_id,
                )
                connection.return_value.__enter__.side_effect = error
                connection.return_value.__enter__.return_value.stat.return_value.st_mode = stat.S_IFREG
                with self.assertRaises(DeliveryError):
                    test_transfer_destination_task.apply(args=[destination.pk], task_id=task_id, throw=True)
                destination.refresh_from_db()
                self.assertEqual(destination.last_test_status, TransferDestination.TestStatus.FAILED)
                self.assertIn(expected, destination.last_test_error)
                self.assertNotIn("synthetic-secret-marker", destination.last_test_error)

    @patch("control_plane.tasks.test_transfer_destination_task.apply_async", side_effect=OperationalError("synthetic-secret-marker"))
    def test_broker_failure_marks_test_failed_without_exposing_details(self, publish):
        destination = self.create_destination()
        with self.assertRaises(DeliveryError), self.captureOnCommitCallbacks(execute=True):
            self.client.post(reverse("control_plane:transfer_destination_test", args=[destination.pk]))
        destination.refresh_from_db()
        self.assertEqual(destination.last_test_status, TransferDestination.TestStatus.FAILED)
        self.assertIn("Unable to queue", destination.last_test_error)
        self.assertNotIn("synthetic-secret-marker", destination.last_test_error)

    @patch("control_plane.tasks.sftp_connection")
    def test_connection_setting_edits_invalidate_pending_and_inflight_results(self, connection):
        destination = self.create_destination()
        url = reverse("control_plane:transfer_destination_edit", args=[destination.pk])
        task_id = str(uuid4())
        TransferDestination.objects.filter(pk=destination.pk).update(
            last_test_status=TransferDestination.TestStatus.QUEUED, last_test_task_id=task_id,
        )
        def change_settings(path):
            response = self.client.post(url, self.destination_data(host="changed.example.test", password=""))
            self.assertRedirects(response, self.list_url)
            return Mock(st_mode=stat.S_IFDIR)
        connection.return_value.__enter__.return_value.stat.side_effect = change_settings
        test_transfer_destination_task.apply(args=[destination.pk], task_id=task_id, throw=True)
        destination.refresh_from_db()
        self.assertEqual(destination.last_test_status, TransferDestination.TestStatus.NOT_TESTED)
        self.assertEqual(destination.last_test_task_id, "")
        connection.reset_mock()
        test_transfer_destination_task.apply(args=[destination.pk], task_id=task_id, throw=True)
        connection.assert_not_called()

    @patch("control_plane.tasks.test_transfer_destination_task.apply_async")
    def test_expired_tests_show_failure_and_allow_a_new_attempt(self, publish):
        destination = self.create_destination()
        old_task = str(uuid4())
        TransferDestination.objects.filter(pk=destination.pk).update(
            last_test_status=TransferDestination.TestStatus.RUNNING, last_test_task_id=old_task,
            last_tested_at=timezone.now() - timedelta(minutes=6),
        )
        response = self.client.get(reverse("control_plane:transfer_destination_test_status", args=[destination.pk]))
        self.assertContains(response, "timed out")
        self.assertNotContains(response, "hx-trigger")
        with self.captureOnCommitCallbacks(execute=True):
            self.client.post(reverse("control_plane:transfer_destination_test", args=[destination.pk]))
        destination.refresh_from_db()
        self.assertEqual(destination.last_test_status, TransferDestination.TestStatus.QUEUED)
        self.assertNotEqual(destination.last_test_task_id, old_task)
        publish.assert_called_once()

    @patch("control_plane.tasks.test_transfer_destination_task.apply_async")
    def test_save_and_test_queues_new_then_updated_configuration(self, publish):
        response = self.client.get(self.create_url)
        self.assertContains(response, 'name="save_and_test"')
        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(self.create_url, self.destination_data(save_and_test="true"))
        self.assertRedirects(response, self.list_url)
        destination = TransferDestination.objects.get()
        self.assertEqual(destination.last_test_status, TransferDestination.TestStatus.QUEUED)
        self.assertEqual(publish.call_args.kwargs["args"], [destination.pk])
        initial_task_id = destination.last_test_task_id
        url = reverse("control_plane:transfer_destination_edit", args=[destination.pk])
        response = self.client.get(url)
        self.assertContains(response, 'name="save_and_test"')
        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(url, self.destination_data(
                host="changed.example.test", password="", save_and_test="true",
            ))
        self.assertRedirects(response, self.list_url)
        destination.refresh_from_db()
        self.assertEqual(destination.host, "changed.example.test")
        self.assertEqual(destination.password, " synthetic-password ")
        self.assertEqual(destination.last_test_status, TransferDestination.TestStatus.QUEUED)
        self.assertNotEqual(destination.last_test_task_id, initial_task_id)
        self.assertEqual(publish.call_count, 2)

    @patch("control_plane.views.transfer_destinations.queue_destination_test")
    def test_plain_save_and_invalid_save_and_test_do_not_queue(self, queue_test):
        destination = self.create_destination()
        url = reverse("control_plane:transfer_destination_edit", args=[destination.pk])
        response = self.client.post(url, self.destination_data(name="Plain save", password=""))
        self.assertRedirects(response, self.list_url)
        response = self.client.post(url, self.destination_data(
            name="Invalid save", password="", port="0", save_and_test="true",
        ))
        self.assertEqual(response.status_code, 200)
        destination.refresh_from_db()
        self.assertEqual(destination.name, "Plain save")
        queue_test.assert_not_called()

    @patch("control_plane.views.transfer_destinations.queue_destination_test", side_effect=DeliveryError("Unable to queue the connection test."))
    def test_save_and_test_queue_failure_keeps_saved_configuration(self, queue_test):
        destination = self.create_destination()
        url = reverse("control_plane:transfer_destination_edit", args=[destination.pk])
        response = self.client.post(url, self.destination_data(
            name="Saved despite queue failure", password="", save_and_test="true",
        ), follow=True)
        self.assertContains(response, "Destination saved, but the connection test could not be queued")
        destination.refresh_from_db()
        self.assertEqual(destination.name, "Saved despite queue failure")
        self.assertEqual(destination.password, " synthetic-password ")


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


class CdcToBatchSourceStatusTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_user(username="transition-admin", is_staff=True)
        self.viewer = get_user_model().objects.create_user(username="transition-viewer")
        self.source = SourceProfile.objects.create(
            name="Transition source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="test_db",
            username="test_user", ingestion_mode=IngestionMode.CDC,
        )
        self.table = SourceTable.objects.create(
            source=self.source, schema_name="public", table_name="events",
            primary_key_fields=["id"], cdc_managed=True,
        )
        self.url = reverse("control_plane:source_cdc_to_batch_status", args=[self.source.pk])
        self.staging_url = reverse("control_plane:source_cdc_to_batch_staging", args=[self.source.pk])
        self.client.force_login(self.admin)

    def test_edit_page_offers_staging_status_not_a_switch(self):
        response = self.client.get(reverse("control_plane:source_edit", args=[self.source.pk]))
        self.assertContains(response, "Switch to batch (not available)")
        self.assertContains(response, "No CDC-to-batch transition")
        self.assertContains(response, self.staging_url)
        self.assertContains(self.client.get(self.url), "No CDC-to-batch transition")
        self.assertEqual(self.client.post(self.url).status_code, 405)
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)

    def test_pending_cutover_blocks_source_edit_toggle_and_delete(self):
        CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.CUTOVER_PENDING
        )
        for name, method in (
            ("source_edit", self.client.post),
            ("source_toggle_active", self.client.get),
            ("source_delete", self.client.post),
        ):
            with self.subTest(name=name):
                response = method(reverse(f"control_plane:{name}", args=[self.source.pk]), follow=True)
                self.assertContains(
                    response,
                    "Source metadata cannot be changed while CDC-to-batch cutover is pending.",
                )
                self.source.refresh_from_db()
                self.assertTrue(self.source.is_active)
                self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)

    def test_pending_cutover_blocks_django_admin_metadata_mutations(self):
        admin = get_user_model().objects.create_superuser(
            username="transition-superuser", password="test-password"
        )
        self.client.force_login(admin)
        CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.CUTOVER_PENDING
        )
        cases = [
            ("admin:control_plane_sourceprofile_change", [self.source.pk], {}),
            ("admin:control_plane_sourceprofile_delete", [self.source.pk], {"post": "yes"}),
            ("admin:control_plane_sourcetable_change", [self.table.pk], {}),
            ("admin:control_plane_sourcetable_delete", [self.table.pk], {"post": "yes"}),
            ("admin:control_plane_sourceprofile_changelist", [], {
                "action": "delete_selected", "_selected_action": str(self.source.pk),
            }),
            ("admin:control_plane_sourcetable_changelist", [], {
                "action": "delete_selected", "_selected_action": str(self.table.pk),
            }),
        ]
        for name, args, data in cases:
            with self.subTest(name=name):
                response = self.client.post(reverse(name, args=args), data, follow=True)
                self.assertContains(response, "cutover is pending")
                self.source.refresh_from_db()
                self.table.refresh_from_db()
                self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)

    def test_django_admin_existing_source_mode_is_read_only(self):
        admin = get_user_model().objects.create_superuser(
            username="mode-superuser", password="test-password"
        )
        self.client.force_login(admin)
        response = self.client.get(reverse("admin:control_plane_sourceprofile_change", args=[self.source.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, 'name="ingestion_mode"')
        self.assertContains(response, "Change data capture (CDC)")

    def test_django_admin_rejects_select_across_for_guarded_sources(self):
        admin = get_user_model().objects.create_superuser(
            username="selection-superuser", password="test-password"
        )
        self.client.force_login(admin)
        response = self.client.post(
            reverse("admin:control_plane_sourceprofile_changelist"),
            {"action": "delete_selected", "_selected_action": str(self.source.pk), "select_across": "1"},
            follow=True,
        )
        self.assertContains(response, "Select the source records explicitly for this action.")
        self.assertTrue(SourceProfile.objects.filter(pk=self.source.pk).exists())

    def test_status_distinguishes_staged_tables_from_verified_staging(self):
        transition = CdcToBatchTransition.objects.create(source=self.source)
        CdcToBatchTableCheckpoint.objects.create(
            transition=transition, source_table=self.table, staging_table="internal-stage",
            status=CdcToBatchTableCheckpoint.Status.STAGED,
        )
        response = self.client.get(self.url)
        self.assertContains(response, "Staging verification has not completed.")
        self.assertNotContains(response, "Staging last verified:")
        transition.staging_verified_at = timezone.now()
        transition.save(update_fields=["staging_verified_at"])
        response = self.client.get(self.url)
        self.assertContains(response, "Staging last verified:")
        self.assertContains(response, "Revalidation is required before cutover.")

    def test_status_explains_legacy_staging_requires_restaging(self):
        transition = CdcToBatchTransition.objects.create(
            source=self.source, staging_verified_at=timezone.now()
        )
        checkpoint = CdcToBatchTableCheckpoint.objects.create(
            transition=transition, source_table=self.table, staging_table="old-stage",
            status=CdcToBatchTableCheckpoint.Status.STAGED, staging_schema={"username": "test_user"},
        )
        response = self.client.get(self.url)
        self.assertContains(response, "1 staged table must be restaged")
        self.assertContains(response, "Retry staging to create fresh tables")
        checkpoint.staging_schema["synchronous_inserts"] = True
        checkpoint.save(update_fields=["staging_schema"])
        self.assertNotContains(self.client.get(self.url), "must be restaged")

    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_admin_can_prepare_and_queue_staging_only(self, enqueue):
        response = self.client.post(
            self.staging_url, {"action": "start", "confirm": "yes"}, follow=True
        )
        self.assertContains(response, "Batch staging queued")
        transition = CdcToBatchTransition.objects.get(source=self.source)
        self.assertEqual(transition.status, CdcToBatchTransition.Status.PREPARING)
        self.assertEqual(transition.table_checkpoints.count(), 1)
        enqueue.assert_called_once_with(args=[transition.pk], queue="queue_backfill")
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.assertContains(self.client.get(self.url), "Retry staging")

    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_repeated_start_is_rejected_but_retry_uses_existing_transition(self, enqueue):
        self.client.post(self.staging_url, {"action": "start", "confirm": "yes"})
        response = self.client.post(
            self.staging_url, {"action": "start", "confirm": "yes"}, follow=True
        )
        self.assertContains(response, "A transition is already active")
        self.assertEqual(enqueue.call_count, 1)
        transition = CdcToBatchTransition.objects.get(source=self.source)
        self.client.post(self.staging_url, {
            "action": "retry", "confirm": "yes", "transition": str(transition.generation),
        })
        self.assertEqual(enqueue.call_count, 2)
        self.assertEqual(enqueue.call_args.kwargs["args"], [transition.pk])

    @patch("control_plane.views.cdc_transition.logger")
    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_publish_failure_retains_transition_and_displays_error(self, enqueue, logger):
        enqueue.side_effect = RuntimeError("sensitive broker details")
        response = self.client.post(
            self.staging_url, {"action": "start", "confirm": "yes"}, follow=True
        )
        self.assertContains(response, "Staging dispatch could not be confirmed")
        self.assertNotContains(response, "sensitive broker details")
        transition = CdcToBatchTransition.objects.get(source=self.source)
        self.assertIn("dispatch could not be confirmed", transition.last_error)
        self.assertEqual(transition.status, CdcToBatchTransition.Status.PREPARING)
        self.assertEqual(self.client.get(self.url).status_code, 200)
        self.assertContains(self.client.get(self.url), "Retry staging")
        logger.exception.assert_called_once()

    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_cancel_staging_retains_checkpoints_and_allows_later_start(self, enqueue):
        self.client.post(self.staging_url, {"action": "start", "confirm": "yes"})
        old = CdcToBatchTransition.objects.get(source=self.source)
        response = self.client.post(
            self.staging_url, {
                "action": "cancel", "confirm": "yes", "transition": str(old.generation),
            }, follow=True
        )
        self.assertContains(response, "Staging cancelled")
        old.refresh_from_db()
        self.assertEqual(old.status, CdcToBatchTransition.Status.FAILED)
        self.assertEqual(old.table_checkpoints.count(), 1)
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.client.post(self.staging_url, {"action": "start", "confirm": "yes"})
        self.assertEqual(self.source.cdc_to_batch_transitions.count(), 2)
        self.assertEqual(enqueue.call_count, 2)

    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_stale_confirmation_cannot_cancel_or_retry_a_new_transition(self, enqueue):
        self.client.post(self.staging_url, {"action": "start", "confirm": "yes"})
        old = CdcToBatchTransition.objects.get(source=self.source)
        self.client.post(self.staging_url, {
            "action": "cancel", "confirm": "yes", "transition": str(old.generation),
        })
        self.client.post(self.staging_url, {"action": "start", "confirm": "yes"})
        current = self.source.cdc_to_batch_transitions.get(status=CdcToBatchTransition.Status.PREPARING)
        for action in ("retry", "cancel"):
            response = self.client.post(self.staging_url, {
                "action": action, "confirm": "yes", "transition": str(old.generation),
            }, follow=True)
            self.assertContains(response, "The staging transition changed.")
        current.refresh_from_db()
        self.assertEqual(current.status, CdcToBatchTransition.Status.PREPARING)
        self.assertEqual(enqueue.call_count, 2)

    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_missing_or_invalid_transition_cannot_retry_or_cancel(self, enqueue):
        transition = CdcToBatchTransition.objects.create(source=self.source)
        for action in ("retry", "cancel"):
            for value in ("", "not-a-uuid"):
                response = self.client.post(self.staging_url, {
                    "action": action, "confirm": "yes", "transition": value,
                }, follow=True)
                self.assertContains(response, "The staging transition changed or is missing.")
        transition.refresh_from_db()
        self.assertEqual(transition.status, CdcToBatchTransition.Status.PREPARING)
        enqueue.assert_not_called()

    @patch("control_plane.views.cdc_transition._clickhouse_table_uuid", return_value="old-uuid")
    @patch("control_plane.views.cdc_transition.clickhouse_connect.get_client")
    def test_abandon_pending_retains_resources_and_source_mode(self, get_client, table_uuid):
        transition = CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.CUTOVER_PENDING,
            staging_verified_at=timezone.now(),
        )
        checkpoint = CdcToBatchTableCheckpoint.objects.create(
            transition=transition, source_table=self.table, staging_table="stage",
            status=CdcToBatchTableCheckpoint.Status.STAGED,
            active_table_checked=True, active_uuid_before="old-uuid",
            generation_table="pending-copy",
        )
        data = {"action": "abandon_pending", "transition": str(transition.generation)}
        response = self.client.post(self.staging_url, data)
        self.assertContains(response, "Abandon the pending cutover")
        self.assertContains(response, f'name="transition" value="{transition.generation}"')
        response = self.client.post(self.staging_url, {**data, "confirm": "yes"}, follow=True)
        self.assertContains(response, "Pending cutover abandoned")
        transition.refresh_from_db()
        checkpoint.refresh_from_db()
        self.source.refresh_from_db()
        self.assertEqual(transition.status, CdcToBatchTransition.Status.FAILED)
        self.assertIsNone(transition.staging_verified_at)
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.assertEqual(checkpoint.generation_table, "pending-copy")
        get_client.return_value.close.assert_called_once()

    @patch("control_plane.views.cdc_transition._clickhouse_table_uuid", return_value="changed")
    @patch("control_plane.views.cdc_transition.clickhouse_connect.get_client")
    def test_abandon_pending_refuses_exchanged_or_uncertain_tables(self, get_client, table_uuid):
        transition = CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.CUTOVER_PENDING,
        )
        checkpoint = CdcToBatchTableCheckpoint.objects.create(
            transition=transition, source_table=self.table, staging_table="stage",
            status=CdcToBatchTableCheckpoint.Status.STAGED,
            active_table_checked=True, active_uuid_before="old-uuid",
        )
        data = {
            "action": "abandon_pending", "transition": str(transition.generation), "confirm": "yes",
        }
        response = self.client.post(self.staging_url, data, follow=True)
        self.assertContains(response, "an active table changed")
        for status, inflight, checked in (
            (CdcToBatchTableCheckpoint.Status.STAGED, True, True),
            (CdcToBatchTableCheckpoint.Status.CUTOVER, False, True),
            (CdcToBatchTableCheckpoint.Status.STAGED, False, False),
        ):
            checkpoint.status, checkpoint.cutover_command_inflight, checkpoint.active_table_checked = (
                status, inflight, checked
            )
            checkpoint.save(update_fields=["status", "cutover_command_inflight", "active_table_checked"])
            response = self.client.post(self.staging_url, data, follow=True)
            self.assertContains(response, "a table exchange may have occurred")
        transition.refresh_from_db()
        self.assertEqual(transition.status, CdcToBatchTransition.Status.CUTOVER_PENDING)

    @patch("control_plane.views.cdc_transition.logger")
    @patch("control_plane.views.cdc_transition.clickhouse_connect.get_client", side_effect=RuntimeError("secret"))
    def test_abandon_pending_fails_closed_when_clickhouse_unavailable(self, get_client, logger):
        transition = CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.CUTOVER_PENDING,
        )
        CdcToBatchTableCheckpoint.objects.create(
            transition=transition, source_table=self.table, staging_table="stage",
            status=CdcToBatchTableCheckpoint.Status.STAGED,
            active_table_checked=True, active_uuid_before="old-uuid",
        )
        response = self.client.post(self.staging_url, {
            "action": "abandon_pending", "transition": str(transition.generation), "confirm": "yes",
        }, follow=True)
        self.assertContains(response, "Could not verify active tables")
        self.assertNotContains(response, "secret")
        transition.refresh_from_db()
        self.assertEqual(transition.status, CdcToBatchTransition.Status.CUTOVER_PENDING)
        logger.exception.assert_called_once()

    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_pending_cutover_cannot_be_cancelled_retried_or_restarted(self, enqueue):
        transition = CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.CUTOVER_PENDING
        )
        for action in ("start", "retry", "cancel"):
            self.client.post(self.staging_url, {
                "action": action, "confirm": "yes", "transition": str(transition.generation),
            })
        transition.refresh_from_db()
        self.assertEqual(transition.status, CdcToBatchTransition.Status.CUTOVER_PENDING)
        enqueue.assert_not_called()

    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_confirmation_permissions_and_post_are_required(self, enqueue):
        self.assertEqual(self.client.get(self.staging_url).status_code, 405)
        for action in ("start", "retry", "cancel"):
            generation = uuid4()
            response = self.client.post(self.staging_url, {
                "action": action, "transition": str(generation),
            })
            self.assertContains(response, "Confirm staging action")
            self.assertContains(response, f'name="action" value="{action}"')
            self.assertContains(response, 'name="confirm" value="yes"')
            if action != "start":
                self.assertContains(response, f'name="transition" value="{generation}"')
        self.assertFalse(CdcToBatchTransition.objects.exists())
        self.client.force_login(self.viewer)
        self.assertEqual(
            self.client.post(self.staging_url, {"action": "start", "confirm": "yes"}).status_code,
            302,
        )
        self.assertFalse(CdcToBatchTransition.objects.exists())
        enqueue.assert_not_called()

    def test_edit_page_renders_status_and_confirmation_forms_without_javascript(self):
        response = self.client.get(reverse("control_plane:source_edit", args=[self.source.pk]))
        self.assertContains(response, "No CDC-to-batch transition")
        self.assertContains(response, 'name="action" value="start"')
        self.assertNotContains(response, 'name="confirm" value="yes"')
        self.assertNotContains(response, "onsubmit=")

        CdcToBatchTransition.objects.create(source=self.source)
        response = self.client.get(reverse("control_plane:source_edit", args=[self.source.pk]))
        self.assertContains(response, "Preparing")
        self.assertContains(response, 'name="action" value="retry"')
        self.assertContains(response, 'name="action" value="cancel"')
        self.assertContains(response, 'hx-trigger="every 5s"')

    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_ineligible_source_does_not_queue(self, enqueue):
        self.source.is_active = False
        self.source.save(update_fields=["is_active"])
        response = self.client.post(
            self.staging_url, {"action": "start", "confirm": "yes"}, follow=True
        )
        self.assertContains(response, "requires an active relational CDC source")
        self.assertFalse(CdcToBatchTransition.objects.exists())
        enqueue.assert_not_called()

    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_active_rebuild_prevents_staging(self, enqueue):
        CdcTableRebuild.objects.create(
            source_table=self.table, connector_name="active-connector",
            topic_prefix="active-prefix", notification_topic="active-notification",
            consumer_group="active-group", staging_table="active-stage",
        )
        response = self.client.post(
            self.staging_url, {"action": "start", "confirm": "yes"}, follow=True
        )
        self.assertContains(response, "Finish or resolve active CDC table rebuilds")
        self.assertFalse(CdcToBatchTransition.objects.exists())
        enqueue.assert_not_called()

    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_csrf_is_required(self, enqueue):
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.admin)
        self.assertEqual(
            client.post(self.staging_url, {"action": "start", "confirm": "yes"}).status_code,
            403,
        )
        enqueue.assert_not_called()

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.cdc_to_batch.stage_cdc_to_batch_table")
    @patch("control_plane.views.cdc_transition.stage_cdc_to_batch_transition_task.apply_async")
    def test_queued_staging_task_after_cancellation_cannot_stage(self, enqueue, stage, logger):
        self.client.post(self.staging_url, {"action": "start", "confirm": "yes"})
        transition = CdcToBatchTransition.objects.get(source=self.source)
        self.client.post(self.staging_url, {
            "action": "cancel", "confirm": "yes", "transition": str(transition.generation),
        })
        with self.assertRaisesRegex(RuntimeError, "not eligible for batch staging"):
            stage_cdc_to_batch_transition_task.apply(args=[transition.pk], throw=True)
        stage.assert_not_called()
        transition.refresh_from_db()
        self.assertEqual(transition.status, CdcToBatchTransition.Status.FAILED)
        self.assertIn("cancelled", transition.last_error)

    @override_settings(
        CDC_TO_BATCH_EXECUTOR_ENABLED=True,
        CDC_TO_BATCH_GENERATION_EXECUTOR_ENABLED=True,
        CDC_EXTERNAL_OPERATION_TRACKING=True,
    )
    def test_experimental_executor_flags_do_not_enable_source_page_switch(self):
        response = self.client.get(reverse("control_plane:source_edit", args=[self.source.pk]))
        self.assertContains(response, "Switch to batch (not available)")
        self.assertEqual(self.client.post(self.url).status_code, 405)

    def test_status_shows_latest_transition(self):
        CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.FAILED,
            last_error="older error",
        )
        CdcToBatchTransition.objects.create(source=self.source)
        response = self.client.get(self.url)
        self.assertContains(response, "Preparing")
        self.assertNotContains(response, "older error")

    def test_status_shows_progress_and_sanitized_error_without_exposing_other_sources(self):
        transition = CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.CUTOVER_PENDING,
            last_error="<unsafe> transition failed",
        )
        CdcToBatchTableCheckpoint.objects.create(
            transition=transition, source_table=self.table, staging_table="internal-stage",
            status=CdcToBatchTableCheckpoint.Status.STAGED, cutover_command_inflight=True,
        )
        response = self.client.get(self.url)
        self.assertContains(response, "Cutover pending")
        self.assertContains(response, "Staged: 1 / 1 tables; cut over: 0 / 1 tables.")
        self.assertContains(response, "public.events")
        self.assertContains(response, "command outcome uncertain")
        self.assertContains(response, "&lt;unsafe&gt; transition failed", html=False)
        self.assertContains(response, "Reports for this source are blocked")
        self.assertContains(response, 'hx-trigger="every 5s"')
        self.assertNotContains(response, "internal-stage")

    def test_status_shows_generation_progress_and_unresolved_operations(self):
        transition = CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.CUTOVER_PENDING,
        )
        checkpoint = CdcToBatchTableCheckpoint.objects.create(
            transition=transition, source_table=self.table, staging_table="internal-stage",
            status=CdcToBatchTableCheckpoint.Status.STAGED,
            generation_table="private-generation",
        )
        CdcSourceExternalOperation.objects.create(
            source=self.source, kind=CdcSourceExternalOperation.Kind.INGESTION,
            task_id="worker-task-123",
        )
        response = self.client.get(self.url)
        self.assertContains(response, "Generation copies verified: 0 / 1 tables.")
        self.assertContains(response, "copy started; completion unconfirmed")
        self.assertContains(response, "active or have an unresolved outcome")
        self.assertContains(response, "CDC ingestion")
        self.assertContains(response, "worker-task-123")
        self.assertNotContains(response, "private-generation")
        checkpoint.generation_uuid = "generation-uuid"
        checkpoint.save(update_fields=["generation_uuid"])
        response = self.client.get(self.url)
        self.assertContains(response, "Generation copies verified: 1 / 1 tables.")
        self.assertNotContains(response, "copy started; completion unconfirmed")

    def test_completed_transition_is_visible_for_batch_source_without_polling(self):
        transition = CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.SUCCEEDED,
        )
        CdcToBatchTableCheckpoint.objects.create(
            transition=transition, source_table=self.table, staging_table="internal-stage",
            status=CdcToBatchTableCheckpoint.Status.CUTOVER,
        )
        self.source.ingestion_mode = IngestionMode.BATCH
        self.source.save(update_fields=["ingestion_mode"])
        self.assertContains(
            self.client.get(reverse("control_plane:source_edit", args=[self.source.pk])),
            "CDC to batch transition",
        )
        response = self.client.get(self.url)
        self.assertContains(response, "Succeeded")
        self.assertContains(response, "cut over: 1 / 1 tables")
        self.assertNotContains(response, 'hx-trigger="every 5s"')

    def test_regular_batch_source_has_no_transition_panel(self):
        self.source.ingestion_mode = IngestionMode.BATCH
        self.source.save(update_fields=["ingestion_mode"])
        response = self.client.get(reverse("control_plane:source_edit", args=[self.source.pk]))
        self.assertNotContains(response, "CDC to batch transition")

    def test_status_requires_admin_and_existing_source(self):
        self.client.force_login(self.viewer)
        self.assertEqual(self.client.get(self.url).status_code, 302)
        self.client.logout()
        self.assertEqual(self.client.get(self.url).status_code, 302)
        self.client.force_login(self.admin)
        self.assertEqual(
            self.client.get(reverse("control_plane:source_cdc_to_batch_status", args=[999999])).status_code,
            404,
        )


@skipUnlessDBFeature("has_select_for_update_nowait")
class CdcToBatchCancellationConcurrencyTests(TransactionTestCase):
    def test_busy_staging_checkpoint_cannot_be_cancelled(self):
        admin = get_user_model().objects.create_user(username="cancel-admin", is_staff=True)
        source = SourceProfile.objects.create(
            name="Busy staging source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="test_db",
            username="test_user", ingestion_mode=IngestionMode.CDC,
        )
        table = SourceTable.objects.create(
            source=source, schema_name="public", table_name="events"
        )
        transition = CdcToBatchTransition.objects.create(source=source)
        checkpoint = CdcToBatchTableCheckpoint.objects.create(
            transition=transition, source_table=table, staging_table="busy-stage"
        )
        self.client.force_login(admin)

        def cancel():
            close_old_connections()
            try:
                return self.client.post(
                    reverse("control_plane:source_cdc_to_batch_staging", args=[source.pk]),
                    {"action": "cancel", "confirm": "yes", "transition": str(transition.generation)},
                )
            finally:
                close_old_connections()

        with patch("control_plane.views.cdc_transition.logger"), \
             ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                CdcToBatchTableCheckpoint.objects.select_for_update().get(pk=checkpoint.pk)
                self.assertEqual(pool.submit(cancel).result(timeout=10).status_code, 302)
                transition.refresh_from_db()
                self.assertEqual(transition.status, CdcToBatchTransition.Status.PREPARING)
        transition.refresh_from_db()
        self.assertEqual(transition.status, CdcToBatchTransition.Status.PREPARING)


@override_settings(CACHES={
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}
})
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

    def test_rebuild_start_is_not_available_as_an_admin_action(self):
        response = self.client.get(
            reverse("admin:control_plane_sourcetable_changelist")
        )

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Safely rebuild selected CDC warehouse tables")

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


@override_settings(CACHES={
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}
})
class CdcRebuildStartViewTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_user(
            username="cdc-start-admin", is_staff=True
        )
        self.client.force_login(self.admin)
        self.source = SourceProfile.objects.create(
            name="CDC rebuild start source",
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
        self.url = reverse("control_plane:cdc_rebuild_start", args=[self.table.pk])

    def test_confirmation_page_describes_the_selected_table(self):
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Confirm CDC table rebuild")
        self.assertContains(response, "public.events")
        self.assertContains(response, "does not clean up old CDC resources")

    def test_confirmed_start_queues_the_rebuild_on_the_cdc_queue(self):
        with patch(
            "control_plane.views.cdc_rebuilds.rebuild_cdc_table_task.apply_async"
        ) as apply_async:
            response = self.client.post(self.url, {"confirm": "yes"})
            rebuild = CdcTableRebuild.objects.get(source_table=self.table)

        self.assertRedirects(response, reverse("control_plane:cdc_rebuild_list"))
        apply_async.assert_called_once_with(
            args=[self.table.pk, rebuild.pk],
            queue="queue_cdc",
        )

    def test_duplicate_start_is_blocked_after_generation_reservation(self):
        with patch(
            "control_plane.views.cdc_rebuilds.rebuild_cdc_table_task.apply_async"
        ) as apply_async:
            self.client.post(self.url, {"confirm": "yes"})
            response = self.client.post(
                self.url,
                {"confirm": "yes"},
                follow=True,
            )

        self.assertContains(response, "already in progress or pending cutover")
        self.assertEqual(CdcTableRebuild.objects.filter(source_table=self.table).count(), 1)
        apply_async.assert_called_once()

    def test_queue_failure_marks_the_reserved_generation_failed(self):
        with patch(
            "control_plane.views.cdc_rebuilds.rebuild_cdc_table_task.apply_async",
            side_effect=RuntimeError("broker unavailable"),
        ):
            response = self.client.post(
                self.url,
                {"confirm": "yes"},
                follow=True,
            )

        self.assertContains(response, "The CDC rebuild could not be queued")
        rebuild = CdcTableRebuild.objects.get(source_table=self.table)
        self.assertEqual(rebuild.status, CdcTableRebuild.Status.FAILED)
        self.assertEqual(
            rebuild.last_error,
            "Could not queue the initial CDC rebuild task.",
        )

    def test_start_requires_confirmation(self):
        with patch(
            "control_plane.views.cdc_rebuilds.rebuild_cdc_table_task.apply_async"
        ) as apply_async:
            response = self.client.post(self.url)

        self.assertRedirects(response, reverse("control_plane:cdc_rebuild_list"))
        apply_async.assert_not_called()

    def test_post_rechecks_for_an_active_transition(self):
        CdcToBatchTransition.objects.create(source=self.source)

        with patch(
            "control_plane.views.cdc_rebuilds.rebuild_cdc_table_task.apply_async"
        ) as apply_async:
            response = self.client.post(
                self.url,
                {"confirm": "yes"},
                follow=True,
            )

        self.assertContains(response, "A CDC-to-batch transition is active")
        apply_async.assert_not_called()

    def test_post_rechecks_source_activity_after_confirmation(self):
        self.assertEqual(self.client.get(self.url).status_code, 200)
        self.source.is_active = False
        self.source.save(update_fields=["is_active"])

        with patch(
            "control_plane.views.cdc_rebuilds.rebuild_cdc_table_task.apply_async"
        ) as apply_async:
            response = self.client.post(
                self.url,
                {"confirm": "yes"},
                follow=True,
            )

        self.assertContains(response, "source must be active")
        apply_async.assert_not_called()

    def test_start_is_blocked_while_a_rebuild_is_active(self):
        CdcTableRebuild.objects.create(
            source_table=self.table,
            connector_name="start-active-generation",
            topic_prefix="start-active-generation",
            notification_topic="start-active-generation.notifications",
            consumer_group="start-active-generation-group",
            staging_table="source_events_start_active",
            status=CdcTableRebuild.Status.SNAPSHOTTING,
        )

        response = self.client.get(self.url, follow=True)

        self.assertContains(response, "already in progress or pending cutover")

    def test_source_admin_can_start_but_viewer_cannot(self):
        source_admin = get_user_model().objects.create_user(
            username="cdc-start-source-admin"
        )
        UserSourceAccess.objects.create(
            user=source_admin,
            source=self.source,
            role=Role.ADMIN,
        )
        self.client.force_login(source_admin)
        response = self.client.get(reverse("control_plane:cdc_rebuild_list"))
        self.assertContains(response, self.url)
        response = self.client.get(self.url)
        self.assertContains(
            response,
            reverse("control_plane:cdc_rebuild_list"),
        )

        with patch(
            "control_plane.views.cdc_rebuilds.rebuild_cdc_table_task.apply_async"
        ) as apply_async:
            response = self.client.post(self.url, {"confirm": "yes"})
            rebuild = CdcTableRebuild.objects.get(source_table=self.table)

        self.assertRedirects(response, reverse("control_plane:cdc_rebuild_list"))
        apply_async.assert_called_once_with(
            args=[self.table.pk, rebuild.pk],
            queue="queue_cdc",
        )

        viewer = get_user_model().objects.create_user(username="cdc-start-viewer")
        UserSourceAccess.objects.create(
            user=viewer,
            source=self.source,
            role=Role.VIEWER,
        )
        self.client.force_login(viewer)
        self.assertEqual(self.client.get(self.url).status_code, 404)


class CdcRebuildMonitorTests(TestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Monitored CDC source",
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
        self.rebuild = CdcTableRebuild.objects.create(
            source_table=self.table,
            connector_name="monitor-generation",
            topic_prefix="monitor-generation",
            notification_topic="monitor-generation.notifications",
            consumer_group="monitor-generation-group",
            staging_table="source_events_monitor_generation",
            status=CdcTableRebuild.Status.CUTOVER_PENDING,
            last_error="Cutover needs attention",
        )
        self.url = reverse("control_plane:cdc_rebuild_list")

    def test_staff_monitor_shows_rebuild_attention_and_resume_action(self):
        admin = get_user_model().objects.create_user(
            username="rebuild-monitor-staff", is_staff=True
        )
        self.client.force_login(admin)

        response = self.client.get(self.url)
        self.assertContains(response, self.source.name)
        self.assertContains(response, "public.events")
        self.assertContains(response, "Cutover needs attention")
        self.assertContains(response, "Resume cutover")
        self.assertContains(response, "CDC Table Rebuilds")
        self.assertContains(response, self.url)

    def test_source_access_limits_visibility_and_admin_role_controls_resume(self):
        other_source = SourceProfile.objects.create(
            name="Unassigned CDC source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="other_db",
            username="test_user",
            ingestion_mode=IngestionMode.CDC,
        )
        other_table = SourceTable.objects.create(
            source=other_source,
            schema_name="public",
            table_name="private_events",
            primary_key_fields=["id"],
            cdc_managed=True,
        )
        CdcTableRebuild.objects.create(
            source_table=other_table,
            connector_name="other-monitor-generation",
            topic_prefix="other-monitor-generation",
            notification_topic="other-monitor-generation.notifications",
            consumer_group="other-monitor-generation-group",
            staging_table="source_private_events_monitor_generation",
            status=CdcTableRebuild.Status.CUTOVER_PENDING,
        )
        admin = get_user_model().objects.create_user(username="source-rebuild-admin")
        UserSourceAccess.objects.create(
            user=admin, source=self.source, role=Role.ADMIN
        )
        self.client.force_login(admin)

        response = self.client.get(self.url)
        self.assertContains(response, self.source.name)
        self.assertNotContains(response, other_source.name)
        self.assertContains(response, "Resume cutover")
        self.assertContains(response, reverse("control_plane:cdc_rebuild_list"))

        with patch("control_plane.views.cdc_rebuilds.rebuild_cdc_table_task.apply_async") as apply_async:
            response = self.client.post(
                reverse("control_plane:cdc_rebuild_resume", args=[self.rebuild.pk])
            )

        self.assertRedirects(response, self.url)
        apply_async.assert_called_once_with(
            args=[self.table.pk, self.rebuild.pk],
            queue="queue_cdc",
        )

    def test_viewer_can_monitor_but_cannot_resume(self):
        viewer = get_user_model().objects.create_user(username="source-rebuild-viewer")
        UserSourceAccess.objects.create(
            user=viewer, source=self.source, role=Role.VIEWER
        )
        self.client.force_login(viewer)

        response = self.client.get(self.url)
        self.assertContains(response, self.source.name)
        self.assertContains(response, "CDC Rebuilds")
        self.assertNotContains(response, "Resume cutover")

        with patch("control_plane.views.cdc_rebuilds.rebuild_cdc_table_task.apply_async") as apply_async:
            response = self.client.post(
                reverse("control_plane:cdc_rebuild_resume", args=[self.rebuild.pk]),
                follow=True,
            )

        self.assertContains(response, "not authorized to resume rebuilds")
        apply_async.assert_not_called()

    def test_unassigned_user_cannot_view_or_resume_another_sources_rebuild(self):
        user = get_user_model().objects.create_user(username="unassigned-rebuild-user")
        self.client.force_login(user)

        response = self.client.get(self.url)
        self.assertNotContains(response, self.source.name)
        self.assertEqual(
            self.client.post(
                reverse("control_plane:cdc_rebuild_resume", args=[self.rebuild.pk])
            ).status_code,
            404,
        )

    def test_only_cutover_pending_rebuilds_can_be_resumed(self):
        admin = get_user_model().objects.create_superuser(
            username="rebuild-monitor-superuser", password="test-password"
        )
        self.client.force_login(admin)
        self.rebuild.status = CdcTableRebuild.Status.SNAPSHOTTING
        self.rebuild.save(update_fields=["status"])

        with patch("control_plane.views.cdc_rebuilds.rebuild_cdc_table_task.apply_async") as apply_async:
            response = self.client.post(
                reverse("control_plane:cdc_rebuild_resume", args=[self.rebuild.pk]),
                follow=True,
            )

        self.assertContains(response, "Only CDC rebuilds pending cutover")
        apply_async.assert_not_called()


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
            {"name": "Pipeline History", "url": ""},
        ])

    def test_cdc_history_displays_persisted_failures(self):
        response = self.client.get(reverse("control_plane:cdc_failure_history"))
        self.assertContains(response, "CDC warehouse source")
        self.assertContains(response, "cdc-task-123")
        self.assertContains(response, "RuntimeError: CDC execution failed.")
        self.assertEqual(response.context["breadcrumbs"], [
            {"name": "Dashboard", "url": reverse("control_plane:dashboard")},
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
            'border-indigo-500 text-white">\n                            <span>History</span>',
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
        audit_url = reverse("control_plane:email_audit_logs")
        for status in ("failed", "bounced", "sent", "delivered"):
            self.assertContains(response, f'href="{audit_url}?status={status}"')
        self.assertContains(response, 'aria-label="Email delivery status filters"')
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
                self.assertNotContains(response, "Failed / Bounced Emails")
                self.assertNotContains(response, "toggleEmailAuditWidget(")
                self.assertEqual(
                    self.client.get(reverse("control_plane:email_audit_logs")).status_code,
                    302,
                )

    @patch("control_plane.views.dashboard.get_redis_client")
    def test_email_audit_filters_failures_without_duplicate_widget(self, redis_client):
        redis_client.return_value.keys.return_value = []
        for number in range(6):
            EmailAuditLog.objects.create(
                recipients=f"failure-{number}@example.com",
                status="failed" if number % 2 else "bounced",
            )
        EmailAuditLog.objects.create(recipients="sent@example.com", status="sent")
        EmailAuditLog.objects.create(recipients="delivered@example.com", status="delivered")
        EmailAuditLog.objects.update(created_at=timezone.now())
        response = self.client.get(reverse("control_plane:dashboard"))
        self.assertNotContains(response, "Failed / Bounced Emails")
        self.assertContains(response, "Recent Email Deliveries")
        audit_url = reverse("control_plane:email_audit_logs")
        for status in ("failed", "bounced", "sent", "delivered"):
            self.assertContains(response, f'href="{audit_url}?status={status}"')
            filtered = self.client.get(audit_url, {"status": status})
            self.assertEqual(
                {log.status for log in filtered.context["page_obj"]},
                {status},
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
            {"name": "Email Audit Logs", "url": ""},
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

    def test_breadcrumbs_link_to_the_dashboard(self):
        response = self.client.get(reverse("control_plane:active_pipeline_executions"))
        self.assertEqual(response.context["breadcrumbs"], [
            {"name": "Dashboard", "url": reverse("control_plane:dashboard")},
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

    def test_breadcrumbs_link_to_the_dashboard(self):
        response = self.client.get(reverse("control_plane:active_report_generations"))
        self.assertEqual(response.context["breadcrumbs"], [
            {"name": "Dashboard", "url": reverse("control_plane:dashboard")},
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

    def test_pending_cutover_rejects_warehouse_selection_without_changes(self):
        CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.CUTOVER_PENDING
        )
        response = self.client.post(
            reverse("control_plane:warehouse_field_selection", args=[self.source.pk]),
            {"selected_fields": ["events.event_id"]},
            follow=True,
        )
        self.assertContains(response, "Source metadata cannot be changed while CDC-to-batch cutover is pending.")
        self.assertFalse(self.source.warehouse_mappings.exists())

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

    def test_field_page_links_to_rebuild_only_for_eligible_cdc_tables(self):
        self.source.ingestion_mode = IngestionMode.CDC
        self.source.save(update_fields=["ingestion_mode"])
        self.table.cdc_managed = True
        self.table.save(update_fields=["cdc_managed"])
        rebuild_url = reverse("control_plane:cdc_rebuild_start", args=[self.table.pk])
        field_page_url = reverse(
            "control_plane:warehouse_field_selection",
            args=[self.source.pk],
        )

        response = self.client.get(field_page_url)

        self.assertContains(response, rebuild_url)
        self.table.primary_key_fields = []
        self.table.save(update_fields=["primary_key_fields"])

        response = self.client.get(field_page_url)

        self.assertNotContains(response, rebuild_url)

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
