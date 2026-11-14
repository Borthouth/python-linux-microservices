from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from control_plane.models import (
    CdcResourceCleanup,
    CdcResourceCleanupItem,
    CdcTableRebuild,
    DbChoice,
    IngestionMode,
    SourceProfile,
    SourceTable,
)
from control_plane.tasks import _claim_cleanup_task


class CdcResourceCleanupViewTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_user(
            username="cleanup-admin",
            password="test-password",
            is_staff=True,
        )
        self.client.force_login(self.admin)
        self.source = SourceProfile.objects.create(
            name="Cleanup source",
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
            connector_name="reporthub-source-1-table-2-deadbeef0001",
            topic_prefix="reporthub-source-1-table-2-deadbeef0001",
            notification_topic=(
                "reporthub-source-1-table-2-deadbeef0001.notifications"
            ),
            consumer_group=(
                "reporthub-source-1-table-2-deadbeef0001-rebuild"
            ),
            staging_table="source_events__rebuild_deadbeef0001",
            status=CdcTableRebuild.Status.FAILED,
        )

    def test_inventory_is_queued_before_any_cleanup_confirmation(self):
        with patch(
            "control_plane.views.cdc_cleanup.scan_cdc_resources_task.apply_async"
        ) as apply_async:
            response = self.client.post(
                reverse(
                    "control_plane:cdc_cleanup_scan",
                    args=[self.rebuild.pk],
                )
            )

        self.assertRedirects(response, reverse("control_plane:cdc_cleanup_list"))
        cleanup = CdcResourceCleanup.objects.get(rebuild=self.rebuild)
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.SCANNING)
        self.assertEqual(cleanup.created_by, self.admin)
        self.assertEqual(cleanup.items.count(), 8)
        apply_async.assert_called_once()
        self.assertEqual(
            apply_async.call_args.kwargs["queue"],
            "queue_maintenance",
        )

    def test_confirmation_requires_explicit_checkbox_and_verified_inventory(self):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild,
            status=CdcResourceCleanup.Status.AWAITING_CONFIRMATION,
            created_by=self.admin,
            updated_by=self.admin,
        )
        CdcResourceCleanupItem.objects.create(
            cleanup=cleanup,
            resource_type=CdcResourceCleanupItem.ResourceType.CONNECTOR,
            resource_name=self.rebuild.connector_name,
            exists_at_scan=True,
            created_by=self.admin,
            updated_by=self.admin,
        )

        response = self.client.post(
            reverse("control_plane:cdc_cleanup_confirm", args=[cleanup.pk])
        )

        self.assertRedirects(response, reverse("control_plane:cdc_cleanup_list"))
        cleanup.refresh_from_db()
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.AWAITING_CONFIRMATION)

    def test_confirmed_inventory_is_queued_for_execution(self):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild,
            status=CdcResourceCleanup.Status.AWAITING_CONFIRMATION,
            task_id="inventory-task",
            started_at=timezone.now(),
            created_by=self.admin,
            updated_by=self.admin,
        )
        CdcResourceCleanupItem.objects.create(
            cleanup=cleanup,
            resource_type=CdcResourceCleanupItem.ResourceType.CONNECTOR,
            resource_name=self.rebuild.connector_name,
            exists_at_scan=True,
            created_by=self.admin,
            updated_by=self.admin,
        )

        with patch(
            "control_plane.views.cdc_cleanup.cleanup_cdc_resources_task.apply_async"
        ) as apply_async:
            response = self.client.post(
                reverse("control_plane:cdc_cleanup_confirm", args=[cleanup.pk]),
                {"confirm_cleanup": "yes"},
            )

        self.assertRedirects(response, reverse("control_plane:cdc_cleanup_list"))
        cleanup.refresh_from_db()
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.QUEUED)
        self.assertIsNone(cleanup.started_at)
        self.assertNotEqual(cleanup.task_id, "inventory-task")
        apply_async.assert_called_once()

    def test_active_generation_is_not_offered_for_cleanup(self):
        self.table.cdc_connector_name = self.rebuild.connector_name
        self.table.cdc_topic_prefix = self.rebuild.topic_prefix
        self.table.save(update_fields=["cdc_connector_name", "cdc_topic_prefix"])

        response = self.client.get(reverse("control_plane:cdc_cleanup_list"))

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, self.rebuild.connector_name)

    def test_interrupted_cleanup_requires_acknowledgement_and_fresh_scan(self):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild,
            status=CdcResourceCleanup.Status.RUNNING,
            created_by=self.admin,
            updated_by=self.admin,
        )
        item = CdcResourceCleanupItem.objects.create(
            cleanup=cleanup,
            resource_type=CdcResourceCleanupItem.ResourceType.CONNECTOR,
            resource_name=self.rebuild.connector_name,
            exists_at_scan=True,
            status=CdcResourceCleanupItem.Status.RUNNING,
            created_by=self.admin,
            updated_by=self.admin,
        )

        response = self.client.post(
            reverse("control_plane:cdc_cleanup_recover", args=[cleanup.pk]),
            {"acknowledge_uncertain": "yes"},
        )

        self.assertRedirects(response, reverse("control_plane:cdc_cleanup_list"))
        cleanup.refresh_from_db()
        item.refresh_from_db()
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.FAILED)
        self.assertIn("may still finish", cleanup.last_error)
        self.assertEqual(item.status, CdcResourceCleanupItem.Status.FAILED)

    def test_duplicate_inventory_task_cannot_claim_the_same_cleanup(self):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild,
            status=CdcResourceCleanup.Status.SCANNING,
            created_by=self.admin,
            updated_by=self.admin,
        )

        claimed = _claim_cleanup_task(
            cleanup.pk,
            "inventory-task-1",
            CdcResourceCleanup.Status.SCANNING,
        )
        duplicate = _claim_cleanup_task(
            cleanup.pk,
            "inventory-task-2",
            CdcResourceCleanup.Status.SCANNING,
        )

        self.assertIsNotNone(claimed)
        self.assertIsNone(duplicate)
        cleanup.refresh_from_db()
        self.assertEqual(cleanup.task_id, "inventory-task-1")
