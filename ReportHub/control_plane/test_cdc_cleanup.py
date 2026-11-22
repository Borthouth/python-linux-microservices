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
from etl.cdc_cleanup import scan_cleanup_resources


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

        response = self.client.get(
            reverse("control_plane:cdc_cleanup_source", args=[self.source.pk])
        )

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, self.rebuild.connector_name)

    def test_source_list_is_filtered_paginated_and_warns_about_hidden_blocks(self):
        inactive = SourceProfile.objects.create(
            name="Inactive cleanup source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="test_db",
            username="test_user", is_active=False,
        )
        table = SourceTable.objects.create(source=inactive, table_name="events")
        rebuild = CdcTableRebuild.objects.create(
            source_table=table, connector_name="inactive-connector",
            topic_prefix="inactive-topic", notification_topic="inactive-notifications",
            consumer_group="inactive-group", staging_table="inactive-staging",
            status=CdcTableRebuild.Status.FAILED,
        )
        CdcResourceCleanup.objects.create(
            rebuild=rebuild, status=CdcResourceCleanup.Status.FAILED,
            external_operation_uncertain=True,
        )
        for index in range(20):
            source = SourceProfile.objects.create(
                name=f"Cleanup source {index:02}", db_type=DbChoice.POSTGRESQL,
                host="localhost", port=5432, database_name="test_db", username="test_user",
            )
            table = SourceTable.objects.create(source=source, table_name="events")
            CdcTableRebuild.objects.create(
                source_table=table, connector_name=f"connector-{index}",
                topic_prefix=f"topic-{index}", notification_topic=f"notification-{index}",
                consumer_group=f"group-{index}", staging_table=f"staging-{index}",
                status=CdcTableRebuild.Status.FAILED,
            )

        list_url = reverse("control_plane:cdc_cleanup_list")
        response = self.client.get(list_url, {"q": "Cleanup", "status": "active"})
        self.assertEqual(response.context["page_obj"].paginator.count, 21)
        self.assertContains(response, "?q=Cleanup&amp;status=active&amp;page=2")
        self.assertContains(response, "1 source with unresolved uncertain cleanup operations")
        self.assertNotContains(response, "Inactive cleanup source")
        self.assertNotContains(response, self.rebuild.connector_name)
        response = self.client.get(list_url, {"q": "Inactive", "status": "inactive"})
        self.assertEqual(list(response.context["page_obj"]), [inactive])
        self.assertContains(response, "Blocked: uncertain operation")
        self.assertContains(response, reverse("control_plane:cdc_cleanup_source", args=[inactive.pk]))
        self.assertContains(response, '<option value="inactive" selected>Inactive</option>', html=True)
        self.assertContains(response, f'href="{list_url}"')
        response = self.client.get(list_url, {"q": "Cleanup", "status": "active", "page": 2})
        self.assertContains(response, "?q=Cleanup&amp;status=active&amp;page=1")

    def test_source_page_scopes_generations_and_uncertainty(self):
        other = SourceProfile.objects.create(
            name="Other source", db_type=DbChoice.POSTGRESQL, host="localhost",
            port=5432, database_name="other_db", username="test_user",
        )
        other_table = SourceTable.objects.create(source=other, table_name="other_events")
        other_rebuild = CdcTableRebuild.objects.create(
            source_table=other_table, connector_name="other-connector",
            topic_prefix="other-topic", notification_topic="other-notifications",
            consumer_group="other-group", staging_table="other-staging",
            status=CdcTableRebuild.Status.FAILED,
        )
        other_cleanup = CdcResourceCleanup.objects.create(
            rebuild=other_rebuild, status=CdcResourceCleanup.Status.FAILED,
            external_operation_uncertain=True,
        )
        source_url = reverse("control_plane:cdc_cleanup_source", args=[self.source.pk])
        response = self.client.get(source_url)
        self.assertContains(response, self.rebuild.connector_name)
        self.assertNotContains(response, "other-connector")
        self.assertNotContains(response, "Other source")
        self.assertIn(
            {"name": "CDC Resource Cleanup", "url": reverse("control_plane:cdc_cleanup_list")},
            response.context["breadcrumbs"],
        )
        self.assertContains(response, 'name="return_to" value="attempt"')

        response = self.client.get(reverse("control_plane:cdc_cleanup_source", args=[other.pk]))
        self.assertContains(response, "Cleanup blocked by uncertain operations")
        self.assertContains(response, reverse("control_plane:cdc_cleanup_attempt", args=[other_cleanup.pk]))
        self.assertNotContains(response, "Accept risk and unblock this attempt")
        self.assertNotContains(response, self.rebuild.connector_name)

    def test_attempt_page_shows_inventory_confirmation_and_breadcrumbs(self):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.AWAITING_CONFIRMATION,
        )
        item = CdcResourceCleanupItem.objects.create(
            cleanup=cleanup, resource_type=CdcResourceCleanupItem.ResourceType.CONNECTOR,
            resource_name="specific-resource", exists_at_scan=True,
        )
        source_url = reverse("control_plane:cdc_cleanup_source", args=[self.source.pk])
        attempt_url = reverse("control_plane:cdc_cleanup_attempt", args=[cleanup.pk])
        source_page = self.client.get(source_url)
        self.assertContains(source_page, attempt_url)
        self.assertNotContains(source_page, item.resource_name)
        self.assertNotContains(source_page, "I reviewed these resources")
        response = self.client.get(attempt_url)
        self.assertContains(response, item.resource_name)
        self.assertContains(response, "Present at scan")
        self.assertContains(response, "I reviewed these resources")
        self.assertContains(response, 'name="return_to" value="attempt"')
        self.assertIn({"name": "Source Resources", "url": source_url}, response.context["breadcrumbs"])
        self.assertIn(
            {"name": "CDC Resource Cleanup", "url": reverse("control_plane:cdc_cleanup_list")},
            response.context["breadcrumbs"],
        )

    def test_unresolved_attempt_remains_visible_outside_recent_activity(self):
        uncertain = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.FAILED,
            external_operation_uncertain=True,
        )
        for _ in range(20):
            CdcResourceCleanup.objects.create(
                rebuild=self.rebuild, status=CdcResourceCleanup.Status.FAILED,
            )
        response = self.client.get(reverse("control_plane:cdc_cleanup_source", args=[self.source.pk]))
        self.assertNotIn(uncertain, response.context["cleanup_jobs"])
        self.assertContains(response, "Cleanup blocked by uncertain operations")
        self.assertContains(response, reverse("control_plane:cdc_cleanup_attempt", args=[uncertain.pk]))
        attempt = self.client.get(reverse("control_plane:cdc_cleanup_attempt", args=[uncertain.pk]))
        self.assertContains(attempt, "may still execute")
        self.assertContains(attempt, "Accept risk and unblock this attempt")

    def test_attempt_scan_redirect_and_ineligible_scan(self):
        scan_url = reverse("control_plane:cdc_cleanup_scan", args=[self.rebuild.pk])
        with patch("control_plane.views.cdc_cleanup.scan_cdc_resources_task.apply_async"):
            response = self.client.post(scan_url, {"return_to": "attempt"})
        cleanup = CdcResourceCleanup.objects.get(rebuild=self.rebuild)
        self.assertRedirects(response, reverse("control_plane:cdc_cleanup_attempt", args=[cleanup.pk]))
        with patch("control_plane.views.cdc_cleanup.scan_cdc_resources_task.apply_async") as queue:
            response = self.client.post(scan_url, {"return_to": "attempt"})
        self.assertRedirects(response, reverse("control_plane:cdc_cleanup_source", args=[self.source.pk]))
        queue.assert_not_called()
        self.assertEqual(CdcResourceCleanup.objects.count(), 1)

    def test_attempt_actions_stay_on_detail_and_preserve_safety(self):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.RUNNING,
        )
        attempt_url = reverse("control_plane:cdc_cleanup_attempt", args=[cleanup.pk])
        recover_url = reverse("control_plane:cdc_cleanup_recover", args=[cleanup.pk])
        self.assertContains(self.client.get(attempt_url), "Mark interrupted")
        response = self.client.post(recover_url, {"return_to": "attempt"})
        self.assertRedirects(response, attempt_url)
        cleanup.refresh_from_db()
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.RUNNING)
        response = self.client.post(recover_url, {"return_to": "attempt", "acknowledge_uncertain": "yes"})
        self.assertRedirects(response, attempt_url)
        cleanup.refresh_from_db()
        self.assertTrue(cleanup.external_operation_uncertain)
        override_url = reverse("control_plane:cdc_cleanup_override", args=[cleanup.pk])
        response = self.client.post(override_url, {
            "return_to": "attempt", "source_name": self.source.name, "reason": "Investigated",
        })
        self.assertRedirects(response, attempt_url)
        cleanup.refresh_from_db()
        self.assertIsNone(cleanup.uncertainty_override_at)
        response = self.client.post(override_url, {
            "return_to": "attempt", "source_name": self.source.name,
            "reason": "Investigated", "accept_risk": "yes",
        })
        self.assertRedirects(response, attempt_url)
        cleanup.refresh_from_db()
        self.assertIsNotNone(cleanup.uncertainty_override_at)
        self.assertContains(self.client.get(attempt_url), "The old request may still execute")

        fresh = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.AWAITING_CONFIRMATION,
        )
        CdcResourceCleanupItem.objects.create(
            cleanup=fresh, resource_type=CdcResourceCleanupItem.ResourceType.CONNECTOR,
            resource_name=self.rebuild.connector_name, exists_at_scan=True,
        )
        fresh_url = reverse("control_plane:cdc_cleanup_attempt", args=[fresh.pk])
        confirm_url = reverse("control_plane:cdc_cleanup_confirm", args=[fresh.pk])
        with patch("control_plane.views.cdc_cleanup.cleanup_cdc_resources_task.apply_async") as queue:
            response = self.client.post(confirm_url, {"return_to": "attempt"})
            self.assertRedirects(response, fresh_url)
            queue.assert_not_called()
            response = self.client.post(confirm_url, {"return_to": "attempt", "confirm_cleanup": "yes"})
        self.assertRedirects(response, fresh_url)
        fresh.refresh_from_db()
        self.assertEqual(fresh.status, CdcResourceCleanup.Status.QUEUED)
        queue.assert_called_once()

    def test_source_page_scan_returns_to_the_source(self):
        with patch("control_plane.views.cdc_cleanup.scan_cdc_resources_task.apply_async"):
            response = self.client.post(
                reverse("control_plane:cdc_cleanup_scan", args=[self.rebuild.pk]),
                {"return_to": "source"},
            )
        self.assertRedirects(
            response, reverse("control_plane:cdc_cleanup_source", args=[self.source.pk]),
        )
        cleanup = CdcResourceCleanup.objects.get(rebuild=self.rebuild)
        response = self.client.post(
            reverse("control_plane:cdc_cleanup_confirm", args=[cleanup.pk]),
            {"return_to": "source"},
        )
        self.assertRedirects(
            response, reverse("control_plane:cdc_cleanup_source", args=[self.source.pk]),
        )
        cleanup.refresh_from_db()
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.SCANNING)

    def test_cleanup_pages_require_staff(self):
        cleanup = CdcResourceCleanup.objects.create(rebuild=self.rebuild)
        viewer = get_user_model().objects.create_user(username="cleanup-viewer")
        self.client.force_login(viewer)
        for url in (
            reverse("control_plane:cdc_cleanup_list"),
            reverse("control_plane:cdc_cleanup_source", args=[self.source.pk]),
            reverse("control_plane:cdc_cleanup_attempt", args=[cleanup.pk]),
        ):
            with self.subTest(url=url):
                self.assertEqual(self.client.get(url).status_code, 302)

    def test_source_actions_return_to_source_without_changing_safety_checks(self):
        source_url = reverse("control_plane:cdc_cleanup_source", args=[self.source.pk])
        interrupted = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.RUNNING,
        )
        response = self.client.post(
            reverse("control_plane:cdc_cleanup_recover", args=[interrupted.pk]),
            {"return_to": "source", "acknowledge_uncertain": "yes"},
        )
        self.assertRedirects(response, source_url)
        interrupted.refresh_from_db()
        self.assertTrue(interrupted.external_operation_uncertain)
        response = self.client.post(
            reverse("control_plane:cdc_cleanup_override", args=[interrupted.pk]),
            {
                "return_to": "source", "source_name": self.source.name,
                "accept_risk": "yes", "reason": "Investigated old worker",
            },
        )
        self.assertRedirects(response, source_url)
        interrupted.refresh_from_db()
        self.assertIsNotNone(interrupted.uncertainty_override_at)

        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.AWAITING_CONFIRMATION,
        )
        CdcResourceCleanupItem.objects.create(
            cleanup=cleanup, resource_type=CdcResourceCleanupItem.ResourceType.CONNECTOR,
            resource_name=self.rebuild.connector_name, exists_at_scan=True,
        )
        with patch("control_plane.views.cdc_cleanup.cleanup_cdc_resources_task.apply_async") as queue:
            response = self.client.post(
                reverse("control_plane:cdc_cleanup_confirm", args=[cleanup.pk]),
                {"return_to": "source", "confirm_cleanup": "yes"},
            )
        self.assertRedirects(response, source_url)
        cleanup.refresh_from_db()
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.QUEUED)
        queue.assert_called_once()

    def test_interrupted_cleanup_blocks_fresh_scan(self):
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
        self.assertTrue(cleanup.external_operation_uncertain)
        self.assertIn("may still finish", cleanup.last_error)
        self.assertEqual(item.status, CdcResourceCleanupItem.Status.FAILED)
        with patch(
            "control_plane.views.cdc_cleanup.scan_cdc_resources_task.apply_async"
        ) as queue:
            response = self.client.post(
                reverse("control_plane:cdc_cleanup_scan", args=[self.rebuild.pk])
            )
        self.assertRedirects(response, reverse("control_plane:cdc_cleanup_list"))
        queue.assert_not_called()
        self.assertEqual(CdcResourceCleanup.objects.count(), 1)

    def test_interrupted_read_only_inventory_allows_fresh_scan(self):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild,
            status=CdcResourceCleanup.Status.SCANNING,
            created_by=self.admin,
            updated_by=self.admin,
        )
        self.client.post(
            reverse("control_plane:cdc_cleanup_recover", args=[cleanup.pk]),
            {"acknowledge_uncertain": "yes"},
        )
        cleanup.refresh_from_db()
        self.assertFalse(cleanup.external_operation_uncertain)
        with patch(
            "control_plane.views.cdc_cleanup.scan_cdc_resources_task.apply_async"
        ) as queue:
            self.client.post(
                reverse("control_plane:cdc_cleanup_scan", args=[self.rebuild.pk])
            )
        queue.assert_called_once()

    def test_uncertain_cleanup_blocks_confirmation_for_source(self):
        CdcResourceCleanup.objects.create(
            rebuild=self.rebuild,
            status=CdcResourceCleanup.Status.FAILED,
            external_operation_uncertain=True,
        )
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild,
            status=CdcResourceCleanup.Status.AWAITING_CONFIRMATION,
        )
        CdcResourceCleanupItem.objects.create(
            cleanup=cleanup,
            resource_type=CdcResourceCleanupItem.ResourceType.CONNECTOR,
            resource_name=self.rebuild.connector_name,
            exists_at_scan=True,
        )
        with patch(
            "control_plane.views.cdc_cleanup.cleanup_cdc_resources_task.apply_async"
        ) as queue:
            self.client.post(
                reverse("control_plane:cdc_cleanup_confirm", args=[cleanup.pk]),
                {"confirm_cleanup": "yes"},
            )
        cleanup.refresh_from_db()
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.AWAITING_CONFIRMATION)
        queue.assert_not_called()

    def test_risk_override_is_audited_and_requires_a_new_inventory(self):
        uncertain = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild,
            status=CdcResourceCleanup.Status.FAILED,
            external_operation_uncertain=True,
        )
        stale = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild,
            status=CdcResourceCleanup.Status.AWAITING_CONFIRMATION,
        )
        response = self.client.post(
            reverse("control_plane:cdc_cleanup_override", args=[uncertain.pk]),
            {
                "source_name": self.source.name,
                "accept_risk": "yes",
                "reason": "The operator isolated the old worker.",
            },
        )
        self.assertRedirects(response, reverse("control_plane:cdc_cleanup_list"))
        uncertain.refresh_from_db()
        stale.refresh_from_db()
        self.assertTrue(uncertain.external_operation_uncertain)
        self.assertIsNotNone(uncertain.uncertainty_override_at)
        self.assertEqual(uncertain.uncertainty_override_reason, "The operator isolated the old worker.")
        history = uncertain.history.first()
        self.assertEqual(history.history_user, self.admin)
        self.assertEqual(history.history_change_reason, "Administrator accepted uncertain cleanup risk")
        self.assertEqual(stale.status, CdcResourceCleanup.Status.FAILED)
        self.assertIn("fresh inventory", stale.last_error)
        with patch("control_plane.views.cdc_cleanup.scan_cdc_resources_task.apply_async") as queue:
            self.client.post(reverse("control_plane:cdc_cleanup_scan", args=[self.rebuild.pk]))
        queue.assert_called_once()
        fresh = CdcResourceCleanup.objects.filter(rebuild=self.rebuild).latest("pk")
        with patch("etl.cdc_cleanup.inspect_resource", return_value=True):
            scan_cleanup_resources(fresh.pk)
        with patch("control_plane.views.cdc_cleanup.cleanup_cdc_resources_task.apply_async") as queue:
            self.client.post(
                reverse("control_plane:cdc_cleanup_confirm", args=[fresh.pk]),
                {"confirm_cleanup": "yes"},
            )
        queue.assert_called_once()

    def test_override_requires_explicit_acknowledgement_and_matching_source(self):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.FAILED,
            external_operation_uncertain=True,
        )
        for data in (
            {"source_name": self.source.name, "reason": "Reviewed"},
            {"source_name": "wrong", "reason": "Reviewed", "accept_risk": "yes"},
            {"source_name": self.source.name, "reason": " ", "accept_risk": "yes"},
            {"source_name": self.source.name, "reason": "x" * 1001, "accept_risk": "yes"},
        ):
            with self.subTest(data=data):
                self.client.post(reverse("control_plane:cdc_cleanup_override", args=[cleanup.pk]), data)
                cleanup.refresh_from_db()
                self.assertIsNone(cleanup.uncertainty_override_at)
        nonstaff = get_user_model().objects.create_user(username="nonstaff")
        self.client.force_login(nonstaff)
        self.client.post(
            reverse("control_plane:cdc_cleanup_override", args=[cleanup.pk]),
            {"source_name": self.source.name, "reason": "Reviewed", "accept_risk": "yes"},
        )
        cleanup.refresh_from_db()
        self.assertIsNone(cleanup.uncertainty_override_at)

    def test_running_cleanup_cannot_be_overridden(self):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.RUNNING,
            external_operation_uncertain=True,
        )
        self.client.post(
            reverse("control_plane:cdc_cleanup_override", args=[cleanup.pk]),
            {"source_name": self.source.name, "reason": "Reviewed", "accept_risk": "yes"},
        )
        cleanup.refresh_from_db()
        self.assertIsNone(cleanup.uncertainty_override_at)

    def test_override_does_not_unblock_other_uncertain_attempts(self):
        first = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.FAILED,
            external_operation_uncertain=True,
        )
        CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.FAILED,
            external_operation_uncertain=True,
        )
        self.client.post(
            reverse("control_plane:cdc_cleanup_override", args=[first.pk]),
            {"source_name": self.source.name, "reason": "Reviewed", "accept_risk": "yes"},
        )
        with patch("control_plane.views.cdc_cleanup.scan_cdc_resources_task.apply_async") as queue:
            self.client.post(reverse("control_plane:cdc_cleanup_scan", args=[self.rebuild.pk]))
        queue.assert_not_called()

    def test_inventory_created_before_override_cannot_be_confirmed(self):
        uncertain = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.FAILED,
            external_operation_uncertain=True,
        )
        stale = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.FAILED,
        )
        CdcResourceCleanupItem.objects.create(
            cleanup=stale, resource_type=CdcResourceCleanupItem.ResourceType.CONNECTOR,
            resource_name=self.rebuild.connector_name, exists_at_scan=True,
        )
        self.client.post(
            reverse("control_plane:cdc_cleanup_override", args=[uncertain.pk]),
            {"source_name": self.source.name, "reason": "Reviewed", "accept_risk": "yes"},
        )
        stale.status = CdcResourceCleanup.Status.AWAITING_CONFIRMATION
        stale.save(update_fields=["status"])
        with patch("control_plane.views.cdc_cleanup.cleanup_cdc_resources_task.apply_async") as queue:
            self.client.post(
                reverse("control_plane:cdc_cleanup_confirm", args=[stale.pk]),
                {"confirm_cleanup": "yes"},
            )
        queue.assert_not_called()

    def test_source_deletion_requires_resolution_of_uncertain_cleanup(self):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.FAILED,
            external_operation_uncertain=True,
        )
        delete_url = reverse("control_plane:source_delete", args=[self.source.pk])
        response = self.client.post(delete_url, follow=True)
        self.assertContains(response, "Source metadata cannot be deleted while a cleanup")
        self.assertTrue(SourceProfile.objects.filter(pk=self.source.pk).exists())
        self.client.post(
            reverse("control_plane:cdc_cleanup_override", args=[cleanup.pk]),
            {"source_name": self.source.name, "reason": "Reviewed", "accept_risk": "yes"},
        )
        self.client.post(delete_url)
        self.assertFalse(SourceProfile.objects.filter(pk=self.source.pk).exists())

    def test_queued_or_running_cleanup_blocks_source_deletion(self):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.QUEUED,
        )
        for status in (CdcResourceCleanup.Status.QUEUED, CdcResourceCleanup.Status.RUNNING):
            with self.subTest(status=status):
                cleanup.status = status
                cleanup.save(update_fields=["status"])
                response = self.client.post(
                    reverse("control_plane:source_delete", args=[self.source.pk]), follow=True,
                )
                self.assertContains(response, "Source metadata cannot be deleted while a cleanup")
                self.assertTrue(SourceProfile.objects.filter(pk=self.source.pk).exists())

    def test_admin_cannot_delete_source_or_table_with_unresolved_cleanup(self):
        CdcResourceCleanup.objects.create(
            rebuild=self.rebuild, status=CdcResourceCleanup.Status.FAILED,
            external_operation_uncertain=True,
        )
        superuser = get_user_model().objects.create_superuser(
            username="cleanup-superuser", password="test-password",
        )
        self.client.force_login(superuser)
        cases = [
            ("admin:control_plane_sourceprofile_delete", [self.source.pk], {"post": "yes"}),
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
                self.assertContains(response, "Source metadata cannot be deleted while a cleanup")
                self.assertTrue(SourceTable.objects.filter(pk=self.table.pk).exists())

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
