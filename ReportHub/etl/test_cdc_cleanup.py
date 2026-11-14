from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from control_plane.models import (
    CdcResourceCleanup,
    CdcResourceCleanupItem,
    CdcTableRebuild,
    DbChoice,
    IngestionMode,
    SourceProfile,
    SourceTable,
)
from etl.cdc_cleanup import (
    CdcCleanupIneligible,
    _assert_connector_owned,
    execute_cleanup,
    scan_cleanup_resources,
)


class CdcResourceCleanupTaskTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_user(
            username="cleanup-task-admin"
        )
        self.source = SourceProfile.objects.create(
            name="Cleanup task source",
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
            connector_name="reporthub-source-1-table-2-deadbeef0002",
            topic_prefix="reporthub-source-1-table-2-deadbeef0002",
            notification_topic=(
                "reporthub-source-1-table-2-deadbeef0002.notifications"
            ),
            consumer_group=(
                "reporthub-source-1-table-2-deadbeef0002-rebuild"
            ),
            staging_table="source_events__rebuild_deadbeef0002",
            status=CdcTableRebuild.Status.FAILED,
        )

    def make_cleanup(self, status, resources):
        cleanup = CdcResourceCleanup.objects.create(
            rebuild=self.rebuild,
            status=status,
            created_by=self.admin,
            updated_by=self.admin,
        )
        items = [
            CdcResourceCleanupItem.objects.create(
                cleanup=cleanup,
                resource_type=resource_type,
                resource_name=resource_name,
                exists_at_scan=exists,
                created_by=self.admin,
                updated_by=self.admin,
            )
            for resource_type, resource_name, exists in resources
        ]
        return cleanup, items

    def test_inventory_is_read_only_and_requires_confirmation(self):
        cleanup, items = self.make_cleanup(
            CdcResourceCleanup.Status.SCANNING,
            [
                (CdcResourceCleanupItem.ResourceType.CONNECTOR, self.rebuild.connector_name, None),
                (CdcResourceCleanupItem.ResourceType.CLICKHOUSE_TABLE, self.rebuild.staging_table, None),
            ],
        )
        with patch(
            "etl.cdc_cleanup.inspect_resource",
            side_effect=[True, False],
        ) as inspect, patch("etl.cdc_cleanup._cleanup_resource") as delete:
            scan_cleanup_resources(cleanup.pk)

        cleanup.refresh_from_db()
        for item in items:
            item.refresh_from_db()
        self.assertEqual(
            cleanup.status,
            CdcResourceCleanup.Status.AWAITING_CONFIRMATION,
        )
        self.assertEqual([item.exists_at_scan for item in items], [True, False])
        inspect.assert_called()
        delete.assert_not_called()

    def test_inventory_error_fails_closed_without_deleting_resources(self):
        cleanup, items = self.make_cleanup(
            CdcResourceCleanup.Status.SCANNING,
            [
                (CdcResourceCleanupItem.ResourceType.CONNECTOR, self.rebuild.connector_name, None),
            ],
        )
        with patch(
            "etl.cdc_cleanup.inspect_resource",
            side_effect=RuntimeError("inventory unavailable"),
        ), patch("etl.cdc_cleanup._cleanup_resource") as delete:
            scan_cleanup_resources(cleanup.pk)

        cleanup.refresh_from_db()
        items[0].refresh_from_db()
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.FAILED)
        self.assertIsNone(items[0].exists_at_scan)
        self.assertIn("inventory unavailable", items[0].scan_error)
        delete.assert_not_called()

    def test_cleanup_rechecks_and_records_each_confirmed_resource(self):
        from etl.cdc_cleanup import _resource_specs

        resource_specs = _resource_specs(self.rebuild)
        first_type, first_name = resource_specs[0]
        last_type, last_name = resource_specs[-1]
        cleanup, items = self.make_cleanup(
            CdcResourceCleanup.Status.QUEUED,
            [
                (first_type, first_name, True),
                (last_type, last_name, True),
            ],
        )
        with patch(
            "etl.cdc_cleanup._cleanup_resource",
            side_effect=["Deleted", "Already absent"],
        ) as delete:
            execute_cleanup(cleanup.pk)

        cleanup.refresh_from_db()
        for item in items:
            item.refresh_from_db()
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.COMPLETED)
        self.assertEqual(delete.call_count, 2)
        self.assertEqual(
            [item.status for item in items],
            [
                CdcResourceCleanupItem.Status.COMPLETED,
                CdcResourceCleanupItem.Status.COMPLETED,
            ],
        )
        self.assertEqual([item.attempts for item in items], [1, 1])

    def test_failed_resource_stops_the_remaining_cleanup(self):
        from etl.cdc_cleanup import _resource_specs

        resource_specs = _resource_specs(self.rebuild)
        cleanup, items = self.make_cleanup(
            CdcResourceCleanup.Status.QUEUED,
            [
                (resource_type, resource_name, True)
                for resource_type, resource_name in resource_specs[:2]
            ],
        )
        with patch(
            "etl.cdc_cleanup._cleanup_resource",
            side_effect=RuntimeError("connector response uncertain"),
        ) as delete:
            execute_cleanup(cleanup.pk)

        cleanup.refresh_from_db()
        for item in items:
            item.refresh_from_db()
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.FAILED)
        self.assertEqual(delete.call_count, 1)
        self.assertEqual(items[0].status, CdcResourceCleanupItem.Status.FAILED)
        self.assertEqual(items[1].status, CdcResourceCleanupItem.Status.PENDING)

    def test_active_connector_reference_blocks_cleanup(self):
        self.table.cdc_connector_name = self.rebuild.connector_name
        self.table.save(update_fields=["cdc_connector_name"])
        cleanup, _ = self.make_cleanup(
            CdcResourceCleanup.Status.QUEUED,
            [
                (
                    CdcResourceCleanupItem.ResourceType.CONNECTOR,
                    self.rebuild.connector_name,
                    True,
                ),
            ],
        )

        with patch("etl.cdc_cleanup._cleanup_resource") as delete:
            with self.assertRaises(RuntimeError):
                execute_cleanup(cleanup.pk)

        cleanup.refresh_from_db()
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.QUEUED)
        delete.assert_not_called()

    def test_connector_ownership_rejects_a_different_table_route(self):
        expected_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "topic.prefix": self.rebuild.topic_prefix,
            "table.include.list": "public.events",
        }
        actual_config = {
            **expected_config,
            "table.include.list": "public.other_table",
        }

        with patch(
            "etl.cdc_cleanup._expected_connector_config",
            return_value=expected_config,
        ):
            with self.assertRaises(CdcCleanupIneligible):
                _assert_connector_owned(self.rebuild, actual_config)
