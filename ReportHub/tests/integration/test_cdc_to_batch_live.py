import os
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest import skipUnless
from unittest.mock import patch
from uuid import uuid4

import clickhouse_connect
import sqlalchemy as sa
from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import close_old_connections
from django.test import Client, TransactionTestCase, override_settings
from django.urls import reverse

from control_plane.models import (
    CdcToBatchTableCheckpoint,
    CdcToBatchTransition,
    DbChoice,
    IngestionMode,
    SourceField,
    SourceProfile,
    SourceTable,
)
from etl.cdc import _clickhouse_table_uuid
from etl.cdc_to_batch import (
    CdcToBatchStepResult,
    _field_signature,
    _copy_cdc_to_batch_generation,
    _finalize_cdc_to_batch_generation,
    _mark_cdc_to_batch_cutover_pending,
    _run_cdc_to_batch_cutover_step,
    _stage_fields,
    prepare_cdc_to_batch_transition,
    stage_cdc_to_batch_table,
)
from etl.tasks import execute_cdc_to_batch_transition_task
from reports.query_builder import clickhouse_table_name, quote_clickhouse_identifier


@skipUnless(
    os.environ.get("REPORTHUB_LIVE_CUTOVER_TEST") == "1",
    "Set REPORTHUB_LIVE_CUTOVER_TEST=1 to run isolated live ClickHouse tests.",
)
@override_settings(CDC_EXTERNAL_OPERATION_TRACKING=True)
class CdcToBatchLiveCutoverTests(TransactionTestCase):
    def setUp(self):
        self.client = clickhouse_connect.get_client(
            host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
            port=getattr(settings, "CLICKHOUSE_PORT", 8123),
            username=getattr(settings, "CLICKHOUSE_USER", "default"),
            password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
        )
        self.addCleanup(self.client.close)
        unique = uuid4().hex
        self.source = SourceProfile.objects.create(
            name=f"live-cutover-{unique}", db_type=DbChoice.POSTGRESQL,
            host="unused", port=5432, database_name="unused", username="unused",
            ingestion_mode=IngestionMode.CDC,
        )
        self.table = SourceTable.objects.create(
            source=self.source, schema_name="public", table_name=f"live_{unique}",
            primary_key_fields=["id"], cdc_managed=True,
            clickhouse_uses_replacing_merge_tree=True, cdc_connector_name="retained",
        )
        SourceField.objects.create(table=self.table, field_name="id", data_type="INTEGER")
        self.transition = prepare_cdc_to_batch_transition(self.source.pk)
        self.checkpoint = self.transition.table_checkpoints.get()
        self.active_name = clickhouse_table_name(
            self.source.pk, self.table.schema_name, self.table.table_name
        )
        self._create_table(self.active_name)
        self._create_table(self.checkpoint.staging_table)
        self.client.command(
            f"INSERT INTO {quote_clickhouse_identifier(self.active_name)} VALUES (1)"
        )
        self.client.command(
            f"INSERT INTO {quote_clickhouse_identifier(self.checkpoint.staging_table)} VALUES (2)"
        )
        self.old_uuid = _clickhouse_table_uuid(self.client, self.active_name)
        self.new_uuid = _clickhouse_table_uuid(self.client, self.checkpoint.staging_table)
        self.checkpoint.status = CdcToBatchTableCheckpoint.Status.STAGED
        self.checkpoint.staging_uuid = self.new_uuid
        self.checkpoint.staging_row_count = 1
        self.checkpoint.staging_schema = _field_signature(
            self.source, _stage_fields(self.source, self.table), self.table
        )
        self.checkpoint.save()

    def _create_table(self, name):
        quoted = quote_clickhouse_identifier(name)
        self.client.command(f"CREATE TABLE {quoted} (id Int64) ENGINE = MergeTree ORDER BY tuple()")
        self.addCleanup(self.client.command, f"DROP TABLE IF EXISTS {quoted} SYNC")

    def test_staging_uses_synchronous_clickhouse_inserts(self):
        engine = sa.create_engine("sqlite://")
        self.addCleanup(engine.dispose)
        with engine.begin() as connection:
            connection.execute(sa.text(f'CREATE TABLE "{self.table.table_name}" (id INTEGER)'))
            connection.execute(sa.text(f'INSERT INTO "{self.table.table_name}" VALUES (10), (20)'))
        self.source.db_type = DbChoice.MYSQL
        self.source.save(update_fields=["db_type"])
        self.checkpoint.status = CdcToBatchTableCheckpoint.Status.PENDING
        self.checkpoint.save(update_fields=["status"])
        original_get_client = clickhouse_connect.get_client
        observed_settings = []

        def staging_client(*args, **kwargs):
            client = original_get_client(*args, **kwargs)
            observed_settings.append(client.query("SELECT getSetting('async_insert')").result_rows)
            command = client.command

            def register_created_table(sql, *args, **kwargs):
                if sql.startswith("CREATE TABLE `"):
                    name = sql.split("`")[1]
                    self.addCleanup(
                        self.client.command,
                        f"DROP TABLE IF EXISTS {quote_clickhouse_identifier(name)} SYNC",
                    )
                return command(sql, *args, **kwargs)

            client.command = register_created_table
            return client

        with patch("etl.cdc_to_batch.create_source_engine", return_value=engine), patch(
            "etl.cdc_to_batch.clickhouse_connect.get_client", side_effect=staging_client
        ):
            checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk, batch_size=1)
        self.assertEqual(observed_settings, [[(0,)]])
        self.assertIs(checkpoint.staging_schema["synchronous_inserts"], True)
        self.assertEqual(checkpoint.staging_row_count, 2)
        self.assertEqual(
            self.client.query(
                f"SELECT id FROM {quote_clickhouse_identifier(checkpoint.staging_table)} ORDER BY id"
            ).result_rows,
            [(10,), (20,)],
        )

    def test_exchange_finalize_and_preserve_old_table(self):
        _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        self.assertEqual(
            _run_cdc_to_batch_cutover_step(self.transition.pk),
            CdcToBatchStepResult.TABLE_CUTOVER,
        )
        self.assertEqual(_clickhouse_table_uuid(self.client, self.active_name), self.new_uuid)
        self.assertEqual(
            _clickhouse_table_uuid(self.client, self.checkpoint.staging_table), self.old_uuid
        )
        self.assertEqual(
            self.client.query(
                f"SELECT id FROM {quote_clickhouse_identifier(self.checkpoint.staging_table)}"
            ).result_rows,
            [(1,)],
        )
        self.assertEqual(
            _run_cdc_to_batch_cutover_step(self.transition.pk),
            CdcToBatchStepResult.SUCCEEDED,
        )
        self.source.refresh_from_db()
        self.table.refresh_from_db()
        self.transition.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.BATCH)
        self.assertFalse(self.table.cdc_managed)
        self.assertEqual(self.table.cdc_connector_name, "retained")
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.SUCCEEDED)
        self.assertEqual(
            self.client.query(
                f"SELECT id FROM {quote_clickhouse_identifier(self.active_name)}"
            ).result_rows,
            [(2,)],
        )

    def test_lost_exchange_response_blocks_retry(self):
        _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        real_get_client = clickhouse_connect.get_client

        class LostResponseClient:
            def __init__(self, *args, **kwargs):
                self.client = real_get_client(*args, **kwargs)

            def query(self, *args, **kwargs):
                return self.client.query(*args, **kwargs)

            def command(self, *args, **kwargs):
                self.client.command(*args, **kwargs)
                raise RuntimeError("Simulated lost ClickHouse response")

            def close(self):
                self.client.close()

        with patch("etl.cdc_to_batch.clickhouse_connect.get_client", side_effect=LostResponseClient):
            with self.assertRaisesRegex(RuntimeError, "Simulated lost"):
                _run_cdc_to_batch_cutover_step(self.transition.pk)
        self.checkpoint.refresh_from_db()
        self.assertTrue(self.checkpoint.cutover_command_inflight)
        self.assertEqual(_clickhouse_table_uuid(self.client, self.active_name), self.new_uuid)
        self.assertEqual(
            _clickhouse_table_uuid(self.client, self.checkpoint.staging_table), self.old_uuid
        )
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            _run_cdc_to_batch_cutover_step(self.transition.pk)
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)

    def test_generation_publication_isolated_from_late_old_and_staging_writes(self):
        _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        candidate = _copy_cdc_to_batch_generation(self.checkpoint.pk)
        quoted_candidate = quote_clickhouse_identifier(candidate.generation_table)
        self.addCleanup(self.client.command, f"DROP TABLE IF EXISTS {quoted_candidate} SYNC")
        _finalize_cdc_to_batch_generation(self.transition.pk)
        self.client.command(
            f"INSERT INTO {quote_clickhouse_identifier(self.active_name)} VALUES (3)"
        )
        self.client.command(
            f"INSERT INTO {quote_clickhouse_identifier(self.checkpoint.staging_table)} VALUES (4)"
        )
        self.table.refresh_from_db()
        self.source.refresh_from_db()
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.table.clickhouse_physical_table, candidate.generation_table)
        self.assertEqual(self.source.ingestion_mode, IngestionMode.BATCH)
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.CUTOVER)
        self.assertEqual(self.client.query(f"SELECT id FROM {quoted_candidate}").result_rows, [(2,)])
        self.assertEqual(_clickhouse_table_uuid(self.client, self.active_name), self.old_uuid)

    def test_superseded_delayed_copy_cannot_publish(self):
        _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        inserted_waiting, release = Event(), Event()
        real_get_client = clickhouse_connect.get_client

        class PausedClient:
            def __init__(self, client):
                self.client = client

            def query(self, *args, **kwargs):
                return self.client.query(*args, **kwargs)

            def command(self, sql, *args, **kwargs):
                if sql.startswith("CREATE TABLE "):
                    candidate = sql.split("`")[1]
                    self_cleanup(candidate)
                if sql.startswith("INSERT INTO "):
                    inserted_waiting.set()
                    if not release.wait(10):
                        raise TimeoutError("Delayed generation copy was not released.")
                return self.client.command(sql, *args, **kwargs)

            def close(self):
                self.client.close()

        def self_cleanup(candidate):
            self.addCleanup(
                self.client.command,
                f"DROP TABLE IF EXISTS {quote_clickhouse_identifier(candidate)} SYNC",
            )

        calls = 0

        def get_client(*args, **kwargs):
            nonlocal calls
            calls += 1
            client = real_get_client(*args, **kwargs)
            return PausedClient(client) if calls == 1 else client

        def delayed_copy():
            close_old_connections()
            try:
                _copy_cdc_to_batch_generation(self.checkpoint.pk)
            finally:
                close_old_connections()

        with patch("etl.cdc_to_batch.clickhouse_connect.get_client", side_effect=get_client):
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(delayed_copy)
                try:
                    self.assertTrue(inserted_waiting.wait(10))
                    winner = _copy_cdc_to_batch_generation(self.checkpoint.pk)
                    self_cleanup(winner.generation_table)
                finally:
                    release.set()
                with self.assertRaisesRegex(RuntimeError, "superseded"):
                    future.result(timeout=10)
                _finalize_cdc_to_batch_generation(self.transition.pk)
        self.table.refresh_from_db()
        self.assertEqual(self.table.clickhouse_physical_table, winner.generation_table)
        self.assertEqual(
            self.client.query(
                f"SELECT id FROM {quote_clickhouse_identifier(winner.generation_table)}"
            ).result_rows,
            [(2,)],
        )

    def test_control_plane_abandons_pending_while_copy_is_delayed(self):
        _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        paused, release = Event(), Event()
        real_get_client = clickhouse_connect.get_client

        class PausedClient:
            def __init__(self, client):
                self.client = client

            def query(self, *args, **kwargs):
                return self.client.query(*args, **kwargs)

            def command(self, sql, *args, **kwargs):
                if sql.startswith("CREATE TABLE "):
                    name = sql.split("`")[1]
                    self_cleanup(name)
                if sql.startswith("INSERT INTO "):
                    paused.set()
                    if not release.wait(10):
                        raise TimeoutError("Delayed copy was not released.")
                return self.client.command(sql, *args, **kwargs)

            def close(self):
                self.client.close()

        def self_cleanup(name):
            self.addCleanup(
                self.client.command, f"DROP TABLE IF EXISTS {quote_clickhouse_identifier(name)} SYNC"
            )

        calls = 0

        def get_client(*args, **kwargs):
            nonlocal calls
            calls += 1
            client = real_get_client(*args, **kwargs)
            return PausedClient(client) if calls == 1 else client

        def copy():
            close_old_connections()
            try:
                _copy_cdc_to_batch_generation(self.checkpoint.pk)
            finally:
                close_old_connections()

        admin = get_user_model().objects.create_user(username="live-cancel", is_staff=True)
        web = Client()
        web.force_login(admin)
        with patch("etl.cdc_to_batch.clickhouse_connect.get_client", side_effect=get_client):
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(copy)
                try:
                    self.assertTrue(paused.wait(10))
                    response = web.post(
                        reverse("control_plane:source_cdc_to_batch_staging", args=[self.source.pk]),
                        {
                            "action": "abandon_pending",
                            "transition": str(self.transition.generation),
                            "confirm": "yes",
                        },
                        follow=True,
                    )
                    self.assertContains(response, "Pending cutover abandoned")
                finally:
                    release.set()
                with self.assertRaisesRegex(RuntimeError, "not eligible for table cutover"):
                    future.result(timeout=10)
        self.transition.refresh_from_db()
        self.source.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.FAILED)
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.assertEqual(_clickhouse_table_uuid(self.client, self.active_name), self.old_uuid)

    def test_lost_copy_response_uses_fresh_generation_on_retry(self):
        _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        real_get_client = clickhouse_connect.get_client

        class LostResponseClient:
            def __init__(self, *args, **kwargs):
                self.client = real_get_client(*args, **kwargs)

            def query(self, *args, **kwargs):
                return self.client.query(*args, **kwargs)

            def command(self, sql, *args, **kwargs):
                result = self.client.command(sql, *args, **kwargs)
                if sql.startswith("INSERT INTO "):
                    raise RuntimeError("Simulated lost copy response")
                return result

            def close(self):
                self.client.close()

        try:
            with patch("etl.cdc_to_batch.clickhouse_connect.get_client", side_effect=LostResponseClient):
                with self.assertRaisesRegex(RuntimeError, "Simulated lost copy response"):
                    _copy_cdc_to_batch_generation(self.checkpoint.pk)
        finally:
            self.checkpoint.refresh_from_db()
            old_candidate = self.checkpoint.generation_table
            if old_candidate:
                self.addCleanup(
                    self.client.command,
                    f"DROP TABLE IF EXISTS {quote_clickhouse_identifier(old_candidate)} SYNC",
                )
        self.assertTrue(old_candidate)
        self.assertFalse(self.checkpoint.generation_uuid)
        with self.assertRaisesRegex(RuntimeError, "Not every generation copy has completed"):
            _finalize_cdc_to_batch_generation(self.transition.pk)
        winner = _copy_cdc_to_batch_generation(self.checkpoint.pk)
        self.addCleanup(
            self.client.command,
            f"DROP TABLE IF EXISTS {quote_clickhouse_identifier(winner.generation_table)} SYNC",
        )
        self.assertNotEqual(winner.generation_table, old_candidate)
        _finalize_cdc_to_batch_generation(self.transition.pk)
        self.table.refresh_from_db()
        self.assertEqual(self.table.clickhouse_physical_table, winner.generation_table)

    @override_settings(
        CDC_TO_BATCH_EXECUTOR_ENABLED=True,
        CDC_TO_BATCH_GENERATION_EXECUTOR_ENABLED=True,
    )
    @patch("etl.tasks._require_cdc_to_batch_deployment_fence")
    @patch("etl.tasks.execute_cdc_to_batch_transition_task.apply_async")
    def test_generation_executor_advances_through_publication(self, enqueue, fence):
        try:
            first = execute_cdc_to_batch_transition_task.apply(
                args=[self.transition.pk], throw=True
            ).get()
            self.assertIn("Cutover is pending", first)
            second = execute_cdc_to_batch_transition_task.apply(
                args=[self.transition.pk], throw=True
            ).get()
            self.assertIn("A table cutover completed", second)
        finally:
            self.checkpoint.refresh_from_db()
            if self.checkpoint.generation_table:
                self.addCleanup(
                    self.client.command,
                    f"DROP TABLE IF EXISTS {quote_clickhouse_identifier(self.checkpoint.generation_table)} SYNC",
                )
        third = execute_cdc_to_batch_transition_task.apply(
            args=[self.transition.pk], throw=True
        ).get()
        self.assertIn("completed", third)
        self.assertEqual(enqueue.call_count, 2)
        self.table.refresh_from_db()
        self.source.refresh_from_db()
        self.assertEqual(self.table.clickhouse_physical_table, self.checkpoint.generation_table)
        self.assertEqual(self.source.ingestion_mode, IngestionMode.BATCH)
        self.assertEqual(_clickhouse_table_uuid(self.client, self.active_name), self.old_uuid)
