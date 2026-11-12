"""Opt-in integration test using a dedicated PostgreSQL table and CDC connector."""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from unittest import skipUnless
from unittest.mock import MagicMock, patch
from uuid import uuid4

import clickhouse_connect
import sqlalchemy as sa
from django.conf import settings
from django.db import close_old_connections
from django.test import SimpleTestCase, TransactionTestCase, override_settings
from kafka.admin import KafkaAdminClient

from control_plane.models import DbChoice, IngestionMode, SourceField, SourceProfile, SourceTable
from etl.cdc import _connect_request, _connector_name, _topic_name, build_debezium_connector_config
from etl.cdc_to_batch import (
    _copy_cdc_to_batch_generation,
    _finalize_cdc_to_batch_generation,
    _mark_cdc_to_batch_cutover_pending,
    prepare_cdc_to_batch_transition,
    stage_cdc_to_batch_table,
)
from etl.tasks import ingest_source_to_clickhouse_task
from reports.cutover_guard import report_query_guard
from reports.models import ReportDefinition
from reports.query_builder import (
    build_clickhouse_report_query,
    quote_clickhouse_identifier,
    source_table_clickhouse_name,
)


@skipUnless(
    os.environ.get("REPORTHUB_LIVE_CDC_STREAM_TEST") == "1",
    "Set REPORTHUB_LIVE_CDC_STREAM_TEST=1 for the isolated PostgreSQL CDC stream test.",
)
@override_settings(CDC_EXTERNAL_OPERATION_TRACKING=True)
class CdcToBatchPostgresStreamTest(TransactionTestCase):
    def setUp(self):
        suffix = uuid4().hex
        self.source = SourceProfile.objects.create(
            pk=uuid4().int % (2**62) + 2**62,
            name=f"stream-test-{suffix}",
            db_type=DbChoice.POSTGRESQL,
            host=os.environ.get("REPORTHUB_TEST_PG_HOST", "postgres-test"),
            port=int(os.environ.get("REPORTHUB_TEST_PG_PORT", "5432")),
            database_name=os.environ.get("REPORTHUB_TEST_PG_DATABASE", "chinook"),
            username=os.environ.get("REPORTHUB_TEST_PG_USER", "test_user"),
            password=os.environ.get("REPORTHUB_TEST_PG_PASSWORD", "test_password"),
            ingestion_mode=IngestionMode.CDC,
        )
        self.table = SourceTable.objects.create(
            source=self.source, schema_name="public", table_name=f"rh_stream_{suffix}",
            primary_key_fields=["id"],
        )
        SourceField.objects.create(table=self.table, field_name="id", data_type="INTEGER")
        SourceField.objects.create(table=self.table, field_name="value", data_type="TEXT")
        self.connector = _connector_name(self.source)
        self.topic = _topic_name(self.source, self.table)
        self.slot = f"reporthub_{self.source.pk}"
        self.source_engine = sa.create_engine(sa.URL.create(
            "postgresql+psycopg2", username=self.source.username, password=self.source.password,
            host=self.source.host, port=self.source.port, database=self.source.database_name,
        ))
        self.addCleanup(self.source_engine.dispose)
        self.ch = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST, port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER, password=settings.CLICKHOUSE_PASSWORD,
        )
        self.addCleanup(self.ch.close)
        active_name = quote_clickhouse_identifier(source_table_clickhouse_name(self.table))
        self.assertEqual(self.ch.query(f"EXISTS TABLE {active_name}").result_rows, [(0,)])
        self.addCleanup(
            self.ch.command, f"DROP TABLE IF EXISTS {active_name} SYNC",
        )
        self.admin = KafkaAdminClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","))
        self.addCleanup(self.admin.close)
        self.assertIsNone(_connect_request("GET", f"/connectors/{self.connector}/config"))
        self.assertNotIn(self.topic, self.admin.list_topics())
        with self.source_engine.begin() as conn:
            self.assertFalse(conn.execute(sa.text(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = :name"
            ), {"name": self.slot}).first())
            self.assertFalse(conn.execute(sa.text(
                "SELECT 1 FROM pg_publication WHERE pubname = :name"
            ), {"name": self.slot}).first())
            conn.execute(sa.text(
                f'CREATE TABLE public."{self.table.table_name}" '
                "(id INTEGER PRIMARY KEY, value TEXT)"
            ))
        self.addCleanup(self._drop_source_table)
        self.connector_created = False
        self.source_cleanup_safe = True
        self.addCleanup(self._cleanup_connector)

    def _drop_source_table(self):
        if not self.source_cleanup_safe:
            raise RuntimeError(
                f"Retaining test source table {self.table.schema_name}.{self.table.table_name}: "
                f"connector {self.connector} may still be active or its outcome is uncertain."
            )
        with self.source_engine.begin() as conn:
            conn.execute(sa.text(f'DROP TABLE IF EXISTS public."{self.table.table_name}"'))

    def _create_connector(self, config):
        self.source_cleanup_safe = False
        try:
            _connect_request("POST", "/connectors", {"name": self.connector, "config": config})
        except RuntimeError as exc:
            raise RuntimeError(
                f"Test connector creation was not confirmed; inspect {self.connector} "
                f"and test slot {self.slot} before cleanup."
            ) from exc
        self.connector_created = True

    def _cleanup_connector(self):
        if not self.connector_created:
            return
        _connect_request("DELETE", f"/connectors/{self.connector}")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            with self.source_engine.connect() as conn:
                active = conn.execute(sa.text(
                    "SELECT active FROM pg_replication_slots WHERE slot_name = :name"
                ), {"name": self.slot}).scalar()
            if not active and _connect_request(
                "GET", f"/connectors/{self.connector}/config"
            ) is None:
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"Test connector slot {self.slot} is still active.")
        with self.source_engine.begin() as conn:
            conn.execute(sa.text(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = :name"
            ), {"name": self.slot})
            conn.execute(sa.text(f'DROP PUBLICATION IF EXISTS "{self.slot}"'))
        if self.topic in self.admin.list_topics():
            self.admin.delete_topics([self.topic])
        self.source_cleanup_safe = True

    def _ingest_until(self, expected):
        name = quote_clickhouse_identifier(source_table_clickhouse_name(self.table))
        deadline = time.monotonic() + 90
        rows = None
        while time.monotonic() < deadline:
            ingest_source_to_clickhouse_task.apply(
                args=[self.source.pk],
                kwargs={"batch_size": 1000, "origin_mode": IngestionMode.CDC},
                throw=True,
            ).get()
            if self.ch.query(f"EXISTS TABLE {name}").result_rows[0][0]:
                rows = self.ch.query(
                    f"SELECT id, value FROM {name} FINAL WHERE _cdc_deleted = 0 ORDER BY id"
                ).result_rows
                if rows == expected:
                    return
            time.sleep(1)
        self.fail(f"CDC rows did not converge: {rows!r}")

    def _record_created_tables(self, function, checkpoint_pk, after_insert=None):
        real_get_client = clickhouse_connect.get_client

        def make_client(*args, **kwargs):
            client = real_get_client(*args, **kwargs)
            command = client.command

            def track_create(sql, *args, **kwargs):
                if sql.startswith("CREATE TABLE `"):
                    name = sql.split("`")[1]
                    quoted = quote_clickhouse_identifier(name)
                    if self.ch.query(f"EXISTS TABLE {quoted}").result_rows != [(0,)]:
                        raise RuntimeError(f"Test table {name} already exists.")
                    result = command(sql, *args, **kwargs)
                    self.addCleanup(
                        self.ch.command, f"DROP TABLE IF EXISTS {quoted} SYNC",
                    )
                    return result
                return command(sql, *args, **kwargs)

            client.command = track_create
            if after_insert is not None:
                insert = client.insert

                def track_insert(*args, **kwargs):
                    result = insert(*args, **kwargs)
                    after_insert()
                    return result

                client.insert = track_insert
            return client

        with patch("etl.cdc_to_batch.clickhouse_connect", SimpleNamespace(get_client=make_client)):
            return function(checkpoint_pk)

    def _report_rows(self, report):
        with report_query_guard(report) as current:
            query, _, _ = build_clickhouse_report_query(current, ["id", "value"])
            return sorted(self.ch.query(query).result_rows)

    def test_stream_stage_publish_and_refresh(self):
        old_table = source_table_clickhouse_name(self.table)
        report = ReportDefinition.objects.create(
            name=f"stream-report-{uuid4().hex}", source=self.source, base_table=self.table,
            selected_fields=["id", "value"],
        )
        with self.source_engine.begin() as conn:
            conn.execute(sa.text(
                f'INSERT INTO public."{self.table.table_name}" VALUES (1, :value), (2, :other)'
            ), {"value": "original", "other": "delete-me"})
        config = build_debezium_connector_config(self.source, [self.table])
        self._create_connector(config)
        self._ingest_until([(1, "original"), (2, "delete-me")])
        transition = prepare_cdc_to_batch_transition(self.source.pk)
        checkpoint = transition.table_checkpoints.get()
        staged_insert = Event()
        resume_stage = Event()

        def pause_insert():
            staged_insert.set()
            if not resume_stage.wait(120):
                raise TimeoutError("Timed out waiting to resume the staging insert.")

        def stage():
            close_old_connections()
            try:
                return self._record_created_tables(
                    stage_cdc_to_batch_table, checkpoint.pk, after_insert=pause_insert
                )
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=1) as pool:
            staging = pool.submit(stage)
            try:
                if not staged_insert.wait(60):
                    if staging.done():
                        staging.result()
                    self.fail("Staging did not reach a ClickHouse insert.")
                with self.source_engine.begin() as conn:
                    conn.execute(sa.text(
                        f'UPDATE public."{self.table.table_name}" SET value = :value WHERE id = 1'
                    ), {"value": "updated"})
                    conn.execute(sa.text(f'DELETE FROM public."{self.table.table_name}" WHERE id = 2'))
                    conn.execute(sa.text(
                        f'INSERT INTO public."{self.table.table_name}" VALUES (3, :value)'
                    ), {"value": "inserted"})
                self._ingest_until([(1, "updated"), (3, "inserted")])
                self.assertFalse(staging.done())
            finally:
                resume_stage.set()
            checkpoint = staging.result(timeout=60)
        self.assertEqual(self._report_rows(report), [(1, "updated"), (3, "inserted")])
        _mark_cdc_to_batch_cutover_pending(transition.pk)
        skipped = ingest_source_to_clickhouse_task.apply(
            args=[self.source.pk], kwargs={"origin_mode": IngestionMode.CDC}, throw=True
        ).get()
        self.assertIn("cutover is pending", skipped)
        with self.assertRaisesRegex(RuntimeError, "cutover is pending"):
            self._report_rows(report)
        candidate = self._record_created_tables(_copy_cdc_to_batch_generation, checkpoint.pk)
        _finalize_cdc_to_batch_generation(transition.pk)
        self.source.refresh_from_db()
        self.table.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.BATCH)
        self.assertEqual(self.table.clickhouse_physical_table, candidate.generation_table)
        name = quote_clickhouse_identifier(candidate.generation_table)
        self.assertEqual(
            self.ch.query(f"SELECT id, value FROM {name} ORDER BY id").result_rows,
            [(1, "original"), (2, "delete-me")],
        )
        self.assertEqual(self._report_rows(report), [(1, "original"), (2, "delete-me")])
        self.ch.insert(
            old_table, [[1, "late-old-write", 2**50, 0]],
            column_names=["id", "value", "_cdc_version", "_cdc_deleted"],
        )
        self.assertEqual(
            self.ch.query(f"SELECT id, value FROM {name} ORDER BY id").result_rows,
            [(1, "original"), (2, "delete-me")],
        )
        self.assertEqual(self._report_rows(report), [(1, "original"), (2, "delete-me")])
        with self.assertRaisesRegex(RuntimeError, "different source mode"):
            ingest_source_to_clickhouse_task.apply(
                args=[self.source.pk], kwargs={"origin_mode": IngestionMode.CDC}, throw=True
            )
        ingest_source_to_clickhouse_task.apply(
            args=[self.source.pk],
            kwargs={"batch_size": 1000, "origin_mode": IngestionMode.BATCH}, throw=True,
        ).get()
        self.assertEqual(
            self.ch.query(f"SELECT id, value FROM {name} ORDER BY id").result_rows,
            [(1, "updated"), (3, "inserted")],
        )
        self.assertEqual(self._report_rows(report), [(1, "updated"), (3, "inserted")])


class CdcStreamTestCleanupTests(SimpleTestCase):
    def setUp(self):
        self.fixture = SimpleNamespace(
            connector="reporthub-source-test", slot="reporthub_test", topic="test-topic",
            table=SimpleNamespace(schema_name="public", table_name="rh_stream_test"),
            connector_created=False, source_cleanup_safe=True, source_engine=MagicMock(),
        )

    @patch("tests.integration.test_cdc_to_batch_stream_live._connect_request", side_effect=RuntimeError("lost response"))
    def test_unconfirmed_creation_retains_source_table(self, connect_request):
        with self.assertRaisesRegex(RuntimeError, "creation was not confirmed"):
            CdcToBatchPostgresStreamTest._create_connector(self.fixture, {})
        self.assertFalse(self.fixture.source_cleanup_safe)
        CdcToBatchPostgresStreamTest._cleanup_connector(self.fixture)
        with self.assertRaisesRegex(RuntimeError, "Retaining test source table"):
            CdcToBatchPostgresStreamTest._drop_source_table(self.fixture)
        self.fixture.source_engine.begin.assert_not_called()

    @patch("tests.integration.test_cdc_to_batch_stream_live._connect_request", side_effect=RuntimeError("lost response"))
    def test_unconfirmed_deletion_retains_source_table(self, connect_request):
        self.fixture.connector_created = True
        self.fixture.source_cleanup_safe = False
        with self.assertRaisesRegex(RuntimeError, "lost response"):
            CdcToBatchPostgresStreamTest._cleanup_connector(self.fixture)
        with self.assertRaisesRegex(RuntimeError, "Retaining test source table"):
            CdcToBatchPostgresStreamTest._drop_source_table(self.fixture)
        self.fixture.source_engine.begin.assert_not_called()

    def test_unstarted_connector_allows_source_table_cleanup(self):
        CdcToBatchPostgresStreamTest._drop_source_table(self.fixture)
        self.fixture.source_engine.begin.assert_called_once()
