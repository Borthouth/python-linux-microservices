import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bson import ObjectId
from django.test import SimpleTestCase, TestCase
import pandas as pd
import sqlalchemy as sa

from control_plane.forms import SourceProfileForm
from control_plane.forms.pipeline_automations import PipelineAutomationForm
from control_plane.models import DbChoice, SourceField, SourceProfile, SourceTable
from etl.tasks import (
    create_source_engine,
    backfill_source_to_clickhouse_task,
    ingest_source_to_clickhouse_task,
    introspect_source,
    parse_ingestion_watermark,
    read_mongo_chunks,
    serialize_ingestion_watermark,
    watermark_is_greater,
)


class SourceProfileFormTests(SimpleTestCase):
    def test_form_offers_mongodb_and_oracle(self):
        choices = dict(SourceProfileForm().fields["db_type"].choices)

        self.assertEqual(choices[DbChoice.MONGODB], "MongoDB")
        self.assertEqual(choices[DbChoice.ORACLE], "Oracle")
        self.assertEqual(
            SourceProfileForm(initial={"db_type": DbChoice.MONGODB})["port"].value(),
            27017,
        )

    def test_pipeline_automation_offers_backfill_task(self):
        task_choices = dict(PipelineAutomationForm().fields["task_path"].choices)

        self.assertIn(
            "etl.tasks.trigger_all_active_clickhouse_backfill_task",
            task_choices,
        )


class SourceEngineTests(SimpleTestCase):
    def test_oracle_engine_uses_service_name(self):
        profile = SimpleNamespace(
            db_type=DbChoice.ORACLE,
            username="app_user",
            password="secret",
            host="localhost",
            port=1522,
            database_name="FREEPDB1",
        )

        engine = create_source_engine(profile)

        try:
            self.assertEqual(engine.url.drivername, "oracle+oracledb")
            self.assertEqual(engine.url.query["service_name"], "FREEPDB1")
        finally:
            engine.dispose()

    @patch("etl.tasks.SourceField.objects.update_or_create")
    @patch("etl.tasks.SourceTable.objects.get_or_create")
    @patch("etl.tasks.sa.inspect")
    @patch("etl.tasks.create_source_engine")
    def test_oracle_introspection_targets_the_user_schema(
        self, create_engine, inspect, get_or_create, update_or_create
    ):
        engine = MagicMock()
        inspector = MagicMock()
        inspector.get_schema_names.return_value = ["APP_USER"]
        inspector.get_table_names.return_value = ["INVOICES"]
        inspector.get_pk_constraint.return_value = {
            "constrained_columns": ["INVOICE_ID"]
        }
        inspector.get_columns.return_value = [
            {"name": "INVOICE_ID", "type": "VARCHAR2(50)"}
        ]
        inspector.get_foreign_keys.return_value = []
        inspect.return_value = inspector
        create_engine.return_value = engine
        source_table = SimpleNamespace(
            primary_key_fields=[],
            save=MagicMock(),
        )
        get_or_create.return_value = (source_table, True)
        profile = SimpleNamespace(
            db_type=DbChoice.ORACLE,
            username="app_user",
            database_name="FREEPDB1",
        )

        with patch("etl.tasks.set_task_progress"):
            introspect_source(profile, "task-id", MagicMock())

        inspector.get_table_names.assert_called_once_with(schema="APP_USER")
        self.assertEqual(source_table.primary_key_fields, ["INVOICE_ID"])
        update_or_create.assert_called_once()
        engine.dispose.assert_called_once()


class MongoIngestionTests(SimpleTestCase):
    def test_documents_are_streamed_with_complex_values_serialized(self):
        object_id = ObjectId()
        collection = MagicMock()
        cursor = MagicMock()
        cursor.sort.return_value = cursor
        cursor.skip.return_value = cursor
        cursor.limit.return_value = cursor
        cursor.batch_size.return_value = iter([
            {"_id": object_id, "metadata": {"browser": "Chrome"}},
        ])
        collection.find.return_value = cursor
        database = MagicMock()
        database.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = database
        profile = SimpleNamespace(database_name="app_analytics")

        with patch("etl.tasks.create_mongo_client", return_value=client):
            chunks = list(
                read_mongo_chunks(
                    profile,
                    "event_logs",
                    ["_id", "metadata"],
                    batch_size=10,
                )
            )

        self.assertEqual(len(chunks), 1)
        cursor.skip.assert_called_once_with(0)
        self.assertEqual(chunks[0].iloc[0]["_id"], str(object_id))
        self.assertEqual(
            json.loads(chunks[0].iloc[0]["metadata"]),
            {"browser": "Chrome"},
        )
        client.close.assert_called_once()


class IngestionWatermarkTests(SimpleTestCase):
    def test_datetime_watermark_round_trips_and_compares_in_utc(self):
        stored = serialize_ingestion_watermark(
            pd.Timestamp("2026-03-01T10:00:00Z")
        )

        self.assertEqual(
            parse_ingestion_watermark(stored).isoformat(),
            "2026-03-01T10:00:00+00:00",
        )
        self.assertTrue(
            watermark_is_greater(
                serialize_ingestion_watermark(
                    pd.Timestamp("2026-03-01T12:00:00+01:00")
                ),
                stored,
            )
        )


class BackfillDispatchTests(SimpleTestCase):
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_backfill_uses_full_refresh_and_dedicated_queue(self, apply_async):
        result = backfill_source_to_clickhouse_task.apply(
            args=[7],
            kwargs={"batch_size": 5000},
            throw=True,
        ).get()

        self.assertIn("Dispatched ClickHouse backfill", result)
        self.assertEqual(apply_async.call_args.kwargs["queue"], "queue_backfill")
        self.assertTrue(apply_async.call_args.kwargs["kwargs"]["full_refresh"])
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["batch_size"], 5000)


class BoundedIngestionTests(TestCase):
    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.create_source_engine")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_ingestion_queues_continuation_after_one_row_batch(
        self, apply_async, get_client, create_engine, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Bounded ingestion source",
            db_type=DbChoice.MYSQL,
            host="localhost",
            port=3306,
            database_name="test_db",
            username="test_user",
        )
        table = SourceTable.objects.create(
            source=source,
            schema_name="test_db",
            table_name="items",
            primary_key_fields=["id"],
        )
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        SourceField.objects.create(table=table, field_name="value", data_type="VARCHAR")

        engine = sa.create_engine("sqlite:///:memory:")
        with engine.begin() as connection:
            connection.execute(sa.text(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, value VARCHAR)"
            ))
            connection.execute(sa.text(
                "INSERT INTO items (id, value) VALUES (1, 'one'), (2, 'two'), (3, 'three')"
            ))
        create_engine.return_value = engine
        clickhouse_client = MagicMock()
        clickhouse_client.query.return_value.result_rows = [[0]]
        get_client.return_value = clickhouse_client

        with patch("etl.tasks.get_etl_logger"):
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk],
                kwargs={"batch_size": 2},
                throw=True,
            ).get()

        self.assertIn("Queued next 2-row ingestion batch", result)
        clickhouse_client.insert.assert_called_once()
        self.assertEqual(len(clickhouse_client.insert.call_args.args[1]), 2)
        continuation = apply_async.call_args.kwargs
        self.assertEqual(continuation["kwargs"]["row_offset"], 0)
        self.assertEqual(continuation["kwargs"]["last_key_values"], ["2"])
        self.assertEqual(continuation["kwargs"]["batch_size"], 2)
        engine.dispose()

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.create_source_engine")
    @patch("etl.tasks.clickhouse_connect.get_client")
    def test_incremental_load_uses_updated_at_watermark(
        self, get_client, create_engine, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Incremental ingestion source",
            db_type=DbChoice.MYSQL,
            host="localhost",
            port=3306,
            database_name="test_db",
            username="test_user",
        )
        table = SourceTable.objects.create(
            source=source,
            schema_name="test_db",
            table_name="items",
            incremental_field_name="updated_at",
            incremental_watermark=serialize_ingestion_watermark(
                datetime(2026, 3, 1, 10, 0)
            ),
            primary_key_fields=["id"],
            incremental_ready=True,
        )
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        SourceField.objects.create(
            table=table,
            field_name="updated_at",
            data_type="DATETIME",
        )
        SourceField.objects.create(table=table, field_name="value", data_type="VARCHAR")

        engine = sa.create_engine("sqlite:///:memory:")
        with engine.begin() as connection:
            connection.execute(sa.text(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, updated_at DATETIME, value VARCHAR)"
            ))
            connection.execute(
                sa.text(
                    "INSERT INTO items (id, updated_at, value) VALUES "
                    "(1, '2026-03-01 11:00:00', 'updated'), "
                    "(2, '2026-03-01 12:00:00', 'new')"
                )
            )
        create_engine.return_value = engine
        clickhouse_client = MagicMock()
        clickhouse_client.query.side_effect = [
            SimpleNamespace(result_rows=[[1]]),
            SimpleNamespace(result_rows=[
                ["id", "Int64"],
                ["updated_at", "String"],
                ["value", "String"],
            ]),
        ]
        get_client.return_value = clickhouse_client

        with patch("etl.tasks.get_etl_logger"):
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk],
                kwargs={"batch_size": 10},
                throw=True,
            ).get()

        self.assertIn("ingestion completed", result)
        clickhouse_client.insert.assert_called_once()
        self.assertTrue(clickhouse_client.insert.call_args.args[0].endswith("items"))
        self.assertEqual(len(clickhouse_client.insert.call_args.args[1]), 2)
        table.refresh_from_db()
        self.assertEqual(
            parse_ingestion_watermark(table.incremental_watermark),
            datetime(2026, 3, 1, 12, 0),
        )
        self.assertTrue(table.clickhouse_uses_replacing_merge_tree)
        engine.dispose()
