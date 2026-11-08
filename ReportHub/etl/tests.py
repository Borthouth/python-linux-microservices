import json
from io import BytesIO
from types import SimpleNamespace
from urllib.error import HTTPError
from unittest.mock import MagicMock, patch

from bson import ObjectId
from django.test import SimpleTestCase, TestCase
import pandas as pd
import sqlalchemy as sa

from control_plane.forms import SourceProfileForm
from control_plane.forms.pipeline_automations import PipelineAutomationForm
from control_plane.models import (
    CdcExecutionFailure,
    DbChoice,
    IngestionMode,
    SourceField,
    SourceProfile,
    SourceTable,
)
from etl.tasks import (
    create_source_engine,
    backfill_source_to_clickhouse_task,
    ingest_source_to_clickhouse_task,
    introspect_source,
    parse_ingestion_watermark,
    read_mongo_chunks,
    serialize_ingestion_watermark,
    trigger_all_active_clickhouse_backfill_task,
    watermark_is_greater,
)
from etl.cdc import (
    _connect_request,
    _coerce_cdc_row,
    _connector_configs_match,
    _create_cdc_table,
    _event_version,
    _required_field_names,
    _record_from_message,
    _topic_name,
    build_debezium_connector_config,
    consume_source_cdc_batch,
    ensure_debezium_connector,
)
from reports.models import ReportDefinition


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

    def test_batch_is_default_ingestion_mode(self):
        self.assertEqual(SourceProfileForm().instance.ingestion_mode, IngestionMode.BATCH)

    def test_oracle_cdb_pdb_and_logminer_fields_are_available(self):
        form = SourceProfileForm()

        self.assertIn("oracle_cdb_name", form.fields)
        self.assertIn("oracle_pdb_name", form.fields)
        self.assertIn("oracle_logminer_username", form.fields)
        self.assertIn("oracle_logminer_password", form.fields)
        self.assertIn("service name", form.fields["database_name"].help_text)

    def test_oracle_container_and_logminer_credentials_must_be_paired(self):
        form = SourceProfileForm(data={
            "name": "Oracle CDC",
            "db_type": DbChoice.ORACLE,
            "host": "oracle.example",
            "port": "1521",
            "database_name": "APP_SERVICE",
            "username": "app_user",
            "password": "app_password",
            "ingestion_mode": IngestionMode.CDC,
            "oracle_cdb_name": "ORCLCDB",
            "oracle_pdb_name": "",
            "oracle_logminer_username": "c##debezium",
            "oracle_logminer_password": "",
        })

        with patch.object(SourceProfile, "validate_unique"):
            self.assertFalse(form.is_valid())
        self.assertIn("oracle_pdb_name", form.errors)
        self.assertIn("oracle_logminer_password", form.errors)


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


class DebeziumConnectorConfigTests(SimpleTestCase):
    def setUp(self):
        self.table = SimpleNamespace(
            schema_name="sales",
            table_name="orders",
            primary_key_fields=["order_id"],
        )

    def profile(self, db_type):
        return SimpleNamespace(
            pk=12,
            db_type=db_type,
            host="source.example",
            port=5432,
            username="cdc_user",
            password="secret",
            database_name="analytics",
            oracle_cdb_name="",
            oracle_pdb_name="",
            oracle_logminer_username="",
            oracle_logminer_password="",
        )

    def test_builds_connector_configs_for_all_supported_databases(self):
        expected_classes = {
            DbChoice.POSTGRESQL: "io.debezium.connector.postgresql.PostgresConnector",
            DbChoice.MYSQL: "io.debezium.connector.mysql.MySqlConnector",
            DbChoice.MSSQL: "io.debezium.connector.sqlserver.SqlServerConnector",
            DbChoice.MONGODB: "io.debezium.connector.mongodb.MongoDbConnector",
            DbChoice.ORACLE: "io.debezium.connector.oracle.OracleConnector",
        }

        for db_type, connector_class in expected_classes.items():
            with self.subTest(db_type=db_type):
                config = build_debezium_connector_config(
                    self.profile(db_type),
                    [self.table],
                )

                self.assertEqual(config["connector.class"], connector_class)
                self.assertEqual(config["snapshot.mode"], "initial")
                self.assertEqual(
                    config["value.converter.schemas.enable"],
                    "false",
                )

    def test_requires_a_discovered_primary_key(self):
        table = SimpleNamespace(
            schema_name="sales",
            table_name="orders",
            primary_key_fields=[],
        )

        with self.assertRaisesMessage(ValueError, "primary key"):
            build_debezium_connector_config(
                self.profile(DbChoice.POSTGRESQL),
                [table],
            )

    def test_oracle_connector_uses_cdb_pdb_and_separate_logminer_credentials(self):
        profile = self.profile(DbChoice.ORACLE)
        profile.database_name = "APP_SERVICE"
        profile.password = "source-password"
        profile.oracle_cdb_name = "ORCLCDB"
        profile.oracle_pdb_name = "APP_PDB"
        profile.oracle_logminer_username = "c##debezium"
        profile.oracle_logminer_password = "logminer-password"

        config = build_debezium_connector_config(profile, [self.table])

        self.assertEqual(config["database.dbname"], "ORCLCDB")
        self.assertEqual(config["database.pdb.name"], "APP_PDB")
        self.assertEqual(config["database.user"], "c##debezium")
        self.assertEqual(config["database.password"], "logminer-password")
        self.assertEqual(profile.database_name, "APP_SERVICE")
        self.assertEqual(profile.username, "cdc_user")
        self.assertEqual(profile.password, "source-password")

    def test_oracle_connector_keeps_legacy_service_and_source_credentials(self):
        profile = self.profile(DbChoice.ORACLE)
        profile.password = "source-password"

        config = build_debezium_connector_config(profile, [self.table])

        self.assertEqual(config["database.dbname"], "analytics")
        self.assertNotIn("database.pdb.name", config)
        self.assertEqual(config["database.user"], "cdc_user")
        self.assertEqual(config["database.password"], "source-password")

    def test_oracle_connector_rejects_partial_cdb_pdb_configuration(self):
        profile = self.profile(DbChoice.ORACLE)
        profile.oracle_cdb_name = "ORCLCDB"

        with self.assertRaisesMessage(ValueError, "both Oracle CDB and PDB"):
            build_debezium_connector_config(profile, [self.table])

    def test_oracle_connector_rejects_partial_logminer_credentials(self):
        profile = self.profile(DbChoice.ORACLE)
        profile.oracle_logminer_username = "c##debezium"

        with self.assertRaisesMessage(ValueError, "both LogMiner credentials"):
            build_debezium_connector_config(profile, [self.table])

    def test_oracle_topic_and_table_filter_use_debezium_identifier_case(self):
        profile = self.profile(DbChoice.ORACLE)
        table = SimpleNamespace(
            schema_name="APP_USER",
            table_name="invoices",
            primary_key_fields=["invoice_id"],
        )

        config = build_debezium_connector_config(profile, [table])

        self.assertEqual(
            _topic_name(profile, table),
            "reporthub-source-12.APP_USER.INVOICES",
        )
        self.assertEqual(config["schema.include.list"], "APP_USER")
        self.assertEqual(config["table.include.list"], r"APP_USER\.INVOICES")

    def test_oracle_topic_preserves_quoted_mixed_case_identifiers(self):
        profile = self.profile(DbChoice.ORACLE)
        table = SimpleNamespace(
            schema_name="SalesRegion",
            table_name="InvoiceItems",
            primary_key_fields=["InvoiceId"],
        )

        config = build_debezium_connector_config(profile, [table])

        self.assertEqual(
            _topic_name(profile, table),
            "reporthub-source-12.SalesRegion.InvoiceItems",
        )
        self.assertEqual(config["schema.include.list"], "SalesRegion")
        self.assertEqual(
            config["table.include.list"],
            r"SalesRegion\.InvoiceItems",
        )

    def test_oracle_event_keys_are_matched_case_insensitively(self):
        table = SimpleNamespace(
            schema_name="APP_USER",
            table_name="invoices",
            primary_key_fields=["invoice_id"],
        )
        message = SimpleNamespace(
            value={"op": "c", "after": {"STATUS": "open"}},
            key={"INVOICE_ID": 42},
            offset=7,
        )

        row = _record_from_message(
            message,
            table,
            ["invoice_id", "status"],
            normalize_identifiers=True,
        )

        self.assertEqual(row["invoice_id"], 42)
        self.assertEqual(row["status"], "open")

    def test_oracle_event_matching_prefers_exact_quoted_identifier_case(self):
        table = SimpleNamespace(
            schema_name="APP_USER",
            table_name="invoices",
            primary_key_fields=["invoice_id"],
        )
        message = SimpleNamespace(
            value={
                "op": "c",
                "after": {"invoice_id": 11, "INVOICE_ID": 22},
            },
            key={"invoice_id": 33},
            offset=7,
        )

        row = _record_from_message(
            message,
            table,
            ["invoice_id"],
            normalize_identifiers=True,
        )

        self.assertEqual(row["invoice_id"], 11)

    def test_oracle_event_matching_rejects_ambiguous_identifier_casing(self):
        table = SimpleNamespace(
            schema_name="APP_USER",
            table_name="invoices",
            primary_key_fields=["invoice_id"],
        )
        message = SimpleNamespace(
            value={"op": "c", "after": {}},
            key={"INVOICE_ID": 11, "Invoice_Id": 22},
            offset=7,
        )

        with self.assertRaisesMessage(RuntimeError, "ambiguous casing"):
            _record_from_message(
                message,
                table,
                ["invoice_id"],
                normalize_identifiers=True,
            )

    @patch("etl.cdc.time.sleep")
    @patch("etl.cdc.time.monotonic", side_effect=[0, 0, 31])
    @patch("etl.cdc._connect_request")
    def test_connector_without_tasks_is_not_reported_ready(
        self, connect_request, monotonic, sleep
    ):
        profile = self.profile(DbChoice.POSTGRESQL)
        profile.tables = SimpleNamespace(all=lambda: [self.table])
        connect_request.side_effect = [
            {"existing": "config"},
            None,
            {"connector": {"state": "RUNNING"}, "tasks": []},
        ]

        with self.assertRaisesMessage(RuntimeError, "did not become ready"):
            ensure_debezium_connector(profile)

    def test_masked_source_password_does_not_restart_the_connector(self):
        desired = {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": "source.example",
            "database.password": "secret",
        }

        self.assertTrue(
            _connector_configs_match(
                {
                    **desired,
                    "database.password": "********",
                },
                desired,
            )
        )
        self.assertFalse(
            _connector_configs_match(
                {
                    **desired,
                    "database.password": "********",
                    "database.hostname": "other.example",
                },
                desired,
            )
        )

    @patch("etl.cdc.urlopen")
    def test_connect_error_includes_response_detail_and_redacts_credentials(
        self, urlopen
    ):
        password = "source-secret"
        urlopen.side_effect = HTTPError(
            "http://connect/connectors",
            400,
            "Bad Request",
            hdrs=None,
            fp=BytesIO(
                json.dumps({
                    "error_code": 400,
                    "message": (
                        f"Invalid value {password} for configuration "
                        "database.password"
                    ),
                }).encode("utf-8")
            ),
        )

        with self.assertRaises(RuntimeError) as raised:
            _connect_request(
                "POST",
                "/connectors",
                {"config": {"database.password": password}},
            )

        self.assertIn("HTTP 400 for POST /connectors", str(raised.exception))
        self.assertIn("Invalid value [redacted]", str(raised.exception))
        self.assertNotIn(password, str(raised.exception))

    @patch("etl.cdc.urlopen")
    def test_connect_error_detail_is_bounded(self, urlopen):
        urlopen.side_effect = HTTPError(
            "http://connect/connectors",
            400,
            "Bad Request",
            hdrs=None,
            fp=BytesIO(json.dumps({"message": "x" * 3000}).encode("utf-8")),
        )

        with self.assertRaises(RuntimeError) as raised:
            _connect_request("POST", "/connectors", {"config": {}})

        detail = str(raised.exception).split("Details: ", 1)[1]
        self.assertLessEqual(len(detail), 2000)

    def test_mongodb_credentials_are_encoded_in_connection_string(self):
        profile = self.profile(DbChoice.MONGODB)
        profile.username = "user@domain"
        profile.password = "pass/word"

        config = build_debezium_connector_config(profile, [self.table])

        self.assertIn("user%40domain:pass%2Fword@", config["mongodb.connection.string"])
        self.assertEqual(config["collection.include.list"], "analytics\\.orders")

    def test_delete_events_use_the_before_image_and_primary_key(self):
        message = SimpleNamespace(
            value={
                "op": "d",
                "before": {"order_id": 42, "status": "cancelled"},
                "source": {"ts_ms": 1_800_000_000_000},
            },
            key={"order_id": 42},
            offset=7,
        )

        row = _record_from_message(
            message,
            self.table,
            ["order_id", "status"],
        )

        self.assertEqual(row["order_id"], 42)
        self.assertEqual(row["status"], "cancelled")
        self.assertEqual(row["_cdc_deleted"], 1)
        self.assertGreater(row["_cdc_version"], 0)

    def test_event_versions_follow_kafka_offsets_without_millisecond_collisions(self):
        self.assertLess(_event_version(999_999), _event_version(1_000_000))

    def test_coerces_debezium_scalar_values_for_clickhouse_types(self):
        table = SimpleNamespace(primary_key_fields=["event_id"])
        row = {
            "event_id": 8,
            "event_date": 20500,
            "created_at": 1_800_000_000_000,
            "enabled": True,
            "amount": "12.5",
        }

        result = _coerce_cdc_row(
            row,
            table,
            {
                "event_id": "INTEGER",
                "event_date": "DATE",
                "created_at": "TIMESTAMP",
                "enabled": "BOOLEAN",
                "amount": "DECIMAL",
            },
        )

        self.assertEqual(result["event_id"], 8)
        self.assertEqual(result["event_date"], "2026-02-16")
        self.assertIn("2027-01-15T", result["created_at"])
        self.assertEqual(result["enabled"], 1)
        self.assertEqual(result["amount"], 12.5)


class CdcReportFieldSelectionTests(TestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="CDC report field source",
            db_type=DbChoice.ORACLE,
            host="oracle.example",
            port=1521,
            database_name="APP_SERVICE",
            username="app_user",
        )
        self.album_table = SourceTable.objects.create(
            source=self.source,
            schema_name="APP_USER",
            table_name="albums",
            primary_key_fields=["album_id"],
        )
        self.track_table = SourceTable.objects.create(
            source=self.source,
            schema_name="APP_USER",
            table_name="tracks",
            primary_key_fields=["track_id"],
        )
        SourceField.objects.create(
            table=self.album_table,
            field_name="album_id",
            data_type="NUMBER",
            is_selected=True,
        )
        SourceField.objects.create(
            table=self.album_table,
            field_name="album_title",
            data_type="VARCHAR2",
            is_selected=False,
        )
        SourceField.objects.create(
            table=self.track_table,
            field_name="track_id",
            data_type="NUMBER",
            is_selected=True,
        )
        SourceField.objects.create(
            table=self.track_table,
            field_name="track_title",
            data_type="VARCHAR2",
            is_selected=False,
        )

    def test_selected_columns_attribution_excludes_joined_fields_from_base(self):
        ReportDefinition.objects.create(
            name="CDC joined report",
            source=self.source,
            base_table=self.album_table,
            selected_fields=["album_title", "track_title"],
            config={
                "selectedColumns": [
                    {
                        "table_id": self.album_table.pk,
                        "field_name": "album_title",
                    },
                    {
                        "table_id": self.track_table.pk,
                        "field_name": "track_title",
                    },
                ],
            },
        )

        required = _required_field_names(self.source, self.album_table)

        self.assertIn("album_id", required)
        self.assertIn("album_title", required)
        self.assertNotIn("track_title", required)

    def test_legacy_selected_fields_are_used_without_attributable_config(self):
        ReportDefinition.objects.create(
            name="Legacy CDC report",
            source=self.source,
            base_table=self.album_table,
            selected_fields=["album_title"],
            config={"selectedColumns": [{"field_name": "album_title"}]},
        )

        required = _required_field_names(self.source, self.album_table)

        self.assertIn("album_title", required)
        self.assertNotIn("track_title", required)


class CdcConsumerTests(SimpleTestCase):
    def setUp(self):
        self.table = SimpleNamespace(
            pk=4,
            schema_name="public",
            table_name="events",
            primary_key_fields=["event_id"],
            fields=SimpleNamespace(all=lambda: [
                SimpleNamespace(field_name="event_id", data_type="INTEGER"),
                SimpleNamespace(field_name="value", data_type="VARCHAR"),
            ]),
            save=MagicMock(),
        )
        self.profile = SimpleNamespace(
            pk=12,
            db_type=DbChoice.POSTGRESQL,
            database_name="analytics",
            tables=SimpleNamespace(all=lambda: [self.table]),
            relationships=SimpleNamespace(all=lambda: []),
        )
        self.message = SimpleNamespace(
            topic="reporthub-source-12.public.events",
            partition=0,
            offset=7,
            key={"event_id": 1},
            value={
                "op": "c",
                "after": {"event_id": 1, "value": "created"},
                "source": {"ts_ms": 1_800_000_000_000},
            },
        )
        self.consumer = MagicMock()
        self.consumer.assignment.return_value = {("topic", 0)}
        self.consumer.poll.return_value = {object(): [self.message]}

    def test_existing_batch_table_is_not_dropped_for_cdc(self):
        self.table.cdc_managed = False
        clickhouse_client = MagicMock()
        clickhouse_client.query.return_value.result_rows = [[1]]

        with self.assertRaisesMessage(RuntimeError, "coordinated CDC snapshot"):
            _create_cdc_table(
                clickhouse_client, self.profile, self.table, ["event_id", "value"]
            )

        clickhouse_client.command.assert_not_called()
        self.table.save.assert_not_called()

    def test_new_cdc_table_is_created(self):
        self.table.cdc_managed = False
        clickhouse_client = MagicMock()
        clickhouse_client.query.return_value.result_rows = [[0]]

        _create_cdc_table(
            clickhouse_client, self.profile, self.table, ["event_id", "value"]
        )

        self.assertIn("CREATE TABLE", clickhouse_client.command.call_args.args[0])
        self.assertTrue(self.table.cdc_managed)

    def test_missing_cdc_table_is_not_recreated_with_old_offsets(self):
        self.table.cdc_managed = True
        clickhouse_client = MagicMock()
        clickhouse_client.query.return_value.result_rows = [[0]]

        with self.assertRaisesMessage(RuntimeError, "fresh coordinated CDC snapshot"):
            _create_cdc_table(
                clickhouse_client, self.profile, self.table, ["event_id", "value"]
            )

        clickhouse_client.command.assert_not_called()
        self.table.save.assert_not_called()

    @patch("etl.cdc._create_cdc_table", return_value=("`warehouse_events`", ["event_id", "value"]))
    @patch("etl.cdc._required_field_names", return_value=["event_id", "value"])
    @patch("etl.cdc.KafkaConsumer")
    def test_commits_offsets_after_clickhouse_insert(
        self, kafka_consumer, required_fields, create_table
    ):
        kafka_consumer.return_value = self.consumer
        clickhouse_client = MagicMock()

        processed = consume_source_cdc_batch(
            self.profile,
            clickhouse_client,
            batch_size=10,
        )

        self.assertEqual(processed, 1)
        clickhouse_client.insert.assert_called_once()
        self.consumer.commit.assert_called_once()
        self.consumer.close.assert_called_once()

    @patch("etl.cdc._create_cdc_table", return_value=("`warehouse_events`", ["event_id", "value"]))
    @patch("etl.cdc._required_field_names", return_value=["event_id", "value"])
    @patch("etl.cdc.KafkaConsumer")
    def test_does_not_commit_offsets_when_clickhouse_write_fails(
        self, kafka_consumer, required_fields, create_table
    ):
        kafka_consumer.return_value = self.consumer
        clickhouse_client = MagicMock()
        clickhouse_client.insert.side_effect = RuntimeError("insert failed")

        with self.assertRaisesMessage(RuntimeError, "insert failed"):
            consume_source_cdc_batch(
                self.profile,
                clickhouse_client,
                batch_size=10,
            )

        self.consumer.commit.assert_not_called()
        self.consumer.close.assert_called_once()

    @patch("etl.cdc._create_cdc_table", return_value=("`warehouse_events`", ["event_id", "value"]))
    @patch("etl.cdc._required_field_names", return_value=["event_id", "value"])
    @patch("etl.cdc.KafkaConsumer")
    def test_missing_partition_assignment_is_a_failure(
        self, kafka_consumer, required_fields, create_table
    ):
        kafka_consumer.return_value = self.consumer
        self.consumer.assignment.return_value = set()
        self.consumer.poll.return_value = {}
        with self.assertRaisesMessage(RuntimeError, "did not receive a partition assignment"):
            consume_source_cdc_batch(self.profile, MagicMock(), batch_size=10)
        self.consumer.commit.assert_not_called()
        self.consumer.close.assert_called_once()


class BackfillDispatchTests(TestCase):
    def test_existing_source_mode_change_is_rejected(self):
        source = SourceProfile.objects.create(
            name="Mode change guard",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
        )
        form = SourceProfileForm(data={
            "name": source.name,
            "db_type": source.db_type,
            "host": source.host,
            "port": source.port,
            "database_name": source.database_name,
            "username": source.username,
            "is_active": True,
            "ingestion_mode": IngestionMode.CDC,
        }, instance=source)
        self.assertFalse(form.is_valid())
        self.assertIn("Changing ingestion mode", str(form.errors["ingestion_mode"]))
        source.refresh_from_db()
        self.assertEqual(source.ingestion_mode, IngestionMode.BATCH)

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

    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_backfill_skips_sources_with_primary_key_tables(self, apply_async):
        source = SourceProfile.objects.create(
            name="CDC backfill dispatch source",
            ingestion_mode=IngestionMode.CDC,
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
        )
        SourceTable.objects.create(
            source=source,
            schema_name="public",
            table_name="items",
            primary_key_fields=["id"],
        )

        with patch("etl.tasks.get_etl_logger"):
            result = backfill_source_to_clickhouse_task.apply(
                args=[source.pk],
                throw=True,
            ).get()

        self.assertIn("Skipped ClickHouse backfill", result)
        apply_async.assert_not_called()

    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_backfill_allows_mongodb_collections_with_primary_keys(self, apply_async):
        source = SourceProfile.objects.create(
            name="Standalone MongoDB backfill",
            db_type=DbChoice.MONGODB,
            host="mongo-test",
            port=27017,
            database_name="test_db",
            username="test_user",
        )
        SourceTable.objects.create(
            source=source,
            schema_name="test_db",
            table_name="items",
            primary_key_fields=["_id"],
        )

        result = backfill_source_to_clickhouse_task.apply(
            args=[source.pk], throw=True
        ).get()

        self.assertIn("Dispatched ClickHouse backfill", result)
        self.assertTrue(apply_async.call_args.kwargs["kwargs"]["full_refresh"])

    @patch("etl.tasks.backfill_source_to_clickhouse_task.delay")
    def test_backfill_all_skips_sources_with_primary_key_tables(self, dispatch):
        keyed_source = SourceProfile.objects.create(
            name="Keyed backfill source",
            ingestion_mode=IngestionMode.CDC,
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
        )
        keyless_source = SourceProfile.objects.create(
            name="Keyless backfill source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
        )
        SourceTable.objects.create(
            source=keyed_source,
            schema_name="public",
            table_name="items",
            primary_key_fields=["id"],
        )

        with patch("etl.tasks.get_etl_logger"):
            result = trigger_all_active_clickhouse_backfill_task.apply(
                throw=True,
            ).get()

        dispatch.assert_called_once_with(keyless_source.pk, batch_size=None)
        self.assertIn("skipped 1 source(s)", result)


class BoundedIngestionTests(TestCase):
    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.create_source_engine")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_batch_source_reads_keyed_rows_in_bounded_refresh_pages(
        self, apply_async, get_client, create_engine, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Batch SQL refresh",
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
        engine = sa.create_engine("sqlite:///:memory:")
        with engine.begin() as connection:
            connection.execute(sa.text("CREATE TABLE items (id INTEGER PRIMARY KEY)"))
            connection.execute(sa.text("INSERT INTO items (id) VALUES (1), (2), (3)"))
        create_engine.return_value = engine
        client = MagicMock()
        client.query.return_value.result_rows = [[0]]
        get_client.return_value = client

        with patch("etl.tasks.get_etl_logger"), patch(
            "etl.cdc.ensure_debezium_connector"
        ) as connector:
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk], kwargs={"batch_size": 2}, throw=True
            ).get()
        self.assertIn("Queued next 2-row", result)
        connector.assert_not_called()
        self.assertEqual(client.insert.call_args.kwargs["column_names"], ["id"])
        self.assertEqual(client.insert.call_args.args[1], [[1], [2]])
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["last_key_values"], ["2"])
        self.assertTrue(apply_async.call_args.kwargs["kwargs"]["full_refresh"])
        self.assertTrue(any("ENGINE = MergeTree()" in call.args[0]
                            for call in client.command.call_args_list))

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks._ingest_source_to_clickhouse_batch")
    @patch("etl.tasks._run_scheduled_cdc_ingestion")
    def test_batch_source_routes_continuations_without_cdc(
        self, run_cdc, batch, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Batch source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
        )
        batch.return_value = "batch complete"
        result = ingest_source_to_clickhouse_task.apply(
            args=[source.pk],
            kwargs={
                "ingestion_run_id": "run123",
                "last_key_values": ["42"],
                "batch_size": 5,
            },
            throw=True,
        ).get()
        self.assertEqual(result, "batch complete")
        self.assertEqual(batch.call_args.kwargs["last_key_values"], ["42"])
        self.assertTrue(batch.call_args.kwargs["full_refresh"])
        run_cdc.assert_not_called()

    @patch("etl.tasks.set_task_progress")
    def test_batch_mode_does_not_replace_a_relational_cdc_table(self, set_progress):
        source = SourceProfile.objects.create(
            name="Batch with existing CDC data",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
        )
        SourceTable.objects.create(
            source=source,
            schema_name="public",
            table_name="items",
            primary_key_fields=["id"],
            cdc_managed=True,
        )
        with patch("etl.tasks.get_etl_logger"), patch(
            "etl.tasks.clickhouse_connect.get_client"
        ) as get_client:
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk], throw=False
            )
        self.assertIn("coordinated mode transition", str(result.result))
        get_client.assert_not_called()

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks._run_scheduled_cdc_ingestion")
    def test_cdc_failure_is_recorded_separately_from_source_sync_status(
        self, run_cdc, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Failed CDC source",
            ingestion_mode=IngestionMode.CDC,
            db_type=DbChoice.MYSQL,
            host="localhost",
            port=3306,
            database_name="test_db",
            username="test_user",
        )
        run_cdc.side_effect = RuntimeError("sensitive source credentials")
        with patch("etl.tasks.get_etl_logger"):
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk], throw=False
            )
        self.assertIsInstance(result.result, RuntimeError)
        failure = CdcExecutionFailure.objects.get(source=source)
        self.assertEqual(failure.task_id, result.id)
        self.assertIn("RuntimeError", failure.error_message)
        self.assertNotIn("sensitive source credentials", failure.error_message)

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.read_mongo_chunks")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_mongodb_scheduled_ingestion_refreshes_all_pages_without_cdc(
        self, apply_async, get_client, read_chunks, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Standalone MongoDB source",
            db_type=DbChoice.MONGODB,
            host="mongo-test",
            port=27017,
            database_name="test_db",
            username="test_user",
        )
        table = SourceTable.objects.create(
            source=source,
            schema_name="test_db",
            table_name="items",
            primary_key_fields=["_id"],
            incremental_field_name="updated_at",
            incremental_watermark='"2026-01-01"',
            incremental_ready=True,
            cdc_managed=True,
            clickhouse_uses_replacing_merge_tree=True,
        )
        SourceField.objects.create(table=table, field_name="_id", data_type="ObjectId")
        SourceField.objects.create(
            table=table, field_name="updated_at", data_type="DateTime"
        )
        clickhouse_client = MagicMock()
        clickhouse_client.query.return_value.result_rows = [[1]]
        get_client.return_value = clickhouse_client
        read_chunks.side_effect = [
            iter([pd.DataFrame([{"_id": "string-id", "updated_at": "2026-01-01"}])]),
            iter([]),
        ]

        with patch("etl.tasks.get_etl_logger"), patch(
            "etl.cdc.ensure_debezium_connector"
        ) as ensure_connector:
            ingest_source_to_clickhouse_task.apply(
                args=[source.pk], kwargs={"batch_size": 1}, throw=True
            ).get()
            continuation = apply_async.call_args.kwargs["kwargs"]
            self.assertEqual(continuation["row_offset"], 1)
            self.assertIsNone(continuation["last_key_values"])
            ingest_source_to_clickhouse_task.apply(
                args=[source.pk], kwargs=continuation, throw=True
            ).get()

        ensure_connector.assert_not_called()
        self.assertEqual(read_chunks.call_args_list[0].kwargs["skip"], 0)
        self.assertEqual(read_chunks.call_args_list[1].kwargs["skip"], 1)
        self.assertEqual(read_chunks.call_args_list[1].kwargs["query_filter"], {})
        commands = [call.args[0] for call in clickhouse_client.command.call_args_list]
        self.assertTrue(any("ENGINE = MergeTree()" in cmd for cmd in commands))
        self.assertTrue(any(cmd.startswith("EXCHANGE TABLES") for cmd in commands))
        table.refresh_from_db()
        self.assertFalse(table.cdc_managed)
        self.assertFalse(table.incremental_ready)
        self.assertIsNone(table.incremental_watermark)
        self.assertFalse(table.clickhouse_uses_replacing_merge_tree)

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.cache.add", return_value=False)
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_scheduled_cdc_batches_retry_when_source_lock_is_held(
        self, apply_async, cache_add, get_client, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Locked CDC source",
            ingestion_mode=IngestionMode.CDC,
            db_type=DbChoice.MYSQL,
            host="localhost",
            port=3306,
            database_name="test_db",
            username="test_user",
        )
        clickhouse_client = MagicMock()
        get_client.return_value = clickhouse_client

        with patch("etl.tasks.get_etl_logger"):
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk],
                kwargs={"batch_size": 10},
                throw=True,
            ).get()

        self.assertIn("already running", result)
        apply_async.assert_called_once()
        self.assertEqual(apply_async.call_args.kwargs["countdown"], 10)
        clickhouse_client.close.assert_called_once()

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.create_source_engine")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_ingestion_queues_continuation_after_one_row_batch(
        self, apply_async, get_client, create_engine, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Bounded ingestion source",
            ingestion_mode=IngestionMode.CDC,
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
            cdc_managed=True,
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
                kwargs={
                    "batch_size": 2,
                    "ingestion_run_id": "test-run",
                },
                throw=True,
            ).get()

        self.assertIn("Queued next 2-row ingestion batch", result)
        clickhouse_client.insert.assert_called_once()
        self.assertEqual(len(clickhouse_client.insert.call_args.args[1]), 2)
        self.assertEqual(
            clickhouse_client.insert.call_args.kwargs["column_names"][-2:],
            ["_cdc_version", "_cdc_deleted"],
        )
        continuation = apply_async.call_args.kwargs
        self.assertEqual(continuation["kwargs"]["row_offset"], 0)
        self.assertEqual(continuation["kwargs"]["last_key_values"], ["2"])
        self.assertEqual(continuation["kwargs"]["batch_size"], 2)
        engine.dispose()

    @patch("etl.tasks.set_task_progress")
    def test_full_refresh_is_rejected_for_sources_with_primary_keys(
        self, set_progress
    ):
        source = SourceProfile.objects.create(
            name="CDC backfill guard source",
            ingestion_mode=IngestionMode.CDC,
            db_type=DbChoice.MYSQL,
            host="localhost",
            port=3306,
            database_name="test_db",
            username="test_user",
        )
        SourceTable.objects.create(
            source=source,
            schema_name="test_db",
            table_name="items",
            primary_key_fields=["id"],
        )

        with patch("etl.tasks.get_etl_logger"):
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk],
                kwargs={"full_refresh": True},
                throw=False,
            )

        self.assertIsInstance(result.result, RuntimeError)
        self.assertIn("coordinated CDC snapshot", str(result.result))

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_scheduled_ingestion_uses_cdc_for_keyed_tables(
        self, apply_async, get_client, set_progress
    ):
        source = SourceProfile.objects.create(
            name="CDC ingestion source",
            ingestion_mode=IngestionMode.CDC,
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
        clickhouse_client = MagicMock()
        get_client.return_value = clickhouse_client

        with patch("etl.tasks.get_etl_logger"), patch(
            "etl.cdc.ensure_debezium_connector"
        ) as ensure_connector, patch(
            "etl.cdc.consume_source_cdc_batch",
            return_value=2,
        ) as consume_cdc:
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk],
                kwargs={"batch_size": 2},
                throw=True,
            ).get()

        self.assertIn("Processed 2 CDC event(s)", result)
        ensure_connector.assert_called_once()
        consume_cdc.assert_called_once()
        self.assertTrue(apply_async.call_args.kwargs["kwargs"]["cdc_continue"])
        clickhouse_client.close.assert_called_once()

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_scheduled_ingestion_queues_full_refresh_for_tables_without_keys(
        self, apply_async, get_client, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Keyless ingestion source",
            ingestion_mode=IngestionMode.CDC,
            db_type=DbChoice.MYSQL,
            host="localhost",
            port=3306,
            database_name="test_db",
            username="test_user",
        )
        SourceTable.objects.create(
            source=source,
            schema_name="test_db",
            table_name="unkeyed_items",
        )
        clickhouse_client = MagicMock()
        get_client.return_value = clickhouse_client

        with patch("etl.tasks.get_etl_logger"), patch(
            "etl.cdc.ensure_debezium_connector"
        ) as ensure_connector, patch(
            "etl.cdc.consume_source_cdc_batch",
            return_value=0,
        ):
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk],
                throw=True,
            ).get()

        self.assertIn("queued polling fallback tables", result)
        ensure_connector.assert_called_once()
        self.assertTrue(
            apply_async.call_args.kwargs["kwargs"]["cdc_fallback_only"]
        )
