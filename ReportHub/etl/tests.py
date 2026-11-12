import json
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from threading import Barrier, Event
from types import SimpleNamespace
from urllib.error import HTTPError
from uuid import uuid4
from unittest.mock import MagicMock, patch

from bson import ObjectId
from django.db import close_old_connections, connection, transaction
from django.db.models.deletion import ProtectedError
from django.test import (
    SimpleTestCase, TestCase, TransactionTestCase, override_settings, skipUnlessDBFeature,
)
from django.utils import timezone
from kafka import TopicPartition
import pandas as pd
import sqlalchemy as sa

from control_plane.forms import SourceProfileForm
from control_plane.forms.pipeline_automations import PipelineAutomationForm
from control_plane.metadata_guard import (
    SourceMetadataCutoverPending,
    acquire_source_metadata_lock,
    source_metadata_write_guard,
)
from control_plane.models import (
    CdcExecutionFailure,
    CdcSourceExternalOperation,
    CdcTableRebuild,
    CdcToBatchTableCheckpoint,
    CdcToBatchTransition,
    DbChoice,
    IngestionMode,
    SourceField,
    SourceProfile,
    SourceTable,
)
from etl.tasks import (
    _run_scheduled_cdc_ingestion,
    _rebuild_cdc_table,
    create_source_engine,
    execute_cdc_to_batch_transition_task,
    backfill_source_to_clickhouse_task,
    ingest_source_to_clickhouse_task,
    introspect_source,
    introspect_source_schema_task,
    map_to_clickhouse_type,
    parse_ingestion_watermark,
    read_mongo_chunks,
    rebuild_cdc_table_task,
    stage_cdc_to_batch_transition_task,
    serialize_ingestion_watermark,
    trigger_all_active_clickhouse_backfill_task,
    trigger_all_active_clickhouse_ingestion_task,
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
    consume_cdc_table_rebuild_batch,
    consume_source_cdc_batch,
    ensure_debezium_connector,
)
from etl.cdc_to_batch import (
    CdcToBatchCutoverAction,
    CdcToBatchStepResult,
    _field_signature,
    _copy_cdc_to_batch_generation,
    _cutover_cdc_to_batch_table,
    _finalize_cdc_to_batch_generation,
    _finalize_cdc_to_batch_transition,
    _mark_cdc_to_batch_cutover_pending,
    _run_cdc_to_batch_cutover_step,
    _stage_fields,
    checkpoint_cdc_to_batch_active_table,
    plan_cdc_to_batch_table_cutover,
    prepare_cdc_to_batch_transition,
    stage_cdc_to_batch_table,
)
from etl.cdc_external_operations import (
    _begin_cdc_external_operation,
    _cdc_external_operation_barrier,
    _complete_cdc_external_operation,
)
from reports.models import ReportDefinition
from reports.cutover_guard import report_query_guard


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

    @patch("etl.tasks.SourceField.objects.update_or_create")
    @patch("etl.tasks.SourceTable.objects.get_or_create")
    @patch("etl.tasks.sa.inspect")
    @patch("etl.tasks.create_source_engine")
    def test_sql_server_introspection_skips_system_schemas_and_tables(
        self, create_engine, inspect, get_or_create, update_or_create
    ):
        engine = MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        connection.execute.return_value = [("dbo", "Orders")]
        inspector = MagicMock()
        inspector.get_schema_names.return_value = [
            "dbo",
            "sys",
            "INFORMATION_SCHEMA",
            "cdc",
        ]
        inspector.get_table_names.return_value = ["Orders", "systranschemas"]
        inspector.get_pk_constraint.return_value = {
            "constrained_columns": ["OrderID"]
        }
        inspector.get_columns.return_value = [
            {"name": "OrderID", "type": "INTEGER"}
        ]
        inspector.get_foreign_keys.return_value = []
        inspect.return_value = inspector
        create_engine.return_value = engine
        get_or_create.return_value = (
            SimpleNamespace(primary_key_fields=["OrderID"]),
            False,
        )
        profile = SimpleNamespace(db_type=DbChoice.MSSQL)

        with patch("etl.tasks.set_task_progress"):
            introspect_source(profile, "task-id", MagicMock())

        inspector.get_table_names.assert_called_once_with(schema="dbo")
        get_or_create.assert_called_once()
        update_or_create.assert_called_once()
        engine.connect.assert_called_once()
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

    def test_sql_server_connector_targets_database_schema_and_table(self):
        profile = self.profile(DbChoice.MSSQL)

        config = build_debezium_connector_config(profile, [self.table])

        self.assertEqual(config["database.names"], "analytics")
        self.assertEqual(
            config["table.include.list"],
            r"sales\.orders",
        )
        self.assertEqual(config["database.encrypt"], "false")

    def test_sql_server_topic_sanitizes_spaces_like_debezium(self):
        profile = self.profile(DbChoice.MSSQL)
        table = SimpleNamespace(
            schema_name="sales",
            table_name="Order Details",
            primary_key_fields=["order_id"],
        )

        self.assertEqual(
            _topic_name(profile, table),
            "reporthub-source-12.analytics.sales.Order_Details",
        )

    def test_table_generation_connector_uses_isolated_offsets_and_notifications(self):
        profile = self.profile(DbChoice.POSTGRESQL)
        connector_name = "reporthub-source-12-table-4-abc123"
        topic_prefix = "reporthub-rebuild-abc123"

        config = build_debezium_connector_config(
            profile,
            [self.table],
            connector_name=connector_name,
            topic_prefix=topic_prefix,
            notification_topic=f"{connector_name}.notifications",
        )

        self.assertEqual(config["topic.prefix"], topic_prefix)
        self.assertEqual(
            config["notification.enabled.channels"],
            "sink",
        )
        self.assertEqual(
            config["notification.sink.topic.name"],
            f"{connector_name}.notifications",
        )
        self.assertNotEqual(config["slot.name"], "reporthub_12")
        self.assertNotEqual(config["publication.name"], "reporthub_12")
        self.assertNotEqual(
            config["schema.history.internal.kafka.topic"],
            "reporthub.schema-history.12",
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

    def test_connector_name_metadata_does_not_trigger_reconfiguration(self):
        desired = {
            "connector.class": "io.debezium.connector.oracle.OracleConnector",
            "database.hostname": "source.example",
        }

        self.assertTrue(
            _connector_configs_match(
                {**desired, "name": "reporthub-source-12"},
                desired,
            )
        )
        self.assertFalse(
            _connector_configs_match(
                {**desired, "name": "reporthub-source-12", "snapshot.mode": "never"},
                desired,
            )
        )

    @patch("etl.cdc._connect_request")
    def test_existing_connector_name_does_not_cause_a_config_update(self, connect_request):
        profile = self.profile(DbChoice.POSTGRESQL)
        profile.tables = SimpleNamespace(all=lambda: [self.table])
        config = build_debezium_connector_config(profile, [self.table])
        connect_request.side_effect = [
            {**config, "name": "reporthub-source-12"},
            {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]},
        ]

        self.assertEqual(ensure_debezium_connector(profile), "reporthub-source-12")
        self.assertEqual(
            [call.args[0] for call in connect_request.call_args_list],
            ["GET", "GET"],
        )

    @patch("etl.cdc.time.sleep")
    @patch("etl.cdc._connect_request")
    def test_connector_status_not_found_is_retried_until_ready(
        self, connect_request, sleep
    ):
        profile = self.profile(DbChoice.POSTGRESQL)
        profile.tables = SimpleNamespace(all=lambda: [self.table])
        config = build_debezium_connector_config(profile, [self.table])
        connect_request.side_effect = [
            None,
            None,
            None,
            {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]},
        ]

        self.assertEqual(ensure_debezium_connector(profile), "reporthub-source-12")
        self.assertEqual(
            [call.args for call in connect_request.call_args_list],
            [
                ("GET", "/connectors/reporthub-source-12/config"),
                ("POST", "/connectors", {"name": "reporthub-source-12", "config": config}),
                ("GET", "/connectors/reporthub-source-12/status"),
                ("GET", "/connectors/reporthub-source-12/status"),
            ],
        )

    @patch("etl.cdc.time.sleep")
    @patch("etl.cdc._connect_request")
    def test_failed_task_is_restarted_without_resetting_offsets(self, connect_request, sleep):
        profile = self.profile(DbChoice.POSTGRESQL)
        profile.tables = SimpleNamespace(all=lambda: [self.table])
        config = build_debezium_connector_config(profile, [self.table])
        connect_request.side_effect = [
            {**config, "name": "reporthub-source-12"},
            {"connector": {"state": "RUNNING"}, "tasks": [{"id": 0, "state": "FAILED"}]},
            None,
            {"connector": {"state": "RUNNING"}, "tasks": [{"id": 0, "state": "RUNNING"}]},
        ]

        self.assertEqual(ensure_debezium_connector(profile), "reporthub-source-12")
        self.assertEqual(
            connect_request.call_args_list[2].args,
            ("POST", "/connectors/reporthub-source-12/tasks/0/restart"),
        )

    @patch("etl.cdc.time.sleep")
    @patch("etl.cdc.time.monotonic", side_effect=[0, 0, 1, 31])
    @patch("etl.cdc._connect_request")
    def test_failed_connector_restart_is_attempted_once(self, connect_request, monotonic, sleep):
        profile = self.profile(DbChoice.POSTGRESQL)
        profile.tables = SimpleNamespace(all=lambda: [self.table])
        config = build_debezium_connector_config(profile, [self.table])
        failed = {"connector": {"state": "FAILED"}, "tasks": []}
        connect_request.side_effect = [
            {**config, "name": "reporthub-source-12"},
            failed,
            None,
            failed,
        ]

        with self.assertRaisesMessage(RuntimeError, "failed to start after a restart"):
            ensure_debezium_connector(profile)
        self.assertEqual(
            [call.args[0] for call in connect_request.call_args_list],
            ["GET", "GET", "POST", "GET"],
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

    @patch("etl.cdc.urlopen")
    def test_missing_restart_endpoint_is_not_treated_as_success(self, urlopen):
        urlopen.side_effect = HTTPError(
            "http://connect/connectors/example/restart",
            404,
            "Not Found",
            hdrs=None,
            fp=BytesIO(b""),
        )

        with self.assertRaisesMessage(RuntimeError, "HTTP 404"):
            _connect_request("POST", "/connectors/example/restart")

    @patch("etl.cdc.urlopen")
    def test_missing_connector_status_is_reported_as_not_ready(self, urlopen):
        urlopen.side_effect = HTTPError(
            "http://connect/connectors/example/status",
            404,
            "Not Found",
            hdrs=None,
            fp=BytesIO(b'{"message":"No status found for connector example"}'),
        )

        self.assertIsNone(
            _connect_request("GET", "/connectors/example/status")
        )

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
            source_id=12,
            clickhouse_physical_table="",
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

    def test_cdc_table_uses_configured_physical_generation(self):
        self.table.clickhouse_physical_table = "rh_generation_123"
        self.table.cdc_managed = False
        clickhouse_client = MagicMock()
        clickhouse_client.query.return_value.result_rows = [[0]]
        physical_name, _ = _create_cdc_table(
            clickhouse_client, self.profile, self.table, ["event_id", "value"]
        )
        self.assertEqual(physical_name, "`rh_generation_123`")
        self.assertIn("`rh_generation_123`", clickhouse_client.command.call_args.args[0])

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

    @patch("etl.cdc.KafkaConsumer")
    @patch("etl.cdc._required_field_names", return_value=["event_id", "value"])
    @patch("etl.cdc._create_cdc_table", return_value=("`stage_events`", ["event_id", "value"]))
    def test_rebuild_batch_waits_for_snapshot_notification_and_drains(
        self, create_table, required_fields, kafka_consumer
    ):
        data_topic = "rebuild-generation.public.events"
        notification_topic = "rebuild-generation.notifications"
        data_partition = TopicPartition(data_topic, 0)
        notification_partition = TopicPartition(notification_topic, 0)
        event = SimpleNamespace(
            topic=data_topic,
            partition=0,
            offset=0,
            key={"event_id": 1},
            value={
                "op": "r",
                "after": {"event_id": 1, "value": "snapshot"},
            },
        )
        notification = SimpleNamespace(
            topic=notification_topic,
            value={
                "type": "COMPLETED",
                "aggregate_type": "Initial Snapshot",
                "additional_data": {"connector_name": "rebuild-generation"},
            },
        )
        consumer = MagicMock()
        consumer.poll.return_value = {
            data_partition: [event],
            notification_partition: [notification],
        }
        consumer.assignment.return_value = {
            data_partition,
            notification_partition,
        }
        consumer.partitions_for_topic.return_value = {0}
        consumer.end_offsets.return_value = {data_partition: 1}
        consumer.position.return_value = 1
        kafka_consumer.return_value = consumer
        recovery = SimpleNamespace(
            pk=7,
            connector_name="rebuild-generation",
            topic_prefix="rebuild-generation",
            notification_topic=notification_topic,
            consumer_group="rebuild-generation-group",
            staging_table="source_12__sales_events__rebuild_abc123",
            snapshot_end_offsets={},
            status="SNAPSHOTTING",
            Status=SimpleNamespace(
                SNAPSHOTTING="SNAPSHOTTING",
                CUTOVER_PENDING="CUTOVER_PENDING",
            ),
            save=MagicMock(),
        )
        profile = SimpleNamespace(
            pk=12,
            db_type=DbChoice.POSTGRESQL,
            database_name="analytics",
            tables=SimpleNamespace(all=lambda: [self.table]),
            relationships=SimpleNamespace(all=lambda: []),
        )
        clickhouse_client = MagicMock()

        drained = consume_cdc_table_rebuild_batch(
            recovery,
            profile,
            self.table,
            clickhouse_client,
            batch_size=10,
        )

        self.assertTrue(drained)
        self.assertEqual(recovery.status, "CUTOVER_PENDING")
        self.assertEqual(recovery.snapshot_end_offsets, {"0": 1})
        clickhouse_client.insert.assert_called_once()
        consumer.commit.assert_called_once()
        consumer.close.assert_called_once()

    @patch("etl.cdc.KafkaConsumer")
    @patch("etl.cdc._required_field_names", return_value=["event_id", "value"])
    @patch("etl.cdc._create_cdc_table", return_value=("`stage_events`", ["event_id", "value"]))
    def test_rebuild_batch_stops_at_checkpoint_for_later_streaming(
        self, create_table, required_fields, kafka_consumer
    ):
        topic = "rebuild-generation.public.events"
        partition = TopicPartition(topic, 0)
        messages = [
            SimpleNamespace(
                topic=topic,
                partition=0,
                offset=offset,
                key={"event_id": 1},
                value={"op": "u", "after": {"event_id": 1, "value": value}},
            )
            for offset, value in [(0, "snapshot"), (1, "after-checkpoint")]
        ]
        consumer = MagicMock()
        consumer.poll.return_value = {partition: messages}
        consumer.assignment.return_value = {partition}
        consumer.position.return_value = 2
        kafka_consumer.return_value = consumer
        recovery = SimpleNamespace(
            pk=7,
            connector_name="rebuild-generation",
            topic_prefix="rebuild-generation",
            notification_topic="rebuild-generation.notifications",
            consumer_group="rebuild-generation-group",
            staging_table="source_events_rebuild",
            snapshot_end_offsets={"0": 1},
            status="CUTOVER_PENDING",
            Status=SimpleNamespace(
                SNAPSHOTTING="SNAPSHOTTING",
                CUTOVER_PENDING="CUTOVER_PENDING",
            ),
        )
        clickhouse_client = MagicMock()

        drained = consume_cdc_table_rebuild_batch(
            recovery, self.profile, self.table, clickhouse_client, batch_size=10
        )

        self.assertTrue(drained)
        self.assertEqual(
            clickhouse_client.insert.call_args.args[1],
            [[1, "snapshot", 1, 0]],
        )
        consumer.commit.assert_called_once()

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
    def test_generation_consumer_uses_snapshot_checkpoint(
        self, kafka_consumer, required_fields, create_table
    ):
        connector_name = "rebuild-generation"
        topic_prefix = "rebuild-generation"
        topic = f"{topic_prefix}.public.events"
        partition = TopicPartition(topic, 0)
        message = SimpleNamespace(
            topic=topic,
            partition=0,
            offset=1,
            key={"event_id": 1},
            value={
                "op": "c",
                "after": {"event_id": 1, "value": "created"},
                "source": {"ts_ms": 1_800_000_000_000},
            },
        )
        consumer = MagicMock()
        consumer.assignment.return_value = {partition}
        consumer.committed.return_value = None
        consumer.poll.side_effect = [{}, {partition: [message]}]
        kafka_consumer.return_value = consumer
        self.table.cdc_connector_name = connector_name
        self.table.cdc_topic_prefix = topic_prefix
        recovery = SimpleNamespace(snapshot_end_offsets={"0": 1})
        clickhouse_client = MagicMock()

        with patch(
            "etl.cdc.CdcTableRebuild.objects.filter"
        ) as rebuild_filter:
            rebuild_filter.return_value.order_by.return_value.first.return_value = (
                recovery
            )
            processed = consume_source_cdc_batch(
                self.profile,
                clickhouse_client,
                batch_size=10,
            )

        self.assertEqual(processed, 1)
        kafka_consumer.assert_called_once()
        self.assertEqual(
            kafka_consumer.call_args.kwargs["group_id"],
            f"{connector_name}-active",
        )
        consumer.subscribe.assert_called_once_with([topic])
        consumer.seek.assert_called_once_with(partition, 1)
        consumer.commit.assert_called_once()
        clickhouse_client.insert.assert_called_once()

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


class CdcToBatchCutoverPlanningTests(SimpleTestCase):
    def setUp(self):
        self.checkpoint = CdcToBatchTableCheckpoint(
            status=CdcToBatchTableCheckpoint.Status.STAGED,
            active_table_checked=True,
            active_uuid_before="original-uuid",
            staging_uuid="staged-uuid",
        )

    def test_existing_table_exchange_and_resume_after_exchange(self):
        self.assertEqual(
            plan_cdc_to_batch_table_cutover(
                self.checkpoint, "original-uuid", "staged-uuid"
            ),
            CdcToBatchCutoverAction.EXCHANGE,
        )
        self.assertEqual(
            plan_cdc_to_batch_table_cutover(
                self.checkpoint, "staged-uuid", "original-uuid"
            ),
            CdcToBatchCutoverAction.COMPLETE,
        )

    def test_missing_active_table_rename_and_resume_after_rename(self):
        self.checkpoint.active_uuid_before = ""
        self.assertEqual(
            plan_cdc_to_batch_table_cutover(self.checkpoint, "", "staged-uuid"),
            CdcToBatchCutoverAction.RENAME,
        )
        self.assertEqual(
            plan_cdc_to_batch_table_cutover(self.checkpoint, "staged-uuid", ""),
            CdcToBatchCutoverAction.COMPLETE,
        )

    def test_completed_checkpoint_must_still_match_exchanged_tables(self):
        self.checkpoint.status = CdcToBatchTableCheckpoint.Status.CUTOVER
        self.assertEqual(
            plan_cdc_to_batch_table_cutover(
                self.checkpoint, "staged-uuid", "original-uuid"
            ),
            CdcToBatchCutoverAction.COMPLETE,
        )
        with self.assertRaisesRegex(RuntimeError, "Completed cutover"):
            plan_cdc_to_batch_table_cutover(
                self.checkpoint, "original-uuid", "staged-uuid"
            )

    def test_rejects_unexpected_or_missing_table_uuids(self):
        for active, stage in [
            ("other-uuid", "staged-uuid"),
            ("original-uuid", "other-uuid"),
            ("", "staged-uuid"),
            ("original-uuid", ""),
            ("staged-uuid", ""),
            ("staged-uuid", "other-uuid"),
        ]:
            with self.subTest(active=active, stage=stage):
                with self.assertRaisesRegex(RuntimeError, "UUIDs changed"):
                    plan_cdc_to_batch_table_cutover(self.checkpoint, active, stage)
        self.checkpoint.active_uuid_before = ""
        with self.assertRaisesRegex(RuntimeError, "UUIDs changed"):
            plan_cdc_to_batch_table_cutover(
                self.checkpoint, "staged-uuid", "other-uuid"
            )

    def test_rejects_incomplete_ambiguous_or_unstaged_checkpoint(self):
        self.checkpoint.active_table_checked = False
        with self.assertRaisesRegex(RuntimeError, "checkpoint is incomplete"):
            plan_cdc_to_batch_table_cutover(self.checkpoint, "", "staged-uuid")
        self.checkpoint.active_table_checked = True
        self.checkpoint.staging_uuid = ""
        with self.assertRaisesRegex(RuntimeError, "checkpoint is incomplete"):
            plan_cdc_to_batch_table_cutover(self.checkpoint, "", "")
        self.checkpoint.staging_uuid = "original-uuid"
        with self.assertRaisesRegex(RuntimeError, "must be distinct"):
            plan_cdc_to_batch_table_cutover(
                self.checkpoint, "original-uuid", "original-uuid"
            )
        self.checkpoint.status = CdcToBatchTableCheckpoint.Status.PENDING
        with self.assertRaisesRegex(RuntimeError, "not staged"):
            plan_cdc_to_batch_table_cutover(self.checkpoint, "", "")


class CdcToBatchFinalizationTests(TestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Cutover finalization source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="analytics",
            username="cdc_user", ingestion_mode=IngestionMode.CDC,
        )
        self.tables = [
            SourceTable.objects.create(
                source=self.source, schema_name="public", table_name=name,
                primary_key_fields=["id"], cdc_managed=True,
                clickhouse_uses_replacing_merge_tree=True, incremental_ready=True,
                incremental_watermark="123", cdc_connector_name=f"connector-{name}",
            )
            for name in ("events", "logs")
        ]
        for table in self.tables:
            SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        self.transition = prepare_cdc_to_batch_transition(self.source.pk)
        self.checkpoints = list(
            self.transition.table_checkpoints.order_by("source_table_id")
        )
        self.uuids = {}
        for table, checkpoint in zip(self.tables, self.checkpoints):
            checkpoint.status = CdcToBatchTableCheckpoint.Status.CUTOVER
            checkpoint.active_table_checked = True
            checkpoint.active_uuid_before = f"old-{table.pk}"
            checkpoint.staging_uuid = f"new-{table.pk}"
            checkpoint.staging_row_count = 1
            checkpoint.staging_schema = _field_signature(
                self.source, _stage_fields(self.source, table), table
            )
            checkpoint.save()
            self.uuids[f"source_{self.source.pk}__public_{table.table_name}"] = checkpoint.staging_uuid
            self.uuids[checkpoint.staging_table] = checkpoint.active_uuid_before
        self.transition.status = CdcToBatchTransition.Status.CUTOVER_PENDING
        self.transition.last_error = "previous error"
        self.transition.save()
        self.client = MagicMock()
        self.client.query.return_value.result_rows = [["Atomic"]]
        for target, kwargs in (
            ("etl.cdc_to_batch.clickhouse_connect.get_client", {"return_value": self.client}),
            ("etl.cdc_to_batch._clickhouse_table_uuid", {
                "side_effect": lambda client, name: self.uuids.get(name, "")
            }),
            ("etl.cdc_to_batch._validate_staging_table", {}),
            ("etl.cdc_to_batch._validate_staging_row_count", {}),
        ):
            mocker = patch(target, **kwargs)
            mocker.start()
            self.addCleanup(mocker.stop)

    def test_atomically_completes_metadata_and_preserves_connectors(self):
        result = _finalize_cdc_to_batch_transition(self.transition.pk)
        self.source.refresh_from_db()
        self.transition.refresh_from_db()
        self.assertEqual(result.status, CdcToBatchTransition.Status.SUCCEEDED)
        self.assertEqual(self.source.ingestion_mode, IngestionMode.BATCH)
        self.assertEqual(self.transition.last_error, "")
        for table in self.tables:
            table.refresh_from_db()
            self.assertFalse(table.cdc_managed)
            self.assertFalse(table.clickhouse_uses_replacing_merge_tree)
            self.assertFalse(table.incremental_ready)
            self.assertIsNone(table.incremental_watermark)
            self.assertEqual(table.cdc_connector_name, f"connector-{table.table_name}")
        self.client.command.assert_not_called()
        self.client.close.assert_called_once()
        _finalize_cdc_to_batch_transition(self.transition.pk)
        self.client.close.assert_called_once()

    def test_rejects_unfinished_or_unexpected_table_identities(self):
        for changes in (
            {"status": CdcToBatchTableCheckpoint.Status.STAGED},
            {"active_table_checked": False},
            {"staging_uuid": "unexpected"},
            {"staging_schema": {}},
        ):
            checkpoint = self.checkpoints[0]
            old_values = {name: getattr(checkpoint, name) for name in changes}
            CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(**changes)
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                _finalize_cdc_to_batch_transition(self.transition.pk)
            CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(**old_values)
            self.source.refresh_from_db()
            self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
            self.tables[0].refresh_from_db()
            self.assertTrue(self.tables[0].cdc_managed)

    def test_rejects_invalid_count_and_rolls_back_completion(self):
        with patch("etl.cdc_to_batch._validate_staging_row_count",
                   side_effect=[None, RuntimeError("row count changed")]):
            with self.assertRaisesRegex(RuntimeError, "row count changed"):
                _finalize_cdc_to_batch_transition(self.transition.pk)
        self.source.refresh_from_db()
        self.transition.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.CUTOVER_PENDING)
        self.assertTrue(all(SourceTable.objects.get(pk=t.pk).cdc_managed for t in self.tables))

    def test_rejects_missing_original_table_and_non_atomic_database(self):
        self.uuids[self.checkpoints[0].staging_table] = ""
        with self.assertRaisesRegex(RuntimeError, "Completed cutover tables changed"):
            _finalize_cdc_to_batch_transition(self.transition.pk)
        self.uuids[self.checkpoints[0].staging_table] = self.checkpoints[0].active_uuid_before
        self.client.query.return_value.result_rows = [["Ordinary"]]
        with self.assertRaisesRegex(RuntimeError, "Atomic ClickHouse database"):
            _finalize_cdc_to_batch_transition(self.transition.pk)
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)

    def test_rejects_active_rebuild(self):
        CdcTableRebuild.objects.create(
            source_table=self.tables[0], connector_name="conflict",
            topic_prefix="conflict", notification_topic="conflict-notifications",
            consumer_group="conflict-group", staging_table="conflict-stage",
        )
        with self.assertRaisesRegex(RuntimeError, "active CDC table rebuild"):
            _finalize_cdc_to_batch_transition(self.transition.pk)
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.client.query.assert_not_called()

    def test_rejects_unresolved_cutover_command(self):
        CdcToBatchTableCheckpoint.objects.filter(pk=self.checkpoints[0].pk).update(
            cutover_command_inflight=True
        )
        with self.assertRaisesRegex(RuntimeError, "Not every transition table"):
            _finalize_cdc_to_batch_transition(self.transition.pk)
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)

    def test_database_failure_rolls_back_all_metadata(self):
        with patch.object(CdcToBatchTransition, "save", side_effect=RuntimeError("database failure")):
            with self.assertRaisesRegex(RuntimeError, "database failure"):
                _finalize_cdc_to_batch_transition(self.transition.pk)
        self.source.refresh_from_db()
        self.transition.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.CUTOVER_PENDING)
        for table in self.tables:
            table.refresh_from_db()
            self.assertTrue(table.cdc_managed)
            self.assertTrue(table.incremental_ready)
            self.assertEqual(table.incremental_watermark, "123")

    def test_rejects_changed_membership_and_ineligible_transition(self):
        SourceTable.objects.create(
            source=self.source, schema_name="public", table_name="new_table"
        )
        with self.assertRaisesRegex(RuntimeError, "table list changed"):
            _finalize_cdc_to_batch_transition(self.transition.pk)
        self.source.tables.filter(table_name="new_table").delete()
        self.transition.status = CdcToBatchTransition.Status.PREPARING
        self.transition.save(update_fields=["status"])
        with self.assertRaisesRegex(RuntimeError, "not eligible"):
            _finalize_cdc_to_batch_transition(self.transition.pk)


@skipUnlessDBFeature("has_select_for_update")
@override_settings(CDC_EXTERNAL_OPERATION_TRACKING=True, CDC_TO_BATCH_EXECUTOR_ENABLED=True)
class CdcToBatchExecutorTests(TransactionTestCase):
    def setUp(self):
        CdcToBatchFinalizationTests.setUp(self)
        self.transition.status = CdcToBatchTransition.Status.PREPARING
        self.transition.save(update_fields=["status"])
        for table, checkpoint in zip(self.tables, self.checkpoints):
            checkpoint.status = CdcToBatchTableCheckpoint.Status.PENDING
            checkpoint.active_table_checked = False
            checkpoint.active_uuid_before = ""
            checkpoint.save(update_fields=["status", "active_table_checked", "active_uuid_before"])
            self.uuids[f"source_{self.source.pk}__public_{table.table_name}"] = f"old-{table.pk}"
            self.uuids[checkpoint.staging_table] = checkpoint.staging_uuid

        def stage(checkpoint_pk, batch_size=None, *, verification_only=False):
            if not verification_only:
                CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint_pk).update(
                    status=CdcToBatchTableCheckpoint.Status.STAGED
                )

        def exchange(sql):
            for table, checkpoint in zip(self.tables, self.checkpoints):
                active = f"source_{self.source.pk}__public_{table.table_name}"
                if f"`{active}`" in sql and f"`{checkpoint.staging_table}`" in sql:
                    self.uuids[active], self.uuids[checkpoint.staging_table] = (
                        self.uuids[checkpoint.staging_table], self.uuids[active]
                    )
                    return
            raise AssertionError(f"Unexpected ClickHouse command: {sql}")

        self.client.command.side_effect = exchange
        self.stage = patch("etl.cdc_to_batch.stage_cdc_to_batch_table", side_effect=stage)
        self.stage_mock = self.stage.start()
        self.addCleanup(self.stage.stop)
        self.enqueue = patch("etl.tasks.execute_cdc_to_batch_transition_task.apply_async")
        self.enqueue_mock = self.enqueue.start()
        self.addCleanup(self.enqueue.stop)
        self.fence = patch("etl.tasks._require_cdc_to_batch_deployment_fence")
        self.fence.start()
        self.addCleanup(self.fence.stop)

    def run_step(self):
        return execute_cdc_to_batch_transition_task.apply(
            args=[self.transition.pk], kwargs={"batch_size": 5}, throw=True
        ).get()

    def test_runs_staging_cutover_and_atomic_finalization(self):
        self.assertIn("Staged checkpoint", self.run_step())
        self.assertIn("Staged checkpoint", self.run_step())
        self.assertIn("Cutover is pending", self.run_step())
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.CUTOVER_PENDING)
        self.assertIn("table cutover completed", self.run_step())
        self.assertIn("table cutover completed", self.run_step())
        self.assertIn("completed", self.run_step())
        self.source.refresh_from_db()
        self.transition.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.BATCH)
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.SUCCEEDED)
        self.assertEqual(self.client.command.call_count, 2)
        self.assertEqual(self.enqueue_mock.call_count, 5)
        for call in self.enqueue_mock.call_args_list:
            self.assertEqual(call.kwargs["queue"], "queue_backfill")
            self.assertEqual(call.kwargs["kwargs"], {"batch_size": 5})
        self.assertIn("completed", self.run_step())
        self.assertEqual(self.enqueue_mock.call_count, 5)

    def test_disabled_gate_prevents_any_work(self):
        with override_settings(CDC_TO_BATCH_EXECUTOR_ENABLED=False):
            with self.assertRaisesRegex(RuntimeError, "executor is not enabled"):
                self.run_step()
        with override_settings(CDC_EXTERNAL_OPERATION_TRACKING=False):
            with self.assertRaisesRegex(RuntimeError, "tracking is not enabled"):
                self.run_step()
        self.assertFalse(self.transition.table_checkpoints.exclude(
            status=CdcToBatchTableCheckpoint.Status.PENDING
        ).exists())
        self.enqueue_mock.assert_not_called()
        self.client.command.assert_not_called()

    @patch("etl.tasks.get_etl_logger")
    def test_missing_deployment_fence_blocks_preparing_and_pending(self, logger):
        self.fence.stop()
        for status in (
            CdcToBatchTransition.Status.PREPARING,
            CdcToBatchTransition.Status.CUTOVER_PENDING,
            CdcToBatchTransition.Status.SUCCEEDED,
        ):
            for generation in (False, True):
                with self.subTest(status=status, generation=generation):
                    CdcToBatchTransition.objects.filter(pk=self.transition.pk).update(status=status)
                    with override_settings(CDC_TO_BATCH_GENERATION_EXECUTOR_ENABLED=generation):
                        with self.assertRaisesRegex(RuntimeError, "deployment fencing"):
                            self.run_step()
                    self.transition.refresh_from_db()
                    self.assertEqual(self.transition.status, status)
                    if status != CdcToBatchTransition.Status.SUCCEEDED:
                        self.assertIn("deployment fencing", self.transition.last_error)
                    self.assertEqual(self.transition.table_checkpoints.filter(
                        status=CdcToBatchTableCheckpoint.Status.PENDING
                    ).count(), len(self.checkpoints))
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.stage_mock.assert_not_called()
        self.enqueue_mock.assert_not_called()
        self.client.command.assert_not_called()

    @patch("etl.tasks.get_etl_logger")
    def test_broker_failure_keeps_staging_checkpoint_for_explicit_resume(self, logger):
        self.enqueue_mock.side_effect = RuntimeError("broker unavailable")
        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            self.run_step()
        self.checkpoints[0].refresh_from_db()
        self.assertEqual(self.checkpoints[0].status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.transition.refresh_from_db()
        self.assertIn("Transition failed", self.transition.last_error)
        self.enqueue_mock.side_effect = None
        self.assertIn("Staged checkpoint", self.run_step())
        self.checkpoints[1].refresh_from_db()
        self.assertEqual(self.checkpoints[1].status, CdcToBatchTableCheckpoint.Status.STAGED)

    @patch("etl.tasks.get_etl_logger")
    def test_broker_failure_after_pending_can_resume(self, logger):
        self.run_step()
        self.run_step()
        self.enqueue_mock.side_effect = RuntimeError("broker unavailable")
        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            self.run_step()
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.CUTOVER_PENDING)
        self.assertIn("Transition failed", self.transition.last_error)
        self.enqueue_mock.side_effect = None
        self.assertIn("table cutover completed", self.run_step())
        self.assertEqual(self.client.command.call_count, 1)

    @patch("etl.tasks.get_etl_logger")
    def test_broker_failure_after_exchange_resumes_next_checkpoint(self, logger):
        self.run_step()
        self.run_step()
        self.run_step()
        self.enqueue_mock.side_effect = RuntimeError("broker unavailable")
        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            self.run_step()
        self.checkpoints[0].refresh_from_db()
        self.assertEqual(self.checkpoints[0].status, CdcToBatchTableCheckpoint.Status.CUTOVER)
        self.enqueue_mock.side_effect = None
        self.assertIn("table cutover completed", self.run_step())
        self.assertEqual(self.client.command.call_count, 2)

    @patch("etl.tasks.get_etl_logger")
    def test_uncertain_exchange_stops_automatic_continuation(self, logger):
        self.run_step()
        self.run_step()
        self.run_step()
        self.enqueue_mock.reset_mock()
        exchange = self.client.command.side_effect

        def lose_response(sql):
            exchange(sql)
            raise RuntimeError("response lost")

        self.client.command.side_effect = lose_response
        with self.assertRaisesRegex(RuntimeError, "response lost"):
            self.run_step()
        self.checkpoints[0].refresh_from_db()
        self.assertTrue(self.checkpoints[0].cutover_command_inflight)
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.CUTOVER_PENDING)
        self.assertIn("Transition failed", self.transition.last_error)
        self.enqueue_mock.assert_not_called()
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            self.run_step()
        self.client.command.assert_called_once()

    @patch("etl.tasks.get_etl_logger")
    def test_active_operation_blocks_pending_until_explicit_retry(self, logger):
        self.run_step()
        self.run_step()
        operation = _begin_cdc_external_operation(
            self.source.pk, CdcSourceExternalOperation.Kind.REPORT
        )
        self.enqueue_mock.reset_mock()
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            self.run_step()
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)
        self.enqueue_mock.assert_not_called()
        _complete_cdc_external_operation(self.source.pk, operation.pk)
        self.assertIn("Cutover is pending", self.run_step())


@skipUnlessDBFeature("has_select_for_update")
@override_settings(CDC_EXTERNAL_OPERATION_TRACKING=True)
class CdcToBatchCutoverStepTests(TransactionTestCase):
    def setUp(self):
        CdcToBatchFinalizationTests.setUp(self)
        self.active_by_checkpoint = {}
        for table, checkpoint in zip(self.tables, self.checkpoints):
            checkpoint.status = CdcToBatchTableCheckpoint.Status.STAGED
            checkpoint.save(update_fields=["status"])
            active = f"source_{self.source.pk}__public_{table.table_name}"
            self.active_by_checkpoint[checkpoint.pk] = active
            self.uuids[active] = checkpoint.active_uuid_before
            self.uuids[checkpoint.staging_table] = checkpoint.staging_uuid

        def execute(sql):
            for checkpoint in self.checkpoints:
                active = self.active_by_checkpoint[checkpoint.pk]
                if f"`{active}`" in sql and f"`{checkpoint.staging_table}`" in sql:
                    self.uuids[active], self.uuids[checkpoint.staging_table] = (
                        self.uuids[checkpoint.staging_table], self.uuids[active]
                    )
                    return
            raise AssertionError(f"Unexpected ClickHouse command: {sql}")

        self.client.command.side_effect = execute

    def test_runs_one_table_per_step_then_finalizes_and_retries_success(self):
        for index in range(2):
            self.assertEqual(
                _run_cdc_to_batch_cutover_step(self.transition.pk),
                CdcToBatchStepResult.TABLE_CUTOVER,
            )
            self.source.refresh_from_db()
            self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
            self.assertEqual(self.client.command.call_count, index + 1)
            self.checkpoints[index].refresh_from_db()
            self.assertEqual(self.checkpoints[index].status, CdcToBatchTableCheckpoint.Status.CUTOVER)
        self.assertEqual(
            _run_cdc_to_batch_cutover_step(self.transition.pk), CdcToBatchStepResult.SUCCEEDED
        )
        self.source.refresh_from_db()
        self.transition.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.BATCH)
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.SUCCEEDED)
        self.assertEqual(
            _run_cdc_to_batch_cutover_step(self.transition.pk), CdcToBatchStepResult.SUCCEEDED
        )
        self.assertEqual(self.client.command.call_count, 2)

    def test_lost_response_blocks_all_further_steps_and_finalization(self):
        execute = self.client.command.side_effect

        def lose_response(sql):
            execute(sql)
            raise RuntimeError("response lost")

        self.client.command.side_effect = lose_response
        with self.assertRaisesRegex(RuntimeError, "response lost"):
            _run_cdc_to_batch_cutover_step(self.transition.pk)
        self.checkpoints[0].refresh_from_db()
        self.assertTrue(self.checkpoints[0].cutover_command_inflight)
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            _run_cdc_to_batch_cutover_step(self.transition.pk)
        self.checkpoints[1].refresh_from_db()
        self.assertEqual(self.checkpoints[1].status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.client.command.assert_called_once()

    def test_completion_failure_resumes_without_repeating_exchanges(self):
        for _ in range(2):
            _run_cdc_to_batch_cutover_step(self.transition.pk)
        with patch("etl.cdc_to_batch._validate_staging_row_count",
                   side_effect=RuntimeError("validation failed")):
            with self.assertRaisesRegex(RuntimeError, "validation failed"):
                _run_cdc_to_batch_cutover_step(self.transition.pk)
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.CUTOVER_PENDING)
        self.assertEqual(
            _run_cdc_to_batch_cutover_step(self.transition.pk), CdcToBatchStepResult.SUCCEEDED
        )
        self.assertEqual(self.client.command.call_count, 2)

    def test_reports_remain_blocked_until_all_tables_and_metadata_are_finalized(self):
        report = ReportDefinition.objects.create(
            name="Stepped cutover report", source=self.source, base_table=self.tables[0],
            selected_fields=["id"],
        )
        for _ in range(2):
            _run_cdc_to_batch_cutover_step(self.transition.pk)
            with self.assertRaisesRegex(RuntimeError, "cutover is pending"):
                with report_query_guard(report):
                    pass
        _run_cdc_to_batch_cutover_step(self.transition.pk)
        with report_query_guard(report) as current:
            self.assertFalse(current.base_table.cdc_managed)

    def test_rejects_bad_state_and_enclosing_transaction(self):
        with override_settings(CDC_EXTERNAL_OPERATION_TRACKING=False):
            with self.assertRaisesRegex(RuntimeError, "tracking is not enabled"):
                _run_cdc_to_batch_cutover_step(self.transition.pk)
        with transaction.atomic():
            with self.assertRaisesRegex(RuntimeError, "inside a database transaction"):
                _run_cdc_to_batch_cutover_step(self.transition.pk)
        self.transition.status = CdcToBatchTransition.Status.PREPARING
        self.transition.save(update_fields=["status"])
        with self.assertRaisesRegex(RuntimeError, "not pending"):
            _run_cdc_to_batch_cutover_step(self.transition.pk)
        self.transition.status = CdcToBatchTransition.Status.CUTOVER_PENDING
        self.transition.save(update_fields=["status"])
        self.checkpoints[1].status = CdcToBatchTableCheckpoint.Status.PENDING
        self.checkpoints[1].save(update_fields=["status"])
        with self.assertRaisesRegex(RuntimeError, "not staged"):
            _run_cdc_to_batch_cutover_step(self.transition.pk)
        self.client.command.assert_not_called()

    def test_unresolved_external_operation_blocks_exchange_and_finalization(self):
        operation = CdcSourceExternalOperation.objects.create(
            source=self.source, kind=CdcSourceExternalOperation.Kind.INGESTION
        )
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            _run_cdc_to_batch_cutover_step(self.transition.pk)
        self.client.command.assert_not_called()
        CdcToBatchTableCheckpoint.objects.filter(transition=self.transition).update(
            status=CdcToBatchTableCheckpoint.Status.CUTOVER
        )
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            _run_cdc_to_batch_cutover_step(self.transition.pk)
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        operation.delete()

    def test_another_inflight_command_blocks_direct_exchange(self):
        CdcToBatchTableCheckpoint.objects.filter(pk=self.checkpoints[1].pk).update(
            cutover_command_inflight=True
        )
        with self.assertRaisesRegex(RuntimeError, "another ClickHouse|Another ClickHouse"):
            _cutover_cdc_to_batch_table(self.checkpoints[0].pk)
        self.client.command.assert_not_called()

    def test_concurrent_step_cannot_repeat_an_inflight_exchange(self):
        command_started, release_command = Event(), Event()
        execute = self.client.command.side_effect

        def slow_command(sql):
            command_started.set()
            if not release_command.wait(timeout=10):
                raise TimeoutError("Command release timed out.")
            execute(sql)

        def first_step():
            close_old_connections()
            try:
                return _run_cdc_to_batch_cutover_step(self.transition.pk)
            finally:
                close_old_connections()

        self.client.command.side_effect = slow_command
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(first_step)
            try:
                self.assertTrue(command_started.wait(timeout=10))
                with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
                    _run_cdc_to_batch_cutover_step(self.transition.pk)
            finally:
                release_command.set()
            self.assertEqual(first.result(timeout=10), CdcToBatchStepResult.TABLE_CUTOVER)
        self.client.command.assert_called_once()


@skipUnlessDBFeature("has_select_for_update")
@override_settings(CDC_EXTERNAL_OPERATION_TRACKING=True)
class CdcToBatchPendingCutoverTests(TransactionTestCase):
    def setUp(self):
        CdcToBatchFinalizationTests.setUp(self)
        self.transition.status = CdcToBatchTransition.Status.PREPARING
        self.transition.save(update_fields=["status"])
        for table, checkpoint in zip(self.tables, self.checkpoints):
            checkpoint.status = CdcToBatchTableCheckpoint.Status.STAGED
            checkpoint.active_table_checked = False
            checkpoint.active_uuid_before = ""
            checkpoint.save(update_fields=["status", "active_table_checked", "active_uuid_before"])
            active_name = f"source_{self.source.pk}__public_{table.table_name}"
            self.uuids[active_name] = f"old-{table.pk}"
            self.uuids[checkpoint.staging_table] = checkpoint.staging_uuid

    def test_commits_all_observations_and_pending_together(self):
        result = _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        self.assertEqual(result.status, CdcToBatchTransition.Status.CUTOVER_PENDING)
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.last_error, "")
        for table, checkpoint in zip(self.tables, self.checkpoints):
            checkpoint.refresh_from_db()
            self.assertTrue(checkpoint.active_table_checked)
            self.assertEqual(checkpoint.active_uuid_before, f"old-{table.pk}")
        self.client.command.assert_not_called()
        self.client.close.assert_called_once()

    def test_pre_synchronous_staging_cannot_enter_pending_cutover(self):
        checkpoint = self.checkpoints[0]
        signature = dict(checkpoint.staging_schema)
        del signature["synchronous_inserts"]
        CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(
            staging_schema=signature
        )
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)
        self.assertFalse(
            self.transition.table_checkpoints.filter(active_table_checked=True).exists()
        )
        self.client.command.assert_not_called()

    def test_metadata_writer_finishes_before_pending_revalidates(self):
        entered, release, waiting = Event(), Event(), Event()

        def metadata_writer():
            close_old_connections()
            try:
                with source_metadata_write_guard(self.source.pk):
                    entered.set()
                    if not release.wait(10):
                        raise TimeoutError("Metadata writer timed out.")
                    SourceField.objects.filter(table=self.tables[0], field_name="id").update(
                        data_type="VARCHAR"
                    )
            finally:
                close_old_connections()

        def cutover():
            close_old_connections()
            try:
                def watch(execute, sql, params, many, context):
                    if "pg_advisory_xact_lock(" in sql:
                        waiting.set()
                    return execute(sql, params, many, context)

                with connection.execute_wrapper(watch):
                    return _mark_cdc_to_batch_cutover_pending(self.transition.pk)
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            writer = pool.submit(metadata_writer)
            try:
                self.assertTrue(entered.wait(10))
                pending = pool.submit(cutover)
                self.assertTrue(waiting.wait(10))
                self.assertFalse(pending.done())
            finally:
                release.set()
            writer.result(timeout=10)
            with self.assertRaisesRegex(RuntimeError, "configuration changed"):
                pending.result(timeout=10)
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)

    def test_waiting_metadata_writer_rechecks_pending_after_lock(self):
        locked, release, waiting = Event(), Event(), Event()

        def mark_pending():
            close_old_connections()
            try:
                with transaction.atomic():
                    acquire_source_metadata_lock(self.source.pk)
                    locked.set()
                    if not release.wait(10):
                        raise TimeoutError("Pending-state transaction timed out.")
                    CdcToBatchTransition.objects.filter(pk=self.transition.pk).update(
                        status=CdcToBatchTransition.Status.CUTOVER_PENDING
                    )
            finally:
                close_old_connections()

        def metadata_writer():
            close_old_connections()
            try:
                def watch(execute, sql, params, many, context):
                    if "pg_advisory_xact_lock(" in sql:
                        waiting.set()
                    return execute(sql, params, many, context)

                with connection.execute_wrapper(watch):
                    with source_metadata_write_guard(self.source.pk):
                        SourceField.objects.filter(table=self.tables[0]).update(data_type="VARCHAR")
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            pending = pool.submit(mark_pending)
            try:
                self.assertTrue(locked.wait(10))
                writer = pool.submit(metadata_writer)
                self.assertTrue(waiting.wait(10))
                self.assertFalse(writer.done())
            finally:
                release.set()
            pending.result(timeout=10)
            with self.assertRaises(SourceMetadataCutoverPending):
                writer.result(timeout=10)
        self.assertEqual(
            SourceField.objects.get(table=self.tables[0], field_name="id").data_type, "INTEGER"
        )

    @patch("etl.tasks.introspect_source")
    @patch("etl.tasks.get_etl_logger")
    @patch("etl.tasks.set_task_progress")
    def test_pending_cutover_blocks_introspection_and_records_failure(
        self, progress, logger, introspect
    ):
        _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        with self.assertRaises(SourceMetadataCutoverPending):
            with source_metadata_write_guard(self.source.pk):
                self.fail("A pending cutover must block metadata writes.")
        result = introspect_source_schema_task.apply(args=[self.source.pk], throw=True).get()
        self.assertIn("cutover is pending", result)
        self.source.refresh_from_db()
        self.assertEqual(self.source.last_sync_status, "FAILED")
        introspect.assert_not_called()

    def test_rejects_tracking_disabled_and_outer_transactions(self):
        with override_settings(CDC_EXTERNAL_OPERATION_TRACKING=False):
            with self.assertRaisesRegex(RuntimeError, "tracking is not enabled"):
                _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        with transaction.atomic():
            with self.assertRaisesRegex(RuntimeError, "durable atomic block"):
                _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        self.client.query.assert_not_called()

    def test_unresolved_operation_blocks_pending(self):
        operation = _begin_cdc_external_operation(
            self.source.pk, CdcSourceExternalOperation.Kind.INGESTION
        )
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)
        self.client.query.assert_not_called()
        _complete_cdc_external_operation(self.source.pk, operation.pk)
        _mark_cdc_to_batch_cutover_pending(self.transition.pk)

    def test_unresolved_report_blocks_pending(self):
        operation = _begin_cdc_external_operation(
            self.source.pk, CdcSourceExternalOperation.Kind.REPORT
        )
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)
        self.client.query.assert_not_called()
        _complete_cdc_external_operation(self.source.pk, operation.pk)

    def test_later_validation_failure_rolls_back_earlier_observations(self):
        self.uuids[self.checkpoints[1].staging_table] = "unexpected"
        with self.assertRaisesRegex(RuntimeError, "staged ClickHouse table"):
            _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)
        for checkpoint in self.checkpoints:
            checkpoint.refresh_from_db()
            self.assertFalse(checkpoint.active_table_checked)
            self.assertEqual(checkpoint.active_uuid_before, "")
        self.client.close.assert_called_once()

    def test_bad_staged_row_count_rolls_back_pending(self):
        with patch("etl.cdc_to_batch._validate_staging_row_count",
                   side_effect=[None, RuntimeError("row count changed")]):
            with self.assertRaisesRegex(RuntimeError, "row count changed"):
                _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)
        self.checkpoints[0].refresh_from_db()
        self.assertFalse(self.checkpoints[0].active_table_checked)

    def test_missing_keyless_active_table_is_observed(self):
        table, checkpoint = self.tables[0], self.checkpoints[0]
        table.cdc_managed = False
        table.primary_key_fields = []
        table.save(update_fields=["cdc_managed", "primary_key_fields"])
        checkpoint.staging_schema = _field_signature(
            self.source, _stage_fields(self.source, table), table
        )
        checkpoint.save(update_fields=["staging_schema"])
        del self.uuids[f"source_{self.source.pk}__public_{table.table_name}"]
        _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        checkpoint.refresh_from_db()
        self.assertTrue(checkpoint.active_table_checked)
        self.assertEqual(checkpoint.active_uuid_before, "")

    def test_rejects_changed_previous_active_observation(self):
        checkpoint = self.checkpoints[0]
        checkpoint.active_table_checked = True
        checkpoint.active_uuid_before = "outdated"
        checkpoint.save(update_fields=["active_table_checked", "active_uuid_before"])
        with self.assertRaisesRegex(RuntimeError, "changed after observation"):
            _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)

    def test_rejects_invalid_checkpoint_and_colliding_names(self):
        checkpoint = self.checkpoints[0]
        CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(
            cutover_command_inflight=True
        )
        with self.assertRaisesRegex(RuntimeError, "not staged"):
            _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(
            cutover_command_inflight=False,
            staging_table=f"source_{self.source.pk}__public_{self.tables[1].table_name}",
        )
        with self.assertRaisesRegex(RuntimeError, "names must be distinct"):
            _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)

    def test_rejects_rebuild_and_table_membership_changes(self):
        rebuild = CdcTableRebuild.objects.create(
            source_table=self.tables[0], connector_name="pending-conflict",
            topic_prefix="pending-conflict", notification_topic="pending-notifications",
            consumer_group="pending-group", staging_table="pending-stage",
        )
        with self.assertRaisesRegex(RuntimeError, "active CDC table rebuild"):
            _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        rebuild.delete()
        SourceTable.objects.create(source=self.source, schema_name="public", table_name="new")
        with self.assertRaisesRegex(RuntimeError, "table list changed"):
            _mark_cdc_to_batch_cutover_pending(self.transition.pk)
        self.client.query.assert_not_called()

    def test_report_reader_finishes_before_pending_commit(self):
        report = ReportDefinition.objects.create(
            name="Pending cutover report", source=self.source, base_table=self.tables[0],
            selected_fields=["id"],
        )
        report_started, release_report, pending_waiting = Event(), Event(), Event()

        def guarded_report():
            close_old_connections()
            try:
                with report_query_guard(report):
                    report_started.set()
                    if not release_report.wait(timeout=10):
                        raise TimeoutError("Report release timed out.")
            finally:
                close_old_connections()

        def watch_queries(execute, sql, params, many, context):
            if "pg_advisory_xact_lock(" in sql:
                pending_waiting.set()
            return execute(sql, params, many, context)

        def mark_pending():
            close_old_connections()
            try:
                with connection.execute_wrapper(watch_queries):
                    return _mark_cdc_to_batch_cutover_pending(self.transition.pk)
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            reader = pool.submit(guarded_report)
            try:
                self.assertTrue(report_started.wait(timeout=10))
                pending = pool.submit(mark_pending)
                self.assertTrue(pending_waiting.wait(timeout=10))
                self.assertFalse(pending.done())
            finally:
                release_report.set()
            reader.result(timeout=10)
            self.assertEqual(
                pending.result(timeout=10).status, CdcToBatchTransition.Status.CUTOVER_PENDING
            )
        with self.assertRaisesRegex(RuntimeError, "cutover is pending"):
            with report_query_guard(report):
                pass

    def test_pending_commit_prevents_writer_registration(self):
        validating, release_validation, writer_waiting = Event(), Event(), Event()

        def validate(*args, **kwargs):
            validating.set()
            if not release_validation.wait(timeout=10):
                raise TimeoutError("Validation release timed out.")

        def mark_pending():
            close_old_connections()
            try:
                return _mark_cdc_to_batch_cutover_pending(self.transition.pk)
            finally:
                close_old_connections()

        def watch_queries(execute, sql, params, many, context):
            if "FOR UPDATE" in sql and "control_plane_sourceprofile" in sql:
                writer_waiting.set()
            return execute(sql, params, many, context)

        def register_writer():
            close_old_connections()
            try:
                with connection.execute_wrapper(watch_queries):
                    return _begin_cdc_external_operation(
                        self.source.pk, CdcSourceExternalOperation.Kind.INGESTION
                    )
            finally:
                close_old_connections()

        with patch("etl.cdc_to_batch._validate_staging_table", side_effect=validate), \
             ThreadPoolExecutor(max_workers=2) as pool:
            pending = pool.submit(mark_pending)
            try:
                self.assertTrue(validating.wait(timeout=10))
                writer = pool.submit(register_writer)
                self.assertTrue(writer_waiting.wait(timeout=10))
                self.assertFalse(writer.done())
            finally:
                release_validation.set()
            self.assertEqual(
                pending.result(timeout=10).status, CdcToBatchTransition.Status.CUTOVER_PENDING
            )
            with self.assertRaisesRegex(RuntimeError, "cutover is pending"):
                writer.result(timeout=10)
        self.assertFalse(CdcSourceExternalOperation.objects.filter(source=self.source).exists())


@skipUnlessDBFeature("has_select_for_update")
class CdcExternalOperationTests(TransactionTestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="External operation source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="analytics",
            username="cdc_user", ingestion_mode=IngestionMode.CDC,
        )
        self.other = SourceProfile.objects.create(
            name="Other external operation source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="analytics",
            username="cdc_user", ingestion_mode=IngestionMode.CDC,
        )
        self.transition = CdcToBatchTransition.objects.create(source=self.source)

    def begin(self, source=None):
        return _begin_cdc_external_operation(
            (source or self.source).pk,
            CdcSourceExternalOperation.Kind.INGESTION,
            task_id="test-task",
        )

    def test_record_is_visible_and_blocks_only_its_source(self):
        operation = self.begin()

        def verify_from_other_session():
            close_old_connections()
            try:
                self.assertTrue(CdcSourceExternalOperation.objects.filter(pk=operation.pk).exists())
                with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
                    with _cdc_external_operation_barrier(self.source.pk):
                        self.fail("An unresolved operation must block the barrier.")
                with _cdc_external_operation_barrier(self.other.pk) as source:
                    self.assertEqual(source.pk, self.other.pk)
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(verify_from_other_session).result(timeout=10)
        _complete_cdc_external_operation(self.source.pk, operation.pk)
        with _cdc_external_operation_barrier(self.source.pk):
            pass

    def test_unresolved_record_protects_source_from_deletion(self):
        operation = self.begin()
        with self.assertRaises(ProtectedError):
            self.source.delete()
        _complete_cdc_external_operation(self.source.pk, operation.pk)
        self.source.delete()

    def test_invalid_operation_cannot_clear_another_sources_record(self):
        operation = self.begin()
        with self.assertRaises(CdcSourceExternalOperation.DoesNotExist):
            _complete_cdc_external_operation(self.other.pk, operation.pk)
        self.assertTrue(CdcSourceExternalOperation.objects.filter(pk=operation.pk).exists())
        with self.assertRaisesRegex(ValueError, "kind"):
            _begin_cdc_external_operation(self.source.pk, "invalid")
        with self.assertRaisesRegex(ValueError, "task ID"):
            _begin_cdc_external_operation(self.source.pk, "INGESTION", "x" * 256)

    def test_pending_or_ineligible_source_rejects_new_operations(self):
        self.transition.status = CdcToBatchTransition.Status.CUTOVER_PENDING
        self.transition.save(update_fields=["status"])
        with self.assertRaisesRegex(RuntimeError, "cutover is pending"):
            self.begin()
        self.transition.status = CdcToBatchTransition.Status.PREPARING
        self.transition.save(update_fields=["status"])
        for changes in ({"is_active": False}, {"ingestion_mode": IngestionMode.BATCH}):
            with self.subTest(changes=changes):
                SourceProfile.objects.filter(pk=self.source.pk).update(**changes)
                with self.assertRaisesRegex(RuntimeError, "not eligible"):
                    self.begin()
                SourceProfile.objects.filter(pk=self.source.pk).update(
                    is_active=True, ingestion_mode=IngestionMode.CDC
                )
        self.assertFalse(CdcSourceExternalOperation.objects.exists())

    def test_primitives_reject_enclosing_transactions(self):
        operation = self.begin()
        with transaction.atomic():
            with self.assertRaisesRegex(RuntimeError, "durable atomic block"):
                self.begin()
            with self.assertRaisesRegex(RuntimeError, "durable atomic block"):
                _complete_cdc_external_operation(self.source.pk, operation.pk)
            with self.assertRaisesRegex(RuntimeError, "durable atomic block"):
                with _cdc_external_operation_barrier(self.source.pk):
                    pass
        self.assertTrue(CdcSourceExternalOperation.objects.filter(pk=operation.pk).exists())

    def test_barrier_rollback_does_not_leave_source_pending(self):
        with self.assertRaisesRegex(RuntimeError, "abort cutover"):
            with _cdc_external_operation_barrier(self.source.pk):
                CdcToBatchTransition.objects.filter(pk=self.transition.pk).update(
                    status=CdcToBatchTransition.Status.CUTOVER_PENDING
                )
                raise RuntimeError("abort cutover")
        self.transition.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)
        operation = self.begin()
        self.assertTrue(CdcSourceExternalOperation.objects.filter(pk=operation.pk).exists())

    def test_primitives_fail_closed_without_row_lock_support(self):
        with patch.object(connection.features, "has_select_for_update", False):
            with self.assertRaisesRegex(RuntimeError, "requires row locks"):
                self.begin()
            with self.assertRaisesRegex(RuntimeError, "requires row locks"):
                with _cdc_external_operation_barrier(self.source.pk):
                    pass
            with self.assertRaisesRegex(RuntimeError, "requires row locks"):
                _complete_cdc_external_operation(self.source.pk, uuid4())

    def test_barrier_waits_for_registration_and_then_rejects_it(self):
        inserted = Event()
        barrier_waiting = Event()
        release_registration = Event()

        def watch_registration(execute, sql, params, many, context):
            result = execute(sql, params, many, context)
            if "INSERT INTO" in sql and "control_plane_cdcsourceexternaloperation" in sql:
                inserted.set()
                if not release_registration.wait(timeout=10):
                    raise TimeoutError("Registration release timed out.")
            return result

        def watch_barrier(execute, sql, params, many, context):
            if "FOR UPDATE" in sql and "control_plane_sourceprofile" in sql:
                barrier_waiting.set()
            return execute(sql, params, many, context)

        def register():
            close_old_connections()
            try:
                with connection.execute_wrapper(watch_registration):
                    return self.begin()
            finally:
                close_old_connections()

        def cutover():
            close_old_connections()
            try:
                with connection.execute_wrapper(watch_barrier):
                    with _cdc_external_operation_barrier(self.source.pk):
                        raise AssertionError("Barrier admitted an unresolved operation.")
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            registration = pool.submit(register)
            try:
                self.assertTrue(inserted.wait(timeout=10))
                barrier = pool.submit(cutover)
                self.assertTrue(barrier_waiting.wait(timeout=10))
                self.assertFalse(barrier.done())
            finally:
                release_registration.set()
            operation = registration.result(timeout=10)
            with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
                barrier.result(timeout=10)
        self.assertTrue(CdcSourceExternalOperation.objects.filter(pk=operation.pk).exists())

    def test_pending_commit_blocks_a_concurrent_operation(self):
        barrier_held = Event()
        operation_waiting = Event()
        release_barrier = Event()

        def watch_queries(execute, sql, params, many, context):
            if "FOR UPDATE" in sql and "control_plane_sourceprofile" in sql:
                operation_waiting.set()
            return execute(sql, params, many, context)

        def cutover():
            close_old_connections()
            try:
                with _cdc_external_operation_barrier(self.source.pk):
                    barrier_held.set()
                    if not release_barrier.wait(timeout=10):
                        raise TimeoutError("Barrier release timed out.")
                    CdcToBatchTransition.objects.filter(pk=self.transition.pk).update(
                        status=CdcToBatchTransition.Status.CUTOVER_PENDING
                    )
            finally:
                close_old_connections()

        def begin_after_barrier():
            close_old_connections()
            try:
                with connection.execute_wrapper(watch_queries):
                    return self.begin()
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            cutover_future = pool.submit(cutover)
            try:
                self.assertTrue(barrier_held.wait(timeout=10))
                begin_future = pool.submit(begin_after_barrier)
                self.assertTrue(operation_waiting.wait(timeout=10))
                self.assertFalse(begin_future.done())
            finally:
                release_barrier.set()
            cutover_future.result(timeout=10)
            with self.assertRaisesRegex(RuntimeError, "cutover is pending"):
                begin_future.result(timeout=10)
        self.assertFalse(CdcSourceExternalOperation.objects.exists())


@skipUnlessDBFeature("has_select_for_update")
@override_settings(CDC_EXTERNAL_OPERATION_TRACKING=True)
class CdcExternalOperationTaskTests(TransactionTestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Tracked operation source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="analytics",
            username="cdc_user", ingestion_mode=IngestionMode.CDC,
        )
        self.table = SourceTable.objects.create(
            source=self.source, schema_name="public", table_name="events",
            primary_key_fields=["id"], cdc_managed=True,
        )

    def test_ingestion_refresh_and_rebuild_register_before_work_and_clear_on_success(self):
        cases = (
            (
                "etl.tasks._run_scheduled_cdc_ingestion_locked",
                CdcSourceExternalOperation.Kind.INGESTION,
                lambda: ingest_source_to_clickhouse_task.apply(
                    args=[self.source.pk], kwargs={"cdc_continue": True}, throw=True
                ).get(),
            ),
            (
                "etl.tasks._ingest_source_to_clickhouse_batch",
                CdcSourceExternalOperation.Kind.REFRESH,
                lambda: ingest_source_to_clickhouse_task.apply(
                    args=[self.source.pk], kwargs={"cdc_fallback_only": True}, throw=True
                ).get(),
            ),
            (
                "etl.tasks._rebuild_cdc_table",
                CdcSourceExternalOperation.Kind.REBUILD,
                lambda: rebuild_cdc_table_task.apply(
                    args=[self.table.pk], throw=True
                ).get(),
            ),
        )
        for target, kind, execute in cases:
            with self.subTest(kind=kind):
                def verify_in_separate_connection():
                    close_old_connections()
                    try:
                        operation = CdcSourceExternalOperation.objects.get(source=self.source)
                        self.assertEqual(operation.kind, kind)
                        self.assertTrue(operation.task_id)
                    finally:
                        close_old_connections()

                def work(*args, **kwargs):
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        pool.submit(verify_in_separate_connection).result(timeout=10)
                    return "completed"

                with patch(target, side_effect=work):
                    self.assertEqual(execute(), "completed")
                self.assertFalse(CdcSourceExternalOperation.objects.exists())

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.tasks.set_task_progress")
    def test_failed_or_interrupted_work_keeps_record(self, progress, logger):
        cases = (
            (
                "etl.tasks._run_scheduled_cdc_ingestion_locked",
                CdcSourceExternalOperation.Kind.INGESTION,
                lambda: ingest_source_to_clickhouse_task.apply(
                    args=[self.source.pk], kwargs={"cdc_continue": True}, throw=True
                ),
            ),
            (
                "etl.tasks._ingest_source_to_clickhouse_batch",
                CdcSourceExternalOperation.Kind.REFRESH,
                lambda: ingest_source_to_clickhouse_task.apply(
                    args=[self.source.pk], kwargs={"cdc_fallback_only": True}, throw=True
                ),
            ),
            (
                "etl.tasks._rebuild_cdc_table",
                CdcSourceExternalOperation.Kind.REBUILD,
                lambda: rebuild_cdc_table_task.apply(args=[self.table.pk], throw=True),
            ),
        )
        for target, kind, execute in cases:
            with self.subTest(kind=kind):
                with patch(target, side_effect=RuntimeError("external response lost")):
                    with self.assertRaisesRegex(RuntimeError, "external response lost"):
                        execute()
                operation = CdcSourceExternalOperation.objects.get(source=self.source)
                self.assertEqual(operation.kind, kind)
                with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
                    with _cdc_external_operation_barrier(self.source.pk):
                        pass
                # The test simulates known-terminated external work.
                _complete_cdc_external_operation(self.source.pk, operation.pk)

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks._ingest_source_to_clickhouse_batch", return_value="completed")
    @patch("etl.cdc_external_operations._complete_cdc_external_operation",
           side_effect=RuntimeError("completion failed"))
    def test_completion_failure_keeps_record(self, complete, work, progress, logger):
        with self.assertRaisesRegex(RuntimeError, "completion failed"):
            ingest_source_to_clickhouse_task.apply(
                args=[self.source.pk], kwargs={"cdc_fallback_only": True}, throw=True
            )
        self.assertTrue(CdcSourceExternalOperation.objects.filter(source=self.source).exists())

    @patch("etl.tasks.clickhouse_connect.get_client")
    def test_rebuild_rejects_source_change_after_registration(self, get_client):
        with self.assertRaisesRegex(RuntimeError, "source changed"):
            _rebuild_cdc_table(
                SimpleNamespace(), self.table.pk, expected_source_pk=self.source.pk + 1
            )
        get_client.assert_not_called()

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.tasks.set_task_progress")
    def test_record_blocks_cutover_after_worker_database_connection_is_lost(
        self, progress, logger
    ):
        for target, kwargs in (
            ("etl.tasks._run_scheduled_cdc_ingestion_locked", {"cdc_continue": True}),
            ("etl.tasks._ingest_source_to_clickhouse_batch", {"cdc_fallback_only": True}),
        ):
            with self.subTest(target=target):
                lost_connection = Event()
                release_worker = Event()

                def work(*args, **work_kwargs):
                    connection.close()
                    lost_connection.set()
                    if not release_worker.wait(timeout=10):
                        raise TimeoutError("Worker release timed out.")
                    raise RuntimeError("external response lost")

                def run_task():
                    close_old_connections()
                    try:
                        return ingest_source_to_clickhouse_task.apply(
                            args=[self.source.pk], kwargs=kwargs, throw=True
                        )
                    finally:
                        close_old_connections()

                with patch(target, side_effect=work), ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(run_task)
                    try:
                        self.assertTrue(lost_connection.wait(timeout=10))
                        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
                            with _cdc_external_operation_barrier(self.source.pk):
                                pass
                    finally:
                        release_worker.set()
                    with self.assertRaisesRegex(RuntimeError, "external response lost"):
                        future.result(timeout=10)
                operation = CdcSourceExternalOperation.objects.get(source=self.source)
                _complete_cdc_external_operation(self.source.pk, operation.pk)

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks._ingest_source_to_clickhouse_batch")
    @patch("etl.tasks._run_scheduled_cdc_ingestion_locked")
    def test_pending_cutover_skips_ingestion_and_refresh(self, ingestion, refresh, progress, logger):
        CdcToBatchTransition.objects.create(
            source=self.source, status=CdcToBatchTransition.Status.CUTOVER_PENDING
        )
        for kwargs in ({"cdc_continue": True}, {"cdc_fallback_only": True}):
            result = ingest_source_to_clickhouse_task.apply(
                args=[self.source.pk], kwargs=kwargs, throw=True
            ).get()
            self.assertIn("cutover is pending", result)
        ingestion.assert_not_called()
        refresh.assert_not_called()
        self.assertFalse(CdcSourceExternalOperation.objects.exists())


@skipUnlessDBFeature("has_select_for_update")
class CdcToBatchFinalizationConcurrencyTests(TransactionTestCase):
    def setUp(self):
        CdcToBatchFinalizationTests.setUp(self)

    def test_finalization_does_not_hold_source_while_waiting_for_checkpoint(self):
        checkpoint_held = Event()
        finalizer_waiting = Event()
        source_acquired = Event()
        release_checkpoint = Event()

        def watch_queries(execute, sql, params, many, context):
            if "FOR UPDATE" in sql and "control_plane_cdctobatchtablecheckpoint" in sql:
                finalizer_waiting.set()
            return execute(sql, params, many, context)

        def finish():
            close_old_connections()
            try:
                with connection.execute_wrapper(watch_queries):
                    return _finalize_cdc_to_batch_transition(self.transition.pk)
            finally:
                close_old_connections()

        def hold_checkpoint():
            close_old_connections()
            try:
                with transaction.atomic():
                    CdcToBatchTableCheckpoint.objects.select_for_update().get(
                        pk=self.checkpoints[0].pk
                    )
                    checkpoint_held.set()
                    if not finalizer_waiting.wait(timeout=10):
                        raise TimeoutError("Finalization did not attempt the checkpoint lock.")
                    SourceProfile.objects.select_for_update().get(pk=self.source.pk)
                    source_acquired.set()
                    if not release_checkpoint.wait(timeout=10):
                        raise TimeoutError("Checkpoint release timed out.")
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            holder = pool.submit(hold_checkpoint)
            try:
                self.assertTrue(checkpoint_held.wait(timeout=10))
                finalizer = pool.submit(finish)
                self.assertTrue(finalizer_waiting.wait(timeout=10))
                self.assertTrue(source_acquired.wait(timeout=10))
                self.assertFalse(finalizer.done())
            finally:
                release_checkpoint.set()
            holder.result(timeout=10)
            self.assertEqual(
                finalizer.result(timeout=10).status, CdcToBatchTransition.Status.SUCCEEDED
            )

    def test_finalization_rejects_checkpoint_added_while_waiting(self):
        checkpoint_held = Event()
        finalizer_waiting = Event()
        release_checkpoint = Event()

        def watch_queries(execute, sql, params, many, context):
            if "FOR UPDATE" in sql and "control_plane_cdctobatchtablecheckpoint" in sql:
                finalizer_waiting.set()
            return execute(sql, params, many, context)

        def finish():
            close_old_connections()
            try:
                with connection.execute_wrapper(watch_queries):
                    return _finalize_cdc_to_batch_transition(self.transition.pk)
            finally:
                close_old_connections()

        def add_checkpoint():
            close_old_connections()
            try:
                with transaction.atomic():
                    CdcToBatchTableCheckpoint.objects.select_for_update().get(
                        pk=self.checkpoints[0].pk
                    )
                    checkpoint_held.set()
                    if not finalizer_waiting.wait(timeout=10):
                        raise TimeoutError("Finalization did not attempt the checkpoint lock.")
                    table = SourceTable.objects.create(
                        source_id=self.source.pk, schema_name="public", table_name="new_table"
                    )
                    CdcToBatchTableCheckpoint.objects.create(
                        transition_id=self.transition.pk, source_table=table,
                        staging_table="new_stage",
                    )
                    if not release_checkpoint.wait(timeout=10):
                        raise TimeoutError("Checkpoint release timed out.")
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            holder = pool.submit(add_checkpoint)
            try:
                self.assertTrue(checkpoint_held.wait(timeout=10))
                finalizer = pool.submit(finish)
                self.assertTrue(finalizer_waiting.wait(timeout=10))
            finally:
                release_checkpoint.set()
            holder.result(timeout=10)
            with self.assertRaisesRegex(RuntimeError, "checkpoints changed"):
                finalizer.result(timeout=10)
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)


class CdcToBatchGenerationPublicationTests(TransactionTestCase):
    def setUp(self):
        CdcToBatchFinalizationTests.setUp(self)
        for checkpoint in self.checkpoints:
            checkpoint.status = CdcToBatchTableCheckpoint.Status.STAGED
            checkpoint.generation_table = (
                f"rh_batch_publish_{self.transition.generation.hex}_"
                f"{checkpoint.source_table_id}_{uuid4().hex}"
            )
            checkpoint.generation_uuid = f"copy-{checkpoint.pk}"
            checkpoint.save(update_fields=["status", "generation_table", "generation_uuid"])
            self.uuids[checkpoint.generation_table] = checkpoint.generation_uuid

    def test_atomically_publishes_all_generations_and_retains_connector_metadata(self):
        _finalize_cdc_to_batch_generation(self.transition.pk)
        self.source.refresh_from_db()
        self.transition.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.BATCH)
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.SUCCEEDED)
        for table, checkpoint in zip(self.tables, self.checkpoints):
            table.refresh_from_db()
            checkpoint.refresh_from_db()
            self.assertEqual(table.clickhouse_physical_table, checkpoint.generation_table)
            self.assertEqual(checkpoint.status, CdcToBatchTableCheckpoint.Status.CUTOVER)
            self.assertFalse(table.cdc_managed)
            self.assertTrue(table.cdc_connector_name)
        self.assertEqual(_finalize_cdc_to_batch_generation(self.transition.pk).pk, self.transition.pk)

    def test_duplicate_copy_does_not_replace_verified_generation(self):
        checkpoint = self.checkpoints[0]
        result = _copy_cdc_to_batch_generation(checkpoint.pk)
        self.assertEqual(result.generation_table, checkpoint.generation_table)
        self.assertEqual(result.generation_uuid, checkpoint.generation_uuid)
        self.client.command.assert_not_called()

    def test_changed_second_generation_leaves_all_metadata_unchanged(self):
        self.uuids[self.checkpoints[1].generation_table] = "unexpected"
        with self.assertRaisesRegex(RuntimeError, "generation is missing or changed"):
            _finalize_cdc_to_batch_generation(self.transition.pk)
        self.source.refresh_from_db()
        self.transition.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.CUTOVER_PENDING)
        for table, checkpoint in zip(self.tables, self.checkpoints):
            table.refresh_from_db()
            checkpoint.refresh_from_db()
            self.assertFalse(table.clickhouse_physical_table)
            self.assertTrue(table.cdc_managed)
            self.assertEqual(checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)

    def test_rejects_foreign_or_missing_candidate(self):
        checkpoint = self.checkpoints[0]
        checkpoint.generation_table = "rh_batch_publish_wrong"
        checkpoint.save(update_fields=["generation_table"])
        with self.assertRaisesRegex(RuntimeError, "invalid for this checkpoint"):
            _finalize_cdc_to_batch_generation(self.transition.pk)
        checkpoint.generation_uuid = ""
        checkpoint.save(update_fields=["generation_uuid"])
        with self.assertRaisesRegex(RuntimeError, "Not every generation copy"):
            _finalize_cdc_to_batch_generation(self.transition.pk)

    def test_exchange_path_cannot_run_after_generation_attempt(self):
        with self.assertRaisesRegex(RuntimeError, "cannot be mixed"):
            _cutover_cdc_to_batch_table(self.checkpoints[0].pk)
        self.client.command.assert_not_called()

    def test_copy_cannot_start_after_any_table_exchange(self):
        checkpoint = self.checkpoints[1]
        checkpoint.status = CdcToBatchTableCheckpoint.Status.CUTOVER
        checkpoint.save(update_fields=["status"])
        with self.assertRaisesRegex(RuntimeError, "cannot be mixed"):
            _copy_cdc_to_batch_generation(self.checkpoints[0].pk)
        self.client.command.assert_not_called()


class CdcToBatchTableCutoverTests(TransactionTestCase):
    def setUp(self):
        CdcToBatchFinalizationTests.setUp(self)
        self.checkpoint = self.checkpoints[0]
        self.table = self.tables[0]
        self.active_name = f"source_{self.source.pk}__public_{self.table.table_name}"
        self.checkpoint.status = CdcToBatchTableCheckpoint.Status.STAGED
        self.checkpoint.save(update_fields=["status"])
        self.uuids[self.active_name] = self.checkpoint.active_uuid_before
        self.uuids[self.checkpoint.staging_table] = self.checkpoint.staging_uuid

        def execute(sql):
            if sql.startswith("EXCHANGE TABLES"):
                self.uuids[self.active_name], self.uuids[self.checkpoint.staging_table] = (
                    self.uuids[self.checkpoint.staging_table], self.uuids[self.active_name]
                )
            elif sql.startswith("RENAME TABLE"):
                self.uuids[self.active_name] = self.uuids.pop(self.checkpoint.staging_table)
            else:
                raise AssertionError(f"Unexpected ClickHouse command: {sql}")

        self.client.command.side_effect = execute

    def test_exchange_and_retry_do_not_repeat_swap(self):
        result = _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.assertEqual(result.status, CdcToBatchTableCheckpoint.Status.CUTOVER)
        self.assertEqual(self.uuids[self.active_name], self.checkpoint.staging_uuid)
        self.assertEqual(self.uuids[self.checkpoint.staging_table], self.checkpoint.active_uuid_before)
        self.assertTrue(self.client.command.call_args.args[0].startswith("EXCHANGE TABLES"))
        _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_called_once()
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)

    def test_rename_missing_keyless_table_and_retry(self):
        self.table.primary_key_fields = []
        self.table.cdc_managed = False
        self.table.save(update_fields=["primary_key_fields", "cdc_managed"])
        self.checkpoint.active_uuid_before = ""
        self.checkpoint.staging_schema = _field_signature(
            self.source, _stage_fields(self.source, self.table), self.table
        )
        self.checkpoint.save(update_fields=["active_uuid_before", "staging_schema"])
        self.uuids.pop(self.active_name)
        _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.assertEqual(self.uuids[self.active_name], self.checkpoint.staging_uuid)
        self.assertNotIn(self.checkpoint.staging_table, self.uuids)
        self.assertTrue(self.client.command.call_args.args[0].startswith("RENAME TABLE"))
        _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_called_once()

    def test_checkpoint_save_failure_blocks_retry_after_exchange(self):
        original_save = CdcToBatchTableCheckpoint.save

        def fail_final_save(checkpoint, *args, **kwargs):
            if checkpoint.status == CdcToBatchTableCheckpoint.Status.CUTOVER:
                raise RuntimeError("save failed")
            return original_save(checkpoint, *args, **kwargs)

        with patch.object(CdcToBatchTableCheckpoint, "save", fail_final_save):
            with self.assertRaisesRegex(RuntimeError, "save failed"):
                _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.assertTrue(self.checkpoint.cutover_command_inflight)
        self.assertEqual(self.uuids[self.active_name], self.checkpoint.staging_uuid)
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_called_once()

    def test_blocks_retry_when_exchange_response_is_lost(self):
        original_command = self.client.command.side_effect

        def lost_response(sql):
            original_command(sql)
            raise RuntimeError("response lost")

        self.client.command.side_effect = lost_response
        with self.assertRaisesRegex(RuntimeError, "response lost"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.side_effect = original_command
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_called_once()

    def test_blocks_retry_when_rename_response_is_lost(self):
        self.table.cdc_managed = False
        self.table.save(update_fields=["cdc_managed"])
        self.checkpoint.active_uuid_before = ""
        self.checkpoint.save(update_fields=["active_uuid_before"])
        self.uuids.pop(self.active_name)
        original_command = self.client.command.side_effect

        def lost_response(sql):
            original_command(sql)
            raise RuntimeError("response lost")

        self.client.command.side_effect = lost_response
        with self.assertRaisesRegex(RuntimeError, "response lost"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.side_effect = original_command
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_called_once()

    def test_marker_is_committed_before_command_and_blocks_concurrent_retry(self):
        original_command = self.client.command.side_effect

        def competing_worker():
            close_old_connections()
            try:
                checkpoint = CdcToBatchTableCheckpoint.objects.get(pk=self.checkpoint.pk)
                self.assertTrue(checkpoint.cutover_command_inflight)
                with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
                    _cutover_cdc_to_batch_table(self.checkpoint.pk)
            finally:
                close_old_connections()

        def execute(sql):
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(competing_worker).result(timeout=10)
            original_command(sql)

        self.client.command.side_effect = execute
        _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertFalse(self.checkpoint.cutover_command_inflight)
        self.client.command.assert_called_once()

    def test_unexecuted_timed_out_command_cannot_be_retried(self):
        self.client.command.side_effect = RuntimeError("request timed out")
        with self.assertRaisesRegex(RuntimeError, "request timed out"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertTrue(self.checkpoint.cutover_command_inflight)
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_called_once()

    def test_cutover_command_must_not_run_inside_outer_transaction(self):
        with transaction.atomic():
            with self.assertRaisesRegex(RuntimeError, "durable atomic block"):
                _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_not_called()
        self.checkpoint.refresh_from_db()
        self.assertFalse(self.checkpoint.cutover_command_inflight)

    def test_rejects_non_atomic_and_missing_cdc_managed_table(self):
        self.client.query.return_value.result_rows = [["Ordinary"]]
        with self.assertRaisesRegex(RuntimeError, "Atomic"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.active_uuid_before = ""
        self.checkpoint.save(update_fields=["active_uuid_before"])
        with self.assertRaisesRegex(RuntimeError, "CDC-managed active.*missing"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_not_called()

    def test_rejects_bad_post_exchange_identity_before_checkpoint(self):
        self.client.command.side_effect = lambda sql: None
        with self.assertRaisesRegex(RuntimeError, "did not complete"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)

    def assert_change_during_exchange_blocks_completion(self, change, error):
        execute = self.client.command.side_effect

        def exchange_and_change(sql):
            execute(sql)
            change()

        self.client.command.side_effect = exchange_and_change
        with self.assertRaisesRegex(RuntimeError, error):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.assertTrue(self.checkpoint.cutover_command_inflight)
        with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_called_once()

    def test_rejects_checkpoint_changes_during_exchange(self):
        execute = self.client.command.side_effect
        changes = (
            {"staging_row_count": 2},
            {"staging_schema": {}},
            {"active_table_checked": False},
        )
        for change in changes:
            with self.subTest(change=change):
                original = {key: getattr(self.checkpoint, key) for key in change}
                self.assert_change_during_exchange_blocks_completion(
                    lambda: CdcToBatchTableCheckpoint.objects.filter(
                        pk=self.checkpoint.pk
                    ).update(**change),
                    "marker changed",
                )
                CdcToBatchTableCheckpoint.objects.filter(pk=self.checkpoint.pk).update(
                    cutover_command_inflight=False, **original
                )
                self.checkpoint.refresh_from_db()
                self.uuids[self.active_name] = self.checkpoint.active_uuid_before
                self.uuids[self.checkpoint.staging_table] = self.checkpoint.staging_uuid
                self.client.command.reset_mock()
                self.client.command.side_effect = execute

    def test_rejects_source_configuration_change_during_exchange(self):
        self.assert_change_during_exchange_blocks_completion(
            lambda: SourceProfile.objects.filter(pk=self.source.pk).update(username="other"),
            "configuration changed",
        )

    def test_rejects_source_deactivation_during_exchange(self):
        self.assert_change_during_exchange_blocks_completion(
            lambda: SourceProfile.objects.filter(pk=self.source.pk).update(is_active=False),
            "not eligible",
        )

    def test_rejects_table_cdc_configuration_change_during_exchange(self):
        self.assert_change_during_exchange_blocks_completion(
            lambda: SourceTable.objects.filter(pk=self.table.pk).update(cdc_managed=False),
            "CDC configuration changed",
        )

    def test_rejects_transition_change_during_exchange(self):
        self.assert_change_during_exchange_blocks_completion(
            lambda: CdcToBatchTransition.objects.filter(pk=self.transition.pk).update(
                status=CdcToBatchTransition.Status.FAILED
            ),
            "not eligible",
        )

    def test_rejects_new_source_table_during_exchange(self):
        self.assert_change_during_exchange_blocks_completion(
            lambda: SourceTable.objects.create(
                source=self.source, schema_name="public", table_name="new_table"
            ),
            "table list changed",
        )

    def test_rejects_new_rebuild_during_exchange(self):
        self.assert_change_during_exchange_blocks_completion(
            lambda: CdcTableRebuild.objects.create(
                source_table=self.table, connector_name="conflict",
                topic_prefix="conflict", notification_topic="conflict-notifications",
                consumer_group="conflict-group", staging_table="conflict-stage",
            ),
            "active CDC table rebuild",
        )

    def test_revalidates_physical_table_after_exchange(self):
        with patch(
            "etl.cdc_to_batch._validate_staging_row_count",
            side_effect=[None, RuntimeError("row count changed")],
        ):
            with self.assertRaisesRegex(RuntimeError, "row count changed"):
                _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertTrue(self.checkpoint.cutover_command_inflight)
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.client.command.assert_called_once()

    def test_revalidates_physical_schema_after_exchange(self):
        with patch(
            "etl.cdc_to_batch._validate_staging_table",
            side_effect=[None, RuntimeError("columns changed")],
        ):
            with self.assertRaisesRegex(RuntimeError, "columns changed"):
                _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertTrue(self.checkpoint.cutover_command_inflight)
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.client.command.assert_called_once()

    def test_rejects_bad_uuid_count_and_non_pending_transition(self):
        self.uuids[self.active_name] = "unexpected"
        with self.assertRaisesRegex(RuntimeError, "UUIDs changed"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.uuids[self.active_name] = self.checkpoint.active_uuid_before
        with patch("etl.cdc_to_batch._validate_staging_row_count",
                   side_effect=RuntimeError("row count changed")):
            with self.assertRaisesRegex(RuntimeError, "row count changed"):
                _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.transition.status = CdcToBatchTransition.Status.PREPARING
        self.transition.save(update_fields=["status"])
        with self.assertRaisesRegex(RuntimeError, "not eligible"):
            _cutover_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_not_called()
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)


class CdcToBatchStagingTests(TestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Transition staging source",
            db_type=DbChoice.MYSQL,
            host="localhost",
            port=3306,
            database_name="analytics",
            username="cdc_user",
            ingestion_mode=IngestionMode.CDC,
        )
        self.table = SourceTable.objects.create(
            source=self.source,
            schema_name="analytics",
            table_name="events",
            primary_key_fields=["id"],
            cdc_managed=True,
        )
        SourceField.objects.create(
            table=self.table, field_name="id", data_type="INTEGER", is_selected=False,
        )
        self.value = SourceField.objects.create(
            table=self.table, field_name="value", data_type="VARCHAR",
        )
        transition = prepare_cdc_to_batch_transition(self.source.pk)
        self.checkpoint = transition.table_checkpoints.get(source_table=self.table)
        self.engine = sa.create_engine("sqlite://")
        self.addCleanup(self.engine.dispose)
        with self.engine.begin() as connection:
            connection.execute(sa.text("CREATE TABLE events (id INTEGER, value TEXT)"))
            connection.execute(sa.text(
                "INSERT INTO events VALUES (1, 'one'), (2, 'two'), (3, 'three')"
            ))
        self.dispose_patch = patch.object(self.engine, "dispose")
        self.dispose_patch.start()
        self.addCleanup(self.dispose_patch.stop)
        self.engine_patch = patch("etl.cdc_to_batch.create_source_engine", return_value=self.engine)
        self.engine_patch.start()
        self.addCleanup(self.engine_patch.stop)
        self.client = MagicMock()
        self.client.query.return_value.result_rows = [["Atomic"]]

        def query(sql, *args, **kwargs):
            if sql.startswith("SELECT count() FROM "):
                counts = {}
                for call in self.client.insert.call_args_list:
                    counts[call.args[0]] = counts.get(call.args[0], 0) + len(call.args[1])
                return SimpleNamespace(result_rows=[[
                    counts.get(sql.split("`")[1], 0)
                ]])
            if "FROM system.tables" in sql:
                return SimpleNamespace(result_rows=[("MergeTree", "", "", "", "")])
            if sql.startswith("DESCRIBE TABLE"):
                table = SourceTable.objects.get(pk=self.table.pk)
                profile = SourceProfile.objects.get(pk=self.source.pk)
                return SimpleNamespace(result_rows=[
                    (field.field_name, map_to_clickhouse_type(
                        field.data_type, field.field_name not in table.primary_key_fields
                    ), "", "")
                    for field in _stage_fields(profile, table)
                ])
            return self.client.query.return_value

        self.client.query.side_effect = query
        self.client_patch = patch("etl.cdc_to_batch.clickhouse_connect.get_client", return_value=self.client)
        self.get_client = self.client_patch.start()
        self.addCleanup(self.client_patch.stop)
        self.uuid_patch = patch("etl.cdc_to_batch._clickhouse_table_uuid", return_value="stage-uuid")
        self.table_uuid = self.uuid_patch.start()
        self.addCleanup(self.uuid_patch.stop)

    def test_streams_batches_without_touching_live_table(self):
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk, batch_size=2)

        self.assertEqual(checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.assertEqual(checkpoint.staging_uuid, "stage-uuid")
        self.assertEqual(checkpoint.staging_row_count, 3)
        self.assertEqual(checkpoint.staging_schema["username"], self.source.username)
        self.assertIs(checkpoint.staging_schema["synchronous_inserts"], True)
        self.assertEqual(self.get_client.call_args.kwargs["settings"], {"async_insert": 0})
        self.assertEqual(checkpoint.staging_schema["primary_key_fields"], ["id"])
        self.assertEqual(
            {tuple(field) for field in checkpoint.staging_schema["fields"]},
            {("id", "INTEGER"), ("value", "VARCHAR")},
        )
        self.assertEqual(self.client.insert.call_count, 2)
        rows = []
        for call in self.client.insert.call_args_list:
            self.assertEqual(call.args[0], checkpoint.staging_table)
            names = call.kwargs["column_names"]
            rows.extend(dict(zip(names, row)) for row in call.args[1])
        self.assertEqual(rows, [
            {"id": 1, "value": "one"}, {"id": 2, "value": "two"},
            {"id": 3, "value": "three"},
        ])
        self.assertEqual(self.client.command.call_count, 1)
        self.assertIn("MergeTree() ORDER BY tuple()", self.client.command.call_args.args[0])
        self.source.refresh_from_db()
        self.table.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.assertTrue(self.table.cdc_managed)

        stage_cdc_to_batch_table(self.checkpoint.pk, batch_size=2)
        self.assertEqual(self.client.command.call_count, 1)
        self.assertEqual(self.client.insert.call_count, 2)

    def test_keyless_and_empty_table(self):
        self.table.primary_key_fields = []
        self.table.save(update_fields=["primary_key_fields"])
        with self.engine.begin() as connection:
            connection.execute(sa.text("DELETE FROM events"))
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk, batch_size=2)
        self.assertEqual(checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.assertEqual(checkpoint.staging_schema["primary_key_fields"], [])
        self.assertEqual(checkpoint.staging_row_count, 0)
        self.client.insert.assert_not_called()

    def test_incomplete_insert_is_rejected_before_checkpoint(self):
        original_query = self.client.query.side_effect
        self.client.query.side_effect = lambda sql, *args, **kwargs: (
            SimpleNamespace(result_rows=[[2]])
            if sql.startswith("SELECT count() FROM ")
            else original_query(sql, *args, **kwargs)
        )
        with self.assertRaisesRegex(RuntimeError, "row count changed"):
            stage_cdc_to_batch_table(self.checkpoint.pk, batch_size=2)
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.PENDING)
        self.assertIsNone(self.checkpoint.staging_row_count)
        self.assertTrue(all(call.args[0].startswith("CREATE TABLE")
                            for call in self.client.command.call_args_list))

    def test_rejects_changed_row_count_on_retry_and_active_observation(self):
        stage_cdc_to_batch_table(self.checkpoint.pk)
        original_query = self.client.query.side_effect
        self.client.query.side_effect = lambda sql, *args, **kwargs: (
            SimpleNamespace(result_rows=[[4]])
            if sql.startswith("SELECT count() FROM ")
            else original_query(sql, *args, **kwargs)
        )
        with self.assertRaisesRegex(RuntimeError, "row count changed"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        with self.assertRaisesRegex(RuntimeError, "row count changed"):
            checkpoint_cdc_to_batch_active_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertFalse(self.checkpoint.active_table_checked)

    def test_rejects_invalid_count_query_results(self):
        stage_cdc_to_batch_table(self.checkpoint.pk)
        original_query = self.client.query.side_effect
        for result in ([], [[3, 0]], [[None]]):
            with self.subTest(result=result):
                self.client.query.side_effect = lambda sql, *args, **kwargs: (
                    SimpleNamespace(result_rows=result)
                    if sql.startswith("SELECT count() FROM ")
                    else original_query(sql, *args, **kwargs)
                )
                with self.assertRaisesRegex(RuntimeError, "row count changed"):
                    stage_cdc_to_batch_table(self.checkpoint.pk)

    def test_legacy_checkpoint_is_restaged_without_deleting_old_table(self):
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk)
        old_table = checkpoint.staging_table
        CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(
            staging_row_count=None, active_table_checked=True, active_uuid_before="old-live"
        )
        with self.assertRaisesRegex(RuntimeError, "row count is missing"):
            checkpoint_cdc_to_batch_active_table(checkpoint.pk)
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk)
        self.assertNotEqual(checkpoint.staging_table, old_table)
        self.assertEqual(checkpoint.staging_row_count, 3)
        self.assertFalse(checkpoint.active_table_checked)
        self.assertEqual(checkpoint.active_uuid_before, "")
        self.assertTrue(all(call.args[0].startswith("CREATE TABLE")
                            for call in self.client.command.call_args_list))

    def test_verification_only_never_stages_pending_or_legacy_checkpoints(self):
        with self.assertRaisesRegex(RuntimeError, "requires staging"):
            stage_cdc_to_batch_table(self.checkpoint.pk, verification_only=True)
        self.client.command.assert_not_called()
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk)
        for changes in (
            {"staging_row_count": None},
            {"staging_row_count": 3, "staging_schema": {}},
        ):
            CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(**changes)
            with self.assertRaisesRegex(RuntimeError, "requires staging"):
                stage_cdc_to_batch_table(checkpoint.pk, verification_only=True)
            self.assertEqual(self.client.command.call_count, 1)

    @patch("etl.tasks.stage_cdc_to_batch_transition_task.apply_async")
    def test_worker_restages_real_legacy_checkpoints_one_table_at_a_time(self, enqueue):
        second_table = SourceTable.objects.create(
            source=self.source, schema_name="analytics", table_name="other_events",
            primary_key_fields=["id"],
        )
        SourceField.objects.create(
            table=second_table, field_name="id", data_type="INTEGER"
        )
        SourceField.objects.create(
            table=second_table, field_name="value", data_type="VARCHAR"
        )
        with self.engine.begin() as connection:
            connection.execute(sa.text("CREATE TABLE other_events (id INTEGER, value TEXT)"))
            connection.execute(sa.text("INSERT INTO other_events VALUES (1, 'other')"))
        second = CdcToBatchTableCheckpoint.objects.create(
            transition=self.checkpoint.transition, source_table=second_table,
            staging_table="unused",
        )
        first = stage_cdc_to_batch_table(self.checkpoint.pk)
        second = stage_cdc_to_batch_table(second.pk)
        old_names = [first.staging_table, second.staging_table]
        CdcToBatchTableCheckpoint.objects.filter(pk__in=[first.pk, second.pk]).update(
            staging_row_count=None
        )
        for expected_count in (3, 4):
            result = stage_cdc_to_batch_transition_task.apply(
                args=[first.transition_id], throw=True
            ).get()
            self.assertIn("queued the next", result)
            self.assertEqual(self.client.command.call_count, expected_count)
        self.assertEqual(enqueue.call_count, 2)
        result = stage_cdc_to_batch_transition_task.apply(
            args=[first.transition_id], throw=True
        ).get()
        self.assertIn("All 2 tables staged", result)
        self.assertEqual(self.client.command.call_count, 4)
        first.refresh_from_db()
        second.refresh_from_db()
        self.assertNotEqual(first.staging_table, old_names[0])
        self.assertNotEqual(second.staging_table, old_names[1])
        self.assertEqual([first.staging_row_count, second.staging_row_count], [3, 1])
        self.assertTrue(all(call.args[0].startswith("CREATE TABLE")
                            for call in self.client.command.call_args_list))

    def test_rejects_malformed_staged_schema_without_restaging(self):
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk)
        CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(staging_schema=[])
        with self.assertRaisesRegex(RuntimeError, "schema is invalid"):
            stage_cdc_to_batch_table(checkpoint.pk)
        self.assertEqual(self.client.command.call_count, 1)

    def test_failed_legacy_restage_preserves_previous_checkpoint(self):
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk)
        old_table = checkpoint.staging_table
        CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(
            staging_row_count=None, active_table_checked=True, active_uuid_before="old-live"
        )
        self.client.insert.side_effect = RuntimeError("insert failed")
        with self.assertRaisesRegex(RuntimeError, "insert failed"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        checkpoint.refresh_from_db()
        self.assertEqual(checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.assertEqual(checkpoint.staging_table, old_table)
        self.assertTrue(checkpoint.active_table_checked)
        self.assertIsNone(checkpoint.staging_row_count)

    def test_pre_username_checkpoint_is_restaged_and_old_table_retained(self):
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk)
        old_table = checkpoint.staging_table
        signature = dict(checkpoint.staging_schema)
        del signature["username"]
        CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(
            staging_schema=signature, active_table_checked=True, active_uuid_before="old-live"
        )
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            checkpoint_cdc_to_batch_active_table(checkpoint.pk)
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk)
        self.assertNotEqual(checkpoint.staging_table, old_table)
        self.assertEqual(checkpoint.staging_schema["username"], self.source.username)
        self.assertEqual(checkpoint.staging_row_count, 3)
        self.assertFalse(checkpoint.active_table_checked)
        self.assertTrue(all(call.args[0].startswith("CREATE TABLE")
                            for call in self.client.command.call_args_list))

    def test_pre_synchronous_checkpoint_is_restaged_and_old_table_retained(self):
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk)
        old_table = checkpoint.staging_table
        signature = dict(checkpoint.staging_schema)
        del signature["synchronous_inserts"]
        CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(
            staging_schema=signature, active_table_checked=True, active_uuid_before="old-live"
        )
        with self.assertRaisesRegex(RuntimeError, "requires staging"):
            stage_cdc_to_batch_table(checkpoint.pk, verification_only=True)
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            checkpoint_cdc_to_batch_active_table(checkpoint.pk)
        checkpoint = stage_cdc_to_batch_table(checkpoint.pk)
        self.assertNotEqual(checkpoint.staging_table, old_table)
        self.assertIs(checkpoint.staging_schema["synchronous_inserts"], True)
        self.assertFalse(checkpoint.active_table_checked)
        self.assertTrue(all(call.args[0].startswith("CREATE TABLE")
                            for call in self.client.command.call_args_list))

    def test_failed_pre_username_restage_preserves_previous_checkpoint(self):
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk)
        old_table = checkpoint.staging_table
        signature = dict(checkpoint.staging_schema)
        del signature["username"]
        CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint.pk).update(
            staging_schema=signature
        )
        self.client.insert.side_effect = RuntimeError("insert failed")
        with self.assertRaisesRegex(RuntimeError, "insert failed"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        checkpoint.refresh_from_db()
        self.assertEqual(checkpoint.staging_table, old_table)
        self.assertEqual(checkpoint.staging_schema, signature)
        self.assertEqual(checkpoint.staging_row_count, 3)

    def test_keyless_table_streams_all_rows(self):
        self.table.primary_key_fields = []
        self.table.save(update_fields=["primary_key_fields"])
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk, batch_size=2)
        self.assertEqual(checkpoint.staging_schema["primary_key_fields"], [])
        self.assertEqual(sum(len(call.args[1]) for call in self.client.insert.call_args_list), 3)
        self.assertEqual(self.client.insert.call_args.kwargs["column_names"], ["value"])

    def test_failed_attempt_is_not_reused_or_dropped(self):
        self.client.insert.side_effect = [None, RuntimeError("insert failed")]
        with self.assertRaisesRegex(RuntimeError, "insert failed"):
            stage_cdc_to_batch_table(self.checkpoint.pk, batch_size=2)
        first_create = self.client.command.call_args.args[0]
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.PENDING)
        self.client.insert.side_effect = None
        checkpoint = stage_cdc_to_batch_table(self.checkpoint.pk, batch_size=2)
        second_create = self.client.command.call_args.args[0]
        self.assertNotEqual(first_create, second_create)
        self.assertIn(checkpoint.staging_table, second_create)
        self.assertTrue(all(
            call.args[0].startswith("CREATE TABLE")
            for call in self.client.command.call_args_list
        ))

    def test_rejects_missing_or_changed_staged_table(self):
        stage_cdc_to_batch_table(self.checkpoint.pk)
        self.table_uuid.return_value = "different-uuid"
        with self.assertRaisesRegex(RuntimeError, "missing or changed"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        self.assertEqual(self.client.command.call_count, 1)

    def test_rejects_changed_physical_columns_during_verification(self):
        stage_cdc_to_batch_table(self.checkpoint.pk)
        original_query = self.client.query.side_effect
        expected = [
            ("id", "Int64", "", ""),
            ("value", "Nullable(String)", "", ""),
        ]
        altered_schemas = [
            expected[:1],
            [expected[0], ("value", "Nullable(Int64)", "", "")],
            expected + [("unexpected", "String", "", "")],
            [expected[0], ("value", "Nullable(String)", "DEFAULT", "'x'")],
            list(reversed(expected)),
        ]
        for schema in altered_schemas:
            with self.subTest(schema=schema):
                self.client.query.side_effect = lambda sql, *args, **kwargs: (
                    SimpleNamespace(result_rows=schema)
                    if sql.startswith("DESCRIBE TABLE")
                    else original_query(sql, *args, **kwargs)
                )
                with self.assertRaisesRegex(RuntimeError, "columns changed"):
                    stage_cdc_to_batch_table(self.checkpoint.pk)
                with self.assertRaisesRegex(RuntimeError, "columns changed"):
                    checkpoint_cdc_to_batch_active_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.assertFalse(self.checkpoint.active_table_checked)

    def test_rejects_changed_physical_columns_before_checkpoint(self):
        original_query = self.client.query.side_effect
        self.client.query.side_effect = lambda sql, *args, **kwargs: (
            SimpleNamespace(result_rows=[("id", "Int64", "", "")])
            if sql.startswith("DESCRIBE TABLE")
            else original_query(sql, *args, **kwargs)
        )
        with self.assertRaisesRegex(RuntimeError, "columns changed"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.PENDING)
        self.assertFalse(self.checkpoint.staging_uuid)

    def test_rejects_changed_staging_engine_or_keys(self):
        stage_cdc_to_batch_table(self.checkpoint.pk)
        original_query = self.client.query.side_effect
        for metadata in (
            [],
            [("ReplacingMergeTree", "", "", "", "")],
            [("MergeTree", "id", "", "", "")],
            [("MergeTree", "", "id", "", "")],
            [("MergeTree", "", "", "id", "")],
            [("MergeTree", "", "", "", "id")],
        ):
            with self.subTest(metadata=metadata):
                self.client.query.side_effect = lambda sql, *args, **kwargs: (
                    SimpleNamespace(result_rows=metadata)
                    if "FROM system.tables" in sql
                    else original_query(sql, *args, **kwargs)
                )
                with self.assertRaisesRegex(RuntimeError, "engine or keys changed"):
                    stage_cdc_to_batch_table(self.checkpoint.pk)
                with self.assertRaisesRegex(RuntimeError, "engine or keys changed"):
                    checkpoint_cdc_to_batch_active_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.assertFalse(self.checkpoint.active_table_checked)

    def test_rejects_changed_engine_before_staging_checkpoint(self):
        original_query = self.client.query.side_effect
        self.client.query.side_effect = lambda sql, *args, **kwargs: (
            SimpleNamespace(result_rows=[("ReplacingMergeTree", "", "", "", "")])
            if "FROM system.tables" in sql
            else original_query(sql, *args, **kwargs)
        )
        with self.assertRaisesRegex(RuntimeError, "engine or keys changed"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.PENDING)
        self.assertFalse(self.checkpoint.staging_uuid)

    def test_rejects_changed_fields_on_retry(self):
        stage_cdc_to_batch_table(self.checkpoint.pk)
        self.value.data_type = "INTEGER"
        self.value.save(update_fields=["data_type"])
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            stage_cdc_to_batch_table(self.checkpoint.pk)

    def test_rejects_changed_source_on_retry(self):
        stage_cdc_to_batch_table(self.checkpoint.pk)
        self.source.host = "other.example"
        self.source.save(update_fields=["host"])
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            stage_cdc_to_batch_table(self.checkpoint.pk)

    def test_rejects_changed_username_on_retry_and_active_observation(self):
        stage_cdc_to_batch_table(self.checkpoint.pk)
        self.source.username = "other_reader"
        self.source.save(update_fields=["username"])
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            checkpoint_cdc_to_batch_active_table(self.checkpoint.pk)
        self.assertEqual(self.client.command.call_count, 1)

    def test_rejects_username_changed_during_staging(self):
        def change_username(*args, **kwargs):
            SourceProfile.objects.filter(pk=self.source.pk).update(username="other_reader")

        self.client.insert.side_effect = change_username
        with self.assertRaisesRegex(RuntimeError, "configuration changed during batch staging"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.PENDING)
        self.assertIsNone(self.checkpoint.staging_row_count)
        self.assertTrue(all(call.args[0].startswith("CREATE TABLE")
                            for call in self.client.command.call_args_list))

    def test_rejects_colliding_active_name_during_staging(self):
        def add_colliding_table(*args, **kwargs):
            SourceTable.objects.create(
                source=self.source, schema_name="ANALYTICS", table_name="EVENTS"
            )

        self.client.insert.side_effect = add_colliding_table
        with self.assertRaisesRegex(RuntimeError, "map to the same ClickHouse table"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.PENDING)
        self.assertTrue(all(call.args[0].startswith("CREATE TABLE")
                            for call in self.client.command.call_args_list))

    def test_rejects_colliding_active_name_on_retry_and_observation(self):
        stage_cdc_to_batch_table(self.checkpoint.pk)
        SourceTable.objects.create(
            source=self.source, schema_name="ANALYTICS", table_name="EVENTS"
        )
        with self.assertRaisesRegex(RuntimeError, "map to the same ClickHouse table"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        with self.assertRaisesRegex(RuntimeError, "map to the same ClickHouse table"):
            checkpoint_cdc_to_batch_active_table(self.checkpoint.pk)
        self.assertEqual(self.client.command.call_count, 1)

    def test_checkpoints_live_uuid_and_revalidates_on_retry(self):
        staged = stage_cdc_to_batch_table(self.checkpoint.pk)
        self.client.reset_mock()
        self.client.query.return_value.result_rows = [["Atomic"]]
        original_query = self.client.query.side_effect
        self.client.query.side_effect = lambda sql, *args, **kwargs: (
            SimpleNamespace(result_rows=[[3]])
            if sql.startswith("SELECT count() FROM ")
            else original_query(sql, *args, **kwargs)
        )
        self.table_uuid.side_effect = ["stage-uuid", "live-uuid"] * 2
        checkpoint = checkpoint_cdc_to_batch_active_table(staged.pk)
        self.assertTrue(checkpoint.active_table_checked)
        self.assertEqual(checkpoint.active_uuid_before, "live-uuid")
        checkpoint = checkpoint_cdc_to_batch_active_table(staged.pk)
        self.assertEqual(checkpoint.active_uuid_before, "live-uuid")
        self.client.command.assert_not_called()
        self.client.insert.assert_not_called()
        self.source.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)

    def test_observes_missing_keyless_table_and_rejects_later_change(self):
        self.table.primary_key_fields = []
        self.table.cdc_managed = False
        self.table.save(update_fields=["primary_key_fields", "cdc_managed"])
        staged = stage_cdc_to_batch_table(self.checkpoint.pk)
        self.table_uuid.side_effect = ["stage-uuid", "", "stage-uuid", "new-uuid"]
        checkpoint = checkpoint_cdc_to_batch_active_table(staged.pk)
        self.assertTrue(checkpoint.active_table_checked)
        self.assertEqual(checkpoint.active_uuid_before, "")
        with self.assertRaisesRegex(RuntimeError, "changed after observation"):
            checkpoint_cdc_to_batch_active_table(staged.pk)
        checkpoint.refresh_from_db()
        self.assertEqual(checkpoint.active_uuid_before, "")

    def test_observation_rejects_missing_live_cdc_table_and_changed_stage(self):
        staged = stage_cdc_to_batch_table(self.checkpoint.pk)
        self.table_uuid.side_effect = ["stage-uuid", "", "changed-uuid"]
        with self.assertRaisesRegex(RuntimeError, "CDC-managed active.*missing"):
            checkpoint_cdc_to_batch_active_table(staged.pk)
        with self.assertRaisesRegex(RuntimeError, "staged ClickHouse table"):
            checkpoint_cdc_to_batch_active_table(staged.pk)
        staged.refresh_from_db()
        self.assertFalse(staged.active_table_checked)

    def test_observation_rejects_replaced_live_table_and_aliased_uuid(self):
        staged = stage_cdc_to_batch_table(self.checkpoint.pk)
        self.table_uuid.side_effect = [
            "stage-uuid", "live-uuid",
            "stage-uuid", "replacement-uuid",
            "stage-uuid", "stage-uuid",
        ]
        checkpoint_cdc_to_batch_active_table(staged.pk)
        with self.assertRaisesRegex(RuntimeError, "changed after observation"):
            checkpoint_cdc_to_batch_active_table(staged.pk)
        with self.assertRaisesRegex(RuntimeError, "UUIDs must be distinct"):
            checkpoint_cdc_to_batch_active_table(staged.pk)
        staged.refresh_from_db()
        self.assertEqual(staged.active_uuid_before, "live-uuid")
        self.assertTrue(staged.active_table_checked)

    def test_observation_rejects_non_atomic_and_changed_configuration(self):
        staged = stage_cdc_to_batch_table(self.checkpoint.pk)
        self.client.query.return_value.result_rows = [["Ordinary"]]
        with self.assertRaisesRegex(RuntimeError, "Atomic"):
            checkpoint_cdc_to_batch_active_table(staged.pk)
        self.client.query.return_value.result_rows = [["Atomic"]]
        self.value.data_type = "INTEGER"
        self.value.save(update_fields=["data_type"])
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            checkpoint_cdc_to_batch_active_table(staged.pk)
        staged.refresh_from_db()
        self.assertFalse(staged.active_table_checked)

    def test_observation_rejects_invalid_state_or_alias(self):
        staged = stage_cdc_to_batch_table(self.checkpoint.pk)
        transition = staged.transition
        transition.status = CdcToBatchTransition.Status.CUTOVER_PENDING
        transition.save(update_fields=["status"])
        with self.assertRaisesRegex(RuntimeError, "not eligible"):
            checkpoint_cdc_to_batch_active_table(staged.pk)
        transition.status = CdcToBatchTransition.Status.PREPARING
        transition.save(update_fields=["status"])
        staged.staging_table = f"source_{self.source.pk}__analytics_events"
        staged.save(update_fields=["staging_table"])
        with self.assertRaisesRegex(RuntimeError, "names must be distinct"):
            checkpoint_cdc_to_batch_active_table(staged.pk)

    def test_new_staging_attempt_clears_obsolete_live_observation(self):
        self.checkpoint.active_table_checked = True
        self.checkpoint.active_uuid_before = "old-uuid"
        self.checkpoint.save(update_fields=["active_table_checked", "active_uuid_before"])
        staged = stage_cdc_to_batch_table(self.checkpoint.pk)
        self.assertFalse(staged.active_table_checked)
        self.assertEqual(staged.active_uuid_before, "")

    def test_rejects_changed_fields_during_staging(self):
        def change_field(*args, **kwargs):
            SourceField.objects.filter(pk=self.value.pk).update(data_type="INTEGER")

        self.client.insert.side_effect = change_field
        with self.assertRaisesRegex(RuntimeError, "configuration changed"):
            stage_cdc_to_batch_table(self.checkpoint.pk, batch_size=2)
        self.checkpoint.refresh_from_db()
        self.assertEqual(self.checkpoint.status, CdcToBatchTableCheckpoint.Status.PENDING)

    def test_rejects_missing_primary_key_and_non_atomic_database(self):
        SourceField.objects.filter(table=self.table, field_name="id").delete()
        with self.assertRaisesRegex(RuntimeError, "Primary key field"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_not_called()
        self.client.query.return_value.result_rows = [["Ordinary"]]
        with self.assertRaisesRegex(RuntimeError, "Atomic"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_not_called()

    def test_rejects_invalid_state_and_batch_size(self):
        with self.assertRaisesRegex(ValueError, "positive"):
            stage_cdc_to_batch_table(self.checkpoint.pk, batch_size=0)
        self.source.is_active = False
        self.source.save(update_fields=["is_active"])
        with self.assertRaisesRegex(RuntimeError, "not eligible"):
            stage_cdc_to_batch_table(self.checkpoint.pk)
        self.client.command.assert_not_called()


class CdcToBatchStagingTaskTests(TestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Transition staging task source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="analytics",
            username="cdc_user",
            ingestion_mode=IngestionMode.CDC,
        )
        self.tables = [
            SourceTable.objects.create(
                source=self.source, schema_name="public", table_name=name
            )
            for name in ("events", "logs")
        ]
        self.transition = prepare_cdc_to_batch_transition(self.source.pk)
        self.checkpoints = list(
            self.transition.table_checkpoints.order_by("source_table_id")
        )

    def mark_staged(self, checkpoint_pk, batch_size=None, *, verification_only=False):
        CdcToBatchTableCheckpoint.objects.filter(pk=checkpoint_pk).update(
            status=CdcToBatchTableCheckpoint.Status.STAGED,
            staging_row_count=0,
            staging_schema={"username": self.source.username, "synchronous_inserts": True},
        )

    @patch("etl.tasks.stage_cdc_to_batch_transition_task.apply_async")
    @patch("etl.cdc_to_batch.stage_cdc_to_batch_table")
    def test_stages_one_table_per_task_then_verifies_all(self, stage, enqueue):
        stage.side_effect = self.mark_staged
        self.assertEqual(stage_cdc_to_batch_transition_task.queue, "queue_backfill")
        first = stage_cdc_to_batch_transition_task.apply(
            args=[self.transition.pk], kwargs={"batch_size": 5}, throw=True
        ).get()
        self.assertIn("queued the next", first)
        stage.assert_called_once_with(self.checkpoints[0].pk, batch_size=5)
        self.transition.refresh_from_db()
        self.assertIsNone(self.transition.staging_verified_at)
        enqueue.assert_called_once_with(
            args=[self.transition.pk], kwargs={"batch_size": 5},
            queue="queue_backfill",
        )
        stage.reset_mock()
        enqueue.reset_mock()
        stage_cdc_to_batch_transition_task.apply(
            args=[self.transition.pk], kwargs={"batch_size": 5}, throw=True
        ).get()
        stage.assert_called_once_with(self.checkpoints[1].pk, batch_size=5)
        self.transition.refresh_from_db()
        self.assertIsNone(self.transition.staging_verified_at)
        stage.reset_mock()
        enqueue.reset_mock()
        result = stage_cdc_to_batch_transition_task.apply(
            args=[self.transition.pk], kwargs={"batch_size": 5}, throw=True
        ).get()
        self.assertEqual(stage.call_count, 2)
        self.assertTrue(all(call.kwargs["verification_only"] for call in stage.call_args_list))
        self.assertIn("All 2 tables staged", result)
        self.assertIn("cutover has not started", result)
        enqueue.assert_not_called()
        self.transition.refresh_from_db()
        self.source.refresh_from_db()
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)
        self.assertIsNotNone(self.transition.staging_verified_at)
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)

    @patch("etl.tasks.stage_cdc_to_batch_transition_task.apply_async")
    @patch("etl.cdc_to_batch.stage_cdc_to_batch_table")
    def test_legacy_checkpoints_are_restaged_one_per_task(self, stage, enqueue):
        self.transition.table_checkpoints.update(
            status=CdcToBatchTableCheckpoint.Status.STAGED,
            staging_row_count=None, staging_schema={"username": self.source.username},
        )
        CdcToBatchTableCheckpoint.objects.filter(pk=self.checkpoints[1].pk).update(
            staging_row_count=0, staging_schema={},
        )
        stage.side_effect = self.mark_staged
        for checkpoint in self.checkpoints:
            stage.reset_mock()
            enqueue.reset_mock()
            result = stage_cdc_to_batch_transition_task.apply(
                args=[self.transition.pk], kwargs={"batch_size": 5}, throw=True
            ).get()
            self.assertIn("queued the next", result)
            stage.assert_called_once_with(checkpoint.pk, batch_size=5)
            enqueue.assert_called_once()
        stage.reset_mock()
        enqueue.reset_mock()
        result = stage_cdc_to_batch_transition_task.apply(
            args=[self.transition.pk], kwargs={"batch_size": 5}, throw=True
        ).get()
        self.assertIn("All 2 tables staged", result)
        self.assertEqual(stage.call_count, 2)
        self.assertTrue(all(call.kwargs["verification_only"] for call in stage.call_args_list))
        enqueue.assert_not_called()

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.tasks.stage_cdc_to_batch_transition_task.apply_async")
    @patch("etl.cdc_to_batch.stage_cdc_to_batch_table")
    def test_legacy_restage_publish_failure_resumes_from_next_checkpoint(
        self, stage, enqueue, logger
    ):
        self.transition.table_checkpoints.update(
            status=CdcToBatchTableCheckpoint.Status.STAGED,
            staging_row_count=None, staging_schema={},
        )
        stage.side_effect = self.mark_staged
        enqueue.side_effect = RuntimeError("broker unavailable")
        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            stage_cdc_to_batch_transition_task.apply(
                args=[self.transition.pk], throw=True
            )
        self.checkpoints[0].refresh_from_db()
        self.assertEqual(self.checkpoints[0].staging_row_count, 0)
        enqueue.side_effect = None
        stage.reset_mock()
        stage_cdc_to_batch_transition_task.apply(
            args=[self.transition.pk], throw=True
        ).get()
        stage.assert_called_once_with(self.checkpoints[1].pk, batch_size=None)

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.tasks.stage_cdc_to_batch_transition_task.apply_async")
    @patch("etl.cdc_to_batch.stage_cdc_to_batch_table")
    def test_failure_is_recorded_and_resumable(self, stage, enqueue, logger):
        stage.side_effect = RuntimeError("sensitive source details")
        with self.assertRaisesRegex(RuntimeError, "sensitive source details"):
            stage_cdc_to_batch_transition_task.apply(
                args=[self.transition.pk], throw=True
            )
        self.transition.refresh_from_db()
        self.assertIn("Staging failed", self.transition.last_error)
        self.assertNotIn("sensitive", self.transition.last_error)
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)
        enqueue.assert_not_called()
        stage.side_effect = self.mark_staged
        stage_cdc_to_batch_transition_task.apply(
            args=[self.transition.pk], throw=True
        ).get()
        self.checkpoints[0].refresh_from_db()
        self.assertEqual(
            self.checkpoints[0].status, CdcToBatchTableCheckpoint.Status.STAGED
        )

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.tasks.stage_cdc_to_batch_transition_task.apply_async")
    @patch("etl.cdc_to_batch.stage_cdc_to_batch_table")
    def test_publish_failure_keeps_staged_checkpoint_for_resume(self, stage, enqueue, logger):
        stage.side_effect = self.mark_staged
        enqueue.side_effect = RuntimeError("broker unavailable")
        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            stage_cdc_to_batch_transition_task.apply(
                args=[self.transition.pk], throw=True
            )
        self.checkpoints[0].refresh_from_db()
        self.assertEqual(self.checkpoints[0].status, CdcToBatchTableCheckpoint.Status.STAGED)
        enqueue.side_effect = None
        stage.reset_mock()
        stage_cdc_to_batch_transition_task.apply(
            args=[self.transition.pk], throw=True
        ).get()
        stage.assert_called_once_with(self.checkpoints[1].pk, batch_size=None)

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.tasks.stage_cdc_to_batch_transition_task.apply_async")
    @patch("etl.cdc_to_batch.stage_cdc_to_batch_table")
    def test_rejects_table_list_changes(self, stage, enqueue, logger):
        SourceTable.objects.create(
            source=self.source, schema_name="public", table_name="new_table"
        )
        with self.assertRaisesRegex(RuntimeError, "table list changed"):
            stage_cdc_to_batch_transition_task.apply(
                args=[self.transition.pk], throw=True
            )
        stage.assert_not_called()
        enqueue.assert_not_called()

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.cdc_to_batch.stage_cdc_to_batch_table")
    def test_rejects_colliding_names_during_final_verification(self, stage, logger):
        self.transition.table_checkpoints.update(
            status=CdcToBatchTableCheckpoint.Status.STAGED,
            staging_row_count=0,
            staging_schema={"username": self.source.username, "synchronous_inserts": True},
        )

        def rename_table(*args, **kwargs):
            SourceTable.objects.filter(pk=self.tables[1].pk).update(
                schema_name="PUBLIC", table_name="EVENTS"
            )

        stage.side_effect = rename_table
        with self.assertRaisesRegex(RuntimeError, "map to the same ClickHouse table"):
            stage_cdc_to_batch_transition_task.apply(
                args=[self.transition.pk], throw=True
            )
        self.transition.refresh_from_db()
        self.assertIn("Staging failed", self.transition.last_error)
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.cdc_to_batch.stage_cdc_to_batch_table")
    def test_verification_failure_never_reports_ready(self, stage, logger):
        self.transition.staging_verified_at = timezone.now()
        self.transition.save(update_fields=["staging_verified_at"])
        self.transition.table_checkpoints.update(
            status=CdcToBatchTableCheckpoint.Status.STAGED,
            staging_row_count=0,
            staging_schema={"username": self.source.username, "synchronous_inserts": True},
        )
        stage.side_effect = RuntimeError("staged table changed")
        with self.assertRaisesRegex(RuntimeError, "staged table changed"):
            stage_cdc_to_batch_transition_task.apply(
                args=[self.transition.pk], throw=True
            )
        self.transition.refresh_from_db()
        self.assertIn("Staging failed", self.transition.last_error)
        self.assertEqual(self.transition.status, CdcToBatchTransition.Status.PREPARING)
        self.assertIsNone(self.transition.staging_verified_at)

    @patch("etl.tasks.get_etl_logger")
    @patch("etl.cdc_to_batch.stage_cdc_to_batch_table")
    def test_inactive_or_non_preparing_transition_is_rejected(self, stage, logger):
        self.source.is_active = False
        self.source.save(update_fields=["is_active"])
        with self.assertRaisesRegex(RuntimeError, "not eligible"):
            stage_cdc_to_batch_transition_task.apply(
                args=[self.transition.pk], throw=True
            )
        self.source.is_active = True
        self.source.save(update_fields=["is_active"])
        self.transition.status = CdcToBatchTransition.Status.CUTOVER_PENDING
        self.transition.save(update_fields=["status"])
        with self.assertRaisesRegex(RuntimeError, "not eligible"):
            stage_cdc_to_batch_transition_task.apply(
                args=[self.transition.pk], throw=True
            )
        stage.assert_not_called()


class CdcToBatchPreparationTests(TestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Transition preparation source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="analytics",
            username="cdc_user",
            ingestion_mode=IngestionMode.CDC,
        )
        self.keyed_table = SourceTable.objects.create(
            source=self.source,
            schema_name="public",
            table_name="events",
            primary_key_fields=["id"],
            cdc_managed=True,
        )
        self.keyless_table = SourceTable.objects.create(
            source=self.source,
            schema_name="public",
            table_name="logs",
            primary_key_fields=[],
        )

    def test_prepares_all_tables_once_without_changing_source(self):
        transition = prepare_cdc_to_batch_transition(self.source.pk)
        checkpoints = list(transition.table_checkpoints.order_by("source_table_id"))

        self.assertEqual(transition.status, CdcToBatchTransition.Status.PREPARING)
        self.assertEqual(
            [checkpoint.source_table_id for checkpoint in checkpoints],
            [self.keyed_table.pk, self.keyless_table.pk],
        )
        self.assertEqual(
            [checkpoint.status for checkpoint in checkpoints],
            [CdcToBatchTableCheckpoint.Status.PENDING] * 2,
        )
        self.assertEqual(
            len({checkpoint.staging_table for checkpoint in checkpoints}), 2
        )
        self.assertEqual(
            prepare_cdc_to_batch_transition(self.source.pk).pk, transition.pk
        )
        self.assertEqual(transition.table_checkpoints.count(), 2)
        self.source.refresh_from_db()
        self.keyed_table.refresh_from_db()
        self.assertEqual(self.source.ingestion_mode, IngestionMode.CDC)
        self.assertTrue(self.keyed_table.cdc_managed)

    def test_rejects_active_rebuilds(self):
        for status in (
            CdcTableRebuild.Status.SNAPSHOTTING,
            CdcTableRebuild.Status.CUTOVER_PENDING,
        ):
            with self.subTest(status=status):
                rebuild = CdcTableRebuild.objects.create(
                    source_table=self.keyed_table,
                    connector_name=f"connector-{status}",
                    topic_prefix=f"topic-{status}",
                    notification_topic=f"notification-{status}",
                    consumer_group=f"group-{status}",
                    staging_table=f"stage-{status}",
                    status=status,
                )
                with self.assertRaisesRegex(RuntimeError, "active CDC table rebuilds"):
                    prepare_cdc_to_batch_transition(self.source.pk)
                self.assertFalse(CdcToBatchTransition.objects.exists())
                rebuild.delete()

    def test_rejects_ineligible_sources(self):
        for changes in (
            {"is_active": False},
            {"ingestion_mode": IngestionMode.BATCH},
            {"db_type": DbChoice.MONGODB},
        ):
            with self.subTest(changes=changes):
                SourceProfile.objects.filter(pk=self.source.pk).update(
                    is_active=True,
                    ingestion_mode=IngestionMode.CDC,
                    db_type=DbChoice.POSTGRESQL,
                )
                SourceProfile.objects.filter(pk=self.source.pk).update(**changes)
                with self.assertRaisesRegex(ValueError, "active relational CDC source"):
                    prepare_cdc_to_batch_transition(self.source.pk)
                self.assertFalse(CdcToBatchTransition.objects.exists())

    def test_existing_transition_does_not_hide_active_rebuild(self):
        CdcToBatchTransition.objects.create(source=self.source)
        CdcTableRebuild.objects.create(
            source_table=self.keyed_table,
            connector_name="conflicting-connector",
            topic_prefix="conflicting-topic",
            notification_topic="conflicting-notifications",
            consumer_group="conflicting-group",
            staging_table="conflicting-stage",
        )
        with self.assertRaisesRegex(RuntimeError, "active CDC table rebuilds"):
            prepare_cdc_to_batch_transition(self.source.pk)

    def test_rejects_source_without_tables(self):
        self.source.tables.all().delete()
        with self.assertRaisesRegex(ValueError, "discovered source tables"):
            prepare_cdc_to_batch_transition(self.source.pk)
        self.assertFalse(CdcToBatchTransition.objects.exists())

    def test_rejects_colliding_clickhouse_names_before_preparation(self):
        for schema, table in (("PUBLIC", "EVENTS"), ("public_events", "extra")):
            with self.subTest(schema=schema, table=table):
                collision = SourceTable.objects.create(
                    source=self.source, schema_name=schema, table_name=table
                )
                if table == "extra":
                    extra = SourceTable.objects.create(
                        source=self.source, schema_name="public",
                        table_name="events_extra"
                    )
                with self.assertRaisesRegex(RuntimeError, "map to the same ClickHouse table"):
                    prepare_cdc_to_batch_transition(self.source.pk)
                self.assertFalse(CdcToBatchTransition.objects.exists())
                collision.delete()
                if table == "extra":
                    extra.delete()

    def test_rejects_colliding_names_when_reusing_transition(self):
        transition = prepare_cdc_to_batch_transition(self.source.pk)
        SourceTable.objects.create(
            source=self.source, schema_name="PUBLIC", table_name="EVENTS"
        )
        with self.assertRaisesRegex(RuntimeError, "map to the same ClickHouse table"):
            prepare_cdc_to_batch_transition(self.source.pk)
        self.assertEqual(self.source.cdc_to_batch_transitions.count(), 1)
        transition.refresh_from_db()
        self.assertEqual(transition.status, CdcToBatchTransition.Status.PREPARING)

    @patch("etl.cdc_to_batch.CdcToBatchTableCheckpoint.objects.bulk_create")
    def test_checkpoint_creation_failure_rolls_back_transition(self, bulk_create):
        bulk_create.side_effect = RuntimeError("checkpoint failure")
        with self.assertRaisesRegex(RuntimeError, "checkpoint failure"):
            prepare_cdc_to_batch_transition(self.source.pk)
        self.assertFalse(CdcToBatchTransition.objects.exists())


@skipUnlessDBFeature("has_select_for_update")
class CdcToBatchPreparationRaceTests(TransactionTestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Transition race source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="analytics",
            username="cdc_user",
            ingestion_mode=IngestionMode.CDC,
        )
        self.table = SourceTable.objects.create(
            source=self.source,
            schema_name="public",
            table_name="events",
            primary_key_fields=["id"],
            cdc_managed=True,
            clickhouse_uses_replacing_merge_tree=True,
        )

    def test_concurrent_preparations_return_the_same_transition(self):
        barrier = Barrier(2)

        def prepare():
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                return prepare_cdc_to_batch_transition(self.source.pk).pk
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: prepare(), range(2)))

        self.assertEqual(results[0], results[1])
        self.assertEqual(CdcToBatchTransition.objects.count(), 1)
        self.assertEqual(CdcToBatchTableCheckpoint.objects.count(), 1)

    @patch.object(rebuild_cdc_table_task, "apply_async")
    @patch("etl.cdc.consume_cdc_table_rebuild_batch", return_value=False)
    @patch("etl.cdc._ensure_connector_config")
    @patch("etl.cdc._required_field_names", return_value=["id"])
    @patch("etl.cdc._create_cdc_table")
    @patch("etl.cdc._clickhouse_table_uuid", return_value="table-uuid")
    @patch("etl.tasks.clickhouse_connect.get_client")
    def test_transition_and_rebuild_cannot_both_start(
        self, get_client, table_uuid, create_table, required_fields,
        ensure_connector, consume_batch, apply_async,
    ):
        get_client.return_value.query.return_value.result_rows = [["Atomic"]]
        barrier = Barrier(2)

        def run(operation):
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                try:
                    operation()
                    return "started"
                except RuntimeError as exc:
                    return str(exc)
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            transition_result = pool.submit(
                run, lambda: prepare_cdc_to_batch_transition(self.source.pk)
            )
            rebuild_result = pool.submit(
                run,
                lambda: rebuild_cdc_table_task.apply(
                    args=[self.table.pk], throw=True
                ),
            )
            transition_result = transition_result.result(timeout=20)
            rebuild_result = rebuild_result.result(timeout=20)

        self.assertEqual(
            (transition_result == "started") + (rebuild_result == "started"), 1
        )
        if transition_result == "started":
            self.assertIn("transition is active", rebuild_result)
            self.assertFalse(CdcTableRebuild.objects.exists())
        else:
            self.assertIn("active CDC table rebuilds", transition_result)
            self.assertEqual(
                CdcTableRebuild.objects.get().status,
                CdcTableRebuild.Status.SNAPSHOTTING,
            )
            self.assertFalse(CdcToBatchTransition.objects.exists())


@skipUnlessDBFeature("has_select_for_update")
class CdcToBatchConcurrentIngestionTests(TransactionTestCase):
    def test_ingestion_continues_while_staging_a_separate_table(self):
        source = SourceProfile.objects.create(
            name="Concurrent staging source",
            db_type=DbChoice.MYSQL,
            host="localhost",
            port=3306,
            database_name="analytics",
            username="cdc_user",
            ingestion_mode=IngestionMode.CDC,
        )
        table = SourceTable.objects.create(
            source=source,
            schema_name="analytics",
            table_name="events",
            primary_key_fields=["id"],
            cdc_managed=True,
        )
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        SourceField.objects.create(table=table, field_name="value", data_type="VARCHAR")
        transition = prepare_cdc_to_batch_transition(source.pk)
        checkpoint = transition.table_checkpoints.get(source_table=table)
        engine = sa.create_engine(
            "sqlite://",
            poolclass=sa.pool.StaticPool,
            connect_args={"check_same_thread": False},
        )
        self.addCleanup(engine.dispose)
        with engine.begin() as connection:
            connection.execute(sa.text("CREATE TABLE events (id INTEGER, value TEXT)"))
            connection.execute(sa.text(
                "INSERT INTO events VALUES (1, 'before'), (2, 'deleted')"
            ))

        stage_client = MagicMock()
        stage_client.query.return_value.result_rows = [["Atomic"]]
        stage_client.query.side_effect = lambda sql, *args, **kwargs: (
            SimpleNamespace(result_rows=[
                ("id", "Int64", "", ""), ("value", "Nullable(String)", "", "")
            ]) if sql.startswith("DESCRIBE TABLE") else (
                SimpleNamespace(result_rows=[[2]]) if sql.startswith("SELECT count() FROM ")
                else (SimpleNamespace(result_rows=[("MergeTree", "", "", "", "")])
                      if "FROM system.tables" in sql else stage_client.query.return_value)
            )
        )
        cdc_client = MagicMock()
        staged = Event()
        resume_stage = Event()

        def pause_stage(*args, **kwargs):
            staged.set()
            if not resume_stage.wait(timeout=15):
                raise TimeoutError("Timed out waiting to resume staging")

        stage_client.insert.side_effect = pause_stage
        topic = _topic_name(source, table)
        partition = TopicPartition(topic, 0)
        messages = [
            SimpleNamespace(
                topic=topic, partition=0, offset=offset, key={"id": row["id"]},
                value={"op": op, "before" if op == "d" else "after": row},
            )
            for offset, op, row in [
                (0, "u", {"id": 1, "value": "updated"}),
                (1, "d", {"id": 2, "value": "deleted"}),
                (2, "c", {"id": 3, "value": "inserted"}),
            ]
        ]
        consumer = MagicMock()
        consumer.assignment.return_value = {partition}
        consumer.poll.return_value = {partition: messages}

        def run_in_thread(func):
            close_old_connections()
            try:
                return func()
            finally:
                close_old_connections()

        with patch.object(engine, "dispose"), \
             patch("etl.cdc_to_batch.create_source_engine", return_value=engine), \
             patch("etl.cdc_to_batch.clickhouse_connect.get_client", side_effect=[stage_client, cdc_client]), \
             patch("etl.cdc_to_batch._clickhouse_table_uuid", return_value="stage-uuid"), \
             patch("etl.cdc.ensure_debezium_connector"), \
             patch("etl.cdc.KafkaConsumer", return_value=consumer), \
             patch("etl.cdc._required_field_names", return_value=["id", "value"]), \
             patch("etl.cdc._create_cdc_table", return_value=("`live_events`", ["id", "value"])), \
             patch("etl.tasks.set_task_progress"), \
             patch("etl.tasks.cache") as cache, \
             ThreadPoolExecutor(max_workers=2) as pool:
            cache.add.return_value = True
            staging = pool.submit(
                run_in_thread, lambda: stage_cdc_to_batch_table(checkpoint.pk, batch_size=10)
            )
            try:
                self.assertTrue(staged.wait(timeout=10), "Staging did not reach its insert")
                ingestion = pool.submit(
                    run_in_thread,
                    lambda: _run_scheduled_cdc_ingestion(
                        SimpleNamespace(request=SimpleNamespace(id="concurrent-cdc")),
                        source.pk, batch_size=10, task_queue="queue_cdc",
                    ),
                )
                self.assertIn("Processed 3 CDC event(s)", ingestion.result(timeout=10))
                self.assertFalse(staging.done())
            finally:
                resume_stage.set()
            result = staging.result(timeout=10)

        self.assertEqual(result.status, CdcToBatchTableCheckpoint.Status.STAGED)
        self.assertEqual(stage_client.insert.call_args.args[0], result.staging_table)
        self.assertEqual(
            stage_client.insert.call_args.args[1], [[1, "before"], [2, "deleted"]]
        )
        self.assertEqual(cdc_client.insert.call_args.args[0], "`live_events`")
        self.assertEqual(
            [(row[0], row[1], row[-1]) for row in cdc_client.insert.call_args.args[1]],
            [(1, "updated", 0), (2, "deleted", 1), (3, "inserted", 0)],
        )
        consumer.commit.assert_called_once()
        stage_client.command.assert_called_once()
        source.refresh_from_db()
        table.refresh_from_db()
        transition.refresh_from_db()
        self.assertEqual(source.ingestion_mode, IngestionMode.CDC)
        self.assertTrue(table.cdc_managed)
        self.assertEqual(transition.status, CdcToBatchTransition.Status.PREPARING)


@skipUnlessDBFeature("has_select_for_update")
class CdcToBatchRefreshFenceTests(TransactionTestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="Keyless transition source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="analytics",
            username="cdc_user",
            ingestion_mode=IngestionMode.CDC,
        )
        self.transition = CdcToBatchTransition.objects.create(source=self.source)

    def test_inflight_refresh_blocks_cutover_and_queued_refresh_is_skipped(self):
        refresh_started = Event()
        release_refresh = Event()
        cutover_waiting = Event()
        cutover_acquired = Event()

        def refresh(*args, **kwargs):
            refresh_started.set()
            if not release_refresh.wait(timeout=15):
                raise TimeoutError("Timed out waiting to release refresh")
            return "refresh completed"

        def run_refresh():
            close_old_connections()
            try:
                return ingest_source_to_clickhouse_task.apply(
                    args=[self.source.pk],
                    kwargs={"cdc_fallback_only": True, "task_queue": "queue_backfill"},
                    throw=True,
                ).get()
            finally:
                close_old_connections()

        def enter_cutover():
            close_old_connections()
            try:
                with transaction.atomic():
                    cutover_waiting.set()
                    SourceProfile.objects.select_for_update().get(pk=self.source.pk)
                    cutover_acquired.set()
                    CdcToBatchTransition.objects.filter(pk=self.transition.pk).update(
                        status=CdcToBatchTransition.Status.CUTOVER_PENDING
                    )
            finally:
                close_old_connections()

        with patch("etl.tasks._ingest_source_to_clickhouse_batch", side_effect=refresh) as ingest, \
             patch("etl.tasks.set_task_progress") as progress, \
             patch("etl.tasks.get_etl_logger"), \
             ThreadPoolExecutor(max_workers=2) as pool:
            running = pool.submit(run_refresh)
            try:
                self.assertTrue(refresh_started.wait(timeout=10))
                cutover = pool.submit(enter_cutover)
                self.assertTrue(cutover_waiting.wait(timeout=10))
                self.assertFalse(cutover_acquired.wait(timeout=0.5))
            finally:
                release_refresh.set()
            self.assertEqual(running.result(timeout=10), "refresh completed")
            cutover.result(timeout=10)
            self.assertTrue(cutover_acquired.is_set())
            result = run_refresh()
            self.assertIn("cutover is pending", result)
            self.assertEqual(ingest.call_count, 1)
            self.assertTrue(any(
                call.kwargs.get("status") == "SKIPPED"
                for call in progress.call_args_list
            ))
        self.assertFalse(CdcExecutionFailure.objects.filter(source=self.source).exists())

    @patch("etl.tasks._ingest_source_to_clickhouse_batch")
    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.get_etl_logger")
    def test_pending_cutover_blocks_other_cdc_refreshes(self, logger, progress, ingest):
        self.transition.status = CdcToBatchTransition.Status.CUTOVER_PENDING
        self.transition.save(update_fields=["status"])
        result = ingest_source_to_clickhouse_task.apply(
            args=[self.source.pk], kwargs={"full_refresh": True}, throw=True
        ).get()
        self.assertIn("cutover is pending", result)
        ingest.assert_not_called()

    @patch("etl.tasks._ingest_source_to_clickhouse_batch")
    @patch("etl.tasks.get_etl_logger")
    def test_queued_fallback_is_rejected_after_mode_changes(self, logger, ingest):
        self.source.ingestion_mode = IngestionMode.BATCH
        self.source.save(update_fields=["ingestion_mode"])
        with self.assertRaisesRegex(RuntimeError, "queued CDC task"):
            ingest_source_to_clickhouse_task.apply(
                args=[self.source.pk], kwargs={"cdc_fallback_only": True}, throw=True
            )
        ingest.assert_not_called()

    @patch("etl.tasks._ingest_source_to_clickhouse_batch")
    @patch("etl.tasks.get_etl_logger")
    def test_waiting_refresh_rechecks_mode_after_source_lock(self, logger, ingest):
        waiting_for_lock = Event()

        def watch_queries(execute, sql, params, many, context):
            if "FOR UPDATE" in sql:
                waiting_for_lock.set()
            return execute(sql, params, many, context)

        def run_refresh():
            close_old_connections()
            try:
                with connection.execute_wrapper(watch_queries):
                    return ingest_source_to_clickhouse_task.apply(
                        args=[self.source.pk],
                        kwargs={"ingestion_run_id": "legacy-refresh"},
                        throw=True,
                    ).get()
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                source = SourceProfile.objects.select_for_update().get(pk=self.source.pk)
                running = pool.submit(run_refresh)
                self.assertTrue(waiting_for_lock.wait(timeout=10))
                source.ingestion_mode = IngestionMode.BATCH
                source.save(update_fields=["ingestion_mode"])
            with self.assertRaisesRegex(RuntimeError, "source mode changes"):
                running.result(timeout=10)
        ingest.assert_not_called()


@skipUnlessDBFeature("has_select_for_update")
class CdcToBatchIngestionFenceTests(TransactionTestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="CDC ingestion fence source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="analytics",
            username="cdc_user",
            ingestion_mode=IngestionMode.CDC,
        )
        self.transition = CdcToBatchTransition.objects.create(source=self.source)

    def run_ingestion(self):
        close_old_connections()
        try:
            return ingest_source_to_clickhouse_task.apply(
                args=[self.source.pk],
                kwargs={"cdc_continue": True},
                throw=True,
            ).get()
        finally:
            close_old_connections()

    def test_inflight_ingestion_blocks_cutover_then_queued_batch_is_skipped(self):
        consuming = Event()
        release = Event()
        cutover_waiting = Event()
        cutover_acquired = Event()

        def consume(*args, **kwargs):
            consuming.set()
            if not release.wait(timeout=15):
                raise TimeoutError("Timed out waiting to release CDC ingestion")
            return 0

        def enter_cutover():
            close_old_connections()
            try:
                def watch_queries(execute, sql, params, many, context):
                    if "FOR UPDATE" in sql:
                        cutover_waiting.set()
                    return execute(sql, params, many, context)

                with connection.execute_wrapper(watch_queries), transaction.atomic():
                    SourceProfile.objects.select_for_update().get(pk=self.source.pk)
                    cutover_acquired.set()
                    CdcToBatchTransition.objects.filter(pk=self.transition.pk).update(
                        status=CdcToBatchTransition.Status.CUTOVER_PENDING
                    )
            finally:
                close_old_connections()

        with patch("etl.cdc.consume_source_cdc_batch", side_effect=consume) as consumer, \
             patch("etl.cdc.ensure_debezium_connector"), \
             patch("etl.tasks.clickhouse_connect.get_client"), \
             patch("etl.tasks.cache") as cache, \
             patch("etl.tasks.set_task_progress") as progress, \
             patch("etl.tasks.get_etl_logger"), \
             ThreadPoolExecutor(max_workers=2) as pool:
            cache.add.return_value = True
            ingestion = pool.submit(self.run_ingestion)
            try:
                self.assertTrue(consuming.wait(timeout=10))
                cutover = pool.submit(enter_cutover)
                self.assertTrue(cutover_waiting.wait(timeout=10))
                self.assertFalse(cutover_acquired.wait(timeout=0.5))
            finally:
                release.set()
            self.assertIn("Processed 0 CDC event(s)", ingestion.result(timeout=10))
            cutover.result(timeout=10)
            self.assertIn("cutover is pending", self.run_ingestion())
            self.assertEqual(consumer.call_count, 1)
            self.assertTrue(any(
                call.kwargs.get("status") == "SKIPPED"
                for call in progress.call_args_list
            ))

    def test_second_ingestion_waits_even_if_redis_lease_is_lost(self):
        consuming = Event()
        release = Event()
        second_waiting = Event()
        second_consuming = Event()
        calls = []

        def consume(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                consuming.set()
                if not release.wait(timeout=15):
                    raise TimeoutError("Timed out waiting to release CDC ingestion")
            else:
                second_consuming.set()
            return 0

        def second_ingestion():
            def watch_queries(execute, sql, params, many, context):
                if "FOR UPDATE" in sql:
                    second_waiting.set()
                return execute(sql, params, many, context)

            with connection.execute_wrapper(watch_queries):
                return self.run_ingestion()

        with patch("etl.cdc.consume_source_cdc_batch", side_effect=consume), \
             patch("etl.cdc.ensure_debezium_connector"), \
             patch("etl.tasks.clickhouse_connect.get_client"), \
             patch("etl.tasks.cache") as cache, \
             patch("etl.tasks.set_task_progress"), \
             patch("etl.tasks.get_etl_logger"), \
             ThreadPoolExecutor(max_workers=2) as pool:
            cache.add.return_value = True
            first = pool.submit(self.run_ingestion)
            try:
                self.assertTrue(consuming.wait(timeout=10))
                second = pool.submit(second_ingestion)
                self.assertTrue(second_waiting.wait(timeout=10))
                self.assertFalse(second_consuming.wait(timeout=0.5))
            finally:
                release.set()
            self.assertIn("Processed 0 CDC event(s)", first.result(timeout=10))
            self.assertIn("Processed 0 CDC event(s)", second.result(timeout=10))
            self.assertEqual(len(calls), 2)


class CdcTableRebuildTaskTests(TestCase):
    def setUp(self):
        self.source = SourceProfile.objects.create(
            name="CDC table rebuild source",
            db_type=DbChoice.POSTGRESQL,
            host="postgres.example",
            port=5432,
            database_name="analytics",
            username="cdc_user",
            password="secret",
            ingestion_mode=IngestionMode.CDC,
        )
        self.table = SourceTable.objects.create(
            source=self.source,
            schema_name="public",
            table_name="events",
            primary_key_fields=["event_id"],
            cdc_managed=True,
            clickhouse_uses_replacing_merge_tree=True,
        )
        SourceField.objects.create(
            table=self.table,
            field_name="event_id",
            data_type="INTEGER",
        )
        SourceField.objects.create(
            table=self.table,
            field_name="value",
            data_type="VARCHAR",
        )

    @patch("etl.tasks.clickhouse_connect.get_client")
    def test_rebuild_rejected_during_active_transition(self, get_client):
        for status in (
            CdcToBatchTransition.Status.PREPARING,
            CdcToBatchTransition.Status.CUTOVER_PENDING,
        ):
            with self.subTest(status=status):
                transition = CdcToBatchTransition.objects.create(
                    source=self.source, status=status
                )
                with self.assertRaisesRegex(RuntimeError, "transition is active"):
                    rebuild_cdc_table_task.apply(args=[self.table.pk], throw=True)
                self.assertFalse(CdcTableRebuild.objects.exists())
                get_client.assert_not_called()
                transition.delete()

    @patch("etl.cdc.consume_cdc_table_rebuild_batch", return_value=False)
    @patch("etl.cdc._ensure_connector_config")
    @patch("etl.cdc._required_field_names", return_value=["event_id", "value"])
    @patch("etl.cdc._create_cdc_table")
    @patch("etl.cdc._clickhouse_table_uuid", side_effect=["active-uuid", "stage-uuid"])
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch.object(rebuild_cdc_table_task, "apply_async")
    def test_snapshot_task_stages_and_queues_continuation(
        self,
        apply_async,
        get_client,
        table_uuid,
        create_table,
        required_fields,
        ensure_connector,
        consume_batch,
    ):
        clickhouse_client = MagicMock()
        clickhouse_client.query.return_value.result_rows = [["Atomic"]]
        get_client.return_value = clickhouse_client

        result = rebuild_cdc_table_task.apply(
            args=[self.table.pk],
            throw=True,
        )

        recovery = CdcTableRebuild.objects.get(source_table=self.table)
        self.assertIn("is snapshotting", result.get())
        self.assertEqual(recovery.status, CdcTableRebuild.Status.SNAPSHOTTING)
        self.assertEqual(recovery.staging_uuid, "stage-uuid")
        self.assertEqual(self.table.cdc_connector_name, "")
        ensure_connector.assert_called_once()
        consume_batch.assert_called_once()
        apply_async.assert_called_once_with(
            args=[self.table.pk, recovery.pk],
            countdown=5,
            queue="queue_cdc",
        )
        clickhouse_client.close.assert_called_once()

    @patch("etl.cdc._connect_request")
    @patch("etl.cdc.consume_cdc_table_rebuild_batch", side_effect=RuntimeError("snapshot failed"))
    @patch("etl.cdc._ensure_connector_config")
    @patch("etl.cdc._required_field_names", return_value=["event_id", "value"])
    @patch("etl.cdc._create_cdc_table")
    @patch("etl.cdc._clickhouse_table_uuid", side_effect=["active-uuid", "stage-uuid"])
    @patch("etl.tasks.clickhouse_connect.get_client")
    def test_failed_snapshot_retains_connector(
        self, get_client, table_uuid, create_table, required_fields,
        ensure_connector, consume_batch, connect_request,
    ):
        get_client.return_value.query.return_value.result_rows = [["Atomic"]]
        with self.assertRaisesRegex(RuntimeError, "snapshot failed"):
            rebuild_cdc_table_task.apply(args=[self.table.pk], throw=True)
        recovery = CdcTableRebuild.objects.get(source_table=self.table)
        self.assertEqual(recovery.status, CdcTableRebuild.Status.FAILED)
        self.assertIn("snapshot failed", recovery.last_error)
        self.assertTrue(recovery.connector_name)
        ensure_connector.assert_called_once()
        connect_request.assert_not_called()
        self.table.refresh_from_db()
        self.assertEqual(self.table.cdc_connector_name, "")

    @patch("etl.cdc._connect_request")
    @patch("etl.cdc.consume_cdc_table_rebuild_batch", return_value=True)
    @patch("etl.cdc._required_field_names", return_value=["event_id", "value"])
    @patch("etl.cdc._create_cdc_table")
    @patch("etl.cdc._clickhouse_table_uuid", side_effect=["old-uuid", "old-uuid", "stage-uuid"])
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.cache.add", return_value=True)
    def test_successful_cutover_retains_superseded_connector(
        self, lock_add, get_client, table_uuid, create_table,
        required_fields, consume_batch, connect_request,
    ):
        self.table.cdc_connector_name = "old-generation"
        self.table.cdc_topic_prefix = "old-generation"
        self.table.save(update_fields=["cdc_connector_name", "cdc_topic_prefix"])
        recovery = CdcTableRebuild.objects.create(
            source_table=self.table,
            connector_name="new-generation",
            topic_prefix="new-generation",
            notification_topic="new-generation.notifications",
            consumer_group="new-generation-group",
            staging_table="source_events_new_generation",
            previous_connector_name="old-generation",
            status=CdcTableRebuild.Status.CUTOVER_PENDING,
            staging_uuid="stage-uuid",
            active_uuid_before="old-uuid",
            active_table_checked=True,
        )
        client = get_client.return_value
        client.query.return_value.result_rows = [["Atomic"]]
        result = rebuild_cdc_table_task.apply(
            args=[self.table.pk, recovery.pk], throw=True
        ).get()
        self.assertIn("completed", result)
        recovery.refresh_from_db()
        self.table.refresh_from_db()
        self.assertEqual(recovery.status, CdcTableRebuild.Status.SUCCEEDED)
        self.assertEqual(recovery.previous_connector_name, "old-generation")
        self.assertEqual(self.table.cdc_connector_name, "new-generation")
        self.assertTrue(client.command.call_args.args[0].startswith("EXCHANGE TABLES"))
        connect_request.assert_not_called()

    @patch("etl.cdc.consume_cdc_table_rebuild_batch", return_value=False)
    @patch("etl.cdc._ensure_connector_config")
    @patch("etl.cdc._required_field_names", return_value=["event_id", "value"])
    @patch("etl.cdc._create_cdc_table")
    @patch("etl.cdc._clickhouse_table_uuid", return_value="old-uuid")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch.object(rebuild_cdc_table_task, "apply_async")
    def test_pending_cutover_waits_for_checkpoint_backlog(
        self,
        apply_async,
        get_client,
        table_uuid,
        create_table,
        required_fields,
        ensure_connector,
        consume_batch,
    ):
        recovery = CdcTableRebuild.objects.create(
            source_table=self.table,
            connector_name="pending-generation",
            topic_prefix="pending-generation",
            notification_topic="pending-generation.notifications",
            consumer_group="pending-generation-group",
            staging_table="source_events_pending",
            status=CdcTableRebuild.Status.CUTOVER_PENDING,
            snapshot_end_offsets={"0": 100},
            staging_uuid="stage-uuid",
            active_uuid_before="old-uuid",
            active_table_checked=True,
        )
        clickhouse_client = MagicMock()
        clickhouse_client.query.return_value.result_rows = [["Atomic"]]
        get_client.return_value = clickhouse_client

        result = rebuild_cdc_table_task.apply(
            args=[self.table.pk, recovery.pk],
            throw=True,
        )

        recovery.refresh_from_db()
        self.assertIn("is snapshotting", result.get())
        self.assertEqual(recovery.status, CdcTableRebuild.Status.CUTOVER_PENDING)
        consume_batch.assert_called_once()
        ensure_connector.assert_not_called()
        clickhouse_client.command.assert_not_called()
        apply_async.assert_called_once_with(
            args=[self.table.pk, recovery.pk],
            countdown=5,
            queue="queue_cdc",
        )

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.cache.add", return_value=True)
    @patch("etl.tasks.cache.get", return_value=None)
    @patch("etl.cdc._clickhouse_table_uuid", return_value="stage-uuid")
    @patch("etl.cdc.ensure_debezium_connector")
    @patch("etl.cdc.consume_source_cdc_batch")
    def test_ingestion_rejects_interrupted_cutover(
        self, consume, ensure, table_uuid, cache_get, cache_add, get_client,
        set_progress,
    ):
        recovery = CdcTableRebuild.objects.create(
            source_table=self.table,
            connector_name="interrupted-generation",
            topic_prefix="interrupted-generation",
            notification_topic="interrupted-generation.notifications",
            consumer_group="interrupted-generation-group",
            staging_table="source_events_interrupted",
            status=CdcTableRebuild.Status.CUTOVER_PENDING,
            staging_uuid="stage-uuid",
            active_uuid_before="old-uuid",
            active_table_checked=True,
        )
        with patch("etl.tasks.get_etl_logger"):
            result = ingest_source_to_clickhouse_task.apply(
                args=[self.source.pk], throw=False,
            )
        self.assertIsInstance(result.result, RuntimeError)
        self.assertIn(f"CDC rebuild {recovery.pk}", str(result.result))
        self.assertIn("Resume the rebuild", str(result.result))
        ensure.assert_not_called()
        consume.assert_not_called()

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.cache.add", return_value=True)
    @patch("etl.tasks.cache.get", return_value=None)
    @patch("etl.cdc._clickhouse_table_uuid", return_value="old-uuid")
    @patch("etl.cdc.ensure_debezium_connector")
    @patch("etl.cdc.consume_source_cdc_batch", return_value=0)
    def test_ingestion_continues_before_pending_cutover(
        self, consume, ensure, table_uuid, cache_get, cache_add, get_client,
        set_progress,
    ):
        CdcTableRebuild.objects.create(
            source_table=self.table,
            connector_name="draining-generation",
            topic_prefix="draining-generation",
            notification_topic="draining-generation.notifications",
            consumer_group="draining-generation-group",
            staging_table="source_events_draining",
            status=CdcTableRebuild.Status.CUTOVER_PENDING,
            staging_uuid="stage-uuid",
            active_uuid_before="old-uuid",
            active_table_checked=True,
        )
        result = ingest_source_to_clickhouse_task.apply(
            args=[self.source.pk], throw=True,
        ).get()
        self.assertIn("Processed 0 CDC event(s)", result)
        ensure.assert_called_once()
        consume.assert_called_once()

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.cache.get", return_value=None)
    @patch("etl.cdc.ensure_debezium_connector")
    @patch("etl.cdc.consume_source_cdc_batch")
    def test_ingestion_rechecks_source_mode_after_acquiring_lock(
        self, consume, ensure, cache_get, get_client, set_progress,
    ):
        def acquire_lock(*args, **kwargs):
            SourceProfile.objects.filter(pk=self.source.pk).update(
                ingestion_mode=IngestionMode.BATCH,
            )
            return True

        with patch("etl.tasks.cache.add", side_effect=acquire_lock), patch(
            "etl.tasks.get_etl_logger"
        ):
            result = ingest_source_to_clickhouse_task.apply(
                args=[self.source.pk], throw=False,
            )

        self.assertIsInstance(result.result, RuntimeError)
        self.assertIn("mode or active state changed", str(result.result))
        ensure.assert_not_called()
        consume.assert_not_called()
        get_client.return_value.close.assert_called_once()

    @patch("etl.cdc.consume_cdc_table_rebuild_batch", return_value=True)
    @patch("etl.cdc._required_field_names", return_value=["event_id", "value"])
    @patch("etl.cdc._create_cdc_table")
    @patch("etl.cdc._clickhouse_table_uuid", return_value="old-uuid")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.cache.get", return_value=None)
    def test_rebuild_rechecks_source_mode_before_cutover(
        self, cache_get, get_client, table_uuid, create_table, required, consume,
    ):
        recovery = CdcTableRebuild.objects.create(
            source_table=self.table,
            connector_name="mode-change-generation",
            topic_prefix="mode-change-generation",
            notification_topic="mode-change-generation.notifications",
            consumer_group="mode-change-generation-group",
            staging_table="source_events_mode_change",
            status=CdcTableRebuild.Status.CUTOVER_PENDING,
            staging_uuid="stage-uuid",
            active_uuid_before="old-uuid",
            active_table_checked=True,
        )
        client = MagicMock()
        client.query.return_value.result_rows = [["Atomic"]]
        get_client.return_value = client

        def acquire_lock(*args, **kwargs):
            SourceProfile.objects.filter(pk=self.source.pk).update(
                ingestion_mode=IngestionMode.BATCH,
            )
            return True

        with patch("etl.tasks.cache.add", side_effect=acquire_lock):
            with self.assertRaisesMessage(RuntimeError, "mode or active state changed"):
                rebuild_cdc_table_task.apply(
                    args=[self.table.pk, recovery.pk], throw=True,
                )

        recovery.refresh_from_db()
        self.assertEqual(recovery.status, CdcTableRebuild.Status.CUTOVER_PENDING)
        self.assertIn("mode or active state changed", recovery.last_error)
        client.command.assert_not_called()
        client.close.assert_called_once()

    @patch("etl.tasks.cache.delete")
    @patch("etl.tasks.cache.get", return_value="lock-token")
    @patch("etl.tasks.cache.add", return_value=True)
    @patch("etl.tasks.uuid4", return_value=SimpleNamespace(hex="lock-token"))
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.cdc._connect_request")
    @patch(
        "etl.cdc._clickhouse_table_uuid",
        side_effect=[
            "old-uuid", "old-uuid", "stage-uuid",
            "stage-uuid", "stage-uuid", "old-uuid",
        ],
    )
    @patch("etl.cdc._create_cdc_table")
    @patch("etl.cdc._required_field_names", return_value=["event_id", "value"])
    @patch("etl.cdc.consume_cdc_table_rebuild_batch", return_value=True)
    def test_retry_after_routing_saved_but_status_save_fails(
        self, consume, required, create_table, table_uuid, connect_request,
        get_client, make_uuid, lock_add, lock_get, lock_delete,
    ):
        recovery = CdcTableRebuild.objects.create(
            source_table=self.table,
            connector_name="partially-saved-generation",
            topic_prefix="partially-saved-generation",
            notification_topic="partially-saved-generation.notifications",
            consumer_group="partially-saved-generation-group",
            staging_table="source_events_partially_saved",
            status=CdcTableRebuild.Status.CUTOVER_PENDING,
            snapshot_end_offsets={"0": 1},
            staging_uuid="stage-uuid",
            active_uuid_before="old-uuid",
            active_table_checked=True,
            previous_connector_name="old-generation",
        )
        clickhouse_client = MagicMock()
        clickhouse_client.query.return_value.result_rows = [["Atomic"]]
        get_client.return_value = clickhouse_client
        original_save = CdcTableRebuild.save

        def fail_success_save(instance, *args, **kwargs):
            if instance.status == CdcTableRebuild.Status.SUCCEEDED:
                raise RuntimeError("status save failed")
            return original_save(instance, *args, **kwargs)

        with patch.object(CdcTableRebuild, "save", fail_success_save):
            with self.assertRaisesMessage(RuntimeError, "status save failed"):
                rebuild_cdc_table_task.apply(
                    args=[self.table.pk, recovery.pk], throw=True,
                )

        self.table.refresh_from_db()
        recovery.refresh_from_db()
        self.assertEqual(self.table.cdc_connector_name, recovery.connector_name)
        self.assertEqual(self.table.cdc_topic_prefix, recovery.topic_prefix)
        self.assertEqual(recovery.status, CdcTableRebuild.Status.CUTOVER_PENDING)
        self.assertIn("status save failed", recovery.last_error)
        clickhouse_client.command.assert_called_once()

        result = rebuild_cdc_table_task.apply(
            args=[self.table.pk, recovery.pk], throw=True,
        ).get()

        recovery.refresh_from_db()
        self.assertIn("completed", result)
        self.assertEqual(recovery.status, CdcTableRebuild.Status.SUCCEEDED)
        self.assertEqual(recovery.last_error, "")
        self.assertEqual(recovery.previous_connector_name, "old-generation")
        connect_request.assert_not_called()
        clickhouse_client.command.assert_called_once()
        consume.assert_called_once()

    @patch("etl.tasks.cache.delete")
    @patch("etl.tasks.cache.get", return_value="lock-token")
    @patch("etl.tasks.cache.add", return_value=True)
    @patch("etl.tasks.uuid4", return_value=SimpleNamespace(hex="lock-token"))
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.cdc._connect_request")
    @patch("etl.cdc._clickhouse_table_uuid", side_effect=["stage-uuid", "stage-uuid", ""])
    def test_cutover_retry_recognizes_exchange_already_completed(
        self,
        table_uuid,
        connect_request,
        get_client,
        make_uuid,
        lock_add,
        lock_get,
        lock_delete,
    ):
        recovery = CdcTableRebuild.objects.create(
            source_table=self.table,
            generation=uuid4(),
            connector_name="reporthub-source-table-rebuild",
            topic_prefix="reporthub-source-table-rebuild",
            notification_topic="reporthub-source-table-rebuild.notifications",
            consumer_group="reporthub-source-table-rebuild-group",
            staging_table="source_1__public_events__rebuild",
            status=CdcTableRebuild.Status.CUTOVER_PENDING,
            staging_uuid="stage-uuid",
            active_uuid_before="old-uuid",
            active_table_checked=True,
        )
        clickhouse_client = MagicMock()
        clickhouse_client.query.return_value.result_rows = [["Atomic"]]
        get_client.return_value = clickhouse_client

        result = rebuild_cdc_table_task.apply(
            args=[self.table.pk, recovery.pk],
            throw=True,
        )

        self.table.refresh_from_db()
        recovery.refresh_from_db()
        self.assertIn("completed", result.get())
        self.assertEqual(recovery.status, CdcTableRebuild.Status.SUCCEEDED)
        self.assertEqual(
            self.table.cdc_connector_name,
            recovery.connector_name,
        )
        self.assertEqual(self.table.cdc_topic_prefix, recovery.topic_prefix)
        self.assertEqual(
            [call.args[0] for call in connect_request.call_args_list],
            [],
        )
        clickhouse_client.command.assert_not_called()
        clickhouse_client.close.assert_called_once()


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
        source = SourceProfile.objects.create(
            name="Batch backfill dispatch source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="test_db", username="test_user",
            ingestion_mode=IngestionMode.BATCH,
        )
        result = backfill_source_to_clickhouse_task.apply(
            args=[source.pk],
            kwargs={"batch_size": 5000},
            throw=True,
        ).get()

        self.assertIn("Dispatched ClickHouse backfill", result)
        self.assertEqual(apply_async.call_args.kwargs["queue"], "queue_backfill")
        self.assertTrue(apply_async.call_args.kwargs["kwargs"]["full_refresh"])
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["origin_mode"], IngestionMode.BATCH)
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

        dispatch.assert_called_once_with(
            keyless_source.pk, batch_size=None, origin_mode=keyless_source.ingestion_mode
        )
        self.assertIn("skipped 1 source(s)", result)

    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_backfill_wrapper_rejects_stale_or_unmarked_work_after_transition(self, dispatch):
        source = SourceProfile.objects.create(
            name="Switched backfill source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="test_db", username="test_user",
            ingestion_mode=IngestionMode.BATCH,
        )
        CdcToBatchTransition.objects.create(
            source=source, status=CdcToBatchTransition.Status.SUCCEEDED
        )
        for kwargs in ({"origin_mode": IngestionMode.CDC}, {}):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(RuntimeError, "different source mode|unmarked legacy"):
                    backfill_source_to_clickhouse_task.apply(
                        args=[source.pk], kwargs=kwargs, throw=True
                    )
        dispatch.assert_not_called()
        backfill_source_to_clickhouse_task.apply(
            args=[source.pk], kwargs={"origin_mode": IngestionMode.BATCH}, throw=True
        ).get()
        self.assertEqual(dispatch.call_args.kwargs["kwargs"]["origin_mode"], IngestionMode.BATCH)

    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    @patch("etl.tasks._source_has_primary_key_tables")
    def test_backfill_wrapper_preserves_origin_when_mode_changes_during_dispatch(
        self, has_primary_keys, dispatch
    ):
        source = SourceProfile.objects.create(
            name="Racing backfill source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="test_db", username="test_user",
            ingestion_mode=IngestionMode.CDC,
        )

        def switch_mode(source_pk):
            SourceProfile.objects.filter(pk=source_pk).update(ingestion_mode=IngestionMode.BATCH)
            return False

        has_primary_keys.side_effect = switch_mode
        backfill_source_to_clickhouse_task.apply(
            args=[source.pk], kwargs={"origin_mode": IngestionMode.CDC}, throw=True
        ).get()
        self.assertEqual(dispatch.call_args.kwargs["kwargs"]["origin_mode"], IngestionMode.CDC)
        with patch("etl.tasks.get_etl_logger"), self.assertRaisesRegex(
            RuntimeError, "different source mode"
        ):
            ingest_source_to_clickhouse_task.apply(
                args=[source.pk], kwargs=dispatch.call_args.kwargs["kwargs"], throw=True
            )


class IngestionQueueRoutingTests(TestCase):
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    @patch("etl.tasks.get_etl_logger")
    def test_scheduled_sources_are_dispatched_to_their_own_queues(self, logger, dispatch):
        sources = []
        for mode, name, active in (
            (IngestionMode.CDC, "CDC queue source", True),
            (IngestionMode.BATCH, "Batch queue source", True),
            (IngestionMode.CDC, "Inactive queue source", False),
        ):
            sources.append(SourceProfile.objects.create(
                name=name, db_type=DbChoice.POSTGRESQL, host="localhost",
                port=5432, database_name="test_db", username="test_user",
                ingestion_mode=mode, is_active=active,
            ))
        trigger_all_active_clickhouse_ingestion_task.apply(throw=True).get()
        self.assertEqual(dispatch.call_count, 2)
        calls = {call.kwargs["args"][0]: call.kwargs for call in dispatch.call_args_list}
        self.assertEqual(calls[sources[0].pk]["queue"], "queue_cdc")
        self.assertEqual(calls[sources[0].pk]["kwargs"]["task_queue"], "queue_cdc")
        self.assertEqual(calls[sources[0].pk]["kwargs"]["origin_mode"], IngestionMode.CDC)
        self.assertEqual(calls[sources[1].pk]["queue"], "queue_backfill")
        self.assertEqual(calls[sources[1].pk]["kwargs"]["task_queue"], "queue_backfill")
        self.assertEqual(calls[sources[1].pk]["kwargs"]["origin_mode"], IngestionMode.BATCH)

    def test_rebuild_default_queue_is_cdc(self):
        self.assertEqual(rebuild_cdc_table_task.queue, "queue_cdc")

    @patch("etl.tasks._ingest_source_to_clickhouse_batch")
    @patch("etl.tasks.get_etl_logger")
    def test_completed_transition_rejects_stale_and_unmarked_work(self, logger, batch):
        source = SourceProfile.objects.create(
            name="Switched source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="test_db", username="test_user",
            ingestion_mode=IngestionMode.BATCH,
        )
        CdcToBatchTransition.objects.create(
            source=source, status=CdcToBatchTransition.Status.SUCCEEDED
        )
        for kwargs in (
            {"origin_mode": IngestionMode.CDC, "ingestion_run_id": "old-run"},
            {"origin_mode": IngestionMode.CDC},
            {"ingestion_run_id": "legacy-run", "table_index": 1},
            {},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(RuntimeError, "different source mode|unmarked legacy"):
                    ingest_source_to_clickhouse_task.apply(
                        args=[source.pk], kwargs=kwargs, throw=True
                    )
        batch.assert_not_called()
        batch.return_value = "batch started"
        result = ingest_source_to_clickhouse_task.apply(
            args=[source.pk], kwargs={"origin_mode": IngestionMode.BATCH}, throw=True
        ).get()
        self.assertEqual(result, "batch started")
        batch.assert_called_once()

    @patch("etl.tasks._ingest_source_to_clickhouse_batch", return_value="legacy accepted")
    def test_unmarked_batch_work_remains_compatible_before_transition(self, batch):
        source = SourceProfile.objects.create(
            name="Legacy batch source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="test_db", username="test_user",
            ingestion_mode=IngestionMode.BATCH,
        )
        self.assertEqual(
            ingest_source_to_clickhouse_task.apply(
                args=[source.pk], kwargs={"ingestion_run_id": "legacy-run"}, throw=True
            ).get(),
            "legacy accepted",
        )
        batch.assert_called_once()

    @patch("etl.tasks.get_etl_logger")
    def test_rejects_invalid_origin_marker(self, logger):
        source = SourceProfile.objects.create(
            name="Invalid origin source", db_type=DbChoice.POSTGRESQL,
            host="localhost", port=5432, database_name="test_db", username="test_user",
        )
        with self.assertRaisesRegex(ValueError, "Invalid ingestion task origin"):
            ingest_source_to_clickhouse_task.apply(
                args=[source.pk], kwargs={"origin_mode": "unknown"}, throw=True
            )


class BoundedIngestionTests(TestCase):
    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.create_source_engine")
    @patch("etl.tasks.clickhouse_connect.get_client")
    def test_batch_refresh_uses_configured_physical_generation(
        self, get_client, create_engine, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Generation refresh", db_type=DbChoice.MYSQL,
            host="localhost", port=3306, database_name="test_db", username="test_user",
            ingestion_mode=IngestionMode.BATCH,
        )
        table = SourceTable.objects.create(
            source=source, schema_name="test_db", table_name="items",
            primary_key_fields=["id"], clickhouse_physical_table="rh_generation_123",
        )
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        engine = sa.create_engine("sqlite:///:memory:")
        with engine.begin() as connection:
            connection.execute(sa.text("CREATE TABLE items (id INTEGER PRIMARY KEY)"))
            connection.execute(sa.text("INSERT INTO items (id) VALUES (1)"))
        create_engine.return_value = engine
        client = get_client.return_value
        client.query.return_value.result_rows = [[1]]
        with patch("etl.tasks.get_etl_logger"):
            ingest_source_to_clickhouse_task.apply(
                args=[source.pk], kwargs={"batch_size": 10}, throw=True
            ).get()
        commands = [call.args[0] for call in client.command.call_args_list]
        self.assertTrue(any(
            command.startswith("EXCHANGE TABLES rh_generation_123 AND rh_generation_123__staging_")
            for command in commands
        ))
        self.assertTrue(client.insert.call_args.args[0].startswith("rh_generation_123__staging_"))
        self.assertFalse(any(f"source_{source.pk}__test_db_items" in command for command in commands))

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.create_source_engine")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_next_table_continuations_keep_origin_for_empty_and_populated_tables(
        self, apply_async, get_client, create_engine, set_progress
    ):
        for selected in (False, True):
            with self.subTest(selected=selected):
                source = SourceProfile.objects.create(
                    name=f"Next table {selected}", db_type=DbChoice.MYSQL,
                    host="localhost", port=3306, database_name="test_db", username="test_user",
                    ingestion_mode=IngestionMode.BATCH,
                )
                first = SourceTable.objects.create(
                    source=source, schema_name="test_db", table_name="items"
                )
                SourceTable.objects.create(
                    source=source, schema_name="test_db", table_name="next_items"
                )
                if selected:
                    SourceField.objects.create(table=first, field_name="id", data_type="INTEGER")
                engine = sa.create_engine("sqlite:///:memory:")
                with engine.begin() as connection:
                    connection.execute(sa.text("CREATE TABLE items (id INTEGER)"))
                create_engine.return_value = engine
                get_client.return_value.query.return_value.result_rows = [[0]]
                apply_async.reset_mock()
                with patch("etl.tasks.get_etl_logger"):
                    result = ingest_source_to_clickhouse_task.apply(
                        args=[source.pk], kwargs={"batch_size": 10}, throw=True
                    ).get()
                self.assertIn("Queued ingestion for next table", result)
                self.assertEqual(apply_async.call_args.kwargs["kwargs"]["table_index"], 1)
                self.assertEqual(
                    apply_async.call_args.kwargs["kwargs"]["origin_mode"], IngestionMode.BATCH
                )

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
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["origin_mode"], IngestionMode.BATCH)
        self.assertTrue(apply_async.call_args.kwargs["kwargs"]["full_refresh"])
        self.assertTrue(any("ENGINE = MergeTree()" in call.args[0]
                            for call in client.command.call_args_list))

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.create_source_engine")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    def test_keyset_continuation_preserves_staging_and_all_pages(
        self, apply_async, get_client, create_engine, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Batch keyset continuation",
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
        client.query.side_effect = [
            SimpleNamespace(result_rows=[[0]]),
            SimpleNamespace(result_rows=[[0]]),
            SimpleNamespace(result_rows=[[1]]),
        ]
        get_client.return_value = client

        with patch("etl.tasks.get_etl_logger"), patch.object(engine, "dispose"):
            first = ingest_source_to_clickhouse_task.apply(
                args=[source.pk], kwargs={"batch_size": 2}, throw=True
            ).get()
            continuation = apply_async.call_args.kwargs
            second = ingest_source_to_clickhouse_task.apply(
                args=continuation["args"],
                kwargs=continuation["kwargs"],
                throw=True,
            ).get()

        self.assertIn("Queued next 2-row", first)
        self.assertIn("completed", second)
        self.assertEqual(
            [call.args[1] for call in client.insert.call_args_list],
            [[[1], [2]], [[3]]],
        )
        commands = [call.args[0] for call in client.command.call_args_list]
        self.assertEqual(sum("CREATE TABLE" in command for command in commands), 1)
        self.assertEqual(sum("DROP TABLE" in command for command in commands), 1)
        self.assertEqual(sum("RENAME TABLE" in command for command in commands), 1)
        engine.dispose()

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.create_source_engine")
    @patch("etl.tasks.clickhouse_connect.get_client")
    def test_keyset_continuation_rejects_missing_staging_table(
        self, get_client, create_engine, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Batch missing staging",
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
        client = MagicMock()
        client.query.return_value.result_rows = [[0]]
        get_client.return_value = client

        with patch("etl.tasks.get_etl_logger"):
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk],
                kwargs={
                    "batch_size": 2,
                    "ingestion_run_id": "existing-run",
                    "last_key_values": ["2"],
                },
                throw=False,
            )

        self.assertIsInstance(result.result, RuntimeError)
        self.assertIn("Staging table for items is missing", str(result.result))
        client.command.assert_not_called()
        client.insert.assert_not_called()

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
        self.assertEqual(apply_async.call_args.kwargs["queue"], "queue_cdc")
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["task_queue"], "queue_cdc")
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["origin_mode"], IngestionMode.CDC)
        clickhouse_client.close.assert_called_once()

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async")
    @patch("etl.tasks.cache")
    @patch("etl.cdc.ensure_debezium_connector")
    @patch("etl.cdc.consume_source_cdc_batch", return_value=0)
    def test_cdc_cutover_guard_rechecks_after_acquiring_lock(
        self, consume, ensure_connector, cache, enqueue, get_client, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Cutover fence source",
            ingestion_mode=IngestionMode.CDC,
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
            last_sync_status="SUCCESS",
            last_sync_error="previous error",
        )

        def acquire_lock(key, token, timeout):
            CdcToBatchTransition.objects.create(
                source=source, status=CdcToBatchTransition.Status.CUTOVER_PENDING
            )
            cache.get.return_value = token
            return True

        cache.add.side_effect = acquire_lock
        with patch("etl.tasks.get_etl_logger"):
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk], kwargs={"cdc_continue": True}, throw=True
            ).get()

        self.assertIn("cutover is pending", result)
        self.assertTrue(any(
            call.kwargs.get("status") == "SKIPPED"
            for call in set_progress.call_args_list
        ))
        ensure_connector.assert_not_called()
        consume.assert_not_called()
        enqueue.assert_not_called()
        get_client.return_value.close.assert_called_once()
        cache.delete.assert_called_once_with(f"reporthub:cdc-ingestion:{source.pk}")
        source.refresh_from_db()
        self.assertEqual(source.last_sync_status, "SUCCESS")
        self.assertEqual(source.last_sync_error, "previous error")
        self.assertFalse(CdcExecutionFailure.objects.filter(source=source).exists())

    @patch("etl.tasks.set_task_progress")
    @patch("etl.tasks.clickhouse_connect.get_client")
    @patch("etl.tasks.cache")
    @patch("etl.cdc.ensure_debezium_connector")
    @patch("etl.cdc.consume_source_cdc_batch", return_value=0)
    def test_cdc_ingestion_remains_enabled_during_transition_staging(
        self, consume, ensure_connector, cache, get_client, set_progress
    ):
        source = SourceProfile.objects.create(
            name="Staging fence source",
            ingestion_mode=IngestionMode.CDC,
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            port=5432,
            database_name="test_db",
            username="test_user",
        )
        CdcToBatchTransition.objects.create(source=source)
        cache.add.return_value = True
        with patch("etl.tasks.get_etl_logger"):
            result = ingest_source_to_clickhouse_task.apply(
                args=[source.pk], kwargs={"cdc_continue": True}, throw=True
            ).get()
        self.assertIn("Processed 0 CDC event(s)", result)
        ensure_connector.assert_called_once()
        consume.assert_called_once()
        get_client.return_value.close.assert_called_once()

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
        self.assertEqual(continuation["kwargs"]["origin_mode"], IngestionMode.CDC)
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
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["origin_mode"], IngestionMode.CDC)
        self.assertEqual(apply_async.call_args.kwargs["queue"], "queue_cdc")
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["task_queue"], "queue_cdc")
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
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["origin_mode"], IngestionMode.CDC)
        self.assertEqual(apply_async.call_args.kwargs["queue"], "queue_backfill")
        self.assertEqual(apply_async.call_args.kwargs["kwargs"]["task_queue"], "queue_backfill")
