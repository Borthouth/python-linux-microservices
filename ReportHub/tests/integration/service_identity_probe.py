"""Disposable database permission probes; never run against an existing stack."""

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys
import time
import unittest
from uuid import uuid4

import clickhouse_connect
import psycopg2
from psycopg2 import errors


sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def postgres_connection(user, password):
    return psycopg2.connect(
        host="postgres", dbname="reporthub", user=user, password=password,
    )


def clickhouse_client(user, password):
    return clickhouse_connect.get_client(
        host="clickhouse", username=user, password=password,
    )


def provision_and_migrate():
    with postgres_connection("identity_admin", os.environ["POSTGRES_PASSWORD"]) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SHOW server_version")
            print(f"PostgreSQL identity test version: {cursor.fetchone()[0]}", flush=True)
            for role, password in (
                ("reporthub_migrator", os.environ["IDENTITY_MIGRATION_PASSWORD"]),
                ("reporthub_runtime", os.environ["IDENTITY_RUNTIME_PASSWORD"]),
            ):
                cursor.execute(
                    f"CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                    "NOREPLICATION NOBYPASSRLS PASSWORD %s", (password,),
                )
            cursor.execute("ALTER DATABASE reporthub OWNER TO reporthub_migrator")
            cursor.execute("REVOKE ALL ON DATABASE reporthub FROM PUBLIC")
            cursor.execute("GRANT CONNECT ON DATABASE reporthub TO reporthub_runtime")
            cursor.execute("ALTER SCHEMA public OWNER TO reporthub_migrator")
            cursor.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC")
            cursor.execute("GRANT USAGE ON SCHEMA public TO reporthub_runtime")
            cursor.execute(
                "ALTER DEFAULT PRIVILEGES FOR ROLE reporthub_migrator IN SCHEMA public "
                "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO reporthub_runtime"
            )
            cursor.execute(
                "ALTER DEFAULT PRIVILEGES FOR ROLE reporthub_migrator IN SCHEMA public "
                "GRANT USAGE, SELECT ON SEQUENCES TO reporthub_runtime"
            )

    client = clickhouse_client("identity_admin", os.environ["CLICKHOUSE_PASSWORD"])
    try:
        print(
            f"ClickHouse identity test version: {client.query('SELECT version()').result_rows[0][0]}",
            flush=True,
        )
        client.command("CREATE DATABASE private_identity_test ENGINE = Atomic")
        client.command(
            "CREATE TABLE private_identity_test.secret (id UInt64) "
            "ENGINE = MergeTree ORDER BY id"
        )
        client.command(
            "CREATE USER reporthub_runtime IDENTIFIED WITH sha256_password BY "
            "{password:String}",
            parameters={"password": os.environ["IDENTITY_RUNTIME_PASSWORD"]},
        )
        client.command(
            "GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER ADD COLUMN "
            "ON default.* TO reporthub_runtime"
        )
        for table in ("tables", "columns", "databases"):
            client.command(f"GRANT SELECT ON system.{table} TO reporthub_runtime")
    finally:
        client.close()

    migration_environment = os.environ.copy()
    migration_environment.update({
        "APP_POSTGRES_USER": "reporthub_migrator",
        "APP_POSTGRES_PASSWORD": os.environ["IDENTITY_MIGRATION_PASSWORD"],
    })
    subprocess.run(
        [sys.executable, "manage.py", "migrate", "--noinput", "--verbosity", "0"],
        env=migration_environment, check=True, timeout=180,
    )
    subprocess.run(
        [sys.executable, "manage.py", "migrate", "--check", "--verbosity", "0"],
        env=migration_environment, check=True, timeout=30,
    )
    with postgres_connection(
        "reporthub_migrator", os.environ["IDENTITY_MIGRATION_PASSWORD"],
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE TABLE identity_future (id bigserial PRIMARY KEY)")
            cursor.execute("ALTER TABLE identity_future ADD COLUMN value text")
            cursor.execute(
                "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls "
                "FROM pg_roles WHERE rolname = current_user"
            )
            if cursor.fetchone() != (False, False, False, False, False):
                raise RuntimeError("The migration identity has administrative attributes.")

    environment = os.environ.copy()
    environment.update({
        "APP_POSTGRES_USER": "reporthub_runtime",
        "APP_POSTGRES_PASSWORD": os.environ["IDENTITY_RUNTIME_PASSWORD"],
        "APP_CLICKHOUSE_USER": "reporthub_runtime",
        "APP_CLICKHOUSE_PASSWORD": os.environ["IDENTITY_RUNTIME_PASSWORD"],
    })
    subprocess.run(
        [sys.executable, __file__, "runtime"],
        env=environment, check=True, timeout=180,
    )
    print("IDENTITY_PERMISSION_TESTS_PASSED", flush=True)


class RuntimePermissionTests(unittest.TestCase):
    def setUp(self):
        self.postgres = postgres_connection(
            "reporthub_runtime", os.environ["IDENTITY_RUNTIME_PASSWORD"],
        )
        self.postgres.autocommit = True
        self.addCleanup(self.postgres.close)
        self.clickhouse = clickhouse_client(
            "reporthub_runtime", os.environ["IDENTITY_RUNTIME_PASSWORD"],
        )
        self.addCleanup(self.clickhouse.close)

    def test_postgres_runtime_cannot_administer_roles_or_schema(self):
        statements = (
            "CREATE ROLE forbidden_role",
            "CREATE DATABASE forbidden_database",
            "CREATE TABLE forbidden_table (id integer)",
            "CREATE SCHEMA forbidden_schema",
            "CREATE TEMP TABLE forbidden_temp (id integer)",
            "ALTER TABLE control_plane_sourceprofile ADD COLUMN forbidden integer",
            "DROP TABLE reports_reportexecution",
            "TRUNCATE TABLE control_plane_sourceprofile CASCADE",
            "SET ROLE reporthub_migrator",
        )
        with self.postgres.cursor() as cursor:
            for statement in statements:
                with self.subTest(statement=statement):
                    with self.assertRaises(errors.InsufficientPrivilege):
                        cursor.execute(statement)

    def test_application_connections_use_runtime_identity(self):
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute("SELECT current_user")
            self.assertEqual(cursor.fetchone(), ("reporthub_runtime",))
        self.assertEqual(
            self.clickhouse.query("SELECT currentUser()").result_rows,
            [("reporthub_runtime",)],
        )

    def test_postgres_runtime_can_use_future_migration_tables_and_sequences(self):
        with self.postgres.cursor() as cursor:
            cursor.execute("INSERT INTO identity_future (value) VALUES ('old') RETURNING id")
            identity = cursor.fetchone()[0]
            cursor.execute(
                "UPDATE identity_future SET value = 'new' WHERE id = %s", (identity,),
            )
            cursor.execute("SELECT value FROM identity_future WHERE id = %s", (identity,))
            self.assertEqual(cursor.fetchone(), ("new",))
            cursor.execute("DELETE FROM identity_future WHERE id = %s", (identity,))

    def test_postgres_runtime_orm_history_locks_sessions_and_schedules(self):
        from django.contrib.auth import get_user_model
        from django.contrib.sessions.backends.db import SessionStore
        from django.db import transaction
        from django_celery_beat.models import IntervalSchedule, PeriodicTask

        from control_plane.models import SourceProfile, SourceTable
        from reports.models import ReportDefinition, ReportExecution

        user = get_user_model().objects.create_user(username="identity-test")
        source = SourceProfile.objects.create(
            name="identity-test", host="unused", username="unused",
            database_name="unused", password="isolated-test-value",
        )
        with transaction.atomic():
            locked = SourceProfile.objects.select_for_update().get(pk=source.pk)
            locked.name = "identity-updated"
            locked.save()
        self.assertEqual(source.history.count(), 2)
        table = SourceTable.objects.create(
            source=source, schema_name="public", table_name="items",
        )
        report = ReportDefinition.objects.create(
            name="identity-report", source=source, base_table=table,
        )
        execution = ReportExecution.objects.create(report=report, created_by=user)
        execution.status = "completed"
        execution.save()
        session = SessionStore()
        session["identity"] = user.pk
        session.save()
        self.assertEqual(SessionStore(session_key=session.session_key)["identity"], user.pk)
        session.delete()
        schedule = IntervalSchedule.objects.create(every=10, period="seconds")
        task = PeriodicTask.objects.create(
            name="identity-task", task="isolated.test", interval=schedule,
        )
        task.delete()
        execution.delete()
        source.delete()
        user.delete()

    def test_clickhouse_runtime_batch_publication_and_metadata(self):
        client = self.clickhouse
        client.command("CREATE TABLE identity_stage (id UInt64) ENGINE = MergeTree ORDER BY id")
        client.insert("identity_stage", [[1]], column_names=["id"])
        client.command("CREATE TABLE identity_active AS identity_stage")
        client.command("INSERT INTO identity_active SELECT * FROM identity_stage")
        client.command("ALTER TABLE identity_stage ADD COLUMN value String DEFAULT ''")
        client.command("EXCHANGE TABLES identity_active AND identity_stage")
        client.command("RENAME TABLE identity_stage TO identity_retained")
        self.assertEqual(client.query("SELECT id FROM identity_active").result_rows, [(1,)])
        self.assertTrue(client.query(
            "SELECT uuid FROM system.tables "
            "WHERE database = currentDatabase() AND name = 'identity_active'"
        ).result_rows)
        self.assertTrue(client.query("DESCRIBE TABLE identity_active").result_rows)
        self.assertEqual(client.query(
            "SELECT engine FROM system.databases WHERE name = currentDatabase()"
        ).result_rows, [("Atomic",)])
        client.command("KILL QUERY WHERE query_id = 'identity-no-active-query' SYNC")
        for table in ("identity_active", "identity_retained"):
            client.command(f"DROP TABLE {table} SYNC")

    def test_clickhouse_runtime_cdc_and_report_query(self):
        from control_plane.models import DbChoice, SourceField, SourceProfile, SourceTable
        from etl.cdc import _create_cdc_table
        from reports.query_builder import clickhouse_table_reference

        source = SourceProfile.objects.create(
            name="identity-cdc", db_type=DbChoice.POSTGRESQL,
            host="unused", username="unused", database_name="unused",
        )
        table = SourceTable.objects.create(
            source=source, schema_name="public", table_name="cdc_items",
            primary_key_fields=["id"],
        )
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        name, fields = _create_cdc_table(self.clickhouse, source, table, ["id"])
        SourceField.objects.create(table=table, field_name="value", data_type="VARCHAR")
        _create_cdc_table(self.clickhouse, source, table, ["id", "value"])
        self.clickhouse.insert(
            name.strip("`"), [[1, "old", 1, 0], [1, "new", 2, 0], [2, "gone", 3, 1]],
            column_names=["id", "value", "_cdc_version", "_cdc_deleted"],
        )
        reference = clickhouse_table_reference(table, name.strip("`"), "t0")
        self.assertEqual(
            self.clickhouse.query(f"SELECT id, value FROM {reference}").result_rows,
            [(1, "new")],
        )
        self.clickhouse.command(f"DROP TABLE {name} SYNC")
        source.delete()

    def test_runtime_can_stage_batch_using_application_workflow(self):
        import sqlalchemy as sa
        from unittest.mock import patch

        from control_plane.models import (
            CdcToBatchTableCheckpoint, DbChoice, IngestionMode,
            SourceField, SourceProfile, SourceTable,
        )
        from etl.cdc_to_batch import (
            prepare_cdc_to_batch_transition, stage_cdc_to_batch_table,
        )

        source = SourceProfile.objects.create(
            name="identity-staging", db_type=DbChoice.MYSQL,
            ingestion_mode=IngestionMode.CDC, host="unused", username="unused",
            database_name="unused",
        )
        table = SourceTable.objects.create(
            source=source, schema_name="public", table_name="staging_items",
            primary_key_fields=["id"],
        )
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        transition = prepare_cdc_to_batch_transition(source.pk)
        checkpoint = transition.table_checkpoints.get()
        engine = sa.create_engine("sqlite://")
        try:
            with engine.begin() as conn:
                conn.execute(sa.text("CREATE TABLE staging_items (id INTEGER PRIMARY KEY)"))
                conn.execute(sa.text("INSERT INTO staging_items VALUES (1), (2)"))
            with patch("etl.cdc_to_batch.create_source_engine", return_value=engine):
                checkpoint = stage_cdc_to_batch_table(checkpoint.pk, batch_size=1)
            self.assertEqual(checkpoint.status, CdcToBatchTableCheckpoint.Status.STAGED)
            self.assertEqual(checkpoint.staging_row_count, 2)
            self.assertTrue(checkpoint.staging_uuid)
            self.assertTrue(checkpoint.staging_schema["synchronous_inserts"])
            self.assertEqual(
                self.clickhouse.query(
                    f"SELECT id FROM `{checkpoint.staging_table}` ORDER BY id"
                ).result_rows,
                [(1,), (2,)],
            )
            source.refresh_from_db()
            self.assertEqual(source.ingestion_mode, IngestionMode.CDC)
            self.clickhouse.command(f"DROP TABLE `{checkpoint.staging_table}` SYNC")
        finally:
            engine.dispose()
        source.delete()

    def test_clickhouse_runtime_cannot_administer_or_access_other_databases(self):
        from clickhouse_connect.driver.exceptions import DatabaseError

        for statement in (
            "CREATE USER forbidden_user",
            "CREATE ROLE forbidden_role",
            "GRANT SELECT ON private_identity_test.* TO reporthub_runtime",
            "CREATE DATABASE forbidden_database",
            "SELECT * FROM private_identity_test.secret",
            "INSERT INTO private_identity_test.secret VALUES (1)",
            "DROP TABLE private_identity_test.secret",
        ):
            with self.subTest(statement=statement):
                with self.assertRaises(DatabaseError) as raised:
                    self.clickhouse.command(statement)
                self.assertIn("ACCESS_DENIED", str(raised.exception))

    def test_clickhouse_cancellation_is_limited_to_own_identity(self):
        from clickhouse_connect.driver.exceptions import DatabaseError
        from config.clickhouse_queries import cancel_clickhouse_query

        observer = clickhouse_client("identity_admin", os.environ["CLICKHOUSE_PASSWORD"])
        self.addCleanup(observer.close)
        for user, password, own_query in (
            ("reporthub_runtime", os.environ["IDENTITY_RUNTIME_PASSWORD"], True),
            ("identity_admin", os.environ["CLICKHOUSE_PASSWORD"], False),
        ):
            with self.subTest(user=user):
                query_id = f"identity_{uuid4().hex}"
                client = clickhouse_client(user, password)
                self.addCleanup(client.close)

                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(
                        client.query,
                        "SELECT sleepEachRow(0.01) FROM numbers(200)",
                        settings={"query_id": query_id},
                    )
                    try:
                        for attempt in range(100):
                            if observer.query(
                                "SELECT count() FROM system.processes "
                                "WHERE query_id = {id:String}",
                                parameters={"id": query_id},
                            ).result_rows == [(1,)]:
                                break
                            if future.done():
                                future.result()
                                self.fail("Cancellation probe ended before it was observed.")
                            time.sleep(0.01)
                        else:
                            self.fail("Cancellation probe never became active.")
                        if own_query:
                            cancel_clickhouse_query(self.clickhouse, query_id)
                            with self.assertRaises(DatabaseError) as raised:
                                future.result(timeout=10)
                            self.assertIn("QUERY_WAS_CANCELLED", str(raised.exception))
                        else:
                            cancel_clickhouse_query(self.clickhouse, query_id)
                            self.assertEqual(len(future.result(timeout=10).result_rows), 200)
                    finally:
                        cancel_clickhouse_query(observer, query_id)

    def test_driver_query_ids_cover_queries_commands_and_inserts(self):
        client = self.clickhouse
        query_id = f"etl_task_{uuid4().hex}"
        self.assertEqual(
            client.query("SELECT queryID()", settings={"query_id": query_id}).result_rows,
            [(query_id,)],
        )
        self.assertEqual(
            client.command("SELECT queryID()", settings={"query_id": query_id}), query_id,
        )
        client.command(
            "CREATE TABLE identity_query_ids "
            "(id UInt64) ENGINE = MergeTree ORDER BY id",
            settings={"query_id": query_id},
        )
        summary = client.insert(
            "identity_query_ids", [[1]], column_names=["id"],
            settings={"query_id": query_id},
        )
        self.assertEqual(summary.query_id(), query_id)
        self.assertEqual(client.query("SELECT id FROM identity_query_ids").result_rows, [(1,)])
        client.command("DROP TABLE identity_query_ids SYNC")

    def test_etl_cancellation_failure_is_logged_and_client_closed(self):
        from unittest.mock import patch
        from etl.tasks import CancellableETLTask

        with patch("etl.tasks.clickhouse_connect.get_client") as get_client:
            client = get_client.return_value
            client.command.side_effect = TimeoutError("Lost cancellation response")
            with self.assertLogs("etl.tasks", level="ERROR") as logged:
                CancellableETLTask()._kill_clickhouse_query("isolated-test")
            self.assertIn("external work may still be active", logged.output[0])
            self.assertIn("Lost cancellation response", logged.output[0])
            client.close.assert_called_once()

    def test_report_task_uses_exact_driver_query_id(self):
        from unittest.mock import patch
        from control_plane.models import SourceField, SourceProfile, SourceTable
        from reports.models import ReportDefinition, ReportExecution
        from reports.tasks import execute_report_task

        source = SourceProfile.objects.create(
            name="identity-query-report", host="unused", username="unused",
            database_name="unused",
        )
        table = SourceTable.objects.create(
            source=source, schema_name="public", table_name="query_report",
        )
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        report = ReportDefinition.objects.create(
            name="identity-query-report", source=source, base_table=table,
            selected_fields=["id"],
        )
        execution = ReportExecution.objects.create(report=report)
        client = clickhouse_client(
            "reporthub_runtime", os.environ["IDENTITY_RUNTIME_PASSWORD"],
        )
        self.addCleanup(client.close)
        with patch("reports.tasks.build_clickhouse_report_query", return_value=(
            "SELECT queryID()", ["query_id"], [None],
        )), patch(
            "reports.tasks.clickhouse_connect.get_client", return_value=client,
        ), patch("reports.tasks.Minio") as minio, patch("reports.tasks.set_task_progress"):
            execute_report_task.apply(args=[report.pk, execution.pk], throw=True).get()
        execution.refresh_from_db()
        self.assertEqual(execution.status, "completed")
        exported = minio.return_value.put_object.call_args.args[2].getvalue()
        self.assertIn(f"report_{execution.pk}", exported.decode())
        source.delete()

    def test_batch_refresh_uses_driver_query_ids(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        import sqlalchemy as sa
        from control_plane.models import DbChoice, SourceField, SourceProfile, SourceTable
        from etl.tasks import _ingest_source_to_clickhouse_batch
        from reports.query_builder import source_table_clickhouse_name

        source = SourceProfile.objects.create(
            name="identity-query-etl", db_type=DbChoice.MYSQL,
            host="unused", username="unused", database_name="unused",
        )
        table = SourceTable.objects.create(
            source=source, schema_name="public", table_name="query_items",
            primary_key_fields=["id"],
        )
        SourceField.objects.create(
            table=table, field_name="id", data_type="INTEGER", is_selected=True,
        )
        task_id = uuid4().hex
        for value in (1, 2):
            engine = sa.create_engine("sqlite://")
            with engine.begin() as conn:
                conn.execute(sa.text("CREATE TABLE query_items (id INTEGER PRIMARY KEY)"))
                conn.execute(sa.text(f"INSERT INTO query_items VALUES ({value})"))
            client = clickhouse_client(
                "reporthub_runtime", os.environ["IDENTITY_RUNTIME_PASSWORD"],
            )
            self.addCleanup(client.close)
            self.addCleanup(engine.dispose)
            with patch(
                "etl.tasks.clickhouse_connect.get_client", return_value=client,
            ), patch("etl.tasks.create_source_engine", return_value=engine), patch(
                "etl.tasks.set_task_progress",
            ), patch("etl.tasks.ingest_source_to_clickhouse_task.apply_async"), patch.object(
                client, "insert", wraps=client.insert,
            ) as insert, patch.object(client, "command", wraps=client.command) as command:
                _ingest_source_to_clickhouse_batch(
                    SimpleNamespace(request=SimpleNamespace(id=task_id)),
                    source.pk, batch_size=10,
                )
            self.assertEqual(
                insert.call_args.kwargs["settings"], {"query_id": f"etl_task_{task_id}"},
            )
            publication = [
                call for call in command.call_args_list
                if call.args[0].startswith(("RENAME TABLE", "EXCHANGE TABLES"))
            ]
            self.assertEqual(len(publication), 1)
            self.assertEqual(
                publication[0].kwargs["settings"], {"query_id": f"etl_task_{task_id}"},
            )
            self.assertEqual(
                self.clickhouse.query(
                    f"SELECT id FROM `{source_table_clickhouse_name(table)}`"
                ).result_rows,
                [(value,)],
            )
        self.clickhouse.command(
            f"DROP TABLE `{source_table_clickhouse_name(table)}` SYNC"
        )
        source.delete()

    def test_uncertain_external_operations_survive_timeouts_and_interruptions(self):
        from unittest.mock import Mock
        from django.test import override_settings
        from config.clickhouse_queries import cancel_clickhouse_query
        from control_plane.models import (
            CdcSourceExternalOperation, IngestionMode, SourceProfile,
        )
        from etl.cdc_external_operations import (
            _assert_no_cdc_external_operations, _track_cdc_external_operation,
        )
        from django.db import transaction

        source = SourceProfile.objects.create(
            name="identity-uncertain-query", ingestion_mode=IngestionMode.CDC,
            host="unused", username="unused", database_name="unused",
        )
        for failure in (TimeoutError("Lost query response"), SystemExit("Worker interrupted")):
            with self.subTest(failure=type(failure).__name__):
                with override_settings(CDC_EXTERNAL_OPERATION_TRACKING=True):
                    with self.assertRaises(type(failure)):
                        with _track_cdc_external_operation(
                            source.pk, CdcSourceExternalOperation.Kind.REPORT,
                        ):
                            raise failure
                operation = source.cdc_external_operations.get()
                cancel_clickhouse_query(self.clickhouse, "already-absent-query")
                lost_response_client = Mock()
                lost_response_client.command.side_effect = TimeoutError("Lost cancel response")
                with self.assertRaises(TimeoutError):
                    cancel_clickhouse_query(lost_response_client, "already-absent-query")
                self.assertTrue(source.cdc_external_operations.filter(pk=operation.pk).exists())
                with transaction.atomic():
                    locked = SourceProfile.objects.select_for_update().get(pk=source.pk)
                    with self.assertRaisesRegex(RuntimeError, "uncertain outcome"):
                        _assert_no_cdc_external_operations(locked)
                operation.delete()
        source.delete()


if __name__ == "__main__":
    if sys.argv[1:] == ["runtime"]:
        import django

        django.setup()
        unittest.main(argv=[sys.argv[0]], verbosity=2)
    else:
        provision_and_migrate()
