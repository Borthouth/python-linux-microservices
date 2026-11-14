import os
import csv
import io
import json
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from uuid import uuid4
from urllib.request import Request, urlopen

import clickhouse_connect
import django
import psycopg2
import redis
from psycopg2 import sql as psycopg2_sql

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from app.core.celery_app import celery_app
from control_plane.models import (
    SourceField,
    SourceProfile,
    SourceTable,
)
from django.core.management import call_command
from django.conf import settings
from django.db import connection
from django.test import RequestFactory
from etl.tasks import CancellableETLTask, ingest_source_to_clickhouse_task
from control_plane.views.dashboard import cancel_pipeline_view, cancel_report_view
from reports.models import ReportDefinition, ReportExecution
from reports.query_builder import source_table_clickhouse_name
from reports.tasks import execute_report_task


@celery_app.task(
    bind=True,
    base=CancellableETLTask,
    queue="queue_hot",
    name="tests.integration.celery_cancellation_probe.run_slow_cancellation_probe",
)
def run_slow_cancellation_probe(self):
    task_id = self.request.id
    client = clickhouse_connect.get_client(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_PORT,
        username=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
    )
    try:
        client.query(
            "SELECT sleepEachRow(1) FROM numbers(60)",
            settings={
                "max_block_size": 1,
                "max_threads": 1,
                "query_id": f"etl_task_{task_id}",
            },
        )
    finally:
        client.close()


@celery_app.task(
    queue="queue_hot",
    name="tests.integration.celery_cancellation_probe.read_worker_identities",
)
def read_worker_identities():
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_user")
        postgres_user = cursor.fetchone()[0]
    client = clickhouse_connect.get_client(
        host=settings.CLICKHOUSE_HOST, port=settings.CLICKHOUSE_PORT,
        username=settings.CLICKHOUSE_USER, password=settings.CLICKHOUSE_PASSWORD,
    )
    try:
        clickhouse_user = client.query("SELECT currentUser()").result_rows[0][0]
    finally:
        client.close()
    return postgres_user, clickhouse_user


class CeleryCancellationProbe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("CANCELLATION_RESTRICTED_IDENTITY") == "1":
            cls._provision_restricted_identities()
        else:
            call_command("migrate", interactive=False, verbosity=0)
        cls.worker_output = tempfile.TemporaryFile(mode="w+t")
        cls.worker = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "celery",
                "-A",
                "config",
                "worker",
                "--pool=prefork",
                "--concurrency=1",
                "--queues=queue_hot",
                "--without-gossip",
                "--without-mingle",
                "--without-heartbeat",
                "--include=tests.integration.celery_cancellation_probe",
                "--loglevel=INFO",
                f"--hostname=cancellation-probe-{uuid4().hex[:8]}@%h",
            ],
            stdout=cls.worker_output,
            stderr=subprocess.STDOUT,
        )
        cls.clickhouse = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_INSPECT_HOST"],
            port=int(os.environ["CLICKHOUSE_PORT"]),
            username=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
        )
        cls.broker = redis.Redis.from_url(os.environ["REDIS_URL"])
        cls._wait_for_worker()

    @classmethod
    def _provision_restricted_identities(cls):
        admin = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"], dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
        )
        admin.autocommit = True
        try:
            with admin.cursor() as cursor:
                for role, password in (
                    ("cancellation_migrator", os.environ["CANCELLATION_MIGRATION_PASSWORD"]),
                    ("cancellation_runtime", os.environ["CANCELLATION_RUNTIME_PASSWORD"]),
                ):
                    cursor.execute(
                        f"CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB "
                        "NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %s",
                        (password,),
                    )
                cursor.execute("ALTER DATABASE reporthub OWNER TO cancellation_migrator")
                cursor.execute("REVOKE ALL ON DATABASE reporthub FROM PUBLIC")
                cursor.execute("GRANT CONNECT ON DATABASE reporthub TO cancellation_runtime")
                cursor.execute("ALTER SCHEMA public OWNER TO cancellation_migrator")
                cursor.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC")
                cursor.execute("GRANT USAGE ON SCHEMA public TO cancellation_runtime")
                cursor.execute(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE cancellation_migrator IN SCHEMA public "
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO cancellation_runtime"
                )
                cursor.execute(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE cancellation_migrator IN SCHEMA public "
                    "GRANT USAGE, SELECT ON SEQUENCES TO cancellation_runtime"
                )
        finally:
            admin.close()
        client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_INSPECT_HOST"],
            port=int(os.environ["CLICKHOUSE_PORT"]),
            username=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
        )
        try:
            client.command(
                "CREATE USER cancellation_runtime IDENTIFIED WITH sha256_password BY "
                "{password:String}",
                parameters={"password": os.environ["CANCELLATION_RUNTIME_PASSWORD"]},
            )
            client.command(
                "GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER ADD COLUMN "
                "ON default.* TO cancellation_runtime"
            )
            for table in ("tables", "columns", "databases"):
                client.command(f"GRANT SELECT ON system.{table} TO cancellation_runtime")
        finally:
            client.close()
        environment = os.environ.copy()
        environment.update({
            "APP_POSTGRES_USER": "cancellation_migrator",
            "APP_POSTGRES_PASSWORD": os.environ["CANCELLATION_MIGRATION_PASSWORD"],
        })
        subprocess.run(
            [sys.executable, "manage.py", "migrate", "--noinput", "--verbosity", "0"],
            env=environment, check=True, timeout=180,
        )

    @classmethod
    def _worker_log(cls):
        cls.worker_output.flush()
        cls.worker_output.seek(0)
        return cls.worker_output.read()

    @classmethod
    def _wait_for_worker(cls):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if cls.worker.poll() is not None:
                raise AssertionError(
                    f"Celery worker exited early:\n{cls._worker_log()}"
                )
            try:
                if celery_app.control.ping(timeout=1):
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise AssertionError(f"Celery worker did not become ready:\n{cls._worker_log()}")

    @classmethod
    def tearDownClass(cls):
        try:
            celery_app.control.broadcast("shutdown", reply=False)
            cls.worker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            cls.worker.send_signal(signal.SIGTERM)
            try:
                cls.worker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                cls.worker.kill()
                cls.worker.wait(timeout=5)
        finally:
            cls.clickhouse.close()
            cls.broker.close()
            cls.worker_output.close()

    def _query_is_active(self, query_id):
        result = self.clickhouse.query(
            "SELECT count() FROM system.processes WHERE query_id = {query_id:String}",
            parameters={"query_id": query_id},
        )
        return result.result_rows[0][0] > 0

    def _proxy_request(self, path, value=None):
        body = json.dumps(value).encode("utf-8") if value is not None else None
        request = Request(
            os.environ["CANCELLATION_PROXY_URL"] + path,
            data=body,
            headers={"Content-Type": "application/json"} if body else {},
        )
        with urlopen(request, timeout=5) as response:
            return json.loads(response.read())

    def _configure_proxy(self, mode, query_id=None):
        return self._proxy_request(
            "/__fault/configure",
            {"mode": mode, "query_id": query_id},
        )

    def _proxy_state(self):
        return self._proxy_request("/__fault/state")

    def _release_proxy(self):
        self._proxy_request("/__fault/release", {})

    def _start_query(self, query_id):
        task_id = query_id.removeprefix("etl_task_")
        result = run_slow_cancellation_probe.apply_async(task_id=task_id)
        self._wait_until(
            lambda: self._query_is_active(query_id),
            timeout=20,
            message=f"Worker never started ClickHouse query {query_id}",
        )
        return result

    def _wait_until(self, predicate, timeout, message):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.2)
        self.fail(message)

    def _wait_for_proxy_block(self, kind, timeout=10):
        def is_blocked():
            blocked = self._proxy_state().get("blocked")
            return bool(blocked and blocked.get("kind") == kind)

        self._wait_until(
            is_blocked,
            timeout,
            f"Proxy never blocked the {kind} request; "
            f"state={self._proxy_state()!r}\n{self._worker_log()}",
        )

    def _wait_for_query_to_stop(self, query_id):
        self._wait_until(
            lambda: not self._query_is_active(query_id),
            timeout=15,
            message=f"ClickHouse query {query_id} remained active after termination.",
        )

    def _terminate(self, task_id):
        celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")

    def _cleanup_query(self, query_id):
        self._release_proxy()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not self._query_is_active(query_id):
            time.sleep(0.2)
        if self._query_is_active(query_id):
            self.clickhouse.command(
                "KILL QUERY WHERE query_id = {query_id:String} SYNC",
                parameters={"query_id": query_id},
            )

    def _invoke_admin_post(self, view, object_id):
        request = RequestFactory().post("/")
        request.user = type(
            "AdminUser",
            (),
            {"is_authenticated": True, "is_staff": True, "is_superuser": False},
        )()
        return view(request, object_id)

    def _wait_for_task(self, result, timeout=30):
        self._wait_until(
            result.ready,
            timeout,
            f"Celery task {result.id} did not finish. Worker log:\n{self._worker_log()}",
        )

    @unittest.skipUnless(
        os.environ.get("CANCELLATION_RESTRICTED_IDENTITY") == "1",
        "Restricted worker identities are not enabled.",
    )
    def test_restricted_application_identities_and_denied_administration(self):
        from clickhouse_connect.driver.exceptions import DatabaseError
        from psycopg2 import errors

        with connection.cursor() as cursor:
            cursor.execute("SELECT current_user")
            self.assertEqual(cursor.fetchone(), ("cancellation_runtime",))
        worker_result = read_worker_identities.apply_async(queue="queue_hot")
        self.assertEqual(
            worker_result.get(timeout=30),
            ["cancellation_runtime", "cancellation_runtime"],
        )
        with psycopg2.connect(
            host=os.environ["POSTGRES_HOST"], dbname=os.environ["POSTGRES_DB"],
            user="cancellation_runtime", password=os.environ["CANCELLATION_RUNTIME_PASSWORD"],
        ) as runtime:
            with runtime.cursor() as cursor:
                with self.assertRaises(errors.InsufficientPrivilege):
                    cursor.execute("CREATE TABLE forbidden_worker_table (id integer)")
        client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST, port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER, password=settings.CLICKHOUSE_PASSWORD,
        )
        try:
            self.assertEqual(
                client.query("SELECT currentUser()").result_rows, [("cancellation_runtime",)],
            )
            with self.assertRaises(DatabaseError) as error:
                client.command("CREATE USER forbidden_worker_user")
            self.assertIn("ACCESS_DENIED", str(error.exception))
        finally:
            client.close()

    @unittest.skipUnless(
        os.environ.get("CANCELLATION_RESTRICTED_IDENTITY") == "1",
        "Restricted worker identities are not enabled.",
    )
    def test_readonly_source_paginated_refresh_and_report_export(self):
        from minio import Minio
        from psycopg2 import errors

        reader_password = secrets.token_urlsafe(32)
        admin = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"], dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
        )
        self.addCleanup(admin.close)
        admin.autocommit = True
        with admin.cursor() as cursor:
            cursor.execute(
                "CREATE ROLE batch_source_reader LOGIN NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %s",
                (reader_password,),
            )
            cursor.execute("CREATE DATABASE batch_source")
            cursor.execute("REVOKE ALL ON DATABASE batch_source FROM PUBLIC")
            cursor.execute("GRANT CONNECT ON DATABASE batch_source TO batch_source_reader")
        source_admin = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"], dbname="batch_source",
            user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
        )
        self.addCleanup(source_admin.close)
        source_admin.autocommit = True
        with source_admin.cursor() as cursor:
            cursor.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC")
            cursor.execute("GRANT USAGE ON SCHEMA public TO batch_source_reader")
            cursor.execute("CREATE TABLE public.batch_items (id INTEGER PRIMARY KEY, value TEXT)")
            cursor.execute(
                "INSERT INTO public.batch_items VALUES (1, 'one'), (2, 'two'), (3, 'three')"
            )
            cursor.execute("CREATE TABLE public.private_items (id INTEGER)")
            cursor.execute("GRANT SELECT ON public.batch_items TO batch_source_reader")

        reader = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"], dbname="batch_source",
            user="batch_source_reader", password=reader_password,
        )
        self.addCleanup(reader.close)
        reader.autocommit = True
        with reader.cursor() as cursor:
            cursor.execute("SELECT id, value FROM public.batch_items ORDER BY id")
            self.assertEqual(cursor.fetchall(), [(1, "one"), (2, "two"), (3, "three")])
            for statement in (
                "INSERT INTO public.batch_items VALUES (4, 'forbidden')",
                "UPDATE public.batch_items SET value = 'forbidden'",
                "DELETE FROM public.batch_items",
                "TRUNCATE TABLE public.batch_items",
                "ALTER TABLE public.batch_items ADD COLUMN forbidden integer",
                "CREATE TABLE public.forbidden (id integer)",
                "CREATE TEMP TABLE forbidden (id integer)",
                "SELECT * FROM public.private_items",
            ):
                with self.subTest(statement=statement):
                    with self.assertRaises(errors.InsufficientPrivilege):
                        cursor.execute(statement)

        source = SourceProfile.objects.create(
            name=f"readonly-refresh-{uuid4().hex[:12]}",
            db_type="postgresql", host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]), database_name="batch_source",
            username="batch_source_reader", password=reader_password,
        )
        self.addCleanup(source.delete)
        table = SourceTable.objects.create(
            source=source, schema_name="public", table_name="batch_items",
            primary_key_fields=["id"],
        )
        SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
        SourceField.objects.create(table=table, field_name="value", data_type="TEXT")
        physical_table = source_table_clickhouse_name(table)
        self.addCleanup(
            self.clickhouse.command, f"DROP TABLE IF EXISTS `{physical_table}` SYNC",
        )

        previous_uuid = None
        expected = [(1, "one"), (2, "two"), (3, "three")]
        for refresh in range(2):
            if refresh:
                with source_admin.cursor() as cursor:
                    cursor.execute("UPDATE public.batch_items SET value = 'updated' WHERE id = 1")
                    cursor.execute("DELETE FROM public.batch_items WHERE id = 2")
                    cursor.execute("INSERT INTO public.batch_items VALUES (4, 'four')")
                expected = [(1, "updated"), (3, "three"), (4, "four")]
            previous_synced_at = table.last_synced_at
            ingestion_run_id = uuid4().hex
            result = ingest_source_to_clickhouse_task.apply_async(
                args=[source.pk],
                kwargs={
                    "batch_size": 2, "full_refresh": True,
                    "ingestion_run_id": ingestion_run_id, "task_queue": "queue_hot",
                },
                queue="queue_hot",
            )
            self._wait_for_task(result)
            self.assertEqual(result.state, "SUCCESS", self._worker_log())
            self.assertIn("Queued next 2-row ingestion batch", result.result)

            def refresh_finished():
                source.refresh_from_db()
                table.refresh_from_db()
                if source.last_sync_status == "FAILED":
                    self.fail(f"Refresh failed: {source.last_sync_error}\n{self._worker_log()}")
                return (
                    source.last_sync_status == "SUCCESS"
                    and table.last_synced_at is not None
                    and table.last_synced_at != previous_synced_at
                )

            # Continuations have separate task IDs; wait for persisted publication.
            self._wait_until(
                refresh_finished, timeout=30,
                message=f"Paginated refresh did not publish.\n{self._worker_log()}",
            )
            self.assertEqual(
                self.clickhouse.query(
                    f"SELECT id, value FROM `{physical_table}` ORDER BY id"
                ).result_rows,
                expected,
            )
            current_uuid = self.clickhouse.query(
                "SELECT toString(uuid) FROM system.tables "
                "WHERE database = currentDatabase() AND name = {name:String}",
                parameters={"name": physical_table},
            ).result_rows[0][0]
            if previous_uuid is not None:
                self.assertNotEqual(current_uuid, previous_uuid)
            previous_uuid = current_uuid
            self.assertEqual(
                self.clickhouse.query(
                    "SELECT name FROM system.tables WHERE database = currentDatabase() "
                    "AND startsWith(name, {prefix:String})",
                    parameters={"prefix": f"{physical_table}__staging_"},
                ).result_rows,
                [],
            )

        report = ReportDefinition.objects.create(
            name=f"readonly-export-{uuid4().hex[:12]}", source=source, base_table=table,
            selected_fields=["id", "value"], export_format="csv",
            config={"showTotals": False}, is_email_active=False,
        )
        execution = ReportExecution.objects.create(report=report)
        result = execute_report_task.apply_async(
            args=[report.pk, execution.pk], queue="queue_hot",
        )
        self._wait_for_task(result)
        execution.refresh_from_db()
        self.assertEqual(result.state, "SUCCESS", self._worker_log())
        self.assertEqual(execution.status, "completed", execution.error_message)
        self.assertEqual(execution.row_count, len(expected))
        client = Minio(
            settings.MINIO_STORAGE_ENDPOINT,
            access_key=settings.AWS_ACCESS_KEY_ID, secret_key=settings.AWS_SECRET_ACCESS_KEY,
            secure=False,
        )
        self.addCleanup(
            client.remove_object, settings.AWS_STORAGE_BUCKET_NAME, execution.file_path,
        )
        response = client.get_object(settings.AWS_STORAGE_BUCKET_NAME, execution.file_path)
        try:
            rows = list(csv.reader(io.StringIO(response.read().decode("utf-8"))))
        finally:
            response.close()
            response.release_conn()
        self.assertEqual(len(rows), len(expected) + 1)
        self.assertEqual(
            sorted((int(row[0]), row[1]) for row in rows[1:]), expected,
        )
        print("READONLY_SOURCE_REFRESH_EXPORT_PASSED", flush=True)

    def test_control_plane_report_cancellation_during_query(self):
        source = None
        physical_table = None
        task_result = None
        self._configure_proxy("delay-query")
        try:
            source = SourceProfile.objects.create(
                name=f"cancel-report-{uuid4().hex[:12]}",
                host="unused",
                database_name="unused",
                username="unused",
            )
            table = SourceTable.objects.create(
                source=source,
                schema_name="public",
                table_name=f"events_{uuid4().hex[:8]}",
            )
            SourceField.objects.create(
                table=table, field_name="id", data_type="INTEGER",
            )
            report = ReportDefinition.objects.create(
                name=f"cancel-report-{uuid4().hex[:12]}",
                source=source,
                base_table=table,
                selected_fields=["id"],
            )
            execution = ReportExecution.objects.create(report=report)
            physical_table = source_table_clickhouse_name(table)
            self.clickhouse.command(
                f"CREATE TABLE `{physical_table}` (id Int32) "
                "ENGINE = MergeTree ORDER BY tuple()"
            )
            self.clickhouse.insert(
                physical_table, [[1]], column_names=["id"],
            )

            task_result = execute_report_task.apply_async(
                args=[report.pk, execution.pk],
                queue="queue_hot",
            )
            self._wait_for_proxy_block("query")
            response = self._invoke_admin_post(cancel_report_view, execution.pk)
            self.assertEqual(response.status_code, 302)
            execution.refresh_from_db()
            self.assertTrue(execution.is_cancelled)

            self._release_proxy()
            self._wait_for_task(task_result)
            execution.refresh_from_db()
            self.assertEqual(execution.status, "cancelled")
            self.assertTrue(execution.is_cancelled)
            self.assertEqual(task_result.state, "FAILURE")
        finally:
            self._release_proxy()
            if task_result is not None and not task_result.ready():
                try:
                    task_result.get(timeout=10, propagate=False)
                except Exception:
                    pass
            if physical_table:
                self.clickhouse.command(
                    f"DROP TABLE IF EXISTS `{physical_table}` SYNC"
                )
            if source is not None:
                source.delete()

    def test_control_plane_pipeline_deactivation_skips_queued_ingestion(self):
        source = None
        source_table_name = f"cancel_ingestion_{uuid4().hex[:10]}"
        physical_table = None
        task_result = None
        try:
            source = SourceProfile.objects.create(
                name=f"cancel-ingestion-{uuid4().hex[:12]}",
                host="unused",
                database_name="unused",
                username="unused",
            )
            table = SourceTable.objects.create(
                source=source, schema_name="public", table_name=source_table_name,
            )
            SourceField.objects.create(
                table=table, field_name="id", data_type="INTEGER",
            )
            SourceField.objects.create(
                table=table, field_name="value", data_type="TEXT",
            )
            physical_table = source_table_clickhouse_name(table)
            self.clickhouse.command(
                f"CREATE TABLE `{physical_table}` "
                "(id Int32, value String) ENGINE = MergeTree ORDER BY tuple()"
            )
            self.clickhouse.insert(
                physical_table, [[99, "old"]], column_names=["id", "value"],
            )

            response = self._invoke_admin_post(cancel_pipeline_view, source.pk)
            self.assertEqual(response.status_code, 302)
            source.refresh_from_db()
            self.assertFalse(source.is_active)

            task_result = ingest_source_to_clickhouse_task.apply_async(
                args=[source.pk],
                kwargs={"full_refresh": True},
                queue="queue_hot",
            )
            self._wait_for_task(task_result)
            self.assertEqual(task_result.state, "SUCCESS")
            self.assertIn("skipped for deactivated source", task_result.result)
            self.assertEqual(
                self.clickhouse.query(
                    f"SELECT id, value FROM `{physical_table}`"
                ).result_rows,
                [(99, "old")],
            )
        finally:
            if physical_table:
                self.clickhouse.command(
                    f"DROP TABLE IF EXISTS `{physical_table}` SYNC"
                )
            if source is not None:
                source.delete()

    def test_control_plane_deactivation_during_source_read_cleans_staging(self):
        source_table_name = f"cancel_read_{uuid4().hex[:10]}"
        physical_table = None
        source = None
        source_connection = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        lock_connection = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        observer = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        task_result = None
        try:
            with source_connection.cursor() as cursor:
                cursor.execute(
                    psycopg2_sql.SQL("CREATE TABLE public.{} (id INTEGER, value TEXT)")
                    .format(psycopg2_sql.Identifier(source_table_name))
                )
                cursor.execute(
                    psycopg2_sql.SQL("INSERT INTO public.{} VALUES (1, 'new')")
                    .format(psycopg2_sql.Identifier(source_table_name))
                )
            source_connection.commit()

            source = SourceProfile.objects.create(
                name=f"cancel-read-{uuid4().hex[:12]}",
                db_type="postgresql",
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ["POSTGRES_PORT"]),
                database_name=os.environ["POSTGRES_DB"],
                username=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
            table = SourceTable.objects.create(
                source=source,
                schema_name="public",
                table_name=source_table_name,
            )
            SourceField.objects.create(
                table=table, field_name="id", data_type="INTEGER",
            )
            SourceField.objects.create(
                table=table, field_name="value", data_type="TEXT",
            )
            physical_table = source_table_clickhouse_name(table)
            self.clickhouse.command(
                f"CREATE TABLE `{physical_table}` "
                "(id Int32, value String) ENGINE = MergeTree ORDER BY tuple()"
            )
            self.clickhouse.insert(
                physical_table, [[99, "old"]], column_names=["id", "value"],
            )

            with lock_connection.cursor() as cursor:
                cursor.execute(
                    psycopg2_sql.SQL(
                        "LOCK TABLE public.{} IN ACCESS EXCLUSIVE MODE"
                    ).format(psycopg2_sql.Identifier(source_table_name))
                )

            task_result = ingest_source_to_clickhouse_task.apply_async(
                args=[source.pk],
                kwargs={"full_refresh": True},
                queue="queue_hot",
            )
            with observer.cursor() as cursor:
                try:
                    self._wait_until(
                        lambda: self._source_read_is_blocked(
                            cursor, source_table_name,
                        ),
                        timeout=20,
                        message=(
                            "Ingestion worker did not block on the held "
                            "source-table lock."
                        ),
                    )
                except AssertionError as exc:
                    self.fail(
                        f"{exc} task_state={task_result.state}, "
                        f"task_result={task_result.result!r}. "
                        f"Worker log:\n{self._worker_log()}"
                    )

            response = self._invoke_admin_post(cancel_pipeline_view, source.pk)
            self.assertEqual(response.status_code, 302)
            source.refresh_from_db()
            self.assertFalse(source.is_active)

            lock_connection.rollback()
            self._wait_for_task(task_result)
            source.refresh_from_db()
            self.assertEqual(
                source.last_sync_status,
                "CANCELLED",
                f"task_state={task_result.state}, "
                f"task_result={task_result.result!r}, "
                f"last_sync_error={source.last_sync_error!r}. "
                f"Worker log:\n{self._worker_log()}",
            )
            self.assertEqual(
                self.clickhouse.query(
                    f"SELECT id, value FROM `{physical_table}`"
                ).result_rows,
                [(99, "old")],
            )
            staging_tables = self.clickhouse.query(
                "SELECT name FROM system.tables "
                "WHERE database = currentDatabase() "
                "AND startsWith(name, {prefix:String})",
                parameters={"prefix": f"{physical_table}__staging_"},
            ).result_rows
            self.assertEqual(staging_tables, [])
        finally:
            lock_connection.rollback()
            if task_result is not None and not task_result.ready():
                try:
                    task_result.get(timeout=10, propagate=False)
                except Exception:
                    pass
            if physical_table:
                self.clickhouse.command(
                    f"DROP TABLE IF EXISTS `{physical_table}` SYNC"
                )
            if source is not None:
                source.delete()
            with source_connection.cursor() as cursor:
                cursor.execute(
                    psycopg2_sql.SQL("DROP TABLE IF EXISTS public.{}").format(
                        psycopg2_sql.Identifier(source_table_name)
                    )
                )
            source_connection.commit()
            observer.close()
            lock_connection.close()
            source_connection.close()

    def _source_read_is_blocked(self, cursor, table_name):
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_locks AS waiting
                JOIN pg_class AS relation ON relation.oid = waiting.relation
                JOIN pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE waiting.pid <> pg_backend_pid()
                  AND NOT waiting.granted
                  AND namespace.nspname = 'public'
                  AND relation.relname = %s
            )
            """,
            (table_name,),
        )
        return cursor.fetchone()[0]

    def test_control_plane_deactivation_during_delayed_staging_insert(self):
        source_table_name = f"cancel_insert_{uuid4().hex[:10]}"
        task_id = uuid4().hex
        physical_table = None
        source = None
        task_result = None
        source_connection = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        try:
            with source_connection.cursor() as cursor:
                cursor.execute(
                    psycopg2_sql.SQL("CREATE TABLE public.{} (id INTEGER, value TEXT)")
                    .format(psycopg2_sql.Identifier(source_table_name))
                )
                cursor.execute(
                    psycopg2_sql.SQL("INSERT INTO public.{} VALUES (1, 'new')")
                    .format(psycopg2_sql.Identifier(source_table_name))
                )
            source_connection.commit()
            source = SourceProfile.objects.create(
                name=f"cancel-insert-{uuid4().hex[:12]}",
                db_type="postgresql",
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ["POSTGRES_PORT"]),
                database_name=os.environ["POSTGRES_DB"],
                username=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
            table = SourceTable.objects.create(
                source=source, schema_name="public", table_name=source_table_name,
            )
            SourceField.objects.create(table=table, field_name="id", data_type="INTEGER")
            SourceField.objects.create(table=table, field_name="value", data_type="TEXT")
            physical_table = source_table_clickhouse_name(table)
            self.clickhouse.command(
                f"CREATE TABLE `{physical_table}` "
                "(id Int32, value String) ENGINE = MergeTree ORDER BY tuple()"
            )
            self.clickhouse.insert(
                physical_table, [[99, "old"]], column_names=["id", "value"],
            )

            self._configure_proxy("delay-insert", query_id=f"etl_task_{task_id}")
            task_result = ingest_source_to_clickhouse_task.apply_async(
                args=[source.pk], kwargs={"full_refresh": True},
                task_id=task_id, queue="queue_hot",
            )
            self._wait_for_proxy_block("insert", timeout=30)
            self.assertEqual(
                self._proxy_state()["blocked"]["query_id"], f"etl_task_{task_id}",
            )
            staging_tables = self.clickhouse.query(
                "SELECT name FROM system.tables WHERE database = currentDatabase() "
                "AND startsWith(name, {prefix:String})",
                parameters={"prefix": f"{physical_table}__staging_"},
            ).result_rows
            self.assertEqual(len(staging_tables), 1)

            response = self._invoke_admin_post(cancel_pipeline_view, source.pk)
            self.assertEqual(response.status_code, 302)
            source.refresh_from_db()
            self.assertFalse(source.is_active)
            self.assertFalse(task_result.ready())

            self._release_proxy()
            self._wait_for_task(task_result)
            source.refresh_from_db()
            self.assertEqual(task_result.state, "SUCCESS", self._worker_log())
            self.assertIn("Ingestion cancelled", task_result.result)
            self.assertEqual(source.last_sync_status, "CANCELLED")
            self.assertEqual(
                self.clickhouse.query(
                    f"SELECT id, value FROM `{physical_table}`"
                ).result_rows,
                [(99, "old")],
            )
            self.assertEqual(
                self.clickhouse.query(
                    "SELECT name FROM system.tables WHERE database = currentDatabase() "
                    "AND startsWith(name, {prefix:String})",
                    parameters={"prefix": f"{physical_table}__staging_"},
                ).result_rows,
                [],
            )
        finally:
            self._release_proxy()
            if task_result is not None and not task_result.ready():
                task_result.get(timeout=30, propagate=False)
            if physical_table:
                self.clickhouse.command(f"DROP TABLE IF EXISTS `{physical_table}` SYNC")
            if source is not None:
                source.delete()
            with source_connection.cursor() as cursor:
                cursor.execute(
                    psycopg2_sql.SQL("DROP TABLE IF EXISTS public.{}").format(
                        psycopg2_sql.Identifier(source_table_name)
                    )
                )
            source_connection.commit()
            source_connection.close()

    def test_worker_termination_stops_its_active_clickhouse_query(self):
        task_id = uuid4().hex
        query_id = f"etl_task_{task_id}"
        try:
            result = self._start_query(query_id)
            self._terminate(task_id)
            self._wait_for_query_to_stop(query_id)
            self.assertEqual(result.state, "REVOKED")
        finally:
            self._cleanup_query(query_id)

    def test_delayed_kill_keeps_query_active_until_kill_reaches_clickhouse(self):
        task_id = uuid4().hex
        query_id = f"etl_task_{task_id}"
        try:
            self._start_query(query_id)
            self._configure_proxy("delay-kill")
            self._terminate(task_id)
            self._wait_for_proxy_block("kill")
            self.assertTrue(self._query_is_active(query_id))

            self._release_proxy()
            self._wait_for_query_to_stop(query_id)
        finally:
            self._cleanup_query(query_id)

    def test_dropped_kill_response_is_logged_but_query_is_stopped(self):
        task_id = uuid4().hex
        query_id = f"etl_task_{task_id}"
        initial_dropped = self._proxy_state()["dropped_kills"]
        try:
            self._start_query(query_id)
            self._configure_proxy("drop-kill-response")
            self._terminate(task_id)
            self._wait_for_query_to_stop(query_id)
            self._wait_until(
                lambda: self._proxy_state()["dropped_kills"] > initial_dropped,
                timeout=10,
                message="The proxy did not drop the ClickHouse KILL response.",
            )
            worker_log = self._worker_log()
            self.assertIn("Could not cancel ClickHouse query", worker_log)
            self.assertIn("external work may still be active", worker_log)
        finally:
            self._cleanup_query(query_id)

    def test_delayed_query_can_start_after_termination_and_absent_query_kill(self):
        task_id = uuid4().hex
        query_id = f"etl_task_{task_id}"
        initial_forwarded = self._proxy_state()["forwarded_kills"]
        try:
            self._configure_proxy("delay-query", query_id=query_id)
            run_slow_cancellation_probe.apply_async(task_id=task_id)
            self._wait_for_proxy_block("query")
            self._terminate(task_id)
            self._wait_until(
                lambda: (
                    self._proxy_state()["forwarded_kills"] > initial_forwarded
                ),
                timeout=10,
                message="The termination handler did not send its query cancellation.",
            )
            self.assertFalse(self._query_is_active(query_id))

            self._release_proxy()
            self._wait_until(
                lambda: self._query_is_active(query_id),
                timeout=15,
                message="The delayed request did not execute after worker termination.",
            )
        finally:
            self._cleanup_query(query_id)


if __name__ == "__main__":
    unittest.main(verbosity=2)
