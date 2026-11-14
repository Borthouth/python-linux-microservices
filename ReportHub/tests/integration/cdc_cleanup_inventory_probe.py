"""Cleanup checks against a disposable, isolated Compose stack."""

import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from uuid import uuid4
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import clickhouse_connect
import django
import psycopg2
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from psycopg2 import sql

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import Client, override_settings
from django.urls import reverse

from app.core.celery_app import celery_app
from control_plane.models import (
    CdcResourceCleanup,
    CdcResourceCleanupItem,
    CdcTableRebuild,
    DbChoice,
    IngestionMode,
    SourceProfile,
    SourceTable,
)
from etl.cdc import (
    _clickhouse_table_uuid,
    _connect_request,
    build_debezium_connector_config,
)
from etl.cdc_cleanup import (
    CdcCleanupIneligible,
    _ensure_rebuild_eligible,
    _resource_specs,
    execute_cleanup,
    scan_cleanup_resources,
)
from reports.query_builder import quote_clickhouse_identifier, source_table_clickhouse_name


def start_interrupted_worker(test, connector_name, *, hold_delete=False):
    blocked = threading.Event()
    release = threading.Event()
    completed = threading.Event()
    deletion_attempted = threading.Event()
    target_path = f"/connectors/{connector_name}/config"

    class CleanupProxy(BaseHTTPRequestHandler):
        def do_GET(self):
            is_target = not hold_delete and self.path == target_path
            try:
                if is_target:
                    blocked.set()
                    if not release.wait(60):
                        self.send_error(504)
                        return
                try:
                    with urlopen(f"http://connect:8083{self.path}", timeout=10) as response:
                        status, body = response.status, response.read()
                except HTTPError as error:
                    status, body = error.code, error.read()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                try:
                    self.wfile.write(body)
                except BrokenPipeError:
                    pass
            finally:
                if is_target:
                    completed.set()

        def do_DELETE(self):
            deletion_attempted.set()
            if not hold_delete or self.path != f"/connectors/{connector_name}":
                self.send_error(403)
                return
            blocked.set()
            try:
                if not release.wait(60):
                    self.send_error(504)
                    return
                request = Request(f"http://connect:8083{self.path}", method="DELETE")
                try:
                    with urlopen(request, timeout=15) as response:
                        status, body = response.status, response.read()
                except HTTPError as error:
                    status, body = error.code, error.read()
                self.send_response(status)
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass
            finally:
                completed.set()

        def log_message(self, format, *args):
            pass

    proxy = ThreadingHTTPServer(("127.0.0.1", 0), CleanupProxy)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()

    def close_proxy():
        release.set()
        proxy.shutdown()
        proxy.server_close()
        thread.join(timeout=10)

    test.addCleanup(close_proxy)
    hostname = f"cleanup-probe-{uuid4().hex}@worker"
    environment = os.environ.copy()
    environment.update({
        "KAFKA_CONNECT_URL": f"http://127.0.0.1:{proxy.server_port}",
        "C_FORCE_ROOT": "true",
    })
    worker_log = tempfile.TemporaryFile(mode="w+t")
    test.addCleanup(worker_log.close)
    worker = subprocess.Popen(
        [
            sys.executable, "-m", "celery", "-A", "config", "worker",
            "--pool=solo", "--concurrency=1", "--queues=queue_maintenance",
            "--without-gossip", "--without-mingle", "--without-heartbeat",
            "--loglevel=WARNING", f"--hostname={hostname}",
        ],
        env=environment, stdout=worker_log, stderr=subprocess.STDOUT,
    )

    def stop_worker():
        if worker.poll() is None:
            worker.terminate()
            try:
                worker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=10)
                response = client.post(reverse("control_plane:source_delete", args=[source.pk]))
                self.assertEqual(response.status_code, 302)
                self.assertTrue(SourceProfile.objects.filter(pk=source.pk).exists())

    test.addCleanup(stop_worker)
    def worker_output():
        worker_log.flush()
        worker_log.seek(0)
        return worker_log.read()

    deadline = time.monotonic() + 45
    while True:
        if worker.poll() is not None:
            test.fail(f"The isolated Celery worker exited with status {worker.returncode}:\n{worker_output()}")
        if celery_app.control.ping(destination=[hostname], timeout=1):
            break
        if time.monotonic() >= deadline:
            test.fail(f"The isolated Celery worker did not become ready:\n{worker_output()}")
        time.sleep(0.5)
    return worker, blocked, release, completed, deletion_attempted, worker_output


class CdcCleanupInventoryProbe(unittest.TestCase):
    def test_inventory_and_eligibility(self):
        # Refuse to run if the explicit disposable Compose endpoints are changed.
        self.assertEqual(settings.DATABASES["default"]["HOST"], "postgres")
        self.assertEqual(settings.KAFKA_CONNECT_URL, "http://connect:8083")
        self.assertEqual(settings.KAFKA_BOOTSTRAP_SERVERS, "kafka:9092")
        self.assertEqual(settings.CLICKHOUSE_HOST, "clickhouse")

        call_command("migrate", interactive=False, verbosity=0)
        deadline = time.monotonic() + 180
        while True:
            try:
                _connect_request("GET", "/connectors")
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(2)

        unique = uuid4().hex
        admin = get_user_model().objects.create_user(username=f"cleanup-{unique}", is_staff=True)
        source = SourceProfile.objects.create(
            name=f"cleanup-{unique}",
            db_type=DbChoice.POSTGRESQL,
            host="postgres",
            port=5432,
            database_name="cleanup_test",
            username="cleanup_test",
            password=os.environ["POSTGRES_PASSWORD"],
            ingestion_mode=IngestionMode.CDC,
        )
        table = SourceTable.objects.create(
            source=source,
            schema_name="public",
            table_name=f"events_{unique}",
            primary_key_fields=["id"],
            cdc_managed=True,
        )
        prefix = f"cleanup-{unique}"
        rebuild = CdcTableRebuild.objects.create(
            source_table=table,
            connector_name=prefix,
            topic_prefix=prefix,
            notification_topic=f"{prefix}.notifications",
            consumer_group=f"{prefix}-group",
            staging_table=f"cleanup_stage_{unique}",
            status=CdcTableRebuild.Status.FAILED,
        )
        specs = _resource_specs(rebuild)
        names = {resource_type: name for resource_type, name in specs}
        slot = names[CdcResourceCleanupItem.ResourceType.POSTGRES_SLOT]
        publication = names[CdcResourceCleanupItem.ResourceType.POSTGRES_PUBLICATION]
        topics = [
            name for resource_type, name in specs
            if resource_type == CdcResourceCleanupItem.ResourceType.KAFKA_TOPIC
        ]

        connection = psycopg2.connect(
            host="postgres", dbname="cleanup_test", user="cleanup_test",
            password=os.environ["POSTGRES_PASSWORD"],
        )
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE TABLE {} (id integer PRIMARY KEY)").format(
                    sql.Identifier(table.table_name)
                ))
                cursor.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
                    sql.Identifier(publication), sql.Identifier(table.table_name)
                ))
                cursor.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
        finally:
            connection.close()

        ch = clickhouse_connect.get_client(
            host="clickhouse", username="cleanup_test",
            password=os.environ["CLICKHOUSE_PASSWORD"],
        )
        self.addCleanup(ch.close)
        active_name = source_table_clickhouse_name(table)
        for name in (active_name, rebuild.staging_table):
            ch.command(
                f"CREATE TABLE {quote_clickhouse_identifier(name)} "
                "(id Int64) ENGINE = MergeTree ORDER BY tuple()"
            )
        rebuild.staging_uuid = _clickhouse_table_uuid(ch, rebuild.staging_table)
        rebuild.save(update_fields=["staging_uuid"])
        active_uuid = _clickhouse_table_uuid(ch, active_name)

        kafka = KafkaAdminClient(bootstrap_servers="kafka:9092", request_timeout_ms=30000)
        self.addCleanup(kafka.close)
        kafka.create_topics([NewTopic(name=topics[0], num_partitions=1, replication_factor=1)])
        group_consumer = KafkaConsumer(
            bootstrap_servers="kafka:9092", group_id=rebuild.consumer_group,
            auto_offset_reset="earliest",
        )
        self.addCleanup(group_consumer.close)
        group_consumer.subscribe([topics[0]])
        deadline = time.monotonic() + 30
        while True:
            group_consumer.poll(timeout_ms=1000)
            if hasattr(kafka, "list_groups"):
                groups = [item["group_id"] for item in kafka.list_groups()]
            else:
                groups = [item[0] for item in kafka.list_consumer_groups()]
            if rebuild.consumer_group in groups:
                break
            if time.monotonic() >= deadline:
                self.fail("The isolated Kafka consumer group did not appear.")
        config = build_debezium_connector_config(
            source, [table], connector_name=rebuild.connector_name,
            topic_prefix=rebuild.topic_prefix, notification_topic=rebuild.notification_topic,
        )
        _connect_request("POST", "/connectors", {
            "name": rebuild.connector_name, "config": config, "initial_state": "PAUSED",
        })
        deadline = time.monotonic() + 30
        while True:
            status = _connect_request("GET", f"/connectors/{rebuild.connector_name}/status")
            if status and status["connector"]["state"] == "PAUSED":
                break
            if time.monotonic() >= deadline:
                self.fail("The isolated connector did not reach the paused state.")
            time.sleep(1)
        connector_config = _connect_request("GET", f"/connectors/{rebuild.connector_name}/config")

        table.cdc_connector_name = rebuild.connector_name
        table.save(update_fields=["cdc_connector_name"])
        with self.assertRaises(CdcCleanupIneligible):
            _ensure_rebuild_eligible(rebuild)
        table.cdc_connector_name = ""
        table.save(update_fields=["cdc_connector_name"])
        unfinished = CdcTableRebuild.objects.create(
            source_table=table,
            connector_name=f"{prefix}-unfinished",
            topic_prefix=f"{prefix}-unfinished",
            notification_topic=f"{prefix}-unfinished.notifications",
            consumer_group=f"{prefix}-unfinished-group",
            staging_table=f"cleanup_unfinished_{unique}",
            status=CdcTableRebuild.Status.SNAPSHOTTING,
        )
        with self.assertRaises(CdcCleanupIneligible):
            _ensure_rebuild_eligible(rebuild)
        unfinished.status = CdcTableRebuild.Status.FAILED
        unfinished.save(update_fields=["status"])

        def make_inventory():
            cleanup = CdcResourceCleanup.objects.create(
                rebuild=rebuild, status=CdcResourceCleanup.Status.SCANNING,
                created_by=admin, updated_by=admin,
            )
            for resource_type, name in specs:
                CdcResourceCleanupItem.objects.create(
                    cleanup=cleanup, resource_type=resource_type, resource_name=name,
                    created_by=admin, updated_by=admin,
                )
            return cleanup

        cleanup = make_inventory()
        scan_cleanup_resources(cleanup.pk)
        cleanup.refresh_from_db()
        items = {(item.resource_type, item.resource_name): item for item in cleanup.items.all()}
        self.assertEqual(cleanup.status, CdcResourceCleanup.Status.AWAITING_CONFIRMATION)
        expected_present = {
            (CdcResourceCleanupItem.ResourceType.CONNECTOR, rebuild.connector_name),
            (CdcResourceCleanupItem.ResourceType.POSTGRES_SLOT, slot),
            (CdcResourceCleanupItem.ResourceType.POSTGRES_PUBLICATION, publication),
            (CdcResourceCleanupItem.ResourceType.KAFKA_GROUP, rebuild.consumer_group),
            (CdcResourceCleanupItem.ResourceType.KAFKA_TOPIC, topics[0]),
            (CdcResourceCleanupItem.ResourceType.CLICKHOUSE_TABLE, rebuild.staging_table),
        }
        self.assertEqual(
            {key for key, item in items.items() if item.exists_at_scan}, expected_present
        )
        self.assertTrue(all(not item.scan_error and item.attempts == 0 for item in items.values()))
        self.assertTrue(all(item.status == CdcResourceCleanupItem.Status.PENDING for item in items.values()))

        self.assertEqual(
            _connect_request("GET", f"/connectors/{rebuild.connector_name}/config"),
            connector_config,
        )
        with psycopg2.connect(
            host="postgres", dbname="cleanup_test", user="cleanup_test",
            password=os.environ["POSTGRES_PASSWORD"],
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM pg_replication_slots WHERE slot_name = %s", (slot,))
                self.assertEqual(cursor.fetchone()[0], 1)
                cursor.execute("SELECT count(*) FROM pg_publication WHERE pubname = %s", (publication,))
                self.assertEqual(cursor.fetchone()[0], 1)
        self.assertEqual(_clickhouse_table_uuid(ch, active_name), active_uuid)
        self.assertEqual(_clickhouse_table_uuid(ch, rebuild.staging_table), rebuild.staging_uuid)
        consumer = KafkaConsumer(bootstrap_servers="kafka:9092", group_id=None)
        try:
            self.assertIn(topics[0], consumer.topics())
        finally:
            consumer.close()
        if hasattr(kafka, "list_groups"):
            groups = [item["group_id"] for item in kafka.list_groups()]
        else:
            groups = [item[0] for item in kafka.list_consumer_groups()]
        self.assertIn(rebuild.consumer_group, groups)

        cleanup.status = CdcResourceCleanup.Status.FAILED
        cleanup.save(update_fields=["status"])
        rebuild.staging_uuid = str(uuid4())
        rebuild.save(update_fields=["staging_uuid"])
        failed = make_inventory()
        scan_cleanup_resources(failed.pk)
        failed.refresh_from_db()
        item = failed.items.get(resource_type=CdcResourceCleanupItem.ResourceType.CLICKHOUSE_TABLE)
        self.assertEqual(failed.status, CdcResourceCleanup.Status.FAILED)
        self.assertIsNone(item.exists_at_scan)
        self.assertIn("UUID does not match", item.scan_error)
        self.assertEqual(item.attempts, 0)
        self.assertEqual(_clickhouse_table_uuid(ch, active_name), active_uuid)
        self.assertNotEqual(_clickhouse_table_uuid(ch, rebuild.staging_table), "")

        scenario = os.environ.get("CLEANUP_TEST_DELETE", "inventory")
        if scenario == "inventory":
            return
        self.assertIn(scenario, {
            "delete", "active-group", "postgres-permission", "clickhouse-permission",
            "worker-interrupted", "worker-delete-interrupted",
        })

        rebuild.staging_uuid = _clickhouse_table_uuid(ch, rebuild.staging_table)
        rebuild.save(update_fields=["staging_uuid"])
        if scenario in {"delete", "postgres-permission", "clickhouse-permission"}:
            group_consumer.close()
        confirmed = make_inventory()
        scan_cleanup_resources(confirmed.pk)
        confirmed.refresh_from_db()
        self.assertEqual(confirmed.status, CdcResourceCleanup.Status.AWAITING_CONFIRMATION)
        if scenario in {"worker-interrupted", "worker-delete-interrupted"}:
            worker, blocked, release, completed, deletion_attempted, worker_output = start_interrupted_worker(
                self, rebuild.connector_name, hold_delete=scenario == "worker-delete-interrupted",
            )
        with override_settings(
            ALLOWED_HOSTS=["testserver"],
            SESSION_ENGINE="django.contrib.sessions.backends.db",
        ):
            client = Client()
            client.force_login(admin)
            if scenario in {"worker-interrupted", "worker-delete-interrupted"}:
                response = client.post(
                    reverse("control_plane:cdc_cleanup_confirm", args=[confirmed.pk]),
                    {"confirm_cleanup": "yes"},
                )
            else:
                with patch("control_plane.views.cdc_cleanup.cleanup_cdc_resources_task.apply_async") as queue:
                    response = client.post(
                        reverse("control_plane:cdc_cleanup_confirm", args=[confirmed.pk]),
                        {"confirm_cleanup": "yes"},
                    )
                queue.assert_called_once()
            self.assertEqual(response.status_code, 302)
            if scenario in {"worker-interrupted", "worker-delete-interrupted"}:
                if not blocked.wait(45):
                    self.fail(f"The worker did not reach the held request:\n{worker_output()}")
                confirmed.refresh_from_db()
                self.assertEqual(confirmed.status, CdcResourceCleanup.Status.RUNNING)
                item = confirmed.items.get(resource_type=CdcResourceCleanupItem.ResourceType.CONNECTOR)
                self.assertEqual(item.status, CdcResourceCleanupItem.Status.RUNNING)
                worker.kill()
                worker.wait(timeout=10)
                if scenario == "worker-interrupted":
                    release.set()
                    self.assertTrue(completed.wait(15), "The read-only request did not finish.")
                    self.assertFalse(deletion_attempted.is_set())
                else:
                    self.assertTrue(deletion_attempted.is_set())
                    self.assertFalse(completed.is_set())
                recover_url = reverse("control_plane:cdc_cleanup_recover", args=[confirmed.pk])
                self.assertEqual(client.post(recover_url).status_code, 302)
                confirmed.refresh_from_db()
                self.assertEqual(confirmed.status, CdcResourceCleanup.Status.RUNNING)
                response = client.post(recover_url, {"acknowledge_uncertain": "yes"})
                self.assertEqual(response.status_code, 302)
                confirmed.refresh_from_db()
                item.refresh_from_db()
                self.assertEqual(confirmed.status, CdcResourceCleanup.Status.FAILED)
                self.assertTrue(confirmed.external_operation_uncertain)
                self.assertIn("may still finish", confirmed.last_error)
                self.assertEqual(item.status, CdcResourceCleanupItem.Status.FAILED)
                response = client.post(reverse("control_plane:source_delete", args=[source.pk]))
                self.assertEqual(response.status_code, 302)
                self.assertTrue(SourceProfile.objects.filter(pk=source.pk).exists())
                with patch("control_plane.views.cdc_cleanup.scan_cdc_resources_task.apply_async") as queue:
                    response = client.post(
                        reverse("control_plane:cdc_cleanup_scan", args=[rebuild.pk]),
                    )
                self.assertEqual(response.status_code, 302)
                queue.assert_not_called()
                self.assertEqual(
                    CdcResourceCleanup.objects.filter(rebuild=rebuild).latest("pk").pk,
                    confirmed.pk,
                )
                response = client.post(
                    reverse("control_plane:cdc_cleanup_override", args=[confirmed.pk]),
                    {
                        "source_name": source.name,
                        "reason": "Isolated worker was terminated for this test.",
                        "accept_risk": "yes",
                    },
                )
                self.assertEqual(response.status_code, 302)
                confirmed.refresh_from_db()
                self.assertTrue(confirmed.external_operation_uncertain)
                self.assertIsNotNone(confirmed.uncertainty_override_at)
                self.assertEqual(confirmed.history.first().history_user, admin)
                with patch("control_plane.views.cdc_cleanup.scan_cdc_resources_task.apply_async") as queue:
                    client.post(reverse("control_plane:cdc_cleanup_scan", args=[rebuild.pk]))
                queue.assert_called_once()
                fresh = CdcResourceCleanup.objects.filter(rebuild=rebuild).latest("pk")
                scan_cleanup_resources(fresh.pk)
                fresh.refresh_from_db()
                self.assertEqual(fresh.status, CdcResourceCleanup.Status.AWAITING_CONFIRMATION)
                self.assertEqual(
                    _connect_request("GET", f"/connectors/{rebuild.connector_name}/config"),
                    connector_config,
                )
                if scenario == "worker-delete-interrupted":
                    release.set()
                    self.assertTrue(completed.wait(20), "The delayed deletion did not finish.")
                    deadline = time.monotonic() + 15
                    while _connect_request("GET", f"/connectors/{rebuild.connector_name}/config") is not None:
                        if time.monotonic() >= deadline:
                            self.fail("The delayed connector deletion did not take effect.")
                        time.sleep(0.5)
                    self.assertTrue(
                        fresh.items.get(
                            resource_type=CdcResourceCleanupItem.ResourceType.CONNECTOR,
                        ).exists_at_scan
                    )
                    print("CDC_CLEANUP_DELAYED_DELETE_TESTS_PASSED")
                self.assertEqual(_clickhouse_table_uuid(ch, active_name), active_uuid)
                self.assertEqual(_clickhouse_table_uuid(ch, rebuild.staging_table), rebuild.staging_uuid)
                self.assertIn(topics[0], kafka.list_topics())
                print("CDC_CLEANUP_WORKER_INTERRUPTION_TESTS_PASSED")
                return
        confirmed.refresh_from_db()
        self.assertEqual(confirmed.status, CdcResourceCleanup.Status.QUEUED)
        if scenario == "postgres-permission":
            connection = psycopg2.connect(
                host="postgres", dbname="cleanup_test", user="cleanup_test",
                password=os.environ["POSTGRES_PASSWORD"],
            )
            try:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute(
                        "CREATE ROLE cleanup_unprivileged LOGIN NOSUPERUSER NOREPLICATION PASSWORD %s",
                        (os.environ["POSTGRES_PASSWORD"],),
                    )
            finally:
                connection.close()
            source.username = "cleanup_unprivileged"
            source.save(update_fields=["username"])
        if scenario == "clickhouse-permission":
            ch.command("CREATE USER cleanup_unprivileged IDENTIFIED WITH plaintext_password BY {password:String}", parameters={
                "password": os.environ["CLICKHOUSE_PASSWORD"],
            })
            ch.command("GRANT SHOW TABLES ON default.* TO cleanup_unprivileged")
            with override_settings(CLICKHOUSE_USER="cleanup_unprivileged"):
                execute_cleanup(confirmed.pk)
        else:
            execute_cleanup(confirmed.pk)
        confirmed.refresh_from_db()
        outcomes = list(confirmed.items.order_by("pk"))
        if scenario == "clickhouse-permission":
            self.assertEqual(confirmed.status, CdcResourceCleanup.Status.FAILED)
            self.assertTrue(confirmed.external_operation_uncertain)
            table_item = confirmed.items.get(resource_type=CdcResourceCleanupItem.ResourceType.CLICKHOUSE_TABLE)
            self.assertEqual(table_item.status, CdcResourceCleanupItem.Status.FAILED)
            self.assertIn("ACCESS_DENIED", table_item.last_error.upper())
            self.assertIn("DROP TABLE", table_item.last_error.upper())
            self.assertNotIn(os.environ["CLICKHOUSE_PASSWORD"], table_item.last_error)
            self.assertEqual(_clickhouse_table_uuid(ch, active_name), active_uuid)
            self.assertEqual(_clickhouse_table_uuid(ch, rebuild.staging_table), rebuild.staging_uuid)
            print("CDC_CLEANUP_CLICKHOUSE_PERMISSION_TESTS_PASSED")
            return
        if scenario == "postgres-permission":
            self.assertEqual(confirmed.status, CdcResourceCleanup.Status.FAILED)
            slot_item = confirmed.items.get(resource_type=CdcResourceCleanupItem.ResourceType.POSTGRES_SLOT)
            self.assertEqual(slot_item.status, CdcResourceCleanupItem.Status.FAILED)
            self.assertIn("InsufficientPrivilege", slot_item.last_error)
            self.assertNotIn(os.environ["POSTGRES_PASSWORD"], slot_item.last_error)
            self.assertEqual(slot_item.attempts, 1)
            self.assertEqual(
                confirmed.items.get(resource_type=CdcResourceCleanupItem.ResourceType.POSTGRES_PUBLICATION).status,
                CdcResourceCleanupItem.Status.PENDING,
            )
            with psycopg2.connect(
                host="postgres", dbname="cleanup_test", user="cleanup_test",
                password=os.environ["POSTGRES_PASSWORD"],
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT count(*) FROM pg_replication_slots WHERE slot_name = %s", (slot,))
                    self.assertEqual(cursor.fetchone()[0], 1)
                    cursor.execute("SELECT count(*) FROM pg_publication WHERE pubname = %s", (publication,))
                    self.assertEqual(cursor.fetchone()[0], 1)
            self.assertIn(topics[0], kafka.list_topics())
            self.assertEqual(_clickhouse_table_uuid(ch, rebuild.staging_table), rebuild.staging_uuid)
            print("CDC_CLEANUP_PERMISSION_TESTS_PASSED")
            return
        if scenario == "active-group":
            self.assertEqual(confirmed.status, CdcResourceCleanup.Status.FAILED)
            group_item = confirmed.items.get(resource_type=CdcResourceCleanupItem.ResourceType.KAFKA_GROUP)
            self.assertEqual(group_item.status, CdcResourceCleanupItem.Status.FAILED)
            self.assertIn("NonEmptyGroup", group_item.last_error)
            self.assertEqual(group_item.attempts, 1)
            for resource_type in (
                CdcResourceCleanupItem.ResourceType.CONNECTOR,
                CdcResourceCleanupItem.ResourceType.POSTGRES_SLOT,
                CdcResourceCleanupItem.ResourceType.POSTGRES_PUBLICATION,
            ):
                completed = confirmed.items.get(resource_type=resource_type)
                self.assertEqual(completed.status, CdcResourceCleanupItem.Status.COMPLETED)
                self.assertEqual(completed.outcome, "Deleted")
            self.assertEqual(
                confirmed.items.get(
                    resource_type=CdcResourceCleanupItem.ResourceType.KAFKA_TOPIC,
                    resource_name=topics[0],
                ).status,
                CdcResourceCleanupItem.Status.PENDING,
            )
            self.assertIn(topics[0], kafka.list_topics())
            self.assertEqual(_clickhouse_table_uuid(ch, active_name), active_uuid)
            self.assertEqual(_clickhouse_table_uuid(ch, rebuild.staging_table), rebuild.staging_uuid)
            print("CDC_CLEANUP_ACTIVE_GROUP_TESTS_PASSED")
            return
        self.assertEqual(confirmed.status, CdcResourceCleanup.Status.COMPLETED, confirmed.last_error)
        self.assertTrue(all(item.status == CdcResourceCleanupItem.Status.COMPLETED for item in outcomes))
        self.assertTrue(all(item.outcome == "Deleted" for item in outcomes if item.exists_at_scan))
        self.assertTrue(all(
            item.outcome == "Not present during inventory"
            for item in outcomes if not item.exists_at_scan
        ))
        self.assertIsNone(_connect_request("GET", f"/connectors/{rebuild.connector_name}/config"))
        with psycopg2.connect(
            host="postgres", dbname="cleanup_test", user="cleanup_test",
            password=os.environ["POSTGRES_PASSWORD"],
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM pg_replication_slots WHERE slot_name = %s", (slot,))
                self.assertEqual(cursor.fetchone()[0], 0)
                cursor.execute("SELECT count(*) FROM pg_publication WHERE pubname = %s", (publication,))
                self.assertEqual(cursor.fetchone()[0], 0)
        self.assertEqual(_clickhouse_table_uuid(ch, active_name), active_uuid)
        self.assertEqual(_clickhouse_table_uuid(ch, rebuild.staging_table), "")
        self.assertNotIn(topics[0], kafka.list_topics())
        if hasattr(kafka, "list_groups"):
            groups = [item["group_id"] for item in kafka.list_groups()]
        else:
            groups = [item[0] for item in kafka.list_consumer_groups()]
        self.assertNotIn(rebuild.consumer_group, groups)
        print("CDC_CLEANUP_DELETION_TESTS_PASSED")


if __name__ == "__main__":
    result = unittest.main(exit=False)
    if not result.result.wasSuccessful():
        raise SystemExit(1)
    print("CDC_CLEANUP_INVENTORY_TESTS_PASSED")
