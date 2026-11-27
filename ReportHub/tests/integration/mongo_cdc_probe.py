"""Capture MongoDB Debezium events in a disposable, isolated stack."""

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import django
import clickhouse_connect
from bson import Decimal128, ObjectId, json_util
from django.core.management import call_command
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


django.setup()

from app.core.celery_app import celery_app
from control_plane.models import DbChoice, IngestionMode, SourceField, SourceProfile, SourceTable
from etl.cdc import (
    _consume_source_cdc_group, _decode_mongo_cdc_event, _mongo_record_from_message, _topic_name,
    build_debezium_connector_config, check_mongodb_cdc_readiness, check_mongodb_replica_set,
    consume_cdc_table_rebuild_batch,
)
from reports.query_builder import quote_clickhouse_identifier, source_table_clickhouse_name
from etl.tasks import ingest_source_to_clickhouse_task


@celery_app.task(bind=True, name="tests.integration.mongo_cdc_probe.consume_disposable_mongo_events")
def consume_disposable_mongo_events(self, source_id, table_id, group, crash_before_commit=False):
    profile = SourceProfile.objects.get(pk=source_id)
    table = SourceTable.objects.get(pk=table_id, source=profile)
    warehouse = clickhouse_connect.get_client(
        host="clickhouse", port=8123, username="mongo_cdc_test",
        password=os.environ["MONGO_CDC_TEST_PASSWORD"],
    )
    try:
        if crash_before_commit:
            class CrashBeforeCommit(KafkaConsumer):
                def commit(self, *args, **kwargs):
                    os._exit(73)

            with patch("etl.cdc.KafkaConsumer", CrashBeforeCommit):
                count = _consume_source_cdc_group(profile, warehouse, [table], 100, group)
        else:
            count = _consume_source_cdc_group(profile, warehouse, [table], 100, group)
        return {"count": count, "worker": self.request.hostname}
    finally:
        warehouse.close()


def wait_for(description, operation, timeout=120):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = operation()
            if result:
                return result
        except (ConnectionFailure, URLError) as error:
            last_error = error
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for {description}: {last_error}")


def request_connect(path, payload=None, *, method=None):
    data = json.dumps(payload).encode() if payload is not None else None
    request = Request(
        f"http://connect:8083{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            body = response.read()
            return json.loads(body) if body else None
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        detail = detail.replace(os.environ["MONGO_CDC_TEST_PASSWORD"], "[redacted]")
        raise RuntimeError(f"Kafka Connect HTTP {error.code}: {detail}") from error


def next_event(consumer, expected_op, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for records in consumer.poll(timeout_ms=1000).values():
            for record in records:
                if record.value and record.value.get("op") == expected_op:
                    return record
    status = request_connect("/connectors/reporthub-source-1/status")
    raise TimeoutError(f"Missing MongoDB {expected_op} event; connector status: {status}")


def main():
    password = os.environ["MONGO_CDC_TEST_PASSWORD"]
    client = MongoClient(
        "mongo", 27017, username="mongo_test", password=password,
        authSource="admin", directConnection=True, serverSelectionTimeoutMS=2000,
    )
    wait_for("MongoDB", lambda: client.admin.command("ping"))
    client.admin.command({
        "replSetInitiate": {
            "_id": "rs0",
            "members": [{"_id": 0, "host": "mongo:27017"}],
        },
    })
    wait_for("MongoDB primary", lambda: client.admin.command("hello").get("isWritablePrimary"))
    collection = client["app_analytics"]["documents"]
    snapshot_id = ObjectId()
    collection.insert_one({
        "_id": snapshot_id,
        "value": 1,
        "nested": {"items": [1, "two"]},
        "created": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "amount": Decimal128("12.34"),
    })
    call_command("migrate", verbosity=0, interactive=False)
    uri = f"mongodb://mongo_test:{password}@mongo:27017/?authSource=admin&replicaSet=rs0"
    profile = SourceProfile.objects.create(
        name="Isolated MongoDB", db_type=DbChoice.MONGODB, host="mongo", port=27017,
        username="mongo_test", password=password, database_name="app_analytics",
        mongodb_connection_string=uri,
    )
    profile.refresh_from_db()
    table = SourceTable.objects.create(
        source=profile,
        schema_name="app_analytics", table_name="documents",
        primary_key_fields=["_id"], cdc_topic_prefix="",
    )
    field_types = {"_id": "ObjectId", "value": "Int64", "nested": "JSON", "created": "DateTime"}
    for field_name, data_type in field_types.items():
        SourceField.objects.create(table=table, field_name=field_name, data_type=data_type)
    SourceField.objects.create(
        table=table, field_name="amount", data_type="Decimal", is_selected=False,
    )
    field_types["amount"] = "Decimal"
    readiness = check_mongodb_cdc_readiness(profile)
    assert readiness.topology.status == "ok"
    assert readiness.version.status in {"ok", "untested"}
    assert readiness.change_stream.status == "ok"
    assert readiness.snapshot_access.status == "ok"
    client.admin.command(
        "createUser", "restricted_probe", pwd=password,
        roles=[{"role": "read", "db": "app_analytics"}],
    )
    profile.mongodb_connection_string = (
        f"mongodb://restricted_probe:{password}@mongo:27017/?authSource=admin&replicaSet=rs0"
    )
    limited = check_mongodb_cdc_readiness(profile)
    assert limited.topology.status == "ok"
    assert limited.change_stream.status == "denied"
    assert limited.snapshot_access.status == "ok"
    client.admin.command("createUser", "no_read_probe", pwd=password, roles=[])
    profile.mongodb_connection_string = (
        f"mongodb://no_read_probe:{password}@mongo:27017/?authSource=admin&replicaSet=rs0"
    )
    no_read = check_mongodb_cdc_readiness(profile)
    assert no_read.snapshot_access.status == "denied"
    missing = SourceTable.objects.create(
        source=profile, schema_name="app_analytics", table_name="missing",
        primary_key_fields=["_id"],
    )
    profile.refresh_from_db()
    assert check_mongodb_cdc_readiness(profile).snapshot_access.status == "unknown"
    missing.delete()
    profile.mongodb_connection_string = (
        f"mongodb://mongo_test:{password}@mongo:1/?authSource=admin&replicaSet=rs0"
    )
    unreachable = check_mongodb_cdc_readiness(profile, timeout_ms=500)
    assert unreachable.topology.status == "unreachable"
    assert unreachable.version.status == unreachable.change_stream.status == "unknown"
    standalone = MongoClient(
        "mongo-standalone", 27017, username="mongo_test", password=password,
        authSource="admin", directConnection=True, serverSelectionTimeoutMS=2000,
    )
    try:
        wait_for("standalone MongoDB", lambda: standalone.admin.command("ping"))
        profile.mongodb_connection_string = (
            f"mongodb://mongo_test:{password}@mongo-standalone:27017/?authSource=admin"
        )
        standalone_result = check_mongodb_cdc_readiness(profile)
        assert standalone_result.topology.status == "not_replica_set"
        assert standalone_result.change_stream.status == "unknown"
    finally:
        standalone.close()
    profile.refresh_from_db()
    assert check_mongodb_replica_set(profile).status == "ok"
    profile.mongodb_connection_string = uri.replace("replicaSet=rs0", "replicaSet=wrong")
    assert check_mongodb_replica_set(profile).status == "replica_set_name_mismatch"
    profile.mongodb_connection_string = uri.replace("&replicaSet=rs0", "")
    assert check_mongodb_replica_set(profile).status == "missing_replica_set_name"
    profile.refresh_from_db()
    config = build_debezium_connector_config(profile, [table])
    assert config["mongodb.connection.string"] == uri
    wait_for("Kafka Connect", lambda: request_connect("/connectors") is not None)
    request_connect("/connectors", {"name": "reporthub-source-1", "config": config})
    consumer = KafkaConsumer(
        _topic_name(profile, table), bootstrap_servers=["kafka:9092"],
        auto_offset_reset="earliest", enable_auto_commit=False,
        key_deserializer=lambda value: json.loads(value) if value else None,
        value_deserializer=lambda value: json.loads(value) if value else None,
    )
    try:
        snapshot = next_event(consumer, "r")
        inserted_id = str(snapshot_id)
        collection.insert_one({"_id": inserted_id, "value": 2})
        inserted = next_event(consumer, "c")
        collection.update_one({"_id": snapshot_id}, {"$set": {"value": 3}})
        updated = next_event(consumer, "u")
        collection.delete_one({"_id": inserted_id})
        deleted = next_event(consumer, "d")

        snapshot_document = json_util.loads(snapshot.value["after"])
        inserted_document = json_util.loads(inserted.value["after"])
        updated_document = json_util.loads(updated.value["after"])
        assert json_util.loads(snapshot.key["id"]) == snapshot_id
        assert snapshot_document["_id"] == snapshot_id
        assert snapshot_document["nested"] == {"items": [1, "two"]}
        assert snapshot_document["amount"] == Decimal128("12.34")
        assert json_util.loads(inserted.key["id"]) == inserted_id
        assert inserted_document == {"_id": inserted_id, "value": 2}
        assert json_util.loads(updated.key["id"]) == snapshot_id
        assert updated_document["_id"] == snapshot_id
        assert updated_document["value"] == 3
        assert json_util.loads(deleted.key["id"]) == inserted_id
        assert deleted.value["before"] is None and deleted.value["after"] is None
        assert collection.find_one({"_id": snapshot_id})["value"] == 3
        decoded_snapshot = _decode_mongo_cdc_event(snapshot)
        decoded_insert = _decode_mongo_cdc_event(inserted)
        decoded_update = _decode_mongo_cdc_event(updated)
        decoded_delete = _decode_mongo_cdc_event(deleted)
        assert decoded_snapshot[1] == decoded_update[1]
        assert decoded_insert[1] == decoded_delete[1]
        assert decoded_snapshot[1] != decoded_insert[1]
        assert decoded_snapshot[2]["amount"] == Decimal128("12.34")
        assert decoded_update[2]["value"] == 3
        assert decoded_delete[2] is None
        fields = ["_id", "value", "nested", "created"]
        snapshot_row = _mongo_record_from_message(snapshot, table, fields, field_types)
        insert_row = _mongo_record_from_message(inserted, table, fields, field_types)
        update_row = _mongo_record_from_message(updated, table, fields, field_types)
        delete_row = _mongo_record_from_message(deleted, table, fields, field_types)
        assert snapshot_row["_id"] == update_row["_id"]
        assert insert_row["_id"] == delete_row["_id"] != snapshot_row["_id"]
        assert snapshot_row["nested"] == '{"items":[{"$numberInt":"1"},"two"]}'
        assert update_row["value"] == 3
        assert delete_row["_cdc_deleted"] == 1
        warehouse = clickhouse_connect.get_client(
            host="clickhouse", port=8123, username="mongo_cdc_test", password=password,
        )
        try:
            class FailingInsert:
                def __getattr__(self, name):
                    return getattr(warehouse, name)

                def insert(self, *args, **kwargs):
                    raise RuntimeError("Simulated ClickHouse write failure")

            group = "reporthub-mongo-probe-consumer"

            def expect_write_failure():
                try:
                    _consume_source_cdc_group(profile, FailingInsert(), [table], 100, group)
                except RuntimeError as error:
                    if str(error) != "Simulated ClickHouse write failure":
                        raise
                    return True
                return False

            wait_for("simulated ClickHouse write failure", expect_write_failure)
            offset_reader = KafkaConsumer(bootstrap_servers=["kafka:9092"], group_id=group)
            try:
                partition = TopicPartition(snapshot.topic, snapshot.partition)
                assert offset_reader.committed(partition) is None
            finally:
                offset_reader.close()

            quoted_table = quote_clickhouse_identifier(source_table_clickhouse_name(table))
            target_offsets = {}
            for event in (snapshot, inserted, updated, deleted):
                partition = TopicPartition(event.topic, event.partition)
                target_offsets[partition] = max(target_offsets.get(partition, 0), event.offset + 1)

            def consume_until_complete(consumer_group, expected_value=3):
                offset_reader = KafkaConsumer(
                    bootstrap_servers=["kafka:9092"], group_id=consumer_group,
                    enable_auto_commit=False,
                )
                try:
                    state = {}

                    def consume_and_check():
                        _consume_source_cdc_group(profile, warehouse, [table], 100, consumer_group)
                        offsets = {
                            partition: offset_reader.committed(partition)
                            for partition in target_offsets
                        }
                        state["offsets"] = offsets
                        if any(
                            offsets[partition] is None or offsets[partition] < target
                            for partition, target in target_offsets.items()
                        ):
                            return False
                        rows = warehouse.query(
                            f"SELECT `_id`, `value`, `_cdc_deleted` FROM {quoted_table} FINAL "
                            "WHERE `_cdc_deleted` = 0"
                        ).result_rows
                        tombstones = warehouse.query(
                            f"SELECT `_id`, `value`, `_cdc_deleted` FROM {quoted_table} "
                            "WHERE `_cdc_deleted` = 1"
                        ).result_rows
                        state["rows"] = rows
                        state["tombstones"] = tombstones
                        return (
                            rows == [(snapshot_row["_id"], expected_value, 0)]
                            and (insert_row["_id"], None, 1) in tombstones
                        )

                    try:
                        wait_for("ClickHouse snapshot and changes", consume_and_check)
                    except TimeoutError as error:
                        raise TimeoutError(
                            f"ClickHouse probe timed out: targets={target_offsets}, state={state}"
                        ) from error
                finally:
                    offset_reader.close()

            consume_until_complete(group)
            consume_until_complete("reporthub-mongo-probe-replay")

            class RecordingInsert:
                def __init__(self):
                    self.inserted = False

                def __getattr__(self, name):
                    return getattr(warehouse, name)

                def insert(self, *args, **kwargs):
                    result = warehouse.insert(*args, **kwargs)
                    self.inserted = True
                    return result

            class FailingCommitConsumer(KafkaConsumer):
                def commit(self, *args, **kwargs):
                    raise RuntimeError("Simulated Kafka offset commit failure")

            commit_failure_group = "reporthub-mongo-probe-commit-failure"
            recording = RecordingInsert()

            def expect_commit_failure():
                with patch("etl.cdc.KafkaConsumer", FailingCommitConsumer):
                    try:
                        _consume_source_cdc_group(
                            profile, recording, [table], 100, commit_failure_group,
                        )
                    except RuntimeError as error:
                        if str(error) != "Simulated Kafka offset commit failure":
                            raise
                        return True
                return False

            wait_for("simulated offset commit failure", expect_commit_failure)
            assert recording.inserted
            offset_reader = KafkaConsumer(
                bootstrap_servers=["kafka:9092"], group_id=commit_failure_group,
                enable_auto_commit=False,
            )
            try:
                assert all(offset_reader.committed(partition) is None for partition in target_offsets)
            finally:
                offset_reader.close()
            consume_until_complete(commit_failure_group)

            rebuild = SimpleNamespace(
                pk=1, topic_prefix="reporthub-source-1", connector_name="reporthub-source-1",
                notification_topic="reporthub-mongo-probe-rebuild-notifications",
                consumer_group="reporthub-mongo-probe-rebuild",
                staging_table="reporthub_mongo_probe_stage",
                snapshot_end_offsets={str(partition.partition): offset for partition, offset in target_offsets.items()},
                status="CUTOVER_PENDING",
                Status=SimpleNamespace(SNAPSHOTTING="SNAPSHOTTING", CUTOVER_PENDING="CUTOVER_PENDING"),
            )
            wait_for(
                "MongoDB rebuild consumer checkpoint",
                lambda: consume_cdc_table_rebuild_batch(rebuild, profile, table, warehouse, 100),
            )
            stage_rows = warehouse.query(
                "SELECT `_id`, `value` FROM `reporthub_mongo_probe_stage` FINAL "
                "WHERE `_cdc_deleted` = 0"
            ).result_rows
            assert stage_rows == [(snapshot_row["_id"], 3)]

            profile.ingestion_mode = IngestionMode.CDC
            profile.save(update_fields=["ingestion_mode"])
            worker_name = "mongo-probe@localhost"
            with tempfile.TemporaryFile(mode="w+t") as worker_output:
                def start_worker():
                    return subprocess.Popen([
                        sys.executable, "-m", "celery", "-A", "config", "worker",
                        "--pool=solo", "--concurrency=1", "--queues=mongo_probe",
                        "--without-gossip", "--without-mingle", "--without-heartbeat",
                        "--include=tests.integration.mongo_cdc_probe", "--loglevel=WARNING",
                        f"--hostname={worker_name}",
                    ], stdout=worker_output, stderr=subprocess.STDOUT)

                worker = start_worker()
                try:
                    def worker_ready():
                        if worker.poll() is not None:
                            raise RuntimeError("The isolated Celery worker exited before becoming ready.")
                        return celery_app.control.ping(destination=[worker_name], timeout=1)

                    wait_for("isolated Celery worker", worker_ready, timeout=60)
                    blocked = ingest_source_to_clickhouse_task.apply_async(
                        args=[profile.pk], queue="mongo_probe",
                    ).get(timeout=45, propagate=False)
                    assert isinstance(blocked, RuntimeError)
                    assert "MongoDB CDC ingestion is not supported" in str(blocked)

                    worker_group = "reporthub-mongo-probe-worker"
                    offset_reader = KafkaConsumer(
                        bootstrap_servers=["kafka:9092"], group_id=worker_group,
                        enable_auto_commit=False,
                    )
                    try:
                        def worker_consumed():
                            result = consume_disposable_mongo_events.apply_async(
                                args=[profile.pk, table.pk, worker_group], queue="mongo_probe",
                            ).get(timeout=45)
                            assert result["worker"] == worker_name
                            offsets = {
                                partition: offset_reader.committed(partition)
                                for partition in target_offsets
                            }
                            return all(
                                offsets[partition] is not None and offsets[partition] >= target
                                for partition, target in target_offsets.items()
                            )

                        wait_for("Celery worker consumption", worker_consumed, timeout=120)
                    finally:
                        offset_reader.close()
                    rows = warehouse.query(
                        f"SELECT `_id`, `value` FROM {quoted_table} FINAL WHERE `_cdc_deleted` = 0"
                    ).result_rows
                    assert rows == [(snapshot_row["_id"], 3)]

                    collection.update_one({"_id": snapshot_id}, {"$set": {"value": 4}})
                    crash_event = next_event(consumer, "u")
                    crash_partition = TopicPartition(crash_event.topic, crash_event.partition)
                    offset_reader = KafkaConsumer(
                        bootstrap_servers=["kafka:9092"], group_id=worker_group,
                        enable_auto_commit=False,
                    )
                    try:
                        before_crash = offset_reader.committed(crash_partition)
                        assert before_crash is not None and before_crash <= crash_event.offset
                        crash_result = consume_disposable_mongo_events.apply_async(
                            args=[profile.pk, table.pk, worker_group, True], queue="mongo_probe",
                        )
                        try:
                            exit_code = worker.wait(timeout=45)
                        except subprocess.TimeoutExpired as error:
                            if crash_result.ready():
                                crash_result.get(propagate=True)
                            raise RuntimeError("The isolated worker did not crash at commit.") from error
                        assert exit_code == 73
                        assert offset_reader.committed(crash_partition) == before_crash
                        rows = warehouse.query(
                            f"SELECT `_id`, `value` FROM {quoted_table} FINAL WHERE `_cdc_deleted` = 0"
                        ).result_rows
                        assert rows == [(snapshot_row["_id"], 4)]

                        worker = start_worker()
                        wait_for("restarted Celery worker", worker_ready, timeout=60)

                        def worker_replayed():
                            try:
                                consume_disposable_mongo_events.apply_async(
                                    args=[profile.pk, table.pk, worker_group], queue="mongo_probe",
                                ).get(timeout=45)
                            except RuntimeError as error:
                                if "did not receive a partition assignment" not in str(error):
                                    raise
                                return False
                            committed = offset_reader.committed(crash_partition)
                            return committed is not None and committed > crash_event.offset

                        wait_for("Celery worker replay after crash", worker_replayed, timeout=120)
                        rows = warehouse.query(
                            f"SELECT `_id`, `value` FROM {quoted_table} FINAL WHERE `_cdc_deleted` = 0"
                        ).result_rows
                        assert rows == [(snapshot_row["_id"], 4)]
                    finally:
                        offset_reader.close()
                finally:
                    if worker.poll() is None:
                        worker.terminate()
                        try:
                            worker.wait(timeout=30)
                        except subprocess.TimeoutExpired:
                            worker.kill()
                            worker.wait(timeout=10)

            collection.update_one({"_id": snapshot_id}, {"$set": {"new_field": "new value"}})
            drift_event = next_event(consumer, "u")
            drift_partition = TopicPartition(drift_event.topic, drift_event.partition)
            offset_reader = KafkaConsumer(
                bootstrap_servers=["kafka:9092"], group_id=group,
                enable_auto_commit=False,
            )
            try:
                before_drift = offset_reader.committed(drift_partition)
                recording = RecordingInsert()

                def expect_schema_drift():
                    try:
                        _consume_source_cdc_group(profile, recording, [table], 100, group)
                    except RuntimeError as error:
                        if "absent from discovered metadata" not in str(error):
                            raise
                        return True
                    return False

                wait_for("MongoDB schema drift rejection", expect_schema_drift)
                assert not recording.inserted
                assert offset_reader.committed(drift_partition) == before_drift
            finally:
                offset_reader.close()
            SourceField.objects.create(
                table=table, field_name="new_field", data_type="String", is_selected=False,
            )
            target_offsets[drift_partition] = drift_event.offset + 1
            consume_until_complete(group, expected_value=4)

            connector_path = "/connectors/reporthub-source-1"
            request_connect(f"{connector_path}/pause", method="PUT")

            def connector_is_paused():
                status = request_connect(f"{connector_path}/status")
                return (
                    status["connector"]["state"] == "PAUSED"
                    and status["tasks"]
                    and all(task["state"] == "PAUSED" for task in status["tasks"])
                )

            wait_for("Debezium connector pause", connector_is_paused)
            collection.update_one({"_id": snapshot_id}, {"$set": {"value": 5}})
            request_connect(f"{connector_path}/resume", method="PUT")

            def connector_is_running():
                status = request_connect(f"{connector_path}/status")
                return (
                    status["connector"]["state"] == "RUNNING"
                    and status["tasks"]
                    and all(task["state"] == "RUNNING" for task in status["tasks"])
                )

            wait_for("Debezium connector resume", connector_is_running)
            resumed = next_event(consumer, "u")
            assert _decode_mongo_cdc_event(resumed)[2]["value"] == 5
            resumed_partition = TopicPartition(resumed.topic, resumed.partition)
            target_offsets[resumed_partition] = resumed.offset + 1
            consume_until_complete(group, expected_value=5)

            added_field = SourceField.objects.get(table=table, field_name="new_field")
            added_field.is_selected = True
            added_field.save(update_fields=["is_selected"])
            offset_reader = KafkaConsumer(
                bootstrap_servers=["kafka:9092"], group_id=group,
                enable_auto_commit=False,
            )
            try:
                before_change = offset_reader.committed(resumed_partition)
                recording = RecordingInsert()
                try:
                    _consume_source_cdc_group(profile, recording, [table], 100, group)
                    raise AssertionError("A new selected field was accepted without a rebuild")
                except RuntimeError as error:
                    assert "coordinated rebuild" in str(error)
                assert not recording.inserted
                assert offset_reader.committed(resumed_partition) == before_change
            finally:
                offset_reader.close()
            added_field.is_selected = False
            added_field.save(update_fields=["is_selected"])
            value_field = SourceField.objects.get(table=table, field_name="value")
            value_field.is_selected = False
            value_field.save(update_fields=["is_selected"])
            collection.update_one({"_id": snapshot_id}, {"$set": {"value": 6}})
            deselected_update = next_event(consumer, "u")
            target_offsets[TopicPartition(
                deselected_update.topic, deselected_update.partition
            )] = deselected_update.offset + 1
            consume_until_complete(group, expected_value=6)
            value_field.is_selected = True
            value_field.save(update_fields=["is_selected"])

            table.cdc_topic_prefix = "reporthub-mongo-probe-invalid"
            invalid_topic = _topic_name(profile, table, table.cdc_topic_prefix)
            producer = KafkaProducer(
                bootstrap_servers=["kafka:9092"],
                key_serializer=lambda value: json.dumps(value).encode("utf-8"),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
            try:
                metadata = producer.send(
                    invalid_topic,
                    key={"id": json_util.dumps(snapshot_id)},
                    value={"op": "c", "after": json_util.dumps({
                        "_id": snapshot_id, "value": "not an integer",
                    })},
                ).get(timeout=30)
            finally:
                producer.close()

            invalid_group = "reporthub-mongo-probe-invalid"
            recording = RecordingInsert()

            def expect_invalid_event():
                try:
                    _consume_source_cdc_group(profile, recording, [table], 100, invalid_group)
                except RuntimeError as error:
                    if "cannot be represented safely" not in str(error):
                        raise
                    return True
                return False

            wait_for("invalid MongoDB event", expect_invalid_event)
            assert not recording.inserted
            offset_reader = KafkaConsumer(
                bootstrap_servers=["kafka:9092"], group_id=invalid_group,
                enable_auto_commit=False,
            )
            try:
                assert offset_reader.committed(
                    TopicPartition(metadata.topic, metadata.partition)
                ) is None
            finally:
                offset_reader.close()

            table.cdc_topic_prefix = "reporthub-mongo-probe-multipartition"
            multi_topic = _topic_name(profile, table, table.cdc_topic_prefix)
            admin = KafkaAdminClient(bootstrap_servers=["kafka:9092"])
            try:
                admin.create_topics([NewTopic(name=multi_topic, num_partitions=2, replication_factor=1)])
            finally:
                admin.close()
            producer = KafkaProducer(
                bootstrap_servers=["kafka:9092"],
                key_serializer=lambda value: json.dumps(value).encode("utf-8"),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
            try:
                metadata = producer.send(
                    multi_topic, key={"id": json_util.dumps(snapshot_id)},
                    value={"op": "c", "after": json_util.dumps({"_id": snapshot_id, "value": 4})},
                ).get(timeout=30)
            finally:
                producer.close()
            multi_group = "reporthub-mongo-probe-multipartition"
            recording = RecordingInsert()
            try:
                _consume_source_cdc_group(profile, recording, [table], 100, multi_group)
                raise AssertionError("A multi-partition MongoDB topic was accepted")
            except RuntimeError as error:
                assert "exactly one partition" in str(error)
            assert not recording.inserted
            offset_reader = KafkaConsumer(
                bootstrap_servers=["kafka:9092"], group_id=multi_group,
                enable_auto_commit=False,
            )
            try:
                assert offset_reader.committed(TopicPartition(metadata.topic, metadata.partition)) is None
            finally:
                offset_reader.close()
        finally:
            warehouse.close()
        print("MongoDB snapshot, insert and full update contain Extended JSON strings; deletes use the key.", flush=True)
        print("Real consumer recovered from write and offset-commit failures and rejected an invalid event.", flush=True)
        print("Isolated Celery worker consumed events; normal MongoDB CDC task remained blocked.", flush=True)
        print("An isolated worker crash after write replayed safely after restart.", flush=True)
        print("MongoDB rebuild rows were mapped and multi-partition topics were rejected.", flush=True)
        print("Undiscovered fields stopped ingestion until metadata was reviewed.", flush=True)
        print("A source update during connector pause was ingested after resume.", flush=True)
        print("Selecting a new field was blocked until a coordinated rebuild.", flush=True)
        print("Previously selected fields continued updating while deselected.", flush=True)
        print("MONGO_CDC_DEBEZIUM_PROBE_PASSED", flush=True)
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
