"""Capture MongoDB Debezium events in a disposable, isolated stack."""

import json
import os
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import django
from bson import Decimal128, ObjectId, json_util
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


django.setup()

from control_plane.models import DbChoice
from etl.cdc import _decode_mongo_cdc_event, _topic_name, build_debezium_connector_config


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


def request_connect(path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    request = Request(
        f"http://connect:8083{path}",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            return json.load(response)
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
    profile = SimpleNamespace(
        pk=1, db_type=DbChoice.MONGODB, host="mongo", port=27017,
        username="mongo_test", password=password, database_name="app_analytics",
    )
    table = SimpleNamespace(
        schema_name="app_analytics", table_name="documents",
        primary_key_fields=["_id"], cdc_topic_prefix="",
    )
    config = build_debezium_connector_config(profile, [table])
    # The current source model does not yet expose the replica-set name.
    config["mongodb.connection.string"] += "&replicaSet=rs0"
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
        print("MongoDB snapshot, insert and full update contain Extended JSON strings; deletes use the key.", flush=True)
        print("MONGO_CDC_DEBEZIUM_PROBE_PASSED", flush=True)
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
