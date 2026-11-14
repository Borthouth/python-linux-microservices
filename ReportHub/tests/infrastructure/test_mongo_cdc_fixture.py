import os
import time
import unittest
from uuid import uuid4

from pymongo import MongoClient


@unittest.skipUnless(
    os.environ.get("REPORTHUB_MONGO_CDC_FIXTURE_TEST") == "1",
    "Set REPORTHUB_MONGO_CDC_FIXTURE_TEST=1 to test the MongoDB CDC fixture.",
)
class MongoCdcFixtureTests(unittest.TestCase):
    def test_replica_set_streams_insert_update_and_delete(self):
        client = MongoClient(
            "mongodb://test_user:test_password@127.0.0.1:27019/"
            "?authSource=admin&directConnection=true",
            serverSelectionTimeoutMS=5000,
        )
        database = client["app_analytics"]
        collection_name = f"reporthub_cdc_probe_{uuid4().hex}"
        try:
            hello = client.admin.command("hello")
            self.assertEqual(hello["setName"], "rs0")
            self.assertTrue(hello["isWritablePrimary"])
            collection = database.create_collection(collection_name)
            with collection.watch(max_await_time_ms=1000) as stream:
                document_id = uuid4().hex
                operations = (
                    ("insert", lambda: collection.insert_one({"_id": document_id, "value": 1})),
                    ("update", lambda: collection.update_one({"_id": document_id}, {"$set": {"value": 2}})),
                    ("delete", lambda: collection.delete_one({"_id": document_id})),
                )
                for expected, operation in operations:
                    operation()
                    deadline = time.monotonic() + 10
                    event = None
                    while event is None and time.monotonic() < deadline:
                        event = stream.try_next()
                    self.assertIsNotNone(event, f"Timed out waiting for {expected} change")
                    self.assertEqual(event["operationType"], expected)
                    self.assertEqual(event["documentKey"]["_id"], document_id)
        finally:
            database.drop_collection(collection_name)
            client.close()
