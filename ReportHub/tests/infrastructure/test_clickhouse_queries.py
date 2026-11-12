import unittest
from pathlib import Path
import runpy
from unittest.mock import Mock

# Keep standalone tests independent of config's Celery initialization.
cancel_clickhouse_query = runpy.run_path(
    str(Path(__file__).resolve().parents[2] / "config" / "clickhouse_queries.py")
)["cancel_clickhouse_query"]


class ClickHouseCancellationTests(unittest.TestCase):
    def test_query_id_is_bound_not_interpolated(self):
        client = Mock()
        query_id = "report_' OR 1 = 1 --"

        result = cancel_clickhouse_query(client, query_id)

        client.command.assert_called_once_with(
            "KILL QUERY WHERE query_id = {query_id:String} SYNC",
            parameters={"query_id": query_id},
        )
        self.assertIs(result, client.command.return_value)

    def test_cancellation_failure_is_not_converted_to_success(self):
        client = Mock()
        client.command.side_effect = TimeoutError("Lost cancellation response")

        with self.assertRaisesRegex(TimeoutError, "Lost cancellation response"):
            cancel_clickhouse_query(client, "etl_task_test")
