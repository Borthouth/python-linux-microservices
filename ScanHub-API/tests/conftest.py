import os
import tempfile


os.environ["API_KEY"] = "replace-with-a-real-secret"

TEST_METRICS_DB = os.path.join(
    tempfile.gettempdir(),
    "scanhub_test_metrics.db"
)

os.environ["SCANHUB_METRICS_DB"] = TEST_METRICS_DB


def pytest_sessionfinish(session, exitstatus):
    """
    Remove the test metrics database files after the test session.
    SQLite WAL mode may create additional -wal and -shm files.
    """

    for path in (
        TEST_METRICS_DB,
        TEST_METRICS_DB + "-wal",
        TEST_METRICS_DB + "-shm",
    ):
        if os.path.exists(path):
            os.remove(path)