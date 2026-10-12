import time
import threading

from scanners.pool import ScannerPool


class DummyScanner:
    def __init__(self, instance_name, engine="dummy", scan_mode="test"):
        self.instance_name = instance_name
        self.name = engine
        self.scan_mode = scan_mode
        self.scan_count = 0

    def scan(self, file_path, fd=None, filename=None, size=None):
        self.scan_count += 1
        return {
            "ok": True,
            "engine": self.name,
            "scan_mode": self.scan_mode,
            "clean": True,
            "infected": False,
            "virus": None,
            "error": None,
        }

    def health(self):
        return {
            "ok": True,
            "engine": self.name,
            "scan_mode": self.scan_mode,
        }


def test_pool_requires_at_least_one_scanner():
    try:
        ScannerPool([])
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "at least one scanner" in str(exc)


def test_first_strategy_always_uses_first_scanner():
    scanner_1 = DummyScanner("scanner_1")
    scanner_2 = DummyScanner("scanner_2")

    pool = ScannerPool(
        scanners=[scanner_1, scanner_2],
        strategy="first",
    )

    result_1 = pool.scan("/tmp/test1.txt")
    result_2 = pool.scan("/tmp/test2.txt")
    result_3 = pool.scan("/tmp/test3.txt")

    assert result_1["scanner_instance"] == "scanner_1"
    assert result_2["scanner_instance"] == "scanner_1"
    assert result_3["scanner_instance"] == "scanner_1"

    assert scanner_1.scan_count == 3
    assert scanner_2.scan_count == 0


def test_round_robin_strategy_rotates_scanners():
    scanner_1 = DummyScanner("scanner_1")
    scanner_2 = DummyScanner("scanner_2")

    pool = ScannerPool(
        scanners=[scanner_1, scanner_2],
        strategy="round_robin",
    )

    result_1 = pool.scan("/tmp/test1.txt")
    result_2 = pool.scan("/tmp/test2.txt")
    result_3 = pool.scan("/tmp/test3.txt")
    result_4 = pool.scan("/tmp/test4.txt")

    assert result_1["scanner_instance"] == "scanner_1"
    assert result_2["scanner_instance"] == "scanner_2"
    assert result_3["scanner_instance"] == "scanner_1"
    assert result_4["scanner_instance"] == "scanner_2"

    assert scanner_1.scan_count == 2
    assert scanner_2.scan_count == 2


def test_least_connections_strategy_returns_pool_metadata():
    scanner_1 = DummyScanner("scanner_1")
    scanner_2 = DummyScanner("scanner_2")

    pool = ScannerPool(
        scanners=[scanner_1, scanner_2],
        strategy="least_connections",
    )

    result = pool.scan("/tmp/test.txt")

    assert result["ok"] is True
    assert result["scanner_instance"] in {
        "scanner_1",
        "scanner_2",
    }
    assert result["pool_strategy"] == "least_connections"


def test_status_does_not_call_scanner_health():
    class HealthFailingScanner(DummyScanner):
        def health(self):
            raise RuntimeError("health should not be called by status")

    scanner_1 = HealthFailingScanner("scanner_1")
    scanner_2 = HealthFailingScanner("scanner_2")

    pool = ScannerPool(
        scanners=[scanner_1, scanner_2],
        strategy="least_connections",
    )

    status = pool.status()

    assert status["pool_strategy"] == "least_connections"
    assert status["scanner_count"] == 2
    assert len(status["instances"]) == 2

    assert status["instances"][0]["scanner_instance"] == "scanner_1"
    assert status["instances"][1]["scanner_instance"] == "scanner_2"


def test_health_calls_each_scanner_health():
    scanner_1 = DummyScanner("scanner_1")
    scanner_2 = DummyScanner("scanner_2")

    pool = ScannerPool(
        scanners=[scanner_1, scanner_2],
        strategy="least_connections",
    )

    health = pool.health()

    assert health["ok"] is True
    assert health["pool_strategy"] == "least_connections"
    assert health["scanner_count"] == 2
    assert len(health["instances"]) == 2

    assert health["instances"][0]["scanner_instance"] == "scanner_1"
    assert health["instances"][1]["scanner_instance"] == "scanner_2"


def test_health_reports_failed_scanner():
    class BrokenScanner(DummyScanner):
        def health(self):
            raise RuntimeError("scanner unavailable")

    scanner_1 = DummyScanner("scanner_1")
    scanner_2 = BrokenScanner("scanner_2")

    pool = ScannerPool(
        scanners=[scanner_1, scanner_2],
        strategy="least_connections",
    )

    health = pool.health()

    assert health["ok"] is False
    assert health["scanner_count"] == 2

    failed = [
        item for item in health["instances"]
        if item["scanner_instance"] == "scanner_2"
    ][0]

    assert failed["ok"] is False
    assert "scanner unavailable" in failed["error"]


def test_unknown_strategy_raises_value_error():
    scanner_1 = DummyScanner("scanner_1")

    pool = ScannerPool(
        scanners=[scanner_1],
        strategy="unknown_strategy",
    )

    try:
        pool.scan("/tmp/test.txt")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "Unsupported scanner pool strategy" in str(exc)