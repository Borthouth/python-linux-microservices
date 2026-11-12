from fastapi.testclient import TestClient

import scan_api
from scan_api import app, scanner_pool
from scanners.pool import ScannerPool


client = TestClient(app)


def test_health_success(monkeypatch):
    """
    Verify /health returns a successful pool response when the
    scanner pool health check succeeds.
    """

    def mock_health():
        return {
            "ok": True,
            "pool_strategy": "least_connections",
            "scanner_count": 2,
            "instances": [
                {
                    "ok": True,
                    "engine": "clamav",
                    "scan_mode": "fildes",
                    "scanner_instance": "clamav_scan_1",
                    "socket": "/run/clamd.scan/clamd.sock",
                    "clamd": "ClamAV 1.4.3",
                    "active_connections": 0,
                },
                {
                    "ok": True,
                    "engine": "clamav",
                    "scan_mode": "fildes",
                    "scanner_instance": "clamav_scan_2",
                    "socket": "/run/clamd.scan2/clamd.sock",
                    "clamd": "ClamAV 1.4.3",
                    "active_connections": 0,
                },
            ],
        }

    monkeypatch.setattr(scanner_pool, "health", mock_health)

    response = client.get("/health")

    assert response.status_code == 200

    data = response.json()

    assert data["ok"] is True
    assert data["pool_strategy"] == "least_connections"
    assert data["scanner_count"] == 2
    assert data["version"] == "1.0.0"

    assert len(data["instances"]) == 2
    assert data["instances"][0]["scanner_instance"] == "clamav_scan_1"
    assert data["instances"][1]["scanner_instance"] == "clamav_scan_2"


def test_health_does_not_expose_scanner_exception(monkeypatch):
    sensitive_detail = "Unable to connect to /run/private/clamd.sock"

    class BrokenScanner:
        instance_name = "clamav_scan_1"

        def health(self):
            raise RuntimeError(sensitive_detail)

    monkeypatch.setattr(
        scan_api,
        "scanner_pool",
        ScannerPool(scanners=[BrokenScanner()])
    )

    response = client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is False
    assert data["instances"][0]["ok"] is False
    assert data["instances"][0]["error"] == "Scanner health check failed"
    assert sensitive_detail not in response.text