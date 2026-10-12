from fastapi.testclient import TestClient

from scan_api import app, scanner_pool


client = TestClient(app, raise_server_exceptions=False)


def mock_pool_status():
    return {
        "pool_strategy": "least_connections",
        "scanner_count": 2,
        "instances": [
            {
                "scanner_instance": "clamav_scan_1",
                "engine": "clamav",
                "scan_mode": "fildes",
                "active_connections": 0,
            },
            {
                "scanner_instance": "clamav_scan_2",
                "engine": "clamav",
                "scan_mode": "fildes",
                "active_connections": 0,
            },
        ],
    }


def test_metrics_endpoint_returns_expected_fields(monkeypatch):
    """
    Verify /metrics returns the expected metrics structure.
    """

    monkeypatch.setattr(scanner_pool, "status", mock_pool_status)

    response = client.get("/metrics")

    assert response.status_code == 200

    data = response.json()

    assert "version" in data
    assert "pid" in data
    assert "uptime_seconds" in data
    assert "scanner_pool" in data

    assert "total_scans" in data
    assert "clean_scans" in data
    assert "infected_scans" in data
    assert "scan_errors" in data
    assert "total_bytes" in data
    assert "largest_file" in data
    assert "average_scan_ms" in data
    assert "average_upload_ms" in data


def test_metrics_values_are_expected_types(monkeypatch):
    """
    Verify /metrics returns sensible value types.
    """

    monkeypatch.setattr(scanner_pool, "status", mock_pool_status)

    response = client.get("/metrics")

    assert response.status_code == 200

    data = response.json()

    assert isinstance(data["version"], str)
    assert isinstance(data["pid"], int)
    assert isinstance(data["uptime_seconds"], int)

    assert isinstance(data["scanner_pool"], dict)
    assert isinstance(data["scanner_pool"]["pool_strategy"], str)
    assert isinstance(data["scanner_pool"]["scanner_count"], int)
    assert isinstance(data["scanner_pool"]["instances"], list)

    assert isinstance(data["total_scans"], int)
    assert isinstance(data["clean_scans"], int)
    assert isinstance(data["infected_scans"], int)
    assert isinstance(data["scan_errors"], int)
    assert isinstance(data["total_bytes"], int)
    assert isinstance(data["largest_file"], int)

    assert isinstance(data["average_scan_ms"], float) or isinstance(
        data["average_scan_ms"],
        int,
    )
    assert isinstance(data["average_upload_ms"], float) or isinstance(
        data["average_upload_ms"],
        int,
    )


def test_metrics_increment_after_clean_scan(monkeypatch):
    """
    Verify metrics are incremented after a clean multipart scan.
    """

    monkeypatch.setattr(scanner_pool, "status", mock_pool_status)

    def mock_scan(*args, **kwargs):
        return {
            "ok": True,
            "engine": "clamav",
            "scan_mode": "fildes",
            "scanner_instance": "clamav_scan_1",
            "clean": True,
            "infected": False,
            "virus": None,
            "error": None,
            "pool_strategy": "least_connections",
        }

    monkeypatch.setattr(scanner_pool, "scan", mock_scan)

    before = client.get("/metrics").json()

    response = client.post(
        "/scan",
        headers={
            "X-API-Key": "replace-with-a-real-secret",
        },
        files={
            "file": ("test.txt", b"hello world"),
        },
    )

    assert response.status_code == 200

    after = client.get("/metrics").json()

    assert after["total_scans"] == before["total_scans"] + 1
    assert after["clean_scans"] == before["clean_scans"] + 1
    assert after["total_bytes"] == before["total_bytes"] + len(b"hello world")


def test_metrics_increment_after_infected_scan(monkeypatch):
    """
    Verify infected scan count is incremented when the scanner reports malware.
    """

    monkeypatch.setattr(scanner_pool, "status", mock_pool_status)

    def mock_scan(*args, **kwargs):
        return {
            "ok": True,
            "engine": "clamav",
            "scan_mode": "fildes",
            "scanner_instance": "clamav_scan_2",
            "clean": False,
            "infected": True,
            "virus": "Eicar-Test-Signature",
            "error": None,
            "pool_strategy": "least_connections",
        }

    monkeypatch.setattr(scanner_pool, "scan", mock_scan)

    before = client.get("/metrics").json()

    response = client.post(
        "/scan",
        headers={
            "X-API-Key": "replace-with-a-real-secret",
        },
        files={
            "file": ("eicar.txt", b"test-virus-content"),
        },
    )

    assert response.status_code == 200

    after = client.get("/metrics").json()

    assert after["total_scans"] == before["total_scans"] + 1
    assert after["infected_scans"] == before["infected_scans"] + 1


def test_metrics_increment_after_scan_error(monkeypatch):
    """
    Verify scan error count is incremented when the scanner returns an error result.
    """

    monkeypatch.setattr(scanner_pool, "status", mock_pool_status)

    def mock_scan(*args, **kwargs):
        return {
            "ok": False,
            "engine": "clamav",
            "scan_mode": "file",
            "scanner_instance": "clamav_file_1",
            "clean": False,
            "infected": False,
            "virus": None,
            "error": "Access denied.",
            "raw_result": {
                "/tmp/test.txt": ["ERROR", "Access denied."],
            },
            "pool_strategy": "least_connections",
        }

    monkeypatch.setattr(scanner_pool, "scan", mock_scan)

    before = client.get("/metrics").json()

    response = client.post(
        "/scan",
        headers={
            "X-API-Key": "replace-with-a-real-secret",
        },
        files={
            "file": ("test.txt", b"hello world"),
        },
    )

    assert response.status_code == 200

    after = client.get("/metrics").json()

    assert after["total_scans"] == before["total_scans"] + 1
    assert after["scan_errors"] == before["scan_errors"] + 1


def test_metrics_increment_after_stream_scan(monkeypatch):
    """
    Verify metrics are incremented after a raw stream scan.
    """

    monkeypatch.setattr(scanner_pool, "status", mock_pool_status)

    def mock_scan(*args, **kwargs):
        return {
            "ok": True,
            "engine": "clamav",
            "scan_mode": "fildes",
            "scanner_instance": "clamav_scan_1",
            "clean": True,
            "infected": False,
            "virus": None,
            "error": None,
            "pool_strategy": "least_connections",
        }

    monkeypatch.setattr(scanner_pool, "scan", mock_scan)

    content = b"stream content"

    before = client.get("/metrics").json()

    response = client.post(
        "/scan/stream",
        headers={
            "X-API-Key": "replace-with-a-real-secret",
            "X-Filename": "stream.txt",
            "Content-Type": "application/octet-stream",
        },
        content=content,
    )

    assert response.status_code == 200

    after = client.get("/metrics").json()

    assert after["total_scans"] == before["total_scans"] + 1
    assert after["clean_scans"] == before["clean_scans"] + 1
    assert after["total_bytes"] == before["total_bytes"] + len(content)


def test_metrics_average_values_do_not_fail_when_no_division_issue(monkeypatch):
    """
    Verify average values are returned without division errors.
    This mainly protects the /metrics endpoint from zero-count failures.
    """

    monkeypatch.setattr(scanner_pool, "status", mock_pool_status)

    response = client.get("/metrics")

    assert response.status_code == 200

    data = response.json()

    assert data["average_scan_ms"] >= 0
    assert data["average_upload_ms"] >= 0