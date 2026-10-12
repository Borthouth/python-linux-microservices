from fastapi.testclient import TestClient

from scan_api import app, scanner_pool


client = TestClient(app, raise_server_exceptions=False)


def test_scan_clean_file(monkeypatch):
    """
    Verify a clean file is reported as clean.
    """

    def mock_scan(*args, **kwargs):
        return {
            "ok": True,
            "engine": "clamav",
            "scan_mode": "fildes",
            "scanner_instance": "clamav_scan_1",
            "socket": "/run/clamd.scan/clamd.sock",
            "clean": True,
            "infected": False,
            "virus": None,
            "error": None,
            "pool_strategy": "least_connections",
        }

    monkeypatch.setattr(scanner_pool, "scan", mock_scan)

    response = client.post(
        "/scan",
        headers={"X-API-Key": "replace-with-a-real-secret"},
        files={"file": ("test.txt", b"hello world")},
    )

    assert response.status_code == 200

    data = response.json()

    assert data["clean"] is True
    assert data["infected"] is False
    assert data["virus"] is None
    assert data["scanner_instance"] == "clamav_scan_1"
    assert data["pool_strategy"] == "least_connections"


def test_scan_infected_file(monkeypatch):
    """
    Verify an infected file is reported correctly.
    """

    def mock_scan(*args, **kwargs):
        return {
            "ok": True,
            "engine": "clamav",
            "scan_mode": "fildes",
            "scanner_instance": "clamav_scan_2",
            "socket": "/run/clamd.scan2/clamd.sock",
            "clean": False,
            "infected": True,
            "virus": "Eicar-Test-Signature",
            "error": None,
            "pool_strategy": "least_connections",
        }

    monkeypatch.setattr(scanner_pool, "scan", mock_scan)

    response = client.post(
        "/scan",
        headers={"X-API-Key": "replace-with-a-real-secret"},
        files={"file": ("eicar.txt", b"eicar")},
    )

    assert response.status_code == 200

    data = response.json()

    assert data["infected"] is True
    assert data["virus"] == "Eicar-Test-Signature"
    assert data["scanner_instance"] == "clamav_scan_2"


def test_scan_invalid_api_key():
    """
    Verify invalid API key is rejected.
    """

    response = client.post(
        "/scan",
        headers={"X-API-Key": "invalid"},
        files={"file": ("test.txt", b"hello")},
    )

    assert response.status_code == 401


def test_scan_missing_api_key():
    """
    Verify missing API key is rejected.
    """

    response = client.post(
        "/scan",
        files={"file": ("test.txt", b"hello")},
    )

    assert response.status_code == 401


def test_scan_empty_file():
    """
    Verify empty files are rejected.
    """

    response = client.post(
        "/scan",
        headers={"X-API-Key": "replace-with-a-real-secret"},
        files={"file": ("empty.txt", b"")},
    )

    assert response.status_code == 400


def test_scan_scanner_exception(monkeypatch):
    """
    Verify scanner failures return HTTP 500.
    """

    def mock_scan(*args, **kwargs):
        raise Exception("Test scanner failure")

    monkeypatch.setattr(scanner_pool, "scan", mock_scan)

    response = client.post(
        "/scan",
        headers={"X-API-Key": "replace-with-a-real-secret"},
        files={"file": ("test.txt", b"hello")},
    )

    assert response.status_code == 500