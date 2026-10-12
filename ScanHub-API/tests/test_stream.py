from fastapi.testclient import TestClient

from scan_api import app, scanner_pool


client = TestClient(app, raise_server_exceptions=False)


def test_stream_clean(monkeypatch):
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
        "/scan/stream",
        headers={
            "X-API-Key": "replace-with-a-real-secret",
            "X-Filename": "test.txt",
            "Content-Type": "application/octet-stream",
        },
        content=b"hello world",
    )

    assert response.status_code == 200

    data = response.json()

    assert data["clean"] is True
    assert data["scanner_instance"] == "clamav_scan_1"
    assert data["pool_strategy"] == "least_connections"


def test_stream_empty():
    response = client.post(
        "/scan/stream",
        headers={
            "X-API-Key": "replace-with-a-real-secret",
            "X-Filename": "test.txt",
            "Content-Type": "application/octet-stream",
        },
        content=b"",
    )

    assert response.status_code == 400


def test_stream_invalid_api_key():
    response = client.post(
        "/scan/stream",
        headers={
            "X-API-Key": "bad-key",
            "X-Filename": "test.txt",
            "Content-Type": "application/octet-stream",
        },
        content=b"hello",
    )

    assert response.status_code == 401