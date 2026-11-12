import os

from fastapi.testclient import TestClient

import scan_api
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


def test_stream_rejects_unsafe_filename_suffix(monkeypatch, tmp_path):
    scanned = {}

    def mock_scan(file_path, fd=None, filename=None, size=None):
        scanned["path"] = file_path
        scanned["filename"] = filename
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

    monkeypatch.setattr(scan_api, "TEMP_DIR", str(tmp_path))
    monkeypatch.setattr(scanner_pool, "scan", mock_scan)
    filename = "../../report.<bad>"

    response = client.post(
        "/scan/stream",
        headers={
            "X-API-Key": "replace-with-a-real-secret",
            "X-Filename": filename,
            "Content-Type": "application/octet-stream",
        },
        content=b"hello world",
    )

    assert response.status_code == 200
    assert response.json()["filename"] == filename
    assert scanned["filename"] == filename
    assert os.path.dirname(scanned["path"]) == str(tmp_path)
    assert os.path.splitext(scanned["path"])[1] == ""


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