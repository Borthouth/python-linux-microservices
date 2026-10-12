import pytest
from fastapi import HTTPException

import scanners.factory as factory
from scanners.pool import ScannerPool


def test_create_clamav_fildes_instance():
    instance_config = {
        "instance_name": "clamav_scan_1",
        "type": "clamav_fildes",
        "socket": "/run/clamd.scan/clamd.sock",
    }

    scanner = factory.create_scanner_instance(instance_config)

    assert scanner.instance_name == "clamav_scan_1"
    assert scanner.name == "clamav"
    assert scanner.scan_mode == "fildes"
    assert scanner.socket_path == "/run/clamd.scan/clamd.sock"


def test_create_clamav_file_instance():
    instance_config = {
        "instance_name": "clamav_file_1",
        "type": "clamav_file",
        "socket": "/run/clamd.scan/clamd.sock",
    }

    scanner = factory.create_scanner_instance(instance_config)

    assert scanner.instance_name == "clamav_file_1"
    assert scanner.name == "clamav"
    assert scanner.scan_mode == "file"
    assert scanner.socket_path == "/run/clamd.scan/clamd.sock"


def test_create_clamav_file_instance_with_network_settings():
    instance_config = {
        "instance_name": "clamav_file_tcp_1",
        "type": "clamav_file",
        "host": "127.0.0.1",
        "port": 3310,
    }

    scanner = factory.create_scanner_instance(instance_config)

    assert scanner.instance_name == "clamav_file_tcp_1"
    assert scanner.name == "clamav"
    assert scanner.scan_mode == "file"
    assert scanner.host == "127.0.0.1"
    assert scanner.port == 3310


def test_create_scanner_instance_rejects_unknown_type():
    instance_config = {
        "instance_name": "unknown_1",
        "type": "unknown_scanner",
    }

    with pytest.raises(HTTPException) as exc:
        factory.create_scanner_instance(instance_config)

    assert exc.value.status_code == 500
    assert "Unsupported scanner type configured" in exc.value.detail


def test_get_scanner_pool_builds_configured_pool(monkeypatch):
    scanner_pools = {
        "test_pool": {
            "strategy": "least_connections",
            "instances": [
                {
                    "instance_name": "clamav_scan_1",
                    "type": "clamav_fildes",
                    "socket": "/run/clamd.scan/clamd.sock",
                },
                {
                    "instance_name": "clamav_scan_2",
                    "type": "clamav_fildes",
                    "socket": "/run/clamd.scan2/clamd.sock",
                },
            ],
        },
    }

    monkeypatch.setattr(factory.config, "SCANNER_POOL", "test_pool")
    monkeypatch.setattr(factory.config, "SCANNER_POOLS", scanner_pools)

    pool = factory.get_scanner_pool()

    assert isinstance(pool, ScannerPool)
    assert pool.strategy == "least_connections"
    assert len(pool.scanners) == 2

    assert pool.scanners[0].instance_name == "clamav_scan_1"
    assert pool.scanners[0].socket_path == "/run/clamd.scan/clamd.sock"

    assert pool.scanners[1].instance_name == "clamav_scan_2"
    assert pool.scanners[1].socket_path == "/run/clamd.scan2/clamd.sock"


def test_get_scanner_pool_supports_round_robin(monkeypatch):
    scanner_pools = {
        "test_pool": {
            "strategy": "round_robin",
            "instances": [
                {
                    "instance_name": "clamav_scan_1",
                    "type": "clamav_fildes",
                    "socket": "/run/clamd.scan/clamd.sock",
                },
                {
                    "instance_name": "clamav_scan_2",
                    "type": "clamav_fildes",
                    "socket": "/run/clamd.scan2/clamd.sock",
                },
            ],
        },
    }

    monkeypatch.setattr(factory.config, "SCANNER_POOL", "test_pool")
    monkeypatch.setattr(factory.config, "SCANNER_POOLS", scanner_pools)

    pool = factory.get_scanner_pool()

    assert isinstance(pool, ScannerPool)
    assert pool.strategy == "round_robin"
    assert len(pool.scanners) == 2


def test_get_scanner_pool_supports_first_strategy(monkeypatch):
    scanner_pools = {
        "test_pool": {
            "strategy": "first",
            "instances": [
                {
                    "instance_name": "clamav_scan_1",
                    "type": "clamav_fildes",
                    "socket": "/run/clamd.scan/clamd.sock",
                },
                {
                    "instance_name": "clamav_scan_2",
                    "type": "clamav_fildes",
                    "socket": "/run/clamd.scan2/clamd.sock",
                },
            ],
        },
    }

    monkeypatch.setattr(factory.config, "SCANNER_POOL", "test_pool")
    monkeypatch.setattr(factory.config, "SCANNER_POOLS", scanner_pools)

    pool = factory.get_scanner_pool()

    assert isinstance(pool, ScannerPool)
    assert pool.strategy == "first"
    assert len(pool.scanners) == 2


def test_get_scanner_pool_rejects_missing_scanner_pools(monkeypatch):
    monkeypatch.setattr(factory.config, "SCANNER_POOL", "test_pool")
    monkeypatch.delattr(factory.config, "SCANNER_POOLS", raising=False)

    with pytest.raises(HTTPException) as exc:
        factory.get_scanner_pool()

    assert exc.value.status_code == 500
    assert "SCANNER_POOLS is not configured" in exc.value.detail


def test_get_scanner_pool_rejects_unknown_pool(monkeypatch):
    scanner_pools = {
        "default": {
            "strategy": "least_connections",
            "instances": [],
        },
    }

    monkeypatch.setattr(factory.config, "SCANNER_POOL", "missing_pool")
    monkeypatch.setattr(factory.config, "SCANNER_POOLS", scanner_pools)

    with pytest.raises(HTTPException) as exc:
        factory.get_scanner_pool()

    assert exc.value.status_code == 500
    assert "Configured scanner pool does not exist" in exc.value.detail


def test_get_scanner_pool_rejects_empty_pool(monkeypatch):
    scanner_pools = {
        "empty_pool": {
            "strategy": "least_connections",
            "instances": [],
        },
    }

    monkeypatch.setattr(factory.config, "SCANNER_POOL", "empty_pool")
    monkeypatch.setattr(factory.config, "SCANNER_POOLS", scanner_pools)

    with pytest.raises(ValueError) as exc:
        factory.get_scanner_pool()

    assert "Scanner pool must contain at least one scanner" in str(exc.value)


def test_get_scanner_returns_scanner_pool(monkeypatch):
    scanner_pools = {
        "test_pool": {
            "strategy": "least_connections",
            "instances": [
                {
                    "instance_name": "clamav_scan_1",
                    "type": "clamav_fildes",
                    "socket": "/run/clamd.scan/clamd.sock",
                },
            ],
        },
    }

    monkeypatch.setattr(factory.config, "SCANNER_POOL", "test_pool")
    monkeypatch.setattr(factory.config, "SCANNER_POOLS", scanner_pools)

    scanner = factory.get_scanner()

    assert isinstance(scanner, ScannerPool)
    assert scanner.strategy == "least_connections"
    assert len(scanner.scanners) == 1
    assert scanner.scanners[0].instance_name == "clamav_scan_1"