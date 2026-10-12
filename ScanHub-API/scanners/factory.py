from fastapi import HTTPException

import config

from scanners.pool import ScannerPool
from scanners.clamav_fildes import ClamAVFildesScanner
from scanners.clamav_file import ClamAVFileScanner


def create_scanner_instance(instance_config: dict):
    """
    Create one scanner instance from config.

    The pool stays generic. Scanner-specific settings such as ClamAV socket
    paths stay inside the scanner instance configuration.
    """
    scanner_type = (instance_config.get("type") or "").lower().strip()
    instance_name = instance_config.get("instance_name") or scanner_type

    if scanner_type == "clamav_fildes":
        return ClamAVFildesScanner(
            instance_name=instance_name,
            socket_path=instance_config.get("socket"),
        )

    if scanner_type == "clamav_file":
        return ClamAVFileScanner(
            instance_name=instance_name,
            socket_path=instance_config.get("socket"),
            host=instance_config.get("host"),
            port=instance_config.get("port"),
        )

    raise HTTPException(
        status_code=500,
        detail=f"Unsupported scanner type configured: {scanner_type}",
    )


def get_scanner_pool() -> ScannerPool:
    """
    Build the configured scanner pool.

    Expected config.py format:

        SCANNER_POOL = "default"

        SCANNER_POOLS = {
            "default": {
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
    """
    scanner_pool_name = getattr(config, "SCANNER_POOL", "default")
    scanner_pools = getattr(config, "SCANNER_POOLS", None)

    if not scanner_pools:
        raise HTTPException(
            status_code=500,
            detail="SCANNER_POOLS is not configured in config.py",
        )

    if scanner_pool_name not in scanner_pools:
        raise HTTPException(
            status_code=500,
            detail=f"Configured scanner pool does not exist: {scanner_pool_name}",
        )

    pool_config = scanner_pools[scanner_pool_name]

    scanner_configs = pool_config.get("instances") or []
    strategy = pool_config.get("strategy", "least_connections")

    scanners = [
        create_scanner_instance(instance_config)
        for instance_config in scanner_configs
    ]

    return ScannerPool(
        scanners=scanners,
        strategy=strategy,
    )


def get_scanner():
    """
    Backwards-compatible wrapper.

    Old code may still call get_scanner(), but internally we now return
    the scanner pool. The pool exposes scan() and health(), so scan_api.py
    can treat it like a scanner object.
    """
    return get_scanner_pool()