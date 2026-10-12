import os
import logging
from typing import Optional, Dict, Any

import pyclamd
from fastapi import HTTPException

import config

from scanners.base import BaseScanner


logger = logging.getLogger("scan_api")


class ClamAVFileScanner(BaseScanner):
    name = "clamav"
    scan_mode = "file"

    def __init__(
        self,
        instance_name: str = "clamav_file",
        socket_path: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ):
        """
        ClamAV file scanner.

        This scanner sends the file path to clamd using pyclamd scan_file().

        It supports both:

            ClamAVFileScanner()

        for older single-scanner configuration, and:

            ClamAVFileScanner(
                instance_name="clamav_file_1",
                socket_path="/run/clamd.scan/clamd.sock"
            )

        for scanner pools.

        Unlike FILDES, this scanner does not pass an open file descriptor.
        clamd must be able to read the file path directly.
        """
        self.instance_name = instance_name

        self.socket_path = socket_path or getattr(
            config,
            "CLAMD_UNIX_SOCKET",
            None
        )

        self.host = (
            host
            if host is not None
            else getattr(config, "CLAMD_HOST", None)
        )

        self.port = (
            port
            if port is not None
            else getattr(config, "CLAMD_PORT", 3310)
        )

    def validate(self) -> None:
        """
        Validate ClamAV connection settings.

        Normal file scanning uses clamd scan_file().

        Unlike FILDES, this does not need an open file descriptor.
        clamd must be able to access/read the file path directly.
        """

        # Network clamd connection.
        if self.host:
            return

        # Unix socket clamd connection.
        if not self.socket_path:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"{self.instance_name}: CLAMD Unix socket is not configured."
                )
            )

        if not os.path.exists(self.socket_path):
            raise HTTPException(
                status_code=503,
                detail=(
                    f"{self.instance_name}: clamd unix socket not found: "
                    f"{self.socket_path}"
                )
            )

    def _clamd(self):
        """
        Return a clamd connection using either network socket or Unix socket.
        """

        if self.host:
            return pyclamd.ClamdNetworkSocket(
                host=self.host,
                port=self.port,
                timeout=30
            )

        return pyclamd.ClamdUnixSocket(
            filename=self.socket_path
        )

    def health(self) -> Dict[str, Any]:
        """
        Health check for this specific clamd instance.
        """

        try:
            self.validate()

            cd = self._clamd()

            if not cd.ping():
                raise RuntimeError("clamd ping failed")

            version = cd.version()

            result = {
                "ok": True,
                "engine": self.name,
                "scan_mode": self.scan_mode,
                "scanner_instance": self.instance_name,
                "clamd": version,
            }

            if self.host:
                result["host"] = self.host
                result["port"] = self.port
            else:
                result["socket"] = self.socket_path

            return result

        except HTTPException:
            raise

        except Exception as exc:
            logger.exception(
                "ClamAV file health check failed instance=%s",
                self.instance_name
            )

            raise HTTPException(
                status_code=503,
                detail=f"Virus scanner unavailable: {self.instance_name}"
            ) from exc

    def scan(
        self,
        file_path: str,
        fd: Optional[int] = None,
        filename: Optional[str] = None,
        size: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Scan using normal ClamAV file scanning.

        This sends the file path to clamd.
        clamd opens and scans the file itself.

        The fd argument is accepted for interface compatibility with the pool,
        but is not used by this scanner.
        """

        self.validate()

        if not file_path:
            raise HTTPException(
                status_code=500,
                detail=f"{self.instance_name}: file scanner requires a file path."
            )

        if not os.path.exists(file_path):
            raise HTTPException(
                status_code=500,
                detail=f"{self.instance_name}: file does not exist: {file_path}"
            )

        try:
            cd = self._clamd()
            raw_result = cd.scan_file(file_path)

            result = self._normalise_file_result(raw_result, file_path)

            response = {
                "ok": result.get("error") is None,
                "engine": self.name,
                "scan_mode": self.scan_mode,
                "scanner_instance": self.instance_name,
                "raw_result": raw_result,
                **result
            }

            if self.host:
                response["host"] = self.host
                response["port"] = self.port
            else:
                response["socket"] = self.socket_path
            return response
        except HTTPException:
            raise

        except Exception as exc:
            logger.exception(
                "ClamAV file scan failed instance=%s file_path=%s",
                self.instance_name,
                file_path
            )

            raise HTTPException(
                status_code=503,
                detail=f"Virus scanner file scan failed: {self.instance_name}"
            ) from exc

    def _normalise_file_result(
        self,
        result: Optional[Dict[str, Any]],
        file_path: str
    ) -> Dict[str, Any]:
        """
        Convert pyclamd scan_file() response into consistent JSON.

        pyclamd usually returns None for a clean file.

        For infected files it can return something like:

            {
                "/path/file.txt": ("FOUND", "Eicar-Test-Signature")
            }
        """

        if result is None:
            return {
                "clean": True,
                "infected": False,
                "virus": None,
                "error": None
            }

        if not result:
            return {
                "clean": False,
                "infected": False,
                "virus": None,
                "error": "Empty response from clamd"
            }

        file_result = result.get(file_path)

        if file_result is None:
            try:
                file_result = next(iter(result.values()))
            except StopIteration:
                return {
                    "clean": False,
                    "infected": False,
                    "virus": None,
                    "error": "Unable to parse clamd response"
                }

        status = None
        virus = None

        if isinstance(file_result, tuple):
            if len(file_result) >= 1:
                status = file_result[0]

            if len(file_result) >= 2:
                virus = file_result[1]
        else:
            status = str(file_result)

        if status == "FOUND":
            return {
                "clean": False,
                "infected": True,
                "virus": virus,
                "error": None
            }

        if status == "OK":
            return {
                "clean": True,
                "infected": False,
                "virus": None,
                "error": None
            }

        return {
            "clean": False,
            "infected": False,
            "virus": None,
            "error": str(file_result)
        }