import os
import socket
import struct
import logging
from typing import Optional, Dict, Any

import pyclamd
from fastapi import HTTPException

import config

from scanners.base import BaseScanner


logger = logging.getLogger("scan_api")


class ClamAVFildesScanner(BaseScanner):
    name = "clamav"
    scan_mode = "fildes"

    def __init__(
        self,
        instance_name: str = "clamav_fildes",
        socket_path: Optional[str] = None,
    ):
        """
        ClamAV FILDES scanner.

        For pooled usage, factory.py should pass socket_path from SCANNER_POOLS.

        Example:
            ClamAVFildesScanner(
                instance_name="clamav_scan_1",
                socket_path="/run/clamd.scan/clamd.sock"
            )

        A fallback to config.CLAMD_UNIX_SOCKET is kept for older single-scanner
        setups, but it is optional.
        """
        self.instance_name = instance_name
        self.socket_path = socket_path or getattr(config, "CLAMD_UNIX_SOCKET", None)

    def validate(self) -> None:
        """
        FILDES requires a local Unix socket.

        It will not work with CLAMD_HOST/CLAMD_PORT because SCM_RIGHTS file
        descriptor passing is only supported over Unix domain sockets.
        """
        clamd_host = getattr(config, "CLAMD_HOST", None)

        if clamd_host:
            raise HTTPException(
                status_code=500,
                detail=(
                    "ClamAV FILDES scanning requires CLAMD_HOST to be empty "
                    "and a Unix socket to be used."
                ),
            )

        if not self.socket_path:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"{self.instance_name}: ClamAV Unix socket is not configured. "
                    "Set socket in SCANNER_POOLS or define CLAMD_UNIX_SOCKET."
                ),
            )

        if not os.path.exists(self.socket_path):
            raise HTTPException(
                status_code=503,
                detail=f"{self.instance_name}: clamd unix socket not found: {self.socket_path}",
            )

    def health(self) -> Dict[str, Any]:
        """
        Health check for this specific clamd instance.
        """
        try:
            self.validate()

            cd = pyclamd.ClamdUnixSocket(filename=self.socket_path)

            if not cd.ping():
                raise RuntimeError("clamd ping failed")

            version = cd.version()

            return {
                "ok": True,
                "engine": self.name,
                "scan_mode": self.scan_mode,
                "scanner_instance": self.instance_name,
                "socket": self.socket_path,
                "clamd": version,
            }

        except HTTPException:
            raise

        except Exception as exc:
            logger.exception(
                "ClamAV health check failed instance=%s socket=%s",
                self.instance_name,
                self.socket_path,
            )
            raise HTTPException(
                status_code=503,
                detail=f"Virus scanner unavailable: {self.instance_name}",
            ) from exc

    def scan(
        self,
        file_path: str,
        fd: Optional[int] = None,
        filename: Optional[str] = None,
        size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Scan using ClamAV FILDES.

        The file descriptor must still be open when this method is called.

        Performance note:
        This keeps the fast FILDES path. The file content is not copied through
        Python into clamd. Instead, the already-open file descriptor is passed
        to clamd over the Unix socket using SCM_RIGHTS.
        """
        self.validate()

        if fd is None:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"{self.instance_name}: ClamAV FILDES scanner requires "
                    "an open file descriptor."
                ),
            )

        raw_result = self._scan_fd_with_clamd(self.socket_path, fd)
        result = self._normalise_fildes_result(raw_result)

        return {
            "ok": result.get("error") is None,
            "engine": self.name,
            "scan_mode": self.scan_mode,
            "scanner_instance": self.instance_name,
            "socket": self.socket_path,
            "raw_result": raw_result,
            **result,
        }

    def _scan_fd_with_clamd(self, clamd_socket_path: str, fd: int) -> str:
        """
        Scan an already-open file descriptor using clamd FILDES.
        """
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        try:
            sock.connect(clamd_socket_path)

            # z-command form: command is null-terminated and response is null-terminated.
            sock.sendall(b"zFILDES\0")

            ancdata = [
                (
                    socket.SOL_SOCKET,
                    socket.SCM_RIGHTS,
                    struct.pack("i", fd),
                )
            ]

            # Send one byte along with the file descriptor.
            sock.sendmsg([b"\0"], ancdata)

            response_chunks = []

            while True:
                chunk = sock.recv(4096)

                if not chunk:
                    break

                response_chunks.append(chunk)

            response = b"".join(response_chunks)

            return response.decode("utf-8", errors="replace").strip().strip("\0")

        except Exception as exc:
            logger.exception(
                "ClamAV FILDES scan failed instance=%s socket=%s",
                self.instance_name,
                clamd_socket_path,
            )
            raise HTTPException(
                status_code=503,
                detail=f"Virus scanner FILDES scan failed: {self.instance_name}",
            ) from exc

        finally:
            sock.close()

    def _normalise_fildes_result(self, result: str) -> Dict[str, Any]:
        """
        Convert clamd FILDES response into consistent JSON.

        Examples:
            stream: OK
            stream: Eicar-Test-Signature FOUND
            stream: Access denied ERROR
        """
        if not result:
            return {
                "clean": False,
                "infected": False,
                "virus": None,
                "error": "No response from clamd",
            }

        if result.endswith("OK"):
            return {
                "clean": True,
                "infected": False,
                "virus": None,
                "error": None,
            }

        if result.endswith("FOUND"):
            virus = result

            if ":" in virus:
                virus = virus.split(":", 1)[1]

            virus = virus.rsplit(" FOUND", 1)[0].strip()

            return {
                "clean": False,
                "infected": True,
                "virus": virus,
                "error": None,
            }

        if result.endswith("ERROR"):
            detail = result

            if ":" in detail:
                detail = detail.split(":", 1)[1]

            detail = detail.rsplit(" ERROR", 1)[0].strip()

            return {
                "clean": False,
                "infected": False,
                "virus": None,
                "error": detail or "Scan error",
            }

        return {
            "clean": False,
            "infected": False,
            "virus": None,
            "error": result,
        }