from abc import ABC, abstractmethod
from typing import Optional, Dict, Any


class BaseScanner(ABC):
    """
    Common interface for all virus scanners.

    Every scanner implementation should return a dictionary like:

    {
        "ok": True,
        "clean": True,
        "infected": False,
        "virus": None,
        "error": None,
        "engine": "clamav",
        "scan_mode": "fildes",
        "raw_result": "stream: OK"
    }
    """

    name: str = "unknown"
    scan_mode: str = "unknown"

    @abstractmethod
    def validate(self) -> None:
        """
        Validate scanner configuration before use.

        Should raise an exception if the scanner is unavailable or misconfigured.
        """
        pass

    @abstractmethod
    def health(self) -> Dict[str, Any]:
        """
        Return scanner health information.
        """
        pass

    @abstractmethod
    def scan(
        self,
        file_path: str,
        fd: Optional[int] = None,
        filename: Optional[str] = None,
        size: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Scan a file.

        file_path:
            Path to the temporary uploaded file.

        fd:
            Optional open file descriptor.

        Some scanners, like ClamAV FILDES, use fd.
        Other scanners may ignore fd and scan by file_path.
        """
        pass