import threading
from contextlib import contextmanager
from typing import List, Dict, Any, Optional


class ScannerPool:
    """
    Generic scanner pool.

    Supported strategies:

        least_connections
        round_robin
        first

    The pool itself knows nothing about ClamAV, Sophos, ICAP,
    REST APIs, sockets, or vendor-specific behaviour.

    A scanner only needs:

        scanner.scan(...)
        scanner.health()
    """

    def __init__(self, scanners: List[object], strategy: str = "least_connections"):
        if not scanners:
            raise ValueError("Scanner pool must contain at least one scanner")

        self.scanners = scanners
        self.strategy = strategy.lower().strip()

        self._lock = threading.Lock()

        self._active_counts = {
            self._instance_name(scanner): 0
            for scanner in self.scanners
        }

        self._next_index = 0

    def _instance_name(self, scanner: object) -> str:
        return getattr(
            scanner,
            "instance_name",
            getattr(scanner, "name", "unknown")
        )

    def _select_first(self) -> object:
        return self.scanners[0]

    def _select_round_robin(self) -> object:
        with self._lock:
            scanner = self.scanners[self._next_index]

            self._next_index = (
                self._next_index + 1
            ) % len(self.scanners)

            return scanner

    def _select_least_connections(self) -> object:
        with self._lock:
            scanner_count = len(self.scanners)

            best_scanner = None
            best_count = None
            best_index = None

            for offset in range(scanner_count):
                index = (self._next_index + offset) % scanner_count

                scanner = self.scanners[index]
                scanner_name = self._instance_name(scanner)

                active_count = self._active_counts.get(
                    scanner_name,
                    0
                )

                if best_count is None or active_count < best_count:
                    best_scanner = scanner
                    best_count = active_count
                    best_index = index

            selected_name = self._instance_name(best_scanner)

            self._active_counts[selected_name] += 1

            # Rotate tie-breaking so equal-load scanners don't
            # always favour scanner[0].
            self._next_index = (
                best_index + 1
            ) % scanner_count

            return best_scanner

    def _release(self, scanner: object) -> None:
        scanner_name = self._instance_name(scanner)

        with self._lock:
            if self._active_counts.get(scanner_name, 0) > 0:
                self._active_counts[scanner_name] -= 1

    @contextmanager
    def acquire_scanner(self):

        if self.strategy == "least_connections":
            scanner = self._select_least_connections()

        elif self.strategy == "round_robin":
            scanner = self._select_round_robin()

        elif self.strategy == "first":
            scanner = self._select_first()

        else:
            raise ValueError(
                f"Unsupported scanner pool strategy: {self.strategy}"
            )

        try:
            yield scanner

        finally:
            if self.strategy == "least_connections":
                self._release(scanner)

    def scan(
        self,
        file_path: str,
        fd: Optional[int] = None,
        filename: Optional[str] = None,
        size: Optional[int] = None,
    ) -> Dict[str, Any]:

        with self.acquire_scanner() as scanner:

            result = scanner.scan(
                file_path=file_path,
                fd=fd,
                filename=filename,
                size=size,
            )

            result["scanner_instance"] = self._instance_name(scanner)
            result["pool_strategy"] = self.strategy

            return result

    def status(self) -> Dict[str, Any]:
        """
        Lightweight pool status.

        Unlike health(), this does not call scanner.health().
        It only reports the pool strategy, configured scanner instances,
        and current in-process active connection counters.
        """
        with self._lock:
            active_counts = dict(self._active_counts)

        instances = []

        for scanner in self.scanners:
            scanner_name = self._instance_name(scanner)

            instances.append({
                "scanner_instance": scanner_name,
                "engine": getattr(scanner, "name", "unknown"),
                "scan_mode": getattr(scanner, "scan_mode", "unknown"),
                "active_connections": active_counts.get(scanner_name, 0),
            })

        return {
            "pool_strategy": self.strategy,
            "scanner_count": len(self.scanners),
            "instances": instances,
        }

    def health(self) -> Dict[str, Any]:

        instances = []
        overall_ok = True

        with self._lock:
            active_counts = dict(self._active_counts)

        for scanner in self.scanners:

            scanner_name = self._instance_name(scanner)

            try:
                result = scanner.health()

                result["scanner_instance"] = scanner_name
                result["active_connections"] = active_counts.get(
                    scanner_name,
                    0
                )

                instances.append(result)

            except Exception as exc:

                overall_ok = False

                instances.append({
                    "ok": False,
                    "scanner_instance": scanner_name,
                    "active_connections": active_counts.get(
                        scanner_name,
                        0
                    ),
                    "error": str(exc),
                })

        return {
            "ok": overall_ok,
            "pool_strategy": self.strategy,
            "scanner_count": len(self.scanners),
            "instances": instances,
        }