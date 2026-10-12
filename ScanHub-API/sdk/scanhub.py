import os
import time
import httpx


class ScanHubError(Exception):
    """Base ScanHub exception."""


class ScanHubAPIError(ScanHubError):
    """API returned an error."""


class ScanHubConnectionError(ScanHubError):
    """Unable to connect to ScanHub."""


class ScanHubClient:
    def __init__(
        self,
        api_url: str,
        api_key: str,
        timeout: int = 300,
        verify_ssl: bool = True,
        retries: int = None,
        retry_delay: float = None,
    ):
        self.api_url = api_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        # If not supplied, default to 1 retry and 1 second delay.
        self.retries = 1 if retries is None else retries
        self.retry_delay = 1 if retry_delay is None else retry_delay

        # HTTP2 with HTTP1 fallback
        self.client = httpx.Client(
            http2=True,
            http1=True,
            verify=self.verify_ssl,
            timeout=self.timeout,
        )

    def health(self):
        response = self._request_with_retries(
            method="get",
            url=f"{self.api_url}/health",
        )

        return self._handle_response(response)

    def scan_file(self, file_path: str):
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)

        def request_factory():
            with open(file_path, "rb") as fh:
                return self.client.post(
                    f"{self.api_url}/scan",
                    headers={
                        "X-API-Key": self.api_key
                    },
                    files={
                        "file": (os.path.basename(file_path), fh)
                    },
                )

        response = self._request_with_retries_from_factory(
            request_factory
        )

        return self._handle_response(response)

    def scan_bytes(self, data: bytes, filename: str = "upload.bin"):
        response = self._request_with_retries(
            method="post",
            url=f"{self.api_url}/scan",
            headers={
                "X-API-Key": self.api_key
            },
            files={
                "file": (filename, data)
            },
        )

        return self._handle_response(response)

    def scan_stream(self, data: bytes, filename: str = "upload.bin"):
        response = self._request_with_retries(
            method="post",
            url=f"{self.api_url}/scan/stream",
            headers={
                "X-API-Key": self.api_key,
                "X-Filename": filename,
                "Content-Type": "application/octet-stream",
            },
            data=data,
        )

        return self._handle_response(response)

    def is_clean(self, file_path: str) -> bool:
        result = self.scan_file(file_path)
        return result.get("clean", False)

    def _request_with_retries(self, method: str, url: str, **kwargs):
        """
        Perform a request with retry support.

        Retries are attempted only for:
            - ConnectionError
            - Timeout
            - HTTP 502
            - HTTP 503
            - HTTP 504

        Retries are not attempted for:
            - 400 Bad Request
            - 401 Unauthorized
            - 403 Forbidden
            - 404 Not Found
            - virus detections
            - scan errors returned in JSON
        """

        def request_factory():
            return self.client.request(
                method=method,
                url=url,
                **kwargs,
            )

        return self._request_with_retries_from_factory(
            request_factory
        )

    def _request_with_retries_from_factory(self, request_factory):
        attempts = self.retries + 1
        last_exception = None

        for attempt in range(1, attempts + 1):
            try:
                response = request_factory()

                if response.status_code in (502, 503, 504):
                    if attempt < attempts:
                        time.sleep(self.retry_delay)
                        continue

                return response

            except (
                httpx.ConnectError,
                httpx.TimeoutException,
            ) as exc:
                last_exception = exc

                if attempt < attempts:
                    time.sleep(self.retry_delay)
                    continue

                raise ScanHubConnectionError(str(exc)) from exc

            except httpx.HTTPError as exc:
                raise ScanHubConnectionError(str(exc)) from exc

        raise ScanHubConnectionError(str(last_exception))

    @staticmethod
    def _handle_response(response):
        try:
            payload = response.json()
        except Exception:
            payload = {
                "ok": False,
                "detail": response.text,
            }

        if response.status_code >= 400:
            raise ScanHubAPIError(
                f"{response.status_code}: "
                f"{payload.get('detail', 'Unknown error')}"
            )

        return payload

    def close(self):
        """
        Close the underlying HTTP client and release connections.
        """
        self.client.close()


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()