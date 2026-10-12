import pytest
import httpx

from sdk.scanhub import (
    ScanHubClient,
    ScanHubAPIError,
    ScanHubConnectionError,
)


def make_response(mocker, status_code=200, json_data=None, text=""):
    response = mocker.Mock()
    response.status_code = status_code
    response.text = text
    response.json.return_value = json_data if json_data is not None else {}
    return response

def test_connection_error_raises_scanhub_connection_error(mocker):
    client = ScanHubClient(
        api_url="https://test.local",
        api_key="secret",
        retries=0,
    )

    mocker.patch(
        "sdk.scanhub.httpx.Client.request",
        side_effect=httpx.ConnectError("Connection failed"),
    )

    with pytest.raises(ScanHubConnectionError):
        client.health()

def test_timeout_raises_scanhub_connection_error(mocker):
    client = ScanHubClient(
        api_url="https://test.local",
        api_key="secret",
        retries=0,
    )

    mocker.patch(
        "sdk.scanhub.httpx.Client.request",
        side_effect=httpx.TimeoutException("Connection timed out"),
    )

    with pytest.raises(ScanHubConnectionError):
        client.health()

def test_retries_on_503_then_succeeds(mocker):
    client = ScanHubClient(
        api_url="https://test.local",
        api_key="secret",
        retries=1,
    )

    response_503 = make_response(
        mocker,
        status_code=503,
        json_data={"detail": "Service unavailable"},
    )

    response_200 = make_response(
        mocker,
        status_code=200,
        json_data={"ok": True},
    )

    mock_request = mocker.patch(
        "sdk.scanhub.httpx.Client.request",
        side_effect=[response_503, response_200],
    )

    result = client.health()

    assert result["ok"] is True
    assert mock_request.call_count == 2

def test_no_retry_on_401(mocker):
    client = ScanHubClient(
        api_url="https://test.local",
        api_key="secret",
        retries=5,
    )

    response = make_response(
        mocker,
        status_code=401,
        json_data={
            "detail": "Unauthorized",
        },
    )

    mock_request = mocker.patch(
        "sdk.scanhub.httpx.Client.request",
        return_value=response,
    )

    with pytest.raises(ScanHubAPIError):
        client.health()

    assert mock_request.call_count == 1

def test_health_success(mocker):
    client = ScanHubClient(
        api_url="https://test.local",
        api_key="secret"
    )

    response = make_response(
        mocker,
        status_code=200,
        json_data={
            "ok": True,
            "pool_strategy": "least_connections",
            "scanner_count": 2,
            "instances": [
                {
                    "ok": True,
                    "engine": "clamav",
                    "scan_mode": "fildes",
                    "scanner_instance": "clamav_scan_1",
                    "active_connections": 0,
                },
                {
                    "ok": True,
                    "engine": "clamav",
                    "scan_mode": "fildes",
                    "scanner_instance": "clamav_scan_2",
                    "active_connections": 0,
                },
            ],
            "version": "1.0.0",
        },
    )

    mock_request = mocker.patch(
        "sdk.scanhub.httpx.Client.request",
        return_value=response
    )

    result = client.health()

    assert result["ok"] is True
    assert result["pool_strategy"] == "least_connections"
    assert result["scanner_count"] == 2
    assert len(result["instances"]) == 2

    mock_request.assert_called_once()
    assert mock_request.call_args.kwargs["method"] == "get"
    assert mock_request.call_args.kwargs["url"] == "https://test.local/health"