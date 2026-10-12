import io
import logging
import requests
from bawelocal.configs import system


class ScanApiError(Exception):
    pass


def call_scan_api(file_obj, filename, companyName):
    """
    Send content to the remote ClamAV API /scan endpoint.
    Returns parsed JSON response or raises ScanApiError.
    """
    try:
        response = requests.post(
            f"{system.CLAM_API_URL.rstrip('/')}/scan",
            headers={"X-API-Key": system.CLAM_API_KEY},
            files={"file": (filename, file_obj, "application/octet-stream")},
            timeout=getattr(system, "CLAM_API_TIMEOUT", 300),
            verify=getattr(system, "CLAM_VERIFY_HTTPS_CERT", True),
        )
        response.raise_for_status()
    except requests.RequestException:
        logging.critical(
            "There was an error connecting to the ClamAV API.",
            extra={"company": companyName},
            exc_info=True
        )
        raise ScanApiError("Could not connect to scan API")

    try:
        result = response.json()
    except ValueError:
        logging.critical(
            "ClamAV API returned a non-JSON response.",
            extra={"company": companyName},
            exc_info=True
        )
        raise ScanApiError("Invalid response from scan API")

    if not isinstance(result, dict):
        logging.critical(
            "ClamAV API returned an unexpected response structure: %r",
            result,
            extra={"company": companyName}
        )
        raise ScanApiError("Unexpected response structure from scan API")

    return result


def scanFile(companyName, fileName):
    errorCode = None
    virusFound = None

    try:
        with open(fileName, "rb") as f:
            result = call_scan_api(f, fileName, companyName)

        if not result.get("ok", False):
            logging.error(
                "Virus scan could not be performed via remote scan API. "
                "filename=%s error=%s",
                fileName,
                result.get("error") or result.get("detail"),
                extra={"company": companyName}
            )
            return errorCode, virusFound

        if result.get("infected"):
            virusFound = result.get("virus")
            errorCode = "E-20101"
            logging.warning(
                "Virus '%s' found when scanning file content for company: %s, filename: %s",
                virusFound,
                companyName,
                fileName,
                extra={"company": companyName}
            )
        else:
            logging.info(
                "File scan clean for company: %s, filename: %s",
                companyName,
                fileName,
                extra={"company": companyName}
            )

    except Exception:
        logging.exception(
            "Virus scan could not be performed via remote scan API",
            extra={"company": companyName}
        )

    return errorCode, virusFound


def scanString(companyName, stringToScan):
    errorCode = None
    virusFound = None

    try:
        if isinstance(stringToScan, str):
            stringToScan = stringToScan.encode("utf-8")

        file_obj = io.BytesIO(stringToScan)
        result = call_scan_api(file_obj, "payload.txt", companyName)

        if not result.get("ok", False):
            logging.error(
                "Virus scan could not be performed via remote scan API. "
                "error=%s",
                result.get("error") or result.get("detail"),
                extra={"company": companyName}
            )
            return errorCode, virusFound

        if result.get("infected"):
            virusFound = result.get("virus")
            errorCode = "E-20101"
            logging.warning(
                "Virus '%s' found when scanning file content for company: %s",
                virusFound,
                companyName,
                extra={"company": companyName}
            )
        else:
            logging.info(
                "String/content scan clean for company: %s",
                companyName,
                extra={"company": companyName}
            )

    except Exception:
        logging.exception(
            "Virus scan could not be performed via remote scan API",
            extra={"company": companyName}
        )

    return errorCode, virusFound