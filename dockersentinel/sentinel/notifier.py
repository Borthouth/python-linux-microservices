import logging
import httpx
from .config import settings

logger = logging.getLogger(__name__)


def dispatch_alerts_if_needed(summary_payload: dict):
    if not settings.WEBHOOK_URL:
        return

    try:
        flagged = summary_payload["containers"]["flagged_count"]
        host = summary_payload["host_metrics"]
        
        message = {
            "content": f"🚨 **DockerSentinel Alert Triggered!**\n"
                       f"System Status: **{summary_payload['overall_status']}**\n"
                       f"Flagged Containers: {flagged}\n"
                       f"RAM Usage: {host['ram']['percent_used']}%\n"
                       f"Disk Usage: {host['disk']['percent_used']}%"
        }

        with httpx.Client() as client:
            response = client.post(settings.WEBHOOK_URL, json=message, timeout=5.0)
            response.raise_for_status()
    except Exception as exc:
        logger.error(f"Failed to dispatch alert webhook: {exc}")