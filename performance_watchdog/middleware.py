import time
import logging
from django.db import connection
from .conf import watchdog_settings
from .tasks import process_slow_request_incident

logger = logging.getLogger(__name__)


class PerformanceWatchdogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if not watchdog_settings.ENABLED or request.path.startswith(("/static/", "/media/", "/health/")):
            return self.get_response(request)

        initial_queries_len = len(connection.queries)
        start_time = time.perf_counter()

        response = self.get_response(request)

        duration = time.perf_counter() - start_time
        total_queries = len(connection.queries) - initial_queries_len

        if (
            duration > watchdog_settings.TIME_THRESHOLD
            or total_queries > watchdog_settings.QUERY_COUNT_THRESHOLD
        ):
            db_settings = connection.settings_dict
            remote_host = db_settings.get("HOST", "unknown")

            incident_data = {
                "path": request.path,
                "method": request.method,
                "duration_seconds": round(duration, 4),
                "query_count": total_queries,
                "remote_db_host": remote_host,
                "user_id": str(request.user.id) if hasattr(request, "user") and request.user.is_authenticated else "Anonymous",
            }

            if total_queries > 0:
                try:
                    recent_queries = connection.queries[initial_queries_len:]
                    incident_data["recent_queries"] = [q.get("sql", "")[:1000] for q in recent_queries[-5:]
]
                except Exception:
                    incident_data["recent_queries"] = []

            try:
                # Fire and forget into RabbitMQ via Celery (< 2ms)
                process_slow_request_incident.delay(incident_data)
            except Exception as exc:
                logger.error(f"Watchdog failed to dispatch incident payload to Celery: {exc}")

        return response

