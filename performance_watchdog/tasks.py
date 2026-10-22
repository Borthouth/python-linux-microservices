import logging

from celery import shared_task

logger = logging.getLogger("performance_watchdog")


@shared_task
def process_slow_request_incident(incident_data):
    path = incident_data.get("path")
    duration = incident_data.get("duration_seconds")
    query_count = incident_data.get("query_count")
    remote_host = incident_data.get("remote_db_host")
    recent_queries = incident_data.get("recent_queries", [])

    report = f"""
======================================================================
PERFORMANCE WATCHDOG INCIDENT
======================================================================

Request Path: {path}
HTTP Method: {incident_data.get('method')}
Total Execution Duration: {duration} seconds
Total Database Queries: {query_count}
Remote Database Host: {remote_host}
User ID: {incident_data.get('user_id')}

Recent SQL Queries Executed:
{chr(10).join([f' - {q}' for q in recent_queries]) if recent_queries else ' - None recorded'}

======================================================================
"""
    logger.warning(report)