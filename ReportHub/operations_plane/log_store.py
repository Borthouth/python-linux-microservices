import logging
from datetime import timedelta

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError
from django.conf import settings
from django.utils import timezone

from otel.source_policy import CLAMAV_JOURNAL_COMMANDS, CONTAINER_SOURCE_PATTERNS, ETL_LOGGER_NAME


logger = logging.getLogger(__name__)

LOGS_TABLE = 'operations.otel_logs'
MAX_SEARCH_RESULTS = 25
MAX_ANALYSIS_SAMPLES = 300

SEARCH_SETTINGS = {
    'max_execution_time': 3,
    'max_rows_to_read': 250_000,
    'max_bytes_to_read': 67_108_864,
    'max_threads': 2,
}
ANALYSIS_SETTINGS = {
    'max_execution_time': 60,
    'max_rows_to_read': 2_000_000,
    'max_bytes_to_read': 268_435_456,
    'max_threads': 2,
}
HISTORICAL_SEARCH_SETTINGS = {
    'max_execution_time': 30,
    'max_rows_to_read': 1_000_000,
    'max_bytes_to_read': 134_217_728,
    'max_threads': 2,
}

SOURCE_PARAMETERS = {
    **{f'source_{key}_pattern': pattern for key, pattern in CONTAINER_SOURCE_PATTERNS.items()},
    'source_etl_logger': ETL_LOGGER_NAME,
    'source_clamav_commands': list(CLAMAV_JOURNAL_COMMANDS),
}
_container_cases = ', '.join(
    f"match(LogAttributes['fluent.tag'], {{source_{key}_pattern:String}}), '{key}'"
    for key in CONTAINER_SOURCE_PATTERNS
)
SOURCE_CATEGORY_SQL = f"""multiIf(
    LogAttributes['fluent.tag'] != '' AND LogAttributes['logger.name'] = {{source_etl_logger:String}}, 'etl',
    LogAttributes['fluent.tag'] != '', multiIf({_container_cases}, 'other'),
    LogAttributes['reporthub.input'] = 'journal' AND has({{source_clamav_commands:Array(String)}}, LogAttributes['journald._COMM']), 'clamav',
    LogAttributes['reporthub.input'] = 'journal' OR LogAttributes['journald._SYSTEMD_UNIT'] != '', 'journal',
    endsWith(LogAttributes['log.file.path'], 'etl_execution.log'), 'etl',
    position(LogAttributes['log.file.path'], '/clamav/') > 0, 'clamav',
    'otlp')"""
LOG_COLUMNS = f"""
    toString(event_id), toDateTime64(Timestamp, 6, 'UTC'), ingested_at,
    ServiceName, SeverityText, Body,
    LogAttributes['task_id'], LogAttributes['task_name'],
    LogAttributes['queue'], LogAttributes['worker'], LogAttributes['source'],
    {SOURCE_CATEGORY_SQL},
    LogAttributes['partial_id'], LogAttributes['partial_ordinal'], LogAttributes['partial_last'],
    LogAttributes['logger.name']
"""
LOG_FIELDS = (
    'event_id', 'event_time', 'ingested_at', 'service', 'severity', 'message',
    'task_id', 'task_name', 'queue', 'worker', 'source', 'source_category',
    'partial_id', 'partial_ordinal', 'partial_last', 'logger',
)


class LogStoreUnavailable(Exception):
    pass


def _client():
    return clickhouse_connect.get_client(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_PORT,
        username=settings.OPERATIONS_LOGS_CLICKHOUSE_USER,
        password=settings.OPERATIONS_LOGS_CLICKHOUSE_PASSWORD,
        connect_timeout=3,
        send_receive_timeout=65,
    )


def _where(filters, time_column='Timestamp'):
    clauses = [
        f"{time_column} >= {{range_start:DateTime64(9, 'UTC')}}",
        f"{time_column} < {{range_end:DateTime64(9, 'UTC')}}",
    ]
    parameters = {
        'range_start': filters['range_start'],
        'range_end': filters['range_end'],
        **SOURCE_PARAMETERS,
    }

    if filters.get('source'):
        clauses.append(f'{SOURCE_CATEGORY_SQL} = {{source:String}}')
        parameters['source'] = filters['source']
    if filters.get('service'):
        clauses.append('ServiceName = {service:String}')
        parameters['service'] = filters['service']
    if filters.get('severity'):
        clauses.append('SeverityText = {severity:String}')
        parameters['severity'] = filters['severity']
    if filters.get('queue'):
        clauses.append("LogAttributes['queue'] = {queue:String}")
        parameters['queue'] = filters['queue']
    if filters.get('task_name'):
        clauses.append("LogAttributes['task_name'] = {task_name:String}")
        parameters['task_name'] = filters['task_name']
    if filters.get('task_id'):
        clauses.append("LogAttributes['task_id'] = {task_id:String}")
        parameters['task_id'] = filters['task_id']
    if filters.get('worker'):
        clauses.append("LogAttributes['worker'] = {worker:String}")
        parameters['worker'] = filters['worker']
    if filters.get('text'):
        clauses.append('positionCaseInsensitive(Body, {text:String}) > 0')
        parameters['text'] = filters['text']

    return ' AND '.join(clauses), parameters


def _query(client, sql, parameters, settings):
    try:
        return client.query(sql, parameters=parameters, settings=settings)
    except (ClickHouseError, OSError, TimeoutError) as exc:
        logger.warning('ClickHouse operational log query failed: %s', type(exc).__name__)
        raise LogStoreUnavailable from exc


def search_logs(
    filters,
    before=None,
    after=None,
    limit=MAX_SEARCH_RESULTS,
    query_settings=None,
):
    where, parameters = _where(filters)
    if before:
        where += (
            " AND (toDateTime64(Timestamp, 6, 'UTC'), event_id) < "
            "({before_time:DateTime64(6, 'UTC')}, {before_id:UUID})"
        )
        parameters['before_time'], parameters['before_id'] = before
    if after:
        where += (
            " AND (toDateTime64(Timestamp, 6, 'UTC'), event_id) > "
            "({after_time:DateTime64(6, 'UTC')}, {after_id:UUID})"
        )
        parameters['after_time'], parameters['after_id'] = after

    result_limit = min(max(int(limit), 1), MAX_SEARCH_RESULTS)
    direction = 'ASC' if after else 'DESC'
    client = None
    try:
        client = _client()
        result = _query(
            client,
            f'''
                SELECT
                    {LOG_COLUMNS}
                FROM {LOGS_TABLE}
                WHERE {where}
                ORDER BY
                    toDateTime64(Timestamp, 6, 'UTC') {direction},
                    event_id {direction}
                LIMIT {result_limit + 1}
            ''',
            parameters,
            query_settings or SEARCH_SETTINGS,
        )
    except (ClickHouseError, OSError, TimeoutError) as exc:
        logger.warning('ClickHouse operational log connection failed: %s', type(exc).__name__)
        raise LogStoreUnavailable from exc
    finally:
        if client is not None:
            client.close()

    rows = [dict(zip(LOG_FIELDS, row)) for row in result.result_rows]
    page_rows = rows[:result_limit]
    if after:
        page_rows.reverse()
    return {
        'rows': page_rows,
        'has_more': len(rows) > result_limit,
    }


def recent_logs(filters=None, limit=25, since=None):
    now = timezone.now()
    filters = {
        **(filters or {}),
        'range_start': max(now - timedelta(hours=24), since - timedelta(seconds=2)) if since else now - timedelta(hours=24),
        'range_end': now,
    }
    where, parameters = _where(filters, time_column='ingested_at')
    limit = min(max(int(limit), 1), 100)
    client = None
    try:
        client = _client()
        result = _query(
            client,
            f'''SELECT {LOG_COLUMNS} FROM {LOGS_TABLE} WHERE {where}
                ORDER BY ingested_at DESC, event_id DESC LIMIT {limit + 1}''',
            parameters, SEARCH_SETTINGS,
        )
    except (ClickHouseError, OSError, TimeoutError) as exc:
        logger.warning('Operations recent log query failed: %s', type(exc).__name__)
        raise LogStoreUnavailable from exc
    finally:
        if client is not None:
            client.close()
    return {
        'rows': [dict(zip(LOG_FIELDS, row)) for row in result.result_rows[:limit]],
        'has_more': len(result.result_rows) > limit,
    }


def log_event(event_id, event_time):
    client = None
    try:
        client = _client()
        result = _query(
            client,
            f'''SELECT {LOG_COLUMNS} FROM {LOGS_TABLE}
                WHERE Timestamp >= {{event_time:DateTime64(9, 'UTC')}}
                  AND Timestamp < {{event_end:DateTime64(9, 'UTC')}}
                  AND event_id = {{event_id:UUID}} LIMIT 1''',
            {
                **SOURCE_PARAMETERS, 'event_id': str(event_id),
                'event_time': event_time, 'event_end': event_time + timedelta(microseconds=1),
            },
            SEARCH_SETTINGS,
        )
        return dict(zip(LOG_FIELDS, result.result_rows[0])) if result.result_rows else None
    except (ClickHouseError, OSError, TimeoutError) as exc:
        logger.warning('Operations log event query failed: %s', type(exc).__name__)
        raise LogStoreUnavailable from exc
    finally:
        if client is not None:
            client.close()


def latest_ingestion_time():
    client = None
    try:
        client = _client()
        result = _query(
            client,
            f'SELECT max(ingested_at) FROM {LOGS_TABLE}',
            {},
            SEARCH_SETTINGS,
        )
    except (ClickHouseError, OSError, TimeoutError) as exc:
        logger.warning('ClickHouse operational log status query failed: %s', type(exc).__name__)
        raise LogStoreUnavailable from exc
    finally:
        if client is not None:
            client.close()
    return result.result_rows[0][0]


def analysis_log_data(filters):
    where, parameters = _where(filters)
    client = None
    try:
        client = _client()
        total_result = _query(
            client,
            f'SELECT count() FROM {LOGS_TABLE} WHERE {where}',
            parameters,
            ANALYSIS_SETTINGS,
        )
        breakdown_result = _query(
            client,
            f'''
                SELECT ServiceName, SeverityText, count()
                FROM {LOGS_TABLE}
                WHERE {where}
                GROUP BY ServiceName, SeverityText
                ORDER BY count() DESC, ServiceName, SeverityText
                LIMIT 30
            ''',
            parameters,
            ANALYSIS_SETTINGS,
        )
        sample_result = _query(
            client,
            f'''
                SELECT
                    toDateTime64(Timestamp, 6, 'UTC'),
                    ServiceName,
                    SeverityText,
                    Body,
                    LogAttributes['task_id'],
                    LogAttributes['task_name'],
                    LogAttributes['worker']
                FROM {LOGS_TABLE}
                WHERE {where}
                ORDER BY
                    multiIf(
                        SeverityText = 'CRITICAL', 0,
                        SeverityText = 'ERROR', 1,
                        SeverityText = 'WARNING', 2,
                        3
                    ),
                    Timestamp DESC,
                    event_id DESC
                LIMIT {MAX_ANALYSIS_SAMPLES}
            ''',
            parameters,
            ANALYSIS_SETTINGS,
        )
    except (ClickHouseError, OSError, TimeoutError) as exc:
        logger.warning('ClickHouse operational log analysis query failed: %s', type(exc).__name__)
        raise LogStoreUnavailable from exc
    finally:
        if client is not None:
            client.close()

    return {
        'total_events': int(total_result.result_rows[0][0]),
        'breakdown': [
            {'service': row[0], 'severity': row[1], 'count': int(row[2])}
            for row in breakdown_result.result_rows
        ],
        'samples': [
            {
                'event_time': row[0].isoformat(),
                'service': row[1],
                'severity': row[2],
                'message': row[3][:1000],
                'task_id': row[4] or '',
                'task_name': row[5] or '',
                'worker': row[6] or '',
            }
            for row in sample_result.result_rows
        ],
    }
