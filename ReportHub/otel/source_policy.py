SOURCE_CHOICES = (
    ('fluent', 'Fluent Forward transport', 'Master switch for all forwarded container output.'),
    ('django', 'Django', 'ReportHub web application output, excluding ETL events.'),
    ('celery', 'Celery', 'Workers and Beat, including Operations workers, excluding ETL events.'),
    ('redis', 'Redis', 'Redis container output.'),
    ('postgres', 'PostgreSQL', 'ReportHub PostgreSQL container output.'),
    ('minio', 'MinIO', 'Object storage container output.'),
    ('ollama', 'Ollama', 'Local AI service container output.'),
    ('kafka', 'Kafka', 'CDC broker container output.'),
    ('debezium', 'Debezium', 'CDC connector container output.'),
    ('scanhub', 'ScanHub API', 'ScanHub container output, not ClamAV daemon output.'),
    ('other', 'Other containers', 'Forwarded containers not matched above.'),
    ('journal', 'Host/system', 'Host journal events, excluding ClamAV daemon and updater output.'),
    ('etl', 'ETL', 'Structured ETL events from container output; local files are not ingested.'),
    ('clamav', 'ClamAV', 'ClamAV daemon and freshclam output from the host journal.'),
    ('otlp', 'OTLP logs', 'Logs submitted directly through the OTLP endpoints.'),
)

SOURCE_CATEGORIES = tuple((key, label) for key, label, _ in SOURCE_CHOICES if key != 'fluent')
CONTAINER_SOURCE_PATTERNS = {
    'django': r'^reporthub\.reporthub-app$',
    'celery': r'^reporthub\.reporthub[-_]celery',
    'redis': r'^reporthub\.reporthub-redis$',
    'postgres': r'^reporthub\.reporthub-postgres$',
    'minio': r'^reporthub\.reporthub-minio$',
    'ollama': r'^reporthub\.reporthub-ollama$',
    'kafka': r'^reporthub\.reporthub-kafka$',
    'debezium': r'^reporthub\.reporthub-debezium-connect$',
    'scanhub': r'^scanhub\.scanhub-api$',
}
ETL_LOGGER_NAME = 'reporthub_etl'
CLAMAV_JOURNAL_COMMANDS = ('clamd', 'freshclam')


def default_sources():
    return [key for key, _, _ in SOURCE_CHOICES]


def filter_conditions(enabled):
    if not isinstance(enabled, list) or any(
        not isinstance(key, str) or key not in default_sources() for key in enabled
    ):
        raise ValueError('Unknown log source selection.')

    tag = 'attributes["fluent.tag"]'
    container = f'{tag} != nil'
    etl = f'({container} and attributes["logger.name"] == "{ETL_LOGGER_NAME}")'
    journal = 'attributes["reporthub.input"] == "journal"'
    clamav_commands = ' or '.join(
        f'attributes["journald._COMM"] == "{command}"' for command in CLAMAV_JOURNAL_COMMANDS
    )
    clamav = f'({journal} and ({clamav_commands}))'
    path = 'attributes["log.file.path"]'
    patterns = CONTAINER_SOURCE_PATTERNS

    def matches(pattern):
        # OTTL string literals require escaped backslashes.
        import json
        return f'IsMatch({tag}, {json.dumps(pattern)})'

    conditions = [
        f'{container} and IsMatch({tag}, "(?i)(clickhouse|otel[-_]collector)")',
        f'{path} != nil and (IsMatch({path}, "etl_execution[.]log$") or IsMatch({path}, "/clamav/"))',
    ]
    if 'fluent' not in enabled:
        conditions.append(container)
    for key, pattern in patterns.items():
        if key not in enabled:
            conditions.append(f'{container} and {matches(pattern)} and not {etl}')
    if 'other' not in enabled:
        known = '|'.join(f'({pattern})' for pattern in patterns.values())
        conditions.append(f'{container} and not {matches(known)} and not {etl}')
    if 'journal' not in enabled:
        conditions.append(f'{journal} and not {clamav}')
    if 'etl' not in enabled:
        conditions.append(etl)
    if 'clamav' not in enabled:
        conditions.append(clamav)
    if 'otlp' not in enabled:
        conditions.append(
            f'{tag} == nil and {path} == nil and attributes["reporthub.input"] != "journal"'
        )
    return conditions
