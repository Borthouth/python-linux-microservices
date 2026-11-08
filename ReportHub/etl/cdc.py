import json
import re
import time
from datetime import (
    date,
    datetime,
    time as datetime_time,
    timedelta,
    timezone as datetime_timezone,
)
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from django.conf import settings
from django.utils import timezone
from kafka import KafkaConsumer

from control_plane.models import (
    DbChoice,
    SourceField,
    SourceProfile,
    SourceTable,
    WarehouseDatasetMapping,
)
from reports.models import ReportDefinition
from reports.query_builder import (
    clickhouse_table_name,
    quote_clickhouse_identifier,
)


CDC_VERSION_COLUMN = "_cdc_version"
CDC_DELETED_COLUMN = "_cdc_deleted"
_SENSITIVE_CONNECTOR_CONFIGS = {
    "database.password",
    "mongodb.connection.string",
}
_MASKED_CONFIG_VALUES = {"********", "[hidden]"}


def _connector_name(profile):
    return f"reporthub-source-{profile.pk}"


def _oracle_identifier(identifier):
    if isinstance(identifier, str) and identifier == identifier.lower():
        return identifier.upper()
    return identifier


def _topic_name(profile, table):
    prefix = _connector_name(profile)
    if profile.db_type == DbChoice.MSSQL:
        return (
            f"{prefix}.{profile.database_name}.{table.schema_name}."
            f"{table.table_name}"
        )
    if profile.db_type == DbChoice.ORACLE:
        return (
            f"{prefix}.{_oracle_identifier(table.schema_name)}."
            f"{_oracle_identifier(table.table_name)}"
        )
    return f"{prefix}.{table.schema_name}.{table.table_name}"


def _qualified_table_name(profile, table):
    if profile.db_type == DbChoice.MYSQL:
        return f"{profile.database_name}.{table.table_name}"
    if profile.db_type == DbChoice.MSSQL:
        return (
            f"{profile.database_name}.{table.schema_name}."
            f"{table.table_name}"
        )
    if profile.db_type == DbChoice.MONGODB:
        return f"{profile.database_name}.{table.table_name}"
    if profile.db_type == DbChoice.ORACLE:
        return (
            f"{_oracle_identifier(table.schema_name)}."
            f"{_oracle_identifier(table.table_name)}"
        )
    return f"{table.schema_name}.{table.table_name}"


def _connector_table_list(profile, tables):
    return ",".join(
        re.escape(_qualified_table_name(profile, table))
        for table in tables
    )


def build_debezium_connector_config(profile, tables=None):
    """Build a Debezium connector config without logging source credentials."""
    tables = list(
        tables if tables is not None
        else SourceTable.objects.filter(source=profile)
    )
    keyed_tables = [table for table in tables if table.primary_key_fields]
    if not keyed_tables:
        raise ValueError(
            "CDC requires at least one source table with an introspected primary key."
        )

    common = {
        "tasks.max": "1",
        "topic.prefix": _connector_name(profile),
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snapshot.mode": "initial",
    }
    host = profile.host
    port = str(profile.port)
    username = profile.username
    password = profile.password or ""
    table_list = _connector_table_list(profile, keyed_tables)

    if profile.db_type == DbChoice.MYSQL:
        config = {
            **common,
            "decimal.handling.mode": "double",
            "time.precision.mode": "connect",
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": host,
            "database.port": port,
            "database.user": username,
            "database.password": password,
            "database.server.id": str(50000 + profile.pk),
            "database.include.list": profile.database_name,
            "table.include.list": table_list,
        }
    elif profile.db_type == DbChoice.POSTGRESQL:
        config = {
            **common,
            "decimal.handling.mode": "double",
            "time.precision.mode": "connect",
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": host,
            "database.port": port,
            "database.user": username,
            "database.password": password,
            "database.dbname": profile.database_name,
            "schema.include.list": ",".join(
                sorted({table.schema_name for table in keyed_tables})
            ),
            "table.include.list": table_list,
            "plugin.name": "pgoutput",
            "slot.name": f"reporthub_{profile.pk}",
            "publication.name": f"reporthub_{profile.pk}",
            "publication.autocreate.mode": "filtered",
            "slot.drop.on.stop": "false",
        }
    elif profile.db_type == DbChoice.MSSQL:
        config = {
            **common,
            "decimal.handling.mode": "double",
            "time.precision.mode": "connect",
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.hostname": host,
            "database.port": port,
            "database.user": username,
            "database.password": password,
            "database.names": profile.database_name,
            "table.include.list": table_list,
            "database.encrypt": "false",
        }
    elif profile.db_type == DbChoice.ORACLE:
        cdb_name = getattr(profile, "oracle_cdb_name", "") or ""
        pdb_name = getattr(profile, "oracle_pdb_name", "") or ""
        logminer_username = (
            getattr(profile, "oracle_logminer_username", "") or ""
        )
        logminer_password = (
            getattr(profile, "oracle_logminer_password", None) or ""
        )
        if bool(cdb_name) != bool(pdb_name):
            raise ValueError(
                "Oracle CDC requires both Oracle CDB and PDB names when either is configured."
            )
        if bool(logminer_username) != bool(logminer_password):
            raise ValueError(
                "Oracle CDC requires both LogMiner credentials when either is configured."
            )
        config = {
            **common,
            "decimal.handling.mode": "double",
            "time.precision.mode": "connect",
            "connector.class": "io.debezium.connector.oracle.OracleConnector",
            "database.hostname": host,
            "database.port": port,
            "database.user": logminer_username or username,
            "database.password": logminer_password or password,
            "database.dbname": cdb_name or profile.database_name,
            "schema.include.list": ",".join(
                sorted({
                    _oracle_identifier(table.schema_name)
                    for table in keyed_tables
                })
            ),
            "table.include.list": table_list,
            "database.connection.adapter": "logminer",
        }
        if pdb_name:
            config["database.pdb.name"] = pdb_name
    elif profile.db_type == DbChoice.MONGODB:
        auth = ""
        if username:
            auth = f"{quote(username, safe='')}:{quote(password, safe='')}@"
        connection_string = (
            f"mongodb://{auth}{host}:{port}/?"
            f"{urlencode({'authSource': 'admin'})}"
        )
        config = {
            **common,
            "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
            "mongodb.connection.string": connection_string,
            "database.include.list": profile.database_name,
            "collection.include.list": table_list,
            "capture.mode": "change_streams_update_full",
        }
    else:
        raise ValueError(f"CDC is not supported for database type {profile.db_type!r}.")

    if profile.db_type in {
        DbChoice.MYSQL,
        DbChoice.POSTGRESQL,
        DbChoice.MSSQL,
        DbChoice.ORACLE,
    }:
        config.update({
            "schema.history.internal.kafka.bootstrap.servers": (
                settings.KAFKA_CONNECT_BOOTSTRAP_SERVERS
            ),
            "schema.history.internal.kafka.topic": (
                f"reporthub.schema-history.{profile.pk}"
            ),
        })
    return config


def _connect_request(method, path, body=None):
    base_url = settings.KAFKA_CONNECT_URL.rstrip("/")
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = Request(
        f"{base_url}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=15) as response:
            response_body = response.read()
            return json.loads(response_body) if response_body else None
    except HTTPError as exc:
        if exc.code == 404:
            return None
        response_body = exc.read().decode("utf-8", errors="replace")
        detail = _connect_error_detail(response_body, body)
        message = (
            f"Kafka Connect returned HTTP {exc.code} for {method} {path}."
        )
        if detail:
            message = f"{message} Details: {detail}"
        raise RuntimeError(message) from exc
    except URLError as exc:
        raise RuntimeError(
            "Kafka Connect is unavailable; check KAFKA_CONNECT_URL and the "
            "Debezium Connect service."
        ) from exc


def _connect_error_detail(response_body, request_body):
    if not response_body:
        return ""
    try:
        parsed_body = json.loads(response_body)
    except (TypeError, ValueError):
        detail = response_body
    else:
        detail = (
            parsed_body.get("message", response_body)
            if isinstance(parsed_body, dict)
            else response_body
        )
        if not isinstance(detail, str):
            detail = json.dumps(detail, sort_keys=True)

    for secret in _connector_secrets(request_body):
        detail = detail.replace(secret, "[redacted]")
    return detail[:2000]


def _connector_secrets(value):
    secrets = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, str) and any(
                marker in str(key).lower()
                for marker in ("password", "connection.string")
            ):
                if item:
                    secrets.add(item)
            else:
                secrets.update(_connector_secrets(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            secrets.update(_connector_secrets(item))
    return secrets


def _connector_configs_match(existing, desired):
    for key, value in desired.items():
        existing_value = existing.get(key)
        if key in _SENSITIVE_CONNECTOR_CONFIGS and (
            existing_value is None
            or existing_value in _MASKED_CONFIG_VALUES
        ):
            continue
        if existing_value != value:
            return False
    return all(
        key in desired or key in _SENSITIVE_CONNECTOR_CONFIGS
        for key in existing
    )


def ensure_debezium_connector(profile):
    tables = list(profile.tables.all())
    keyed_tables = [table for table in tables if table.primary_key_fields]
    if not keyed_tables:
        return None

    name = _connector_name(profile)
    config = build_debezium_connector_config(profile, keyed_tables)
    existing = _connect_request("GET", f"/connectors/{name}/config")
    if existing is None:
        _connect_request(
            "POST",
            "/connectors",
            {"name": name, "config": config},
        )
    elif not _connector_configs_match(existing, config):
        _connect_request("PUT", f"/connectors/{name}/config", config)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status = _connect_request("GET", f"/connectors/{name}/status")
        if status:
            connector_state = status.get("connector", {}).get("state")
            task_states = [
                task.get("state") for task in status.get("tasks", [])
            ]
            if connector_state == "FAILED" or "FAILED" in task_states:
                raise RuntimeError(
                    f"Debezium connector {name} failed to start. "
                    "Check Kafka Connect and source database logs."
                )
            if connector_state == "RUNNING" and task_states and all(
                state == "RUNNING" for state in task_states
            ):
                return name
        time.sleep(1)
    raise RuntimeError(
        f"Debezium connector {name} did not become ready within 30 seconds."
    )


def _required_field_names(profile, table):
    required = set(table.primary_key_fields or [])
    for relation in profile.relationships.all():
        if (
            relation.from_schema == table.schema_name
            and relation.from_table == table.table_name
        ):
            required.add(relation.from_field)
        if (
            relation.to_schema == table.schema_name
            and relation.to_table == table.table_name
        ):
            required.add(relation.to_field)
    for report in ReportDefinition.objects.filter(source=profile):
        selected_columns = (report.config or {}).get("selectedColumns", [])
        selected_columns = (
            selected_columns if isinstance(selected_columns, list) else []
        )
        table_ids = {
            column.get("table_id")
            for column in selected_columns
            if isinstance(column, dict) and column.get("table_id") is not None
        }
        report_tables = {
            str(report_table.pk): report_table
            for report_table in SourceTable.objects.filter(
                source=profile,
                pk__in=table_ids,
            )
        }
        has_attributable_config = False
        for column in selected_columns:
            if not isinstance(column, dict):
                continue
            report_table = report_tables.get(str(column.get("table_id")))
            if report_table is None:
                continue
            has_attributable_config = True
            if report_table.pk == table.pk and column.get("field_name"):
                required.add(column["field_name"])
        if report.base_table_id == table.pk and not has_attributable_config:
            required.update(
                field for field in (report.selected_fields or [])
                if isinstance(field, str)
            )
    mappings = WarehouseDatasetMapping.objects.filter(
        source_profile=profile,
        table_name=table.table_name,
    )
    if mappings.exists():
        selected = set(
            mappings.filter(is_enabled=True).values_list("field_name", flat=True)
        )
        return selected | required
    return set(
        table.fields.filter(is_selected=True).values_list("field_name", flat=True)
    ) | required


def _normalize_value(value):
    if isinstance(value, dict):
        if set(value) == {"$oid"}:
            return str(value["$oid"])
        if set(value) == {"$date"}:
            date_value = value["$date"]
            if isinstance(date_value, (int, float)):
                return datetime.fromtimestamp(
                    date_value / 1000,
                    tz=datetime_timezone.utc,
                ).isoformat()
            return date_value
        if set(value) in ({"$numberInt"}, {"$numberLong"}):
            return int(next(iter(value.values())))
        if set(value) in ({"$numberDouble"}, {"$numberDecimal"}):
            return float(next(iter(value.values())))
        return json.dumps(value, default=str)
    if isinstance(value, list):
        return json.dumps(value, default=str)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _coerce_cdc_row(row, table, field_types):
    from etl.tasks import map_to_clickhouse_type

    for field_name, value in list(row.items()):
        if field_name in {CDC_VERSION_COLUMN, CDC_DELETED_COLUMN} or value is None:
            continue
        source_type = field_types.get(field_name, "").lower()
        clickhouse_type = map_to_clickhouse_type(
            field_types.get(field_name, ""),
            is_nullable=field_name not in table.primary_key_fields,
        ).lower()
        if isinstance(value, (int, float)) and "timestamp" in source_type:
            row[field_name] = datetime.fromtimestamp(
                value / 1000,
                tz=datetime_timezone.utc,
            ).isoformat()
        elif isinstance(value, (int, float)) and "datetime" in source_type:
            row[field_name] = datetime.fromtimestamp(
                value / 1000,
                tz=datetime_timezone.utc,
            ).isoformat()
        elif isinstance(value, (int, float)) and "time" in source_type:
            row[field_name] = (
                datetime_time(0) + timedelta(milliseconds=value)
            ).isoformat()
        elif isinstance(value, (int, float)) and "date" in source_type:
            row[field_name] = (
                date(1970, 1, 1) + timedelta(days=value)
            ).isoformat()
        elif "string" in clickhouse_type:
            if isinstance(value, (datetime, date, datetime_time)):
                row[field_name] = value.isoformat()
            elif not isinstance(value, str):
                row[field_name] = str(value)
        elif "uint8" in clickhouse_type and isinstance(value, bool):
            row[field_name] = int(value)
        elif "int" in clickhouse_type:
            row[field_name] = int(value)
        elif "float" in clickhouse_type:
            row[field_name] = float(value)
    return row


def _event_version(kafka_offset):
    # A primary key stays on one Kafka partition, whose offsets are monotonic.
    return int(kafka_offset) + 1


def _oracle_field_value(values, field_name):
    if field_name in values:
        return values[field_name], True
    if not isinstance(field_name, str) or field_name != field_name.lower():
        return None, False

    canonical_name = field_name.upper()
    candidates = [
        name for name in values
        if isinstance(name, str) and name.upper() == canonical_name
    ]
    if len(candidates) > 1:
        raise RuntimeError(
            f"Oracle CDC event has ambiguous casing for identifier "
            f"{field_name!r}."
        )
    if canonical_name in values:
        return values[canonical_name], True
    return None, False


def _create_cdc_table(ch_client, profile, table, field_names):
    from etl.tasks import map_to_clickhouse_type

    physical_name = clickhouse_table_name(
        profile.pk, table.schema_name, table.table_name
    )
    quoted_name = quote_clickhouse_identifier(physical_name)
    existing = ch_client.query(
        f"EXISTS TABLE {quoted_name}"
    ).result_rows[0][0]
    if not existing and table.cdc_managed:
        raise RuntimeError(
            f"Cannot resume CDC for {table.schema_name}.{table.table_name}: "
            "its warehouse table is missing. A fresh coordinated CDC snapshot "
            "is required before ingestion can resume."
        )
    if existing and not table.cdc_managed:
        raise RuntimeError(
            f"Cannot switch {table.schema_name}.{table.table_name} to CDC: "
            "its existing warehouse table must remain available until a "
            "coordinated CDC snapshot is ready."
        )
    if existing and table.cdc_managed:
        actual_columns = {
            row[0] for row in ch_client.query(
                f"DESCRIBE TABLE {quoted_name}"
            ).result_rows
        }
        source_fields = {
            field.field_name: field for field in table.fields.all()
        }
        for field_name in field_names:
            if field_name in actual_columns:
                continue
            field = source_fields[field_name]
            ch_type = map_to_clickhouse_type(
                field.data_type,
                is_nullable=field_name not in table.primary_key_fields,
            )
            safe_name = field_name.replace("`", "``")
            ch_client.command(
                f"ALTER TABLE {quoted_name} ADD COLUMN IF NOT EXISTS "
                f"`{safe_name}` {ch_type}"
            )
        if CDC_VERSION_COLUMN not in actual_columns:
            ch_client.command(
                f"ALTER TABLE {quoted_name} ADD COLUMN IF NOT EXISTS "
                f"`{CDC_VERSION_COLUMN}` UInt64 DEFAULT 0"
            )
        if CDC_DELETED_COLUMN not in actual_columns:
            ch_client.command(
                f"ALTER TABLE {quoted_name} ADD COLUMN IF NOT EXISTS "
                f"`{CDC_DELETED_COLUMN}` UInt8 DEFAULT 0"
            )
        return quoted_name, field_names

    source_fields = {field.field_name: field for field in table.fields.all()}
    all_fields = list(field_names)
    column_defs = []
    for field_name in all_fields:
        field = source_fields[field_name]
        ch_type = map_to_clickhouse_type(
            field.data_type,
            is_nullable=field_name not in table.primary_key_fields,
        )
        safe_name = field_name.replace("`", "``")
        column_defs.append(f"`{safe_name}` {ch_type}")
    column_defs.extend([
        f"`{CDC_VERSION_COLUMN}` UInt64",
        f"`{CDC_DELETED_COLUMN}` UInt8",
    ])
    order_by = ", ".join(
        f"`{key.replace('`', '``')}`" for key in table.primary_key_fields
    )
    ch_client.command(
        f"CREATE TABLE {quoted_name} ({', '.join(column_defs)}) "
        f"ENGINE = ReplacingMergeTree({CDC_VERSION_COLUMN}, {CDC_DELETED_COLUMN}) "
        f"ORDER BY ({order_by})"
    )
    table.cdc_managed = True
    table.clickhouse_uses_replacing_merge_tree = True
    table.save(update_fields=[
        "cdc_managed",
        "clickhouse_uses_replacing_merge_tree",
    ])
    return quoted_name, all_fields


def _record_from_message(message, table, field_names, normalize_identifiers=False):
    payload = message.value
    if not isinstance(payload, dict):
        return None
    operation = payload.get("op")
    if operation not in {"c", "u", "d", "r"}:
        return None
    deleted = operation == "d"
    source_row = payload.get("before") if deleted else payload.get("after")
    source_row = source_row if isinstance(source_row, dict) else {}
    key = message.key if isinstance(message.key, dict) else {}
    row = {}
    for field_name in field_names:
        if not normalize_identifiers:
            value = source_row.get(field_name, key.get(field_name))
        elif field_name in source_row:
            value = source_row[field_name]
        elif field_name in key:
            value = key[field_name]
        else:
            value, found = _oracle_field_value(source_row, field_name)
            if not found:
                value, _ = _oracle_field_value(key, field_name)
        row[field_name] = _normalize_value(value)
    if any(row.get(primary_key) is None for primary_key in table.primary_key_fields):
        raise RuntimeError(
            f"CDC event for {table.schema_name}.{table.table_name} has no "
            "complete primary key; the Kafka offset was not committed."
        )
    row[CDC_VERSION_COLUMN] = _event_version(message.offset)
    row[CDC_DELETED_COLUMN] = 1 if deleted else 0
    return row


def consume_source_cdc_batch(profile, ch_client, batch_size=None):
    tables = [
        table for table in profile.tables.all()
        if table.primary_key_fields
    ]
    if not tables:
        return 0
    if batch_size is None:
        batch_size = settings.DEFAULT_ETL_BATCH_SIZE
    topic_tables = {_topic_name(profile, table): table for table in tables}
    consumer = KafkaConsumer(
        bootstrap_servers=[
            server.strip()
            for server in settings.KAFKA_BOOTSTRAP_SERVERS.split(",")
            if server.strip()
        ],
        group_id=_connector_name(profile),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        key_deserializer=lambda value: (
            json.loads(value.decode("utf-8")) if value is not None else None
        ),
        value_deserializer=lambda value: (
            json.loads(value.decode("utf-8")) if value is not None else None
        ),
        consumer_timeout_ms=1000,
    )
    messages = []
    try:
        consumer.subscribe(list(topic_tables))
        field_names_by_table = {
            table.pk: _required_field_names(profile, table)
            for table in tables
        }
        physical_tables = {
            table.pk: _create_cdc_table(
                ch_client,
                profile,
                table,
                sorted(field_names_by_table[table.pk]),
            )
            for table in tables
        }
        field_types_by_table = {
            table.pk: {
                field.field_name: field.data_type
                for field in table.fields.all()
            }
            for table in tables
        }
        records = consumer.poll(timeout_ms=10000, max_records=batch_size)
        if not consumer.assignment():
            raise RuntimeError(
                f"Kafka consumer for {_connector_name(profile)} did not receive "
                "a partition assignment; CDC progress was not checked."
            )
        for partition_records in records.values():
            messages.extend(partition_records)
        messages.sort(key=lambda message: (message.topic, message.partition, message.offset))

        inserts_by_table = {}
        for message in messages:
            table = topic_tables.get(message.topic)
            if table is None:
                continue
            row = _record_from_message(
                message,
                table,
                field_names_by_table[table.pk],
                normalize_identifiers=profile.db_type == DbChoice.ORACLE,
            )
            if row is None:
                continue
            row = _coerce_cdc_row(
                row,
                table,
                field_types_by_table[table.pk],
            )
            inserts_by_table.setdefault(table.pk, (table, []))[1].append(row)

        for table, rows in inserts_by_table.values():
            physical_name, field_names = physical_tables[table.pk]
            ch_client.insert(
                physical_name,
                [[row.get(field) for field in field_names] +
                 [row[CDC_VERSION_COLUMN], row[CDC_DELETED_COLUMN]]
                 for row in rows],
                column_names=field_names + [
                    CDC_VERSION_COLUMN,
                    CDC_DELETED_COLUMN,
                ],
            )
            table.last_synced_at = timezone.now()
            table.save(update_fields=["last_synced_at"])
        if messages:
            consumer.commit()
        return len(messages)
    finally:
        consumer.close()
