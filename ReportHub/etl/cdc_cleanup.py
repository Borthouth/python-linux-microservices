import logging
import uuid
from urllib.parse import quote

import clickhouse_connect
import sqlalchemy as sa
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from control_plane.models import (
    CdcResourceCleanup,
    CdcResourceCleanupItem,
    CdcSourceExternalOperation,
    CdcTableRebuild,
    CdcToBatchTableCheckpoint,
    CdcToBatchTransition,
    DbChoice,
    SourceTable,
)
from etl.cdc import (
    _clickhouse_table_uuid,
    _connector_name,
    _connector_suffix,
    _connect_request,
    _topic_name,
    build_debezium_connector_config,
)
from reports.query_builder import (
    quote_clickhouse_identifier,
    source_table_clickhouse_name,
)


logger = logging.getLogger(__name__)

ACTIVE_CDC_STATUSES = (
    CdcTableRebuild.Status.SNAPSHOTTING,
    CdcTableRebuild.Status.CUTOVER_PENDING,
)
ACTIVE_TRANSITION_STATUSES = (
    CdcToBatchTransition.Status.PREPARING,
    CdcToBatchTransition.Status.CUTOVER_PENDING,
)


class CdcCleanupIneligible(RuntimeError):
    pass


def cleanup_candidates(limit=50):
    candidates = []
    rebuilds = CdcTableRebuild.objects.select_related(
        "source_table__source"
    ).filter(
        status__in=[
            CdcTableRebuild.Status.FAILED,
            CdcTableRebuild.Status.SUCCEEDED,
        ]
    ).order_by("-updated_at", "-pk")
    for rebuild in rebuilds.iterator():
        try:
            rebuild = _ensure_rebuild_eligible(rebuild)
        except CdcCleanupIneligible:
            continue
        candidates.append({
            "rebuild": rebuild,
            "resources": _resource_specs(rebuild),
        })
        if len(candidates) >= limit:
            break
    return candidates


def _kafka_bootstrap_servers():
    return [
        server.strip()
        for server in settings.KAFKA_BOOTSTRAP_SERVERS.split(",")
        if server.strip()
    ]


def _clickhouse_client():
    return clickhouse_connect.get_client(
        host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
        port=getattr(settings, "CLICKHOUSE_PORT", 8123),
        username=getattr(settings, "CLICKHOUSE_USER", "default"),
        password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
        connect_timeout=10,
        send_receive_timeout=30,
    )


def _resource_specs(rebuild):
    profile = rebuild.source_table.source
    table = rebuild.source_table
    suffix = _connector_suffix(rebuild.connector_name)
    specs = [
        (CdcResourceCleanupItem.ResourceType.CONNECTOR, rebuild.connector_name),
    ]
    if profile.db_type == DbChoice.POSTGRESQL:
        postgres_resource = (
            f"reporthub_{profile.pk}"
            if rebuild.connector_name == _connector_name(profile)
            else f"rh_{profile.pk}_{suffix}"
        )
        specs.extend([
            (
                CdcResourceCleanupItem.ResourceType.POSTGRES_SLOT,
                postgres_resource,
            ),
            (
                CdcResourceCleanupItem.ResourceType.POSTGRES_PUBLICATION,
                postgres_resource,
            ),
        ])
    specs.append((
        CdcResourceCleanupItem.ResourceType.KAFKA_GROUP,
        rebuild.consumer_group,
    ))
    specs.append((
        CdcResourceCleanupItem.ResourceType.KAFKA_TOPIC,
        _topic_name(profile, table, rebuild.topic_prefix),
    ))
    specs.append((
        CdcResourceCleanupItem.ResourceType.KAFKA_TOPIC,
        rebuild.notification_topic,
    ))
    if profile.db_type in {
        DbChoice.MYSQL,
        DbChoice.POSTGRESQL,
        DbChoice.MSSQL,
        DbChoice.ORACLE,
    }:
        history_topic = (
            f"reporthub.schema-history.{profile.pk}"
            if rebuild.connector_name == _connector_name(profile)
            else f"reporthub.schema-history.{profile.pk}.{suffix}"
        )
        specs.append((
            CdcResourceCleanupItem.ResourceType.KAFKA_TOPIC,
            history_topic,
        ))
    specs.append((
        CdcResourceCleanupItem.ResourceType.CLICKHOUSE_TABLE,
        rebuild.staging_table,
    ))
    return specs


def _ensure_rebuild_eligible(rebuild, cleanup_id=None):
    rebuild = CdcTableRebuild.objects.select_related(
        "source_table__source"
    ).get(pk=rebuild.pk)
    if rebuild.status not in (
        CdcTableRebuild.Status.FAILED,
        CdcTableRebuild.Status.SUCCEEDED,
    ):
        raise CdcCleanupIneligible(
            "Only failed or superseded successful rebuild generations can be reviewed."
        )

    source_id = rebuild.source_table.source_id
    if SourceTable.objects.filter(
        source_id=source_id,
        cdc_connector_name=rebuild.connector_name,
    ).exists():
        raise CdcCleanupIneligible(
            "This connector generation is still referenced by an active source table."
        )
    if CdcTableRebuild.objects.filter(
        source_table__source_id=source_id,
        status__in=ACTIVE_CDC_STATUSES,
    ).exists():
        raise CdcCleanupIneligible(
            "The source has an unfinished CDC table rebuild."
        )
    if CdcToBatchTransition.objects.filter(
        source_id=source_id,
        status__in=ACTIVE_TRANSITION_STATUSES,
    ).exists() or CdcToBatchTableCheckpoint.objects.filter(
        transition__source_id=source_id,
        cutover_command_inflight=True,
    ).exists():
        raise CdcCleanupIneligible(
            "The source has an unfinished CDC-to-batch transition."
        )
    if CdcSourceExternalOperation.objects.filter(source_id=source_id).exists():
        raise CdcCleanupIneligible(
            "The source has unresolved external operations."
        )
    if CdcResourceCleanup.objects.filter(
        rebuild__source_table__source_id=source_id,
        status__in=[
            CdcResourceCleanup.Status.SCANNING,
            CdcResourceCleanup.Status.AWAITING_CONFIRMATION,
            CdcResourceCleanup.Status.QUEUED,
            CdcResourceCleanup.Status.RUNNING,
        ],
    ).exclude(pk=cleanup_id).exists():
        raise CdcCleanupIneligible(
            "Another resource cleanup for this source is already in progress."
        )
    return rebuild


def _expected_connector_config(rebuild):
    return build_debezium_connector_config(
        rebuild.source_table.source,
        [rebuild.source_table],
        connector_name=rebuild.connector_name,
        topic_prefix=rebuild.topic_prefix,
        notification_topic=rebuild.notification_topic,
    )


def _assert_connector_owned(rebuild, config):
    if not isinstance(config, dict):
        raise CdcCleanupIneligible(
            f"Connector {rebuild.connector_name} returned an invalid configuration."
        )
    expected = _expected_connector_config(rebuild)
    stable_keys = (
        "connector.class",
        "topic.prefix",
        "database.hostname",
        "database.port",
        "database.dbname",
        "database.server.id",
        "database.names",
        "database.include.list",
        "schema.include.list",
        "slot.name",
        "publication.name",
        "plugin.name",
        "database.pdb.name",
        "database.connection.adapter",
        "table.include.list",
        "collection.include.list",
        "mongodb.connection.string",
        "capture.mode",
        "schema.history.internal.kafka.bootstrap.servers",
        "schema.history.internal.kafka.topic",
        "notification.sink.topic.name",
    )
    for key in stable_keys:
        if key in expected and config.get(key) != expected[key]:
            raise CdcCleanupIneligible(
                f"Connector {rebuild.connector_name} no longer matches its recorded generation."
            )


def _connectors():
    names = _connect_request("GET", "/connectors") or []
    if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
        raise RuntimeError("Kafka Connect returned an invalid connector inventory.")
    return names


def _get_connector_config(name):
    return _connect_request("GET", f"/connectors/{name}/config")


def _connector_references_topic(topic_name, excluded_connector=None):
    for connector_name in _connectors():
        if connector_name == excluded_connector:
            continue
        config = _get_connector_config(connector_name)
        if not config:
            continue
        prefix = config.get("topic.prefix")
        if prefix and topic_name.startswith(f"{prefix}."):
            return True
        if topic_name in {
            config.get("notification.sink.topic.name"),
            config.get("schema.history.internal.kafka.topic"),
        }:
            return True
    return False


def _assert_connector_removed(rebuild):
    if _get_connector_config(rebuild.connector_name) is not None:
        raise CdcCleanupIneligible(
            "The generation connector must be confirmed absent first."
        )


def _assert_no_connector_uses_postgres_resource(resource_type, resource_name):
    config_key = (
        "slot.name"
        if resource_type == CdcResourceCleanupItem.ResourceType.POSTGRES_SLOT
        else "publication.name"
    )
    for connector_name in _connectors():
        config = _get_connector_config(connector_name)
        if config and config.get(config_key) == resource_name:
            raise CdcCleanupIneligible(
                "Another Kafka Connect connector still references this PostgreSQL resource."
            )


def _safe_error(error, profile):
    message = str(error) or "The resource operation failed."
    for secret in (
        getattr(profile, "password", ""),
        getattr(profile, "oracle_logminer_password", ""),
    ):
        if secret:
            message = message.replace(secret, "[redacted]")
            message = message.replace(quote(secret, safe=""), "[redacted]")
    return f"{type(error).__name__}: {message[:1000]}"


def _postgres_resource_state(
    rebuild, resource_type, resource_name, allow_active_slot=False
):
    from etl.tasks import create_source_engine

    if rebuild.source_table.source.db_type != DbChoice.POSTGRESQL:
        raise CdcCleanupIneligible(
            "PostgreSQL resources cannot be verified for this source type."
        )
    engine = create_source_engine(
        rebuild.source_table.source,
        connect_args={"connect_timeout": 10},
    )
    try:
        with engine.connect() as connection:
            if resource_type == CdcResourceCleanupItem.ResourceType.POSTGRES_SLOT:
                row = connection.execute(
                    sa.text(
                        "SELECT plugin, slot_type, database, active "
                        "FROM pg_replication_slots WHERE slot_name = :name"
                    ),
                    {"name": resource_name},
                ).first()
                if row is None:
                    return False
                if (
                    row.plugin != "pgoutput"
                    or row.slot_type != "logical"
                    or row.database != rebuild.source_table.source.database_name
                ):
                    raise CdcCleanupIneligible(
                        "The replication slot does not match the recorded CDC generation."
                    )
                if row.active and not allow_active_slot:
                    raise CdcCleanupIneligible(
                        "The replication slot is active and cannot be cleaned up."
                    )
                return True
            row = connection.execute(
                sa.text(
                    "SELECT puballtables FROM pg_publication WHERE pubname = :name"
                ),
                {"name": resource_name},
            ).first()
            if row is None:
                return False
            publication_tables = set(connection.execute(
                sa.text(
                    "SELECT schemaname, tablename FROM pg_publication_tables "
                    "WHERE pubname = :name"
                ),
                {"name": resource_name},
            ).all())
            expected_table = (
                rebuild.source_table.schema_name,
                rebuild.source_table.table_name,
            )
            if row.puballtables or publication_tables != {expected_table}:
                raise CdcCleanupIneligible(
                    "The publication contains tables outside the recorded CDC generation."
                )
            return True
    finally:
        engine.dispose()


def _clickhouse_table_state(rebuild):
    table = rebuild.source_table
    active_table = source_table_clickhouse_name(table)
    if rebuild.staging_table == active_table:
        raise CdcCleanupIneligible(
            "The retained ClickHouse table name is currently active."
        )
    client = _clickhouse_client()
    try:
        staging_uuid = _clickhouse_table_uuid(client, rebuild.staging_table)
        if not staging_uuid:
            return False
        active_uuid = _clickhouse_table_uuid(client, active_table)
        if not active_uuid:
            raise CdcCleanupIneligible(
                "The active ClickHouse table cannot be verified."
            )
        if not rebuild.staging_uuid:
            raise CdcCleanupIneligible(
                "The retained ClickHouse table has no recorded generation UUID."
            )
        if staging_uuid == active_uuid:
            raise CdcCleanupIneligible(
                "The retained ClickHouse table is the active physical table."
            )
        valid_stage = staging_uuid == rebuild.staging_uuid
        valid_exchanged_table = (
            rebuild.status == CdcTableRebuild.Status.SUCCEEDED
            and bool(rebuild.active_uuid_before)
            and staging_uuid == rebuild.active_uuid_before
            and active_uuid == rebuild.staging_uuid
        )
        if not valid_stage and not valid_exchanged_table:
            raise CdcCleanupIneligible(
                "The ClickHouse table UUID does not match the recorded generation."
            )
        return True
    finally:
        client.close()


def _kafka_topic_state(topic_name, excluded_connector=None):
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        bootstrap_servers=_kafka_bootstrap_servers(),
        group_id=None,
        enable_auto_commit=False,
        request_timeout_ms=15000,
        api_version_auto_timeout_ms=10000,
        client_id="reporthub-cdc-cleanup-inventory",
    )
    try:
        exists = topic_name in consumer.topics()
    finally:
        consumer.close()
    if exists and _connector_references_topic(topic_name, excluded_connector):
        raise CdcCleanupIneligible(
            "The Kafka topic is still referenced by a configured connector."
        )
    return exists


def _kafka_group_state(group_name):
    from kafka.admin import KafkaAdminClient

    admin = KafkaAdminClient(
        bootstrap_servers=_kafka_bootstrap_servers(),
        client_id="reporthub-cdc-cleanup-inventory",
        request_timeout_ms=15000,
    )
    try:
        return any(
            item[0] == group_name
            for item in admin.list_consumer_groups()
        )
    finally:
        admin.close()


def inspect_resource(rebuild, resource_type, resource_name, cleanup_id=None):
    rebuild = _ensure_rebuild_eligible(rebuild, cleanup_id=cleanup_id)
    if (resource_type, resource_name) not in _resource_specs(rebuild):
        raise CdcCleanupIneligible(
            "The resource does not match the recorded CDC generation."
        )
    if resource_type == CdcResourceCleanupItem.ResourceType.CONNECTOR:
        config = _get_connector_config(resource_name)
        if config is None:
            return False
        if resource_name != rebuild.connector_name:
            raise CdcCleanupIneligible(
                "The connector name does not match the recorded generation."
            )
        _assert_connector_owned(rebuild, config)
        return True
    if resource_type in {
        CdcResourceCleanupItem.ResourceType.POSTGRES_SLOT,
        CdcResourceCleanupItem.ResourceType.POSTGRES_PUBLICATION,
    }:
        return _postgres_resource_state(
            rebuild,
            resource_type,
            resource_name,
            allow_active_slot=True,
        )
    if resource_type == CdcResourceCleanupItem.ResourceType.KAFKA_TOPIC:
        return _kafka_topic_state(
            resource_name,
            excluded_connector=rebuild.connector_name,
        )
    if resource_type == CdcResourceCleanupItem.ResourceType.KAFKA_GROUP:
        if resource_name != rebuild.consumer_group:
            raise CdcCleanupIneligible(
                "The consumer group does not match the recorded generation."
            )
        return _kafka_group_state(resource_name)
    if resource_type == CdcResourceCleanupItem.ResourceType.CLICKHOUSE_TABLE:
        if resource_name != rebuild.staging_table:
            raise CdcCleanupIneligible(
                "The ClickHouse table does not match the recorded generation."
            )
        return _clickhouse_table_state(rebuild)
    raise CdcCleanupIneligible("The resource type is not supported.")


def scan_cleanup_resources(cleanup_id):
    cleanup = CdcResourceCleanup.objects.select_related(
        "rebuild__source_table__source"
    ).get(pk=cleanup_id)
    rebuild = _ensure_rebuild_eligible(cleanup.rebuild, cleanup_id=cleanup.pk)
    items = list(cleanup.items.order_by("pk"))
    failures = []
    for item in items:
        try:
            exists = inspect_resource(
                rebuild,
                item.resource_type,
                item.resource_name,
                cleanup_id=cleanup.pk,
            )
        except Exception as error:
            safe_message = _safe_error(
                error, rebuild.source_table.source
            )
            logger.error(
                "Could not inspect CDC resource %s for cleanup %s: %s",
                item.resource_name,
                cleanup.pk,
                safe_message,
            )
            item.scan_error = safe_message
            item.updated_by = cleanup.created_by
            item._history_user = cleanup.created_by
            item.save(update_fields=["scan_error", "updated_by", "updated_at"])
            failures.append(item.resource_name)
        else:
            item.exists_at_scan = exists
            item.scan_error = ""
            item.updated_by = cleanup.created_by
            item._history_user = cleanup.created_by
            item.save(update_fields=[
                "exists_at_scan", "scan_error", "updated_by", "updated_at",
            ])

    with transaction.atomic():
        cleanup = CdcResourceCleanup.objects.select_for_update().get(pk=cleanup_id)
        if cleanup.status != CdcResourceCleanup.Status.SCANNING:
            return
        if failures:
            cleanup.status = CdcResourceCleanup.Status.FAILED
            cleanup.last_error = (
                "Some resources could not be verified. No cleanup was attempted; "
                "start a new inventory after reviewing the worker logs."
            )
            cleanup.completed_at = timezone.now()
        elif any(item.exists_at_scan for item in items):
            cleanup.status = CdcResourceCleanup.Status.AWAITING_CONFIRMATION
            cleanup.last_error = ""
        else:
            cleanup.status = CdcResourceCleanup.Status.COMPLETED
            cleanup.last_error = "No retained resources were found."
            cleanup.completed_at = timezone.now()
        cleanup.updated_by = cleanup.created_by
        cleanup._history_user = cleanup.created_by
        cleanup.save(update_fields=[
            "status", "last_error", "completed_at", "updated_by", "updated_at",
        ])


def _delete_connector(rebuild):
    config = _get_connector_config(rebuild.connector_name)
    if config is None:
        return "Already absent"
    _assert_connector_owned(rebuild, config)
    _connect_request("DELETE", f"/connectors/{rebuild.connector_name}")
    if _get_connector_config(rebuild.connector_name) is not None:
        raise RuntimeError("Kafka Connect still reports the connector as present.")
    return "Deleted"


def _delete_postgres_resource(rebuild, resource_type, resource_name):
    from etl.tasks import create_source_engine

    _assert_connector_removed(rebuild)
    _assert_no_connector_uses_postgres_resource(resource_type, resource_name)
    engine = create_source_engine(
        rebuild.source_table.source,
        connect_args={"connect_timeout": 10},
    )
    try:
        with engine.connect() as connection:
            connection = connection.execution_options(isolation_level="AUTOCOMMIT")
            if resource_type == CdcResourceCleanupItem.ResourceType.POSTGRES_SLOT:
                state = _postgres_resource_state(
                    rebuild, resource_type, resource_name
                )
                if not state:
                    return "Already absent"
                connection.execute(
                    sa.text(
                        "SELECT pg_drop_replication_slot(:name)"
                    ),
                    {"name": resource_name},
                )
                if _postgres_resource_state(
                    rebuild, resource_type, resource_name
                ):
                    raise RuntimeError("PostgreSQL still reports the replication slot.")
                return "Deleted"
            state = _postgres_resource_state(
                rebuild, resource_type, resource_name
            )
            if not state:
                return "Already absent"
            quoted_name = connection.dialect.identifier_preparer.quote(resource_name)
            connection.exec_driver_sql(f"DROP PUBLICATION {quoted_name}")
            if _postgres_resource_state(
                rebuild, resource_type, resource_name
            ):
                raise RuntimeError("PostgreSQL still reports the publication.")
            return "Deleted"
    finally:
        engine.dispose()


def _delete_kafka_group(group_name):
    from kafka.admin import KafkaAdminClient

    admin = KafkaAdminClient(
        bootstrap_servers=_kafka_bootstrap_servers(),
        client_id=f"reporthub-cdc-cleanup-{uuid.uuid4().hex[:12]}",
        request_timeout_ms=30000,
    )
    try:
        if not _kafka_group_state(group_name):
            return "Already absent"
        result = admin.delete_consumer_groups([group_name])[group_name]
        result.get(timeout=30)
        if _kafka_group_state(group_name):
            raise RuntimeError("Kafka still reports the consumer group as present.")
        return "Deleted"
    finally:
        admin.close()


def _delete_kafka_topic(rebuild, topic_name):
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient

    _assert_connector_removed(rebuild)
    if not _kafka_topic_state(topic_name, excluded_connector=rebuild.connector_name):
        return "Already absent"
    consumer = KafkaConsumer(
        bootstrap_servers=_kafka_bootstrap_servers(),
        group_id=None,
        enable_auto_commit=False,
        request_timeout_ms=15000,
        api_version_auto_timeout_ms=10000,
        client_id="reporthub-cdc-cleanup-delete",
    )
    try:
        if topic_name not in consumer.topics():
            return "Already absent"
    finally:
        consumer.close()

    admin = KafkaAdminClient(
        bootstrap_servers=_kafka_bootstrap_servers(),
        client_id=f"reporthub-cdc-cleanup-{uuid.uuid4().hex[:12]}",
        request_timeout_ms=30000,
    )
    try:
        future = admin.delete_topics([topic_name], timeout_ms=30000)[topic_name]
        future.get(timeout=30)
    finally:
        admin.close()
    if _kafka_topic_state(topic_name, excluded_connector=rebuild.connector_name):
        raise RuntimeError("Kafka still reports the topic as present.")
    return "Deleted"


def _delete_clickhouse_table(rebuild, table_name, cleanup_id, item_id):
    if not _clickhouse_table_state(rebuild):
        return "Already absent"
    client = _clickhouse_client()
    try:
        query_id = f"cdc_cleanup_{cleanup_id}_{item_id}"
        client.command(
            f"DROP TABLE {quote_clickhouse_identifier(table_name)} SYNC",
            settings={"query_id": query_id},
        )
        if _clickhouse_table_uuid(client, table_name):
            raise RuntimeError("ClickHouse still reports the table as present.")
        return "Deleted"
    finally:
        client.close()


def _cleanup_resource(cleanup, item):
    rebuild = _ensure_rebuild_eligible(
        cleanup.rebuild,
        cleanup_id=cleanup.pk,
    )
    if (item.resource_type, item.resource_name) not in _resource_specs(rebuild):
        raise CdcCleanupIneligible(
            "The resource does not match the recorded CDC generation."
        )
    if item.resource_type == CdcResourceCleanupItem.ResourceType.CONNECTOR:
        return _delete_connector(rebuild)
    if item.resource_type in {
        CdcResourceCleanupItem.ResourceType.POSTGRES_SLOT,
        CdcResourceCleanupItem.ResourceType.POSTGRES_PUBLICATION,
    }:
        return _delete_postgres_resource(
            rebuild, item.resource_type, item.resource_name
        )
    if item.resource_type == CdcResourceCleanupItem.ResourceType.KAFKA_GROUP:
        if item.resource_name != rebuild.consumer_group:
            raise CdcCleanupIneligible(
                "The consumer group does not match the recorded generation."
            )
        _assert_connector_removed(rebuild)
        return _delete_kafka_group(item.resource_name)
    if item.resource_type == CdcResourceCleanupItem.ResourceType.KAFKA_TOPIC:
        if item.resource_name not in {
            name for kind, name in _resource_specs(rebuild)
            if kind == CdcResourceCleanupItem.ResourceType.KAFKA_TOPIC
        }:
            raise CdcCleanupIneligible(
                "The Kafka topic does not match the recorded generation."
            )
        return _delete_kafka_topic(rebuild, item.resource_name)
    if item.resource_type == CdcResourceCleanupItem.ResourceType.CLICKHOUSE_TABLE:
        if item.resource_name != rebuild.staging_table:
            raise CdcCleanupIneligible(
                "The ClickHouse table does not match the recorded generation."
            )
        return _delete_clickhouse_table(
            rebuild, item.resource_name, cleanup.pk, item.pk
        )
    raise CdcCleanupIneligible("The resource type is not supported.")


def execute_cleanup(cleanup_id):
    with transaction.atomic():
        cleanup = CdcResourceCleanup.objects.select_for_update().select_related(
            "rebuild__source_table__source"
        ).get(pk=cleanup_id)
        if cleanup.status != CdcResourceCleanup.Status.QUEUED:
            return
        _ensure_rebuild_eligible(cleanup.rebuild, cleanup_id=cleanup.pk)
        if cleanup.items.filter(scan_error__gt="").exists():
            raise CdcCleanupIneligible(
                "The resource inventory contains unverified items."
            )
        if cleanup.items.filter(exists_at_scan__isnull=True).exists():
            raise CdcCleanupIneligible(
                "The resource inventory is incomplete."
            )
        cleanup.status = CdcResourceCleanup.Status.RUNNING
        cleanup.started_at = timezone.now()
        cleanup.last_error = ""
        cleanup.updated_by = cleanup.created_by
        cleanup._history_user = cleanup.created_by
        cleanup.save(update_fields=[
            "status", "started_at", "last_error", "updated_by", "updated_at",
        ])

    items = list(cleanup.items.order_by("pk"))
    for item in items:
        cleanup.refresh_from_db(fields=["status"])
        if cleanup.status != CdcResourceCleanup.Status.RUNNING:
            return
        if not item.exists_at_scan or item.status == CdcResourceCleanupItem.Status.COMPLETED:
            if item.status != CdcResourceCleanupItem.Status.COMPLETED:
                item.status = CdcResourceCleanupItem.Status.COMPLETED
                item.outcome = "Not present during inventory"
                item.updated_by = cleanup.created_by
                item._history_user = cleanup.created_by
                item.save(update_fields=[
                    "status", "outcome", "updated_by", "updated_at",
                ])
            continue
        item.status = CdcResourceCleanupItem.Status.RUNNING
        item.attempts += 1
        item.last_error = ""
        item.updated_by = cleanup.created_by
        item._history_user = cleanup.created_by
        item.save(update_fields=[
            "status", "attempts", "last_error", "updated_by", "updated_at",
        ])
        try:
            outcome = _cleanup_resource(cleanup, item)
        except Exception as error:
            safe_message = _safe_error(
                error, cleanup.rebuild.source_table.source
            )
            logger.error(
                "CDC resource cleanup %s failed for %s %s: %s",
                cleanup.pk,
                item.resource_type,
                item.resource_name,
                safe_message,
            )
            item.status = CdcResourceCleanupItem.Status.FAILED
            item.last_error = safe_message
            item.updated_by = cleanup.created_by
            item._history_user = cleanup.created_by
            item.save(update_fields=[
                "status", "last_error", "updated_by", "updated_at",
            ])
            cleanup.status = CdcResourceCleanup.Status.FAILED
            cleanup.last_error = (
                f"Cleanup stopped at {item.resource_type} "
                f"{item.resource_name}. {safe_message}"
            )
            cleanup.completed_at = timezone.now()
            cleanup.updated_by = cleanup.created_by
            cleanup._history_user = cleanup.created_by
            cleanup.save(update_fields=[
                "status", "last_error", "completed_at", "updated_by", "updated_at",
            ])
            return
        current_cleanup = CdcResourceCleanup.objects.get(pk=cleanup.pk)
        item.status = CdcResourceCleanupItem.Status.COMPLETED
        item.outcome = outcome
        item.last_error = ""
        item.updated_by = cleanup.created_by
        item._history_user = cleanup.created_by
        item.save(update_fields=[
            "status", "outcome", "last_error", "updated_by", "updated_at",
        ])
        if current_cleanup.status != CdcResourceCleanup.Status.RUNNING:
            return

    with transaction.atomic():
        cleanup = CdcResourceCleanup.objects.select_for_update().get(pk=cleanup.pk)
        if cleanup.status == CdcResourceCleanup.Status.RUNNING:
            cleanup.status = CdcResourceCleanup.Status.COMPLETED
            cleanup.last_error = ""
            cleanup.completed_at = timezone.now()
            cleanup.updated_by = cleanup.created_by
            cleanup._history_user = cleanup.created_by
            cleanup.save(update_fields=[
                "status", "last_error", "completed_at", "updated_by", "updated_at",
            ])
