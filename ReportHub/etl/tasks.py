import os
import logging
import json
from datetime import date, datetime
from uuid import UUID, uuid4
import pymysql
import psycopg2
import pyodbc
import sqlalchemy as sa
import pandas as pd
import clickhouse_connect
from bson import Decimal128, ObjectId
from django.db.models import Q
from django.db import transaction
from django.conf import settings
from django.core.cache import cache
from celery import shared_task, Task
from celery.signals import task_revoked
from django.utils import timezone
from pymongo import MongoClient
from control_plane.models import (
    CdcExecutionFailure,
    CdcSourceExternalOperation,
    CdcTableRebuild,
    CdcToBatchTableCheckpoint,
    CdcToBatchTransition,
    SourceProfile, 
    SourceTable, 
    SourceField, 
    SourceRelationship, 
    WarehouseDatasetMapping,
    SyncStatus, 
    DbChoice,
    IngestionMode,
)
from control_plane.utils import set_task_progress
from config.clickhouse_queries import cancel_clickhouse_query
from control_plane.metadata_guard import source_metadata_write_guard
from etl.progress import (
    CONNECTING_TO_SOURCE,
    INITIALIZING_INGESTION,
    INTROSPECTING_SCHEMA,
    PREPARING_INGESTION,
)
from etl.cdc_external_operations import (
    CdcExternalOperationCutoverPending,
    _track_cdc_external_operation,
)
from reports.models import ReportDefinition
from reports.query_builder import source_table_clickhouse_name


def create_source_engine(profile, connect_args=None):
    """Build a SQLAlchemy engine for a supported relational source."""
    if profile.db_type == DbChoice.MYSQL:
        url = sa.engine.URL.create(
            "mysql+pymysql",
            username=profile.username,
            password=profile.password,
            host=profile.host,
            port=profile.port,
            database=profile.database_name,
        )
    elif profile.db_type == DbChoice.POSTGRESQL:
        url = sa.engine.URL.create(
            "postgresql+psycopg2",
            username=profile.username,
            password=profile.password,
            host=profile.host,
            port=profile.port,
            database=profile.database_name,
        )
    elif profile.db_type == DbChoice.MSSQL:
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={profile.host},{profile.port};"
            f"DATABASE={profile.database_name};"
            f"UID={profile.username};"
            f"PWD={profile.password};"
            "TrustServerCertificate=yes;"
        )
        url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
    elif profile.db_type == DbChoice.ORACLE:
        url = sa.engine.URL.create(
            "oracle+oracledb",
            username=profile.username,
            password=profile.password,
            host=profile.host,
            port=profile.port,
            query={"service_name": profile.database_name},
        )
    else:
        raise ValueError(f"Unsupported SQL database type: {profile.db_type}")
    return sa.create_engine(url, connect_args=connect_args or {})


def _clickhouse_ingestion_settings():
    if getattr(settings, "CDC_EXTERNAL_OPERATION_TRACKING", False):
        return {"async_insert": 0}
    return {}


def create_mongo_client(profile):
    return MongoClient(
        host=profile.host,
        port=profile.port,
        username=profile.username,
        password=profile.password,
        authSource="admin",
        serverSelectionTimeoutMS=5000,
    )


def infer_mongo_type(value):
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Int64"
    if isinstance(value, float):
        return "Float64"
    if isinstance(value, ObjectId):
        return "ObjectId"
    if isinstance(value, (dict, list)):
        return "JSON"
    if value is not None and value.__class__.__name__ in {"datetime", "date"}:
        return "DateTime"
    if isinstance(value, Decimal128):
        return "Decimal"
    return "String"


def is_datetime_source_type(data_type):
    normalized = str(data_type).lower()
    return any(kind in normalized for kind in ("date", "datetime", "timestamp"))


def introspect_mongodb(profile, task_id):
    client = create_mongo_client(profile)
    try:
        database = client[profile.database_name]
        collections = database.list_collection_names()
        for index, collection_name in enumerate(collections, start=1):
            set_task_progress(
                task_id,
                percentage=int(index / max(len(collections), 1) * 50),
                status="RUNNING",
                current_table=f"{profile.database_name}.{collection_name}",
            )
            table, _ = SourceTable.objects.get_or_create(
                source=profile,
                schema_name=profile.database_name,
                table_name=collection_name,
            )
            if table.primary_key_fields != ["_id"]:
                table.primary_key_fields = ["_id"]
                table.incremental_watermark = None
                table.incremental_ready = False
                table.save(update_fields=[
                    "primary_key_fields",
                    "incremental_watermark",
                    "incremental_ready",
                ])
            field_types = {}
            for document in database[collection_name].find().limit(100):
                for field_name, value in document.items():
                    value_type = infer_mongo_type(value)
                    if field_name not in field_types or field_types[field_name] == "String":
                        field_types[field_name] = value_type
            for field_name, data_type in field_types.items():
                SourceField.objects.update_or_create(
                    table=table,
                    field_name=field_name,
                    defaults={"data_type": data_type},
                )
            SourceField.objects.update_or_create(
                table=table,
                field_name="_id",
                defaults={"data_type": "ObjectId"},
            )
    finally:
        client.close()


def read_mongo_chunks(
    profile, table_name, field_names, batch_size, query_filter=None, skip=0,
):
    client = create_mongo_client(profile)
    try:
        projection = {field_name: 1 for field_name in field_names}
        cursor = client[profile.database_name][table_name].find(
            query_filter or {}, projection
        ).sort("_id", 1).skip(skip).limit(batch_size).batch_size(batch_size)
        rows = []
        for document in cursor:
            row = []
            for field_name in field_names:
                value = document.get(field_name)
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, default=str)
                elif isinstance(value, (ObjectId, Decimal128)):
                    value = str(value)
                row.append(value)
            rows.append(row)
            if len(rows) >= batch_size:
                yield pd.DataFrame(rows, columns=field_names)
                rows = []
        if rows:
            yield pd.DataFrame(rows, columns=field_names)
    finally:
        client.close()


def serialize_ingestion_watermark(value):
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if hasattr(value, "isoformat"):
        value = value.isoformat()
    return json.dumps(value, default=str)


def parse_ingestion_watermark(value):
    decoded = json.loads(value)
    if not isinstance(decoded, str):
        return decoded
    try:
        return pd.Timestamp(decoded).to_pydatetime()
    except (TypeError, ValueError):
        return decoded


def watermark_is_greater(candidate, current):
    candidate_time = pd.Timestamp(parse_ingestion_watermark(candidate))
    current_time = pd.Timestamp(parse_ingestion_watermark(current))
    if candidate_time.tz is None:
        candidate_time = candidate_time.tz_localize("UTC")
    else:
        candidate_time = candidate_time.tz_convert("UTC")
    if current_time.tz is None:
        current_time = current_time.tz_localize("UTC")
    else:
        current_time = current_time.tz_convert("UTC")
    return candidate_time > current_time


def serialize_source_key(value):
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    elif hasattr(value, "item"):
        value = value.item()
    if hasattr(value, "isoformat"):
        value = value.isoformat()
    return json.dumps(value, default=str)


def parse_source_key(value, source_column):
    decoded = json.loads(value)
    if not isinstance(decoded, str):
        return decoded
    try:
        python_type = source_column.type.python_type
    except (AttributeError, NotImplementedError):
        return decoded
    if python_type is datetime:
        return datetime.fromisoformat(decoded)
    if python_type is date:
        return date.fromisoformat(decoded)
    if python_type is UUID:
        return UUID(decoded)
    if python_type is str:
        return decoded
    try:
        return python_type(decoded)
    except (TypeError, ValueError):
        return decoded


def get_source_watermark(profile, source_engine, table, field_name):
    if profile.db_type == DbChoice.MONGODB:
        client = create_mongo_client(profile)
        try:
            row = client[profile.database_name][table.table_name].find(
                {field_name: {"$ne": None}},
                {field_name: 1},
            ).sort(field_name, -1).limit(1)
            document = next(row, None)
            return (
                serialize_ingestion_watermark(document[field_name])
                if document and document.get(field_name) is not None
                else None
            )
        finally:
            client.close()

    schema_name = (
        None if profile.db_type == DbChoice.MYSQL
        else table.schema_name
    )
    source_table = sa.Table(
        table.table_name,
        sa.MetaData(),
        schema=schema_name,
        autoload_with=source_engine,
    )
    with source_engine.connect() as connection:
        value = connection.execute(
            sa.select(sa.func.max(source_table.c[field_name]))
        ).scalar_one_or_none()
    return serialize_ingestion_watermark(value) if value is not None else None


def introspect_source(profile, task_id, logger):
    if profile.db_type == DbChoice.MONGODB:
        introspect_mongodb(profile, task_id)
        return

    engine = create_source_engine(profile)
    try:
        inspector = sa.inspect(engine)
        schemas = inspector.get_schema_names()
        if profile.db_type == DbChoice.POSTGRESQL:
            target_schemas = [
                schema for schema in schemas
                if schema not in {"information_schema", "pg_catalog", "pg_toast"}
            ]
        elif profile.db_type == DbChoice.MSSQL:
            target_schemas = [
                schema for schema in schemas
                if schema.casefold() not in {
                    "sys",
                    "information_schema",
                    "cdc",
                }
            ]
            with engine.connect() as connection:
                user_tables = {
                    tuple(row)
                    for row in connection.execute(sa.text(
                        "SELECT schemas.name, tables.name "
                        "FROM sys.tables AS tables "
                        "JOIN sys.schemas AS schemas "
                        "ON schemas.schema_id = tables.schema_id "
                        "WHERE tables.is_ms_shipped = 0"
                    ))
                }
        elif profile.db_type == DbChoice.ORACLE:
            target_schemas = [profile.username.upper()]
        else:
            target_schemas = [profile.database_name]

        for schema_index, schema_name in enumerate(target_schemas, start=1):
            try:
                tables = inspector.get_table_names(
                    schema=schema_name if profile.db_type != DbChoice.MYSQL else None
                )
                if profile.db_type == DbChoice.MSSQL:
                    tables = [
                        table for table in tables
                        if (schema_name, table) in user_tables
                    ]
            except Exception as exc:
                logger.warning(f"Could not fetch tables for schema {schema_name}: {exc}")
                continue

            for table_name in tables:
                current_schema = (
                    schema_name if profile.db_type != DbChoice.MYSQL
                    else profile.database_name
                )
                set_task_progress(
                    task_id,
                    percentage=int(schema_index / max(len(target_schemas), 1) * 50),
                    status="RUNNING",
                    current_table=f"{current_schema}.{table_name}",
                )
                source_table, _ = SourceTable.objects.get_or_create(
                    source=profile,
                    schema_name=current_schema,
                    table_name=table_name,
                )

                try:
                    primary_key = inspector.get_pk_constraint(
                        table_name,
                        schema=schema_name if profile.db_type != DbChoice.MYSQL else None,
                    )
                    primary_key_fields = (
                        primary_key.get("constrained_columns") or []
                    )
                    if source_table.primary_key_fields != primary_key_fields:
                        source_table.primary_key_fields = primary_key_fields
                        source_table.incremental_watermark = None
                        source_table.incremental_ready = False
                        source_table.save(update_fields=[
                            "primary_key_fields",
                            "incremental_watermark",
                            "incremental_ready",
                        ])
                except Exception as exc:
                    logger.warning(
                        f"Could not fetch primary key for {current_schema}.{table_name}: {exc}"
                    )

                try:
                    columns = inspector.get_columns(
                        table_name,
                        schema=schema_name if profile.db_type != DbChoice.MYSQL else None,
                    )
                except Exception as exc:
                    logger.warning(
                        f"Could not fetch columns for {current_schema}.{table_name}: {exc}"
                    )
                    continue

                for column in columns:
                    SourceField.objects.update_or_create(
                        table=source_table,
                        field_name=column["name"],
                        defaults={"data_type": str(column["type"])},
                    )

                try:
                    foreign_keys = inspector.get_foreign_keys(
                        table_name,
                        schema=schema_name if profile.db_type != DbChoice.MYSQL else None,
                    )
                    for foreign_key in foreign_keys:
                        referred_schema = foreign_key.get("referred_schema") or schema_name
                        for from_field, to_field in zip(
                            foreign_key.get("constrained_columns", []),
                            foreign_key.get("referred_columns", []),
                        ):
                            SourceRelationship.objects.update_or_create(
                                source=profile,
                                from_schema=current_schema,
                                from_table=table_name,
                                from_field=from_field,
                                to_table=foreign_key.get("referred_table"),
                                to_field=to_field,
                                defaults={
                                    "to_schema": (
                                        referred_schema
                                        if profile.db_type != DbChoice.MYSQL
                                        else profile.database_name
                                    ),
                                    "constraint_name": foreign_key.get("name"),
                                },
                            )
                except Exception as exc:
                    logger.warning(
                        f"Could not fetch foreign keys for {current_schema}.{table_name}: {exc}"
                    )
    finally:
        engine.dispose()


class CancellableETLTask(Task):
    """Base Celery task that kills the corresponding ClickHouse query if revoked or cancelled."""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        if isinstance(exc, (KeyboardInterrupt, SystemExit, InterruptedError)):
            self._kill_clickhouse_query(task_id)
        status = "CANCELLED" if isinstance(exc, InterruptedError) else "FAILED"
        set_task_progress(
            task_id,
            percentage=100,
            status=status,
            error=str(exc),
        )
        super().on_failure(exc, task_id, args, kwargs, einfo)

    def _kill_clickhouse_query(self, task_id):
        query_id = f"etl_task_{task_id}"
        ch_client = None
        try:
            ch_client = clickhouse_connect.get_client(
                host=getattr(settings, 'CLICKHOUSE_HOST', 'localhost'),
                port=getattr(settings, 'CLICKHOUSE_PORT', 8123),
                username=getattr(settings, 'CLICKHOUSE_USER', 'default'),
                password=getattr(settings, 'CLICKHOUSE_PASSWORD', ''),
            )
            cancel_clickhouse_query(ch_client, query_id)
        except Exception:
            logging.getLogger(__name__).exception(
                "Could not cancel ClickHouse query %s; external work may still be active.",
                query_id,
            )
        finally:
            if ch_client is not None:
                ch_client.close()


@task_revoked.connect
def cancel_terminated_etl_query(sender=None, request=None, terminated=False, **kwargs):
    if not terminated or not isinstance(sender, CancellableETLTask):
        return
    task_id = getattr(request, "id", None)
    if task_id:
        sender._kill_clickhouse_query(task_id)


def get_etl_logger():
    """Configures and returns a dedicated logger writing outside Docker to host volume."""
    logger = logging.getLogger("reporthub_etl")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        log_dir = "/app/logs"
        os.makedirs(log_dir, exist_ok=True)
        
        file_handler = logging.FileHandler(os.path.join(log_dir, "etl_execution.log"))
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


@shared_task(bind=True, queue='queue_ux')
def test_external_logging_task(self):
    """A test task to verify host-mounted file logging from inside Docker."""
    logger = get_etl_logger()
    logger.info("TEST_LOG_EVENT: Celery worker successfully wrote to host-mounted log directory!")
    return "Test log entry written to /app/logs/etl_execution.log"


@shared_task(bind=True, queue='queue_hot')
def sync_data_source_task(self, source_pk):
    """
    ETL pipeline task for database connection checks and basic status updates,
    recording detailed execution logs to the host-mounted volume.
    """
    logger = get_etl_logger()
    task_id = self.request.id
    set_task_progress(task_id, percentage=10, status="RUNNING", current_table=CONNECTING_TO_SOURCE)
    
    try:
        profile = SourceProfile.objects.get(pk=source_pk)
    except SourceProfile.DoesNotExist:
        logger.error(f"ETL Error: SourceProfile with ID {source_pk} not found.")
        set_task_progress(task_id, percentage=0, status="FAILED", error="SourceProfile not found")
        return f"SourceProfile {source_pk} not found."

    logger.info(f"Starting ETL sync pipeline for data source: '{profile.name}' ({profile.db_type}) at {profile.host}:{profile.port}")

    profile.last_sync_status = SyncStatus.RUNNING
    profile.last_sync_error = ''
    profile.save(update_fields=['last_sync_status', 'last_sync_error'])

    try:
        if profile.db_type in (DbChoice.MONGODB, DbChoice.ORACLE):
            if profile.db_type == DbChoice.MONGODB:
                client = create_mongo_client(profile)
                try:
                    client.admin.command("ping")
                finally:
                    client.close()
            else:
                engine = create_source_engine(profile)
                try:
                    with engine.connect():
                        pass
                finally:
                    engine.dispose()

            profile.last_synced = timezone.now()
            profile.last_sync_status = SyncStatus.SUCCESS
            profile.last_sync_error = ''
            profile.save(update_fields=['last_synced', 'last_sync_status', 'last_sync_error'])
            set_task_progress(
                task_id,
                percentage=100,
                status="SUCCESS",
                current_table="Connection Verified",
            )
            logger.info(f"ETL sync successfully completed for data source: '{profile.name}'")
            return f"ETL pipeline successfully synced {profile.name}"

        if profile.db_type == DbChoice.MYSQL:
            connection = pymysql.connect(
                host=profile.host,
                port=profile.port,
                user=profile.username,
                password=profile.password,
                database=profile.database_name,
                connect_timeout=5
            )
            connection.close()
            
        elif profile.db_type == DbChoice.POSTGRESQL:
            connection = psycopg2.connect(
                host=profile.host,
                port=profile.port,
                user=profile.username,
                password=profile.password,
                dbname=profile.database_name,
                connect_timeout=5
            )
            connection.close()

        elif profile.db_type == DbChoice.MSSQL:
            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={profile.host},{profile.port};"
                f"DATABASE={profile.database_name};"
                f"UID={profile.username};"
                f"PWD={profile.password};"
                f"TrustServerCertificate=yes;"
            )
            connection = pyodbc.connect(conn_str, timeout=5)
            connection.close()

        else:
            raise ValueError(f"Unsupported database type: {profile.db_type}")

        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.SUCCESS
        profile.last_sync_error = ''
        profile.save(update_fields=['last_synced', 'last_sync_status', 'last_sync_error'])
        
        set_task_progress(task_id, percentage=100, status="SUCCESS", current_table="Connection Verified")
        logger.info(f"ETL sync successfully completed for data source: '{profile.name}'")
        return f"ETL pipeline successfully synced {profile.name}"

    except Exception as e:
        error_message = str(e)
        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.FAILED
        profile.last_sync_error = error_message
        profile.save(update_fields=['last_synced', 'last_sync_status', 'last_sync_error'])
        
        set_task_progress(task_id, percentage=100, status="FAILED", error=error_message)
        logger.error(f"ETL sync failed for data source '{profile.name}': {error_message}")
        return f"ETL pipeline failed for {profile.name}: {error_message}"


@shared_task(bind=True, queue='queue_ux')
def test_source_connection_task(self, source_pk):
    """Background task wrapper specifically for Save & Test Connection workflow."""
    return sync_data_source_task(source_pk)


@shared_task(bind=True, queue='queue_periodic')
def introspect_source_schema_task(self, source_pk):
    """
    Introspects remote source database tables, columns, and foreign key relationships using SQLAlchemy,
    updating SourceTable, SourceField, and SourceRelationship control plane models automatically.
    """
    logger = get_etl_logger()
    task_id = self.request.id
    set_task_progress(task_id, percentage=5, status="RUNNING", current_table=INTROSPECTING_SCHEMA)
    
    try:
        profile = SourceProfile.objects.get(pk=source_pk)
    except SourceProfile.DoesNotExist:
        logger.error(f"Introspection Error: SourceProfile with ID {source_pk} not found.")
        set_task_progress(task_id, percentage=0, status="FAILED", error="SourceProfile not found")
        return f"SourceProfile {source_pk} not found."

    logger.info(f"Starting schema introspection for data source: '{profile.name}' ({profile.db_type})")

    profile.last_sync_status = SyncStatus.RUNNING
    profile.last_sync_error = ''
    profile.save(update_fields=['last_sync_status', 'last_sync_error'])

    try:
        with source_metadata_write_guard(source_pk):
            profile = SourceProfile.objects.select_for_update().get(pk=source_pk)
            introspect_source(profile, task_id, logger)

        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.SUCCESS
        profile.last_sync_error = ''
        profile.save(update_fields=['last_synced', 'last_sync_status', 'last_sync_error'])

        set_task_progress(task_id, percentage=100, status="SUCCESS", current_table="Introspection Complete")
        logger.info(f"Schema introspection successfully completed for data source: '{profile.name}'")
        return f"Schema introspection successfully completed for {profile.name}"

    except Exception as e:
        error_message = str(e)
        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.FAILED
        profile.last_sync_error = error_message
        profile.save(update_fields=['last_synced', 'last_sync_status', 'last_sync_error'])

        set_task_progress(task_id, percentage=100, status="FAILED", error=error_message)
        logger.error(f"Schema introspection failed for data source '{profile.name}': {error_message}")
        return f"Schema introspection failed for {profile.name}: {error_message}"


@shared_task(bind=True, queue='queue_cold')
def cleanup_postgres_warehouse_task(self):
    """Cleans up stale warehouse data."""
    pass


@shared_task(bind=True, queue='queue_periodic')
def trigger_all_active_source_introspection_task(self):
    """
    Periodic wrapper task that queries all active SourceProfiles 
    and triggers schema introspection for each.
    """
    logger = get_etl_logger()
    active_sources = SourceProfile.objects.filter(is_active=True)
    
    logger.info(f"Periodic Beat: Triggering schema introspection for {active_sources.count()} active source(s).")
    
    for source in active_sources:
        introspect_source_schema_task.delay(source.pk)
        
    return f"Successfully dispatched schema introspection for {active_sources.count()} active sources."


def _batch_required_fields_by_table(profile):
    required = {}
    for relationship in SourceRelationship.objects.filter(source=profile):
        required.setdefault(
            (relationship.from_schema, relationship.from_table), set()
        ).add(relationship.from_field)
        required.setdefault(
            (relationship.to_schema, relationship.to_table), set()
        ).add(relationship.to_field)
    for report in ReportDefinition.objects.filter(source=profile):
        for column in (report.config or {}).get('selectedColumns', []):
            if not isinstance(column, dict):
                continue
            table_id = column.get('table_id')
            field_name = column.get('field_name')
            if not table_id or not field_name:
                continue
            source_table = SourceTable.objects.filter(source=profile, pk=table_id).first()
            if source_table:
                required.setdefault(
                    (source_table.schema_name, source_table.table_name), set()
                ).add(field_name)
        for field_name in report.selected_fields or []:
            if isinstance(field_name, str):
                required.setdefault(
                    (report.base_table.schema_name, report.base_table.table_name), set()
                ).add(field_name)
    return required


def _batch_selected_fields(profile, table, required_fields_by_table):
    required = required_fields_by_table.get(
        (table.schema_name, table.table_name), set()
    )
    mappings = WarehouseDatasetMapping.objects.filter(
        source_profile=profile, table_name=table.table_name,
    )
    if mappings.exists():
        selected = mappings.filter(is_enabled=True).values_list('field_name', flat=True)
        return list(table.fields.filter(
            Q(field_name__in=selected) | Q(field_name__in=required)
        ))
    return list(table.fields.filter(
        Q(is_selected=True) | Q(field_name__in=required)
    ))


def _batch_sanitized_rows(chunk, ingestion_fields, primary_key_fields):
    field_names = [field.field_name for field in ingestion_fields]
    field_ch_types = {
        field.field_name: map_to_clickhouse_type(
            field.data_type, is_nullable=field.field_name not in primary_key_fields
        ).lower()
        for field in ingestion_fields
    }
    chunk = chunk.astype(object).where(pd.notnull(chunk), None)
    rows = []
    for row in chunk[field_names].values.tolist():
        sanitized = []
        for field_name, value in zip(field_names, row):
            ch_type = field_ch_types[field_name]
            if value is None or (isinstance(value, float) and pd.isna(value)):
                sanitized.append(None)
            elif 'string' in ch_type:
                if hasattr(value, 'isoformat'):
                    sanitized.append(value.isoformat())
                elif isinstance(value, float):
                    sanitized.append(str(int(value)) if value.is_integer() else str(value))
                else:
                    sanitized.append(str(value))
            elif 'date' in ch_type or 'time' in ch_type:
                sanitized.append(value.isoformat() if hasattr(value, 'isoformat') else str(value))
            else:
                sanitized.append(value)
        rows.append(sanitized)
    return rows


def _ingest_source_to_clickhouse_batch(
    task,
    source_pk,
    batch_size=None,
    cdc_fallback_only=False,
    full_refresh=False,
    table_index=0,
    row_offset=0,
    last_key_values=None,
    ingestion_run_id=None,
    incremental_run=None,
    watermark_value=None,
    watermark_upper=None,
    watermark_max=None,
    task_queue='queue_hot',
):
    """
    Ingest one bounded batch from a source, then enqueue a continuation if needed.
    """
    logger = get_etl_logger()
    task_id = task.request.id
    query_id = f"etl_task_{task_id}"
    set_task_progress(task_id, percentage=0, status="RUNNING", current_table=INITIALIZING_INGESTION)

    if batch_size is None:
        batch_size = getattr(settings, 'DEFAULT_ETL_BATCH_SIZE', 10000)
    batch_size = max(1, int(batch_size))
    ingestion_run_id = ingestion_run_id or uuid4().hex

    try:
        profile = SourceProfile.objects.get(pk=source_pk)
    except SourceProfile.DoesNotExist:
        logger.error(f"Ingestion Error: SourceProfile {source_pk} not found.")
        set_task_progress(task_id, percentage=0, status="FAILED", error="SourceProfile not found")
        return f"SourceProfile {source_pk} not found."

    if not profile.is_active:
        logger.warning(f"Ingestion skipped: Data source '{profile.name}' is currently deactivated.")
        set_task_progress(task_id, percentage=0, status="SKIPPED", error="Source deactivated")
        return f"Ingestion skipped for deactivated source: {profile.name}"

    if profile.ingestion_mode == IngestionMode.BATCH and SourceTable.objects.filter(
        source=profile, cdc_managed=True
    ).exists() and profile.db_type != DbChoice.MONGODB:
        raise RuntimeError(
            "Cannot run batch ingestion on a CDC-managed source until a "
            "coordinated mode transition is implemented."
        )

    if full_refresh and profile.ingestion_mode == IngestionMode.CDC and any(
        table.primary_key_fields
        for table in SourceTable.objects.filter(source=profile)
    ):
        raise RuntimeError(
            "Full-refresh backfills are disabled for sources with primary-key "
            "tables managed by CDC. Use Debezium's initial snapshot; a "
            "backfill requires a coordinated CDC snapshot to preserve event "
            "ordering."
        )

    set_task_progress(
        task_id,
        percentage=int(
            table_index
            / max(
                SourceTable.objects.filter(source=profile).count(),
                1,
            )
            * 100
        ),
        status="RUNNING",
        current_table=PREPARING_INGESTION,
        source_name=profile.name,
    )
    logger.info(
        f"Starting ClickHouse ingestion batch for '{profile.name}' "
        f"(Batch Size: {batch_size}, table index: {table_index}, row offset: {row_offset})"
    )

    profile.last_sync_status = SyncStatus.RUNNING
    profile.last_sync_error = ''
    profile.save(update_fields=['last_sync_status', 'last_sync_error'])

    ch_client = clickhouse_connect.get_client(
        host=getattr(settings, 'CLICKHOUSE_HOST', 'localhost'),
        port=getattr(settings, 'CLICKHOUSE_PORT', 8123),
        username=getattr(settings, 'CLICKHOUSE_USER', 'default'),
        password=getattr(settings, 'CLICKHOUSE_PASSWORD', ''),
        settings=_clickhouse_ingestion_settings(),
    )

    try:
        source_engine = (
            None if profile.db_type == DbChoice.MONGODB
            else create_source_engine(profile)
        )
    except Exception as e:
        logger.error(f"Failed to create source engine for {profile.name}: {e}")
        profile.last_sync_status = SyncStatus.FAILED
        profile.last_sync_error = str(e)
        profile.save(update_fields=['last_sync_status', 'last_sync_error'])
        set_task_progress(task_id, percentage=0, status="FAILED", error=str(e))
        return f"Engine creation failed: {e}"

    source_tables = list(
        SourceTable.objects.filter(
            source=profile,
            source__is_active=True,
        ).order_by('pk')
    )
    if cdc_fallback_only:
        source_tables = [
            source_table for source_table in source_tables
            if not source_table.primary_key_fields
        ]
    total_tables = len(source_tables)

    if total_tables == 0:
        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.SUCCESS
        profile.last_sync_error = ''
        profile.save(update_fields=['last_synced', 'last_sync_status', 'last_sync_error'])
        set_task_progress(task_id, percentage=100, status="SUCCESS", current_table="No active tables")
        if source_engine:
            source_engine.dispose()
        ch_client.close()
        return f"No active tables to ingest for {profile.name}"

    if table_index >= total_tables:
        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.SUCCESS
        profile.last_sync_error = ''
        profile.save(update_fields=['last_synced', 'last_sync_status', 'last_sync_error'])
        if source_engine:
            source_engine.dispose()
        ch_client.close()
        set_task_progress(task_id, percentage=100, status="SUCCESS", current_table="Completed")
        return f"ClickHouse ingestion completed for {profile.name}"

    required_fields_by_table = _batch_required_fields_by_table(profile)

    table = source_tables[table_index]
    ch_table_name = source_table_clickhouse_name(table)
    staging_table_name = (
        f"{ch_table_name}__staging_{ingestion_run_id}"
    ).lower()

    def cancel_if_source_deactivated():
        profile.refresh_from_db()
        if profile.is_active:
            return False

        profile.last_sync_status = SyncStatus.CANCELLED
        profile.save(update_fields=['last_sync_status'])
        set_task_progress(
            task_id,
            percentage=int((table_index / total_tables) * 100),
            status="CANCELLED",
        )
        if not incremental_run:
            ch_client.command(f"DROP TABLE IF EXISTS {staging_table_name}")
        if source_engine:
            source_engine.dispose()
        ch_client.close()
        return True

    if cancel_if_source_deactivated():
        return f"Ingestion cancelled for {profile.name}"

    selected_fields = _batch_selected_fields(profile, table, required_fields_by_table)

    if not selected_fields:
        if full_refresh:
            ch_client.command(f"DROP TABLE IF EXISTS {ch_table_name}")
            table.incremental_ready = False
            table.incremental_watermark = None
            table.clickhouse_uses_replacing_merge_tree = False
            table.cdc_managed = False
            table.save(update_fields=[
                'incremental_ready',
                'incremental_watermark',
                'clickhouse_uses_replacing_merge_tree',
                'cdc_managed',
            ])
        next_table_index = table_index + 1
        if source_engine:
            source_engine.dispose()
        ch_client.close()
        if next_table_index < total_tables:
            ingest_source_to_clickhouse_task.apply_async(
                args=[source_pk],
                kwargs={
                    'batch_size': batch_size,
                    'cdc_fallback_only': cdc_fallback_only,
                    'full_refresh': full_refresh,
                    'origin_mode': profile.ingestion_mode,
                    'table_index': next_table_index,
                    'ingestion_run_id': ingestion_run_id,
                    'task_queue': task_queue,
                },
                queue=task_queue,
            )
            return f"Queued ingestion for next table in {profile.name}"
        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.SUCCESS
        profile.last_sync_error = ''
        profile.save(update_fields=['last_synced', 'last_sync_status', 'last_sync_error'])
        set_task_progress(task_id, percentage=100, status="SUCCESS", current_table="Completed")
        return f"ClickHouse ingestion completed for {profile.name}"

    source_fields = {field.field_name: field for field in table.fields.all()}
    incremental_field = source_fields.get(table.incremental_field_name)
    incremental_eligible = bool(
        profile.ingestion_mode == IngestionMode.CDC
        and profile.db_type != DbChoice.MONGODB
        and table.primary_key_fields
        and incremental_field
        and is_datetime_source_type(incremental_field.data_type)
        and all(key in source_fields for key in table.primary_key_fields)
    )
    cdc_full_refresh = bool(
        profile.ingestion_mode == IngestionMode.CDC
        and profile.db_type != DbChoice.MONGODB
        and table.primary_key_fields
        and (table.cdc_managed or full_refresh)
    )

    target_exists = ch_client.query(
        f"EXISTS TABLE {ch_table_name}",
        settings={"query_id": query_id},
    ).result_rows[0][0]
    if incremental_run is None:
        incremental_run = bool(
            incremental_eligible
            and table.incremental_ready
            and table.incremental_watermark
            and target_exists
            and not full_refresh
        )
        if incremental_run:
            watermark_value = table.incremental_watermark
            watermark_upper = get_source_watermark(
                profile,
                source_engine,
                table,
                table.incremental_field_name,
            )
            if watermark_upper is None:
                watermark_upper = watermark_value
            if (
                watermark_upper
                and watermark_is_greater(watermark_value, watermark_upper)
            ):
                watermark_upper = watermark_value
    elif not incremental_eligible:
        incremental_run = False

    internal_fields = [
        source_fields[key]
        for key in table.primary_key_fields
        if key in source_fields
    ]
    if incremental_eligible:
        internal_fields.append(incremental_field)
    field_by_name = {field.field_name: field for field in selected_fields}
    field_by_name.update({field.field_name: field for field in internal_fields})
    ingestion_fields = list(field_by_name.values())
    field_names = [field.field_name for field in ingestion_fields]

    if incremental_run:
        actual_schema = {
            row[0]: row[1] for row in ch_client.query(
                f"DESCRIBE TABLE {ch_table_name}",
                settings={"query_id": query_id},
            ).result_rows
        }
        expected_schema = {
            field.field_name: map_to_clickhouse_type(
                field.data_type,
                is_nullable=(field.field_name not in table.primary_key_fields)
            )
            for field in ingestion_fields
        }
        if actual_schema != expected_schema:
            incremental_run = False

    if not incremental_run and row_offset == 0 and not last_key_values:
        column_defs = []
        for field in ingestion_fields:
            nullable = field.field_name not in table.primary_key_fields
            ch_type = map_to_clickhouse_type(field.data_type, is_nullable=nullable)
            safe_name = field.field_name.replace('`', '``')
            column_defs.append(f"`{safe_name}` {ch_type}")
        columns_sql = ", ".join(column_defs)

        if cdc_full_refresh:
            order_by = ", ".join(
                f"`{key.replace('`', '``')}`" for key in table.primary_key_fields
            )
            column_defs.extend([
                "`_cdc_version` UInt64",
                "`_cdc_deleted` UInt8",
            ])
            columns_sql = ", ".join(column_defs)
            engine_sql = (
                "ReplacingMergeTree(_cdc_version, _cdc_deleted) "
                f"ORDER BY ({order_by})"
            )
        elif incremental_eligible:
            order_by = ", ".join(
                f"`{key.replace('`', '``')}`" for key in table.primary_key_fields
            )
            engine_sql = f"ReplacingMergeTree() ORDER BY ({order_by})"
        else:
            engine_sql = "MergeTree() ORDER BY tuple()"
        ch_client.command(f"DROP TABLE IF EXISTS {staging_table_name}")
        ch_client.command(
            f"CREATE TABLE {staging_table_name} ({columns_sql}) ENGINE = {engine_sql}",
            settings={"query_id": query_id},
        )
    elif not incremental_run:
        stage_exists = ch_client.query(
            f"EXISTS TABLE {staging_table_name}",
            settings={"query_id": query_id},
        ).result_rows[0][0]
        if not stage_exists:
            raise RuntimeError(
                f"Staging table for {table.table_name} is missing; rerun a backfill."
            )

    try:
        if profile.db_type == DbChoice.MONGODB:
            query_filter = {}
            if incremental_run and watermark_value:
                query_filter[table.incremental_field_name] = {
                    "$gte": parse_ingestion_watermark(watermark_value),
                    "$lte": parse_ingestion_watermark(watermark_upper),
                }
            chunks = read_mongo_chunks(
                profile,
                table.table_name,
                field_names,
                batch_size,
                query_filter=query_filter,
                skip=row_offset,
            )
            chunk = next(chunks, pd.DataFrame(columns=field_names))
        else:
            schema_name = (
                None if profile.db_type == DbChoice.MYSQL
                else table.schema_name
            )
            source_table = sa.Table(
                table.table_name,
                sa.MetaData(),
                schema=schema_name,
                autoload_with=source_engine,
            )
            statement = sa.select(
                *(source_table.c[field_name] for field_name in field_names)
            )
            params = None
            if incremental_run and watermark_value:
                statement = statement.where(
                    source_table.c[table.incremental_field_name]
                    >= sa.bindparam("ingestion_watermark"),
                    source_table.c[table.incremental_field_name]
                    <= sa.bindparam("ingestion_upper_bound"),
                )
                params = {
                    "ingestion_watermark": parse_ingestion_watermark(watermark_value),
                    "ingestion_upper_bound": parse_ingestion_watermark(watermark_upper),
                }
            use_keyset = bool(
                not incremental_run
                and table.primary_key_fields
                and last_key_values
            )
            if use_keyset:
                key_columns = [
                    source_table.c[key] for key in table.primary_key_fields
                ]
                key_values = [
                    parse_source_key(value, column)
                    for value, column in zip(last_key_values, key_columns)
                ]
                keyset_conditions = []
                for index, key_column in enumerate(key_columns):
                    equal_prefix = [
                        key_columns[prefix] == key_values[prefix]
                        for prefix in range(index)
                    ]
                    keyset_conditions.append(
                        sa.and_(*equal_prefix, key_column > key_values[index])
                    )
                statement = statement.where(sa.or_(*keyset_conditions))
            if table.primary_key_fields:
                statement = statement.order_by(
                    *(source_table.c[key] for key in table.primary_key_fields)
                )
            elif profile.db_type == DbChoice.MSSQL:
                first_col = list(source_table.c)[0]
                statement = statement.order_by(first_col)

            if not use_keyset:
                statement = statement.offset(row_offset)
            statement = statement.limit(batch_size)
            with source_engine.connect() as connection:
                chunk = pd.read_sql(
                    statement,
                    con=connection,
                    params=params,
                )

        if cancel_if_source_deactivated():
            return f"Ingestion cancelled for {profile.name}"

        if not chunk.empty and incremental_eligible:
            values = chunk[table.incremental_field_name].dropna()
            if not values.empty:
                chunk_watermark = serialize_ingestion_watermark(values.max())
                if (
                    watermark_max is None
                    or watermark_is_greater(chunk_watermark, watermark_max)
                ):
                    watermark_max = chunk_watermark

        if (
            not chunk.empty
            and not incremental_run
            and table.primary_key_fields
        ):
            last_key_values = [
                serialize_source_key(chunk.iloc[-1][key])
                for key in table.primary_key_fields
            ]

        data_rows = _batch_sanitized_rows(chunk, ingestion_fields, table.primary_key_fields)
        if data_rows:
            insert_field_names = field_names
            if cdc_full_refresh:
                snapshot_version = int(datetime.now().timestamp() * 1000) * 1_000_000
                data_rows = [
                    row + [snapshot_version + index, 0]
                    for index, row in enumerate(data_rows)
                ]
                insert_field_names = field_names + [
                    "_cdc_version",
                    "_cdc_deleted",
                ]
            ch_client.insert(
                ch_table_name if incremental_run else staging_table_name,
                data_rows,
                column_names=insert_field_names,
                settings={"query_id": query_id},
            )
            if cancel_if_source_deactivated():
                return f"Ingestion cancelled for {profile.name}"
    except Exception as exc:
        profile.last_sync_status = SyncStatus.FAILED
        profile.last_sync_error = str(exc)
        profile.save(update_fields=['last_sync_status', 'last_sync_error'])
        if not incremental_run:
            try:
                ch_client.command(f"DROP TABLE IF EXISTS {staging_table_name}")
            except Exception:
                logger.exception("Could not clean up failed ClickHouse staging table.")
        if source_engine:
            source_engine.dispose()
        ch_client.close()
        raise

    rows_in_batch = len(chunk.index)
    if rows_in_batch >= batch_size:
        use_keyset = bool(
            profile.db_type != DbChoice.MONGODB
            and not incremental_run and table.primary_key_fields
        )
        next_offset = (
            row_offset
            if use_keyset
            else row_offset + rows_in_batch
        )
        continuation_watermark = (
            watermark_value
            if incremental_run and watermark_value
            else None
        )
        ingest_source_to_clickhouse_task.apply_async(
            args=[source_pk],
            kwargs={
                'batch_size': batch_size,
                'cdc_fallback_only': cdc_fallback_only,
                'full_refresh': full_refresh,
                'origin_mode': profile.ingestion_mode,
                'table_index': table_index,
                'row_offset': next_offset,
                'last_key_values': last_key_values if use_keyset else None,
                'ingestion_run_id': ingestion_run_id,
                'incremental_run': incremental_run,
                'watermark_value': continuation_watermark,
                'watermark_upper': watermark_upper,
                'watermark_max': watermark_max,
                'task_queue': task_queue,
            },
            queue=task_queue,
        )
        if source_engine:
            source_engine.dispose()
        ch_client.close()
        return f"Queued next {batch_size}-row ingestion batch for {table.table_name}"

    if not incremental_run:
        if cancel_if_source_deactivated():
            return f"Ingestion cancelled for {profile.name}"
        if target_exists:
            ch_client.command(
                f"EXCHANGE TABLES {ch_table_name} AND {staging_table_name}",
                settings={"query_id": query_id},
            )
            ch_client.command(
                f"DROP TABLE {staging_table_name}",
                settings={"query_id": query_id},
            )
        else:
            ch_client.command(
                f"RENAME TABLE {staging_table_name} TO {ch_table_name}",
                settings={"query_id": query_id},
            )
    if incremental_eligible:
        if incremental_run and watermark_upper:
            table.incremental_watermark = watermark_upper
        elif watermark_max:
            table.incremental_watermark = watermark_max
        table.incremental_ready = True
    else:
        table.incremental_ready = False
        table.incremental_watermark = None
    table.clickhouse_uses_replacing_merge_tree = bool(
        cdc_full_refresh or incremental_eligible
    )
    if profile.ingestion_mode == IngestionMode.BATCH:
        table.cdc_managed = False
    elif full_refresh:
        table.cdc_managed = bool(table.primary_key_fields)
    elif not table.primary_key_fields:
        table.cdc_managed = False
    table.last_synced_at = timezone.now()
    table.save(update_fields=[
        'incremental_watermark',
        'incremental_ready',
        'clickhouse_uses_replacing_merge_tree',
        'cdc_managed',
        'last_synced_at',
    ])
    logger.info(
        f"Successfully ingested table {table.table_name} into ClickHouse as {ch_table_name}"
    )

    next_table_index = table_index + 1
    if source_engine:
        source_engine.dispose()
    ch_client.close()
    if next_table_index < total_tables:
        ingest_source_to_clickhouse_task.apply_async(
            args=[source_pk],
            kwargs={
                'batch_size': batch_size,
                'cdc_fallback_only': cdc_fallback_only,
                'full_refresh': full_refresh,
                'origin_mode': profile.ingestion_mode,
                'table_index': next_table_index,
                'ingestion_run_id': ingestion_run_id,
                'task_queue': task_queue,
            },
            queue=task_queue,
        )
        return f"Queued ingestion for next table in {profile.name}"

    profile.last_synced = timezone.now()
    profile.last_sync_status = SyncStatus.SUCCESS
    profile.last_sync_error = ''
    profile.save(update_fields=['last_synced', 'last_sync_status', 'last_sync_error'])
    set_task_progress(task_id, percentage=100, status="SUCCESS", current_table="Completed")
    return f"ClickHouse ingestion completed for {profile.name}"


def _validate_ingestion_task_origin(profile, origin_mode):
    if origin_mode is not None and origin_mode not in IngestionMode.values:
        raise ValueError("Invalid ingestion task origin mode.")
    if profile and origin_mode is not None and origin_mode != profile.ingestion_mode:
        raise RuntimeError(
            "The queued ingestion task originated in a different source mode."
        )
    if (
        profile and origin_mode is None
        and profile.ingestion_mode == IngestionMode.BATCH
        and CdcToBatchTransition.objects.filter(
            source_id=profile.pk,
            status=CdcToBatchTransition.Status.SUCCEEDED,
        ).exists()
    ):
        raise RuntimeError(
            "An unmarked legacy ingestion task cannot run after a CDC-to-batch transition."
        )


@shared_task(bind=True, base=CancellableETLTask, queue='queue_hot')
def ingest_source_to_clickhouse_task(
    self,
    source_pk,
    batch_size=None,
    cdc_fallback_only=False,
    full_refresh=False,
    table_index=0,
    row_offset=0,
    last_key_values=None,
    ingestion_run_id=None,
    incremental_run=None,
    watermark_value=None,
    watermark_upper=None,
    watermark_max=None,
    cdc_continue=False,
    task_queue='queue_hot',
    origin_mode=None,
):
    try:
        profile = SourceProfile.objects.filter(pk=source_pk).only(
            "db_type", "ingestion_mode"
        ).first()
        _validate_ingestion_task_origin(profile, origin_mode)
        if profile and profile.ingestion_mode == IngestionMode.BATCH:
            if cdc_continue or cdc_fallback_only:
                raise RuntimeError(
                    "A queued CDC task cannot run for a batch-mode source."
                )
            return _ingest_source_to_clickhouse_batch(
                self,
                source_pk,
                batch_size=batch_size,
                full_refresh=True,
                table_index=table_index,
                row_offset=row_offset,
                last_key_values=last_key_values,
                ingestion_run_id=ingestion_run_id,
                task_queue=task_queue,
            )
        if profile and profile.db_type == DbChoice.MONGODB:
            raise RuntimeError(
                "MongoDB CDC ingestion is not supported; use batch refresh."
            )
        if cdc_continue or (
            not full_refresh
            and not cdc_fallback_only
            and table_index == 0
            and row_offset == 0
            and last_key_values is None
            and ingestion_run_id is None
            and incremental_run is None
            and watermark_value is None
            and watermark_upper is None
            and watermark_max is None
        ):
            try:
                return _run_scheduled_cdc_ingestion(
                    self,
                    source_pk,
                    batch_size=batch_size,
                    task_queue="queue_cdc",
                )
            except Exception as exc:
                try:
                    CdcExecutionFailure.objects.create(
                        source_id=source_pk,
                        task_id=self.request.id or "",
                        error_message=(
                            f"{type(exc).__name__}: CDC execution failed. "
                            "See the ETL worker logs for details."
                        ),
                    )
                except Exception:
                    get_etl_logger().exception(
                        "Could not record CDC execution failure for source %s",
                        source_pk,
                    )
                raise
        batch_options = {
            "batch_size": batch_size,
            "cdc_fallback_only": cdc_fallback_only,
            "full_refresh": full_refresh,
            "table_index": table_index,
            "row_offset": row_offset,
            "last_key_values": last_key_values,
            "ingestion_run_id": ingestion_run_id,
            "incremental_run": incremental_run,
            "watermark_value": watermark_value,
            "watermark_upper": watermark_upper,
            "watermark_max": watermark_max,
            "task_queue": task_queue,
        }
        if profile and profile.ingestion_mode == IngestionMode.CDC:
            try:
                with _track_cdc_external_operation(
                    source_pk, CdcSourceExternalOperation.Kind.REFRESH,
                    self.request.id or "",
                ):
                    return _run_cdc_mode_refresh(self, source_pk, batch_options)
            except CdcExternalOperationCutoverPending:
                return _skip_cdc_cutover_pending(self, source_pk, "refresh")
        return _ingest_source_to_clickhouse_batch(self, source_pk, **batch_options)
    except Exception as exc:
        status = (
            SyncStatus.CANCELLED
            if isinstance(exc, InterruptedError)
            else SyncStatus.FAILED
        )
        SourceProfile.objects.filter(pk=source_pk).update(
            last_sync_status=status,
            last_sync_error=str(exc),
        )
        get_etl_logger().exception(
            "Source ingestion batch failed for source %s: %s",
            source_pk,
            exc,
        )
        raise


def _skip_cdc_cutover_pending(task, source_pk, operation, source_name=None):
    set_task_progress(
        task.request.id, percentage=0, status="SKIPPED",
        error="CDC-to-batch cutover is pending.",
    )
    get_etl_logger().info(
        "Skipped CDC %s for source %s: CDC-to-batch cutover is pending.",
        operation, source_pk,
    )
    if source_name is None:
        source_name = SourceProfile.objects.get(pk=source_pk).name
    return f"CDC {operation} skipped for {source_name}: CDC-to-batch cutover is pending."


def _run_cdc_mode_refresh(task, source_pk, batch_options):
    with transaction.atomic():
        locked_source = SourceProfile.objects.select_for_update().get(pk=source_pk)
        if locked_source.ingestion_mode != IngestionMode.CDC:
            raise RuntimeError(
                "A queued CDC refresh cannot run after the source mode changes."
            )
        if CdcToBatchTransition.objects.filter(
            source_id=source_pk,
            status=CdcToBatchTransition.Status.CUTOVER_PENDING,
        ).exists():
            return _skip_cdc_cutover_pending(task, source_pk, "refresh", locked_source.name)
        return _ingest_source_to_clickhouse_batch(task, source_pk, **batch_options)


def _run_scheduled_cdc_ingestion(
    task,
    source_pk,
    batch_size=None,
    task_queue='queue_hot',
):
    try:
        with _track_cdc_external_operation(
            source_pk, CdcSourceExternalOperation.Kind.INGESTION,
            task.request.id or "",
        ):
            with transaction.atomic():
                try:
                    SourceProfile.objects.select_for_update().get(pk=source_pk)
                except SourceProfile.DoesNotExist:
                    pass
                return _run_scheduled_cdc_ingestion_locked(
                    task, source_pk, batch_size=batch_size, task_queue=task_queue
                )
    except CdcExternalOperationCutoverPending:
        return _skip_cdc_cutover_pending(task, source_pk, "ingestion")


def _run_scheduled_cdc_ingestion_locked(
    task,
    source_pk,
    batch_size=None,
    task_queue='queue_hot',
):
    task_id = task.request.id
    try:
        profile = SourceProfile.objects.get(pk=source_pk)
    except SourceProfile.DoesNotExist:
        set_task_progress(
            task_id,
            percentage=0,
            status="FAILED",
            error="SourceProfile not found",
        )
        return f"SourceProfile {source_pk} not found."

    if not profile.is_active:
        set_task_progress(
            task_id,
            percentage=0,
            status="SKIPPED",
            error="Source deactivated",
        )
        return f"Ingestion skipped for deactivated source: {profile.name}"

    if batch_size is None:
        batch_size = settings.DEFAULT_ETL_BATCH_SIZE
    batch_size = max(1, int(batch_size))
    ch_client = clickhouse_connect.get_client(
        host=getattr(settings, 'CLICKHOUSE_HOST', 'localhost'),
        port=getattr(settings, 'CLICKHOUSE_PORT', 8123),
        username=getattr(settings, 'CLICKHOUSE_USER', 'default'),
        password=getattr(settings, 'CLICKHOUSE_PASSWORD', ''),
        settings=_clickhouse_ingestion_settings(),
    )
    lock_key = f"reporthub:cdc-ingestion:{source_pk}"
    lock_token = uuid4().hex
    try:
        lock_acquired = cache.add(lock_key, lock_token, timeout=900)
    except Exception:
        ch_client.close()
        raise
    if not lock_acquired:
        ch_client.close()
        ingest_source_to_clickhouse_task.apply_async(
            args=[source_pk],
            kwargs={
                "batch_size": batch_size,
                "cdc_continue": True,
                "origin_mode": IngestionMode.CDC,
                "task_queue": task_queue,
            },
            countdown=10,
            queue=task_queue,
        )
        return f"CDC ingestion for {profile.name} is already running; queued a retry."

    try:
        from etl.cdc import (
            _clickhouse_table_uuid,
            consume_source_cdc_batch,
            ensure_debezium_connector,
        )
        profile.refresh_from_db(fields=["ingestion_mode", "is_active"])
        if profile.ingestion_mode != IngestionMode.CDC or not profile.is_active:
            raise RuntimeError(
                "CDC ingestion cannot continue because the source mode or "
                "active state changed while waiting for the ingestion lock."
            )

        if CdcToBatchTransition.objects.filter(
            source_id=source_pk,
            status=CdcToBatchTransition.Status.CUTOVER_PENDING,
        ).exists():
            set_task_progress(
                task_id,
                percentage=0,
                status="SKIPPED",
                error="CDC-to-batch cutover is pending.",
            )
            get_etl_logger().info(
                "Skipped CDC ingestion for source %s: CDC-to-batch cutover is pending.",
                source_pk,
            )
            return f"CDC ingestion skipped for {profile.name}: CDC-to-batch cutover is pending."

        profile.last_sync_status = SyncStatus.RUNNING
        profile.last_sync_error = ""
        profile.save(update_fields=["last_sync_status", "last_sync_error"])

        pending_rebuilds = CdcTableRebuild.objects.filter(
            source_table__source=profile,
            status=CdcTableRebuild.Status.CUTOVER_PENDING,
        ).select_related("source_table")
        for recovery in pending_rebuilds:
            table = recovery.source_table
            active_table_name = source_table_clickhouse_name(table)
            if (
                recovery.staging_uuid
                and _clickhouse_table_uuid(ch_client, active_table_name)
                == recovery.staging_uuid
            ):
                raise RuntimeError(
                    f"CDC rebuild {recovery.pk} for "
                    f"{table.schema_name}.{table.table_name} has exchanged "
                    "the warehouse table but has not finished cutover. "
                    "Resume the rebuild before restarting source ingestion."
                )

        ensure_debezium_connector(profile)
        processed = consume_source_cdc_batch(
            profile,
            ch_client,
            batch_size=batch_size,
        )
        has_polling_tables = any(
            not source_table.primary_key_fields
            for source_table in profile.tables.all()
        )
        if has_polling_tables:
            ingest_source_to_clickhouse_task.apply_async(
                args=[source_pk],
                kwargs={
                    "batch_size": batch_size,
                    "cdc_fallback_only": True,
                    "origin_mode": IngestionMode.CDC,
                    "task_queue": "queue_backfill",
                },
                queue="queue_backfill",
            )
        else:
            profile.last_synced = timezone.now()
            profile.last_sync_status = SyncStatus.SUCCESS
            profile.last_sync_error = ""
            profile.save(update_fields=[
                "last_synced",
                "last_sync_status",
                "last_sync_error",
            ])
            set_task_progress(
                task_id,
                percentage=100,
                status="SUCCESS",
                current_table="CDC batch completed",
            )
        if processed >= batch_size:
            ingest_source_to_clickhouse_task.apply_async(
                args=[source_pk],
                kwargs={
                    "batch_size": batch_size,
                    "cdc_continue": True,
                    "origin_mode": IngestionMode.CDC,
                    "task_queue": task_queue,
                },
                queue=task_queue,
            )
        return (
            f"Processed {processed} CDC event(s) for {profile.name}"
            + ("; queued polling fallback tables." if has_polling_tables else ".")
            + (" Queued another CDC batch." if processed >= batch_size else "")
        )
    finally:
        ch_client.close()
        if cache.get(lock_key) == lock_token:
            cache.delete(lock_key)


def map_to_clickhouse_type(source_type, is_nullable=True):
    """Simple helper mapper to convert source SQL data types to ClickHouse types supporting Nullable."""
    source_type_lower = str(source_type).lower()
    if 'int' in source_type_lower:
        base_type = 'Int64'
    elif 'char' in source_type_lower or 'text' in source_type_lower or 'varchar' in source_type_lower:
        base_type = 'String'
    elif 'bool' in source_type_lower:
        base_type = 'UInt8'
    elif 'date' in source_type_lower or 'time' in source_type_lower:
        base_type = 'String'
    elif 'float' in source_type_lower or 'double' in source_type_lower or 'decimal' in source_type_lower:
        base_type = 'Float64'
    else:
        base_type = 'String'
        
    return f"Nullable({base_type})" if is_nullable else base_type


@shared_task(bind=True, queue='queue_periodic')
def trigger_all_active_clickhouse_ingestion_task(self):
    """
    Periodic wrapper task that queries all active SourceProfiles 
    and triggers ClickHouse batch ingestion for each.
    """
    logger = get_etl_logger()
    active_sources = SourceProfile.objects.filter(is_active=True)
    
    logger.info(f"Periodic Beat: Triggering ClickHouse data ingestion for {active_sources.count()} active source(s).")
    
    for source in active_sources:
        queue = (
            "queue_cdc"
            if source.ingestion_mode == IngestionMode.CDC
            else "queue_backfill"
        )
        ingest_source_to_clickhouse_task.apply_async(
            args=[source.pk],
            kwargs={"task_queue": queue, "origin_mode": source.ingestion_mode},
            queue=queue,
        )
        
    return f"Successfully dispatched ClickHouse ingestion for {active_sources.count()} active sources."


@shared_task(queue="queue_backfill")
def stage_cdc_to_batch_transition_task(transition_pk, batch_size=None):
    """Stage one table per task; revalidate all checkpoints when complete."""
    message, staged_one = _stage_cdc_to_batch_transition_step(transition_pk, batch_size)
    if staged_one:
        try:
            stage_cdc_to_batch_transition_task.apply_async(
                args=[transition_pk], kwargs={"batch_size": batch_size},
                queue="queue_backfill",
            )
        except Exception as exc:
            _record_cdc_to_batch_task_failure(transition_pk, "Staging", exc)
            raise
        return f"{message}; queued the next staging task."
    return message


def _record_cdc_to_batch_task_failure(transition_pk, phase, exc):
    CdcToBatchTransition.objects.filter(
        pk=transition_pk,
        status__in=[
            CdcToBatchTransition.Status.PREPARING,
            CdcToBatchTransition.Status.CUTOVER_PENDING,
        ],
    ).update(last_error=(
        f"{type(exc).__name__}: {phase} failed; see ETL worker logs for details."
    ), updated_at=timezone.now())
    CdcToBatchTransition.objects.filter(
        pk=transition_pk, status=CdcToBatchTransition.Status.PREPARING
    ).update(staging_verified_at=None)
    get_etl_logger().exception(
        "CDC-to-batch %s failed for transition %s", phase.lower(), transition_pk
    )


def _stage_cdc_to_batch_transition_step(transition_pk, batch_size=None):
    """Stage one table or verify all staged checkpoints."""
    from etl.cdc_to_batch import (
        cdc_to_batch_checkpoint_needs_staging,
        stage_cdc_to_batch_table,
        validate_cdc_to_batch_table_names,
    )

    try:
        transition = CdcToBatchTransition.objects.select_related("source").get(
            pk=transition_pk
        )
        source = transition.source
        if (
            transition.status != CdcToBatchTransition.Status.PREPARING
            or not source.is_active
            or source.ingestion_mode != IngestionMode.CDC
            or source.db_type == DbChoice.MONGODB
        ):
            raise RuntimeError("The transition is not eligible for batch staging.")

        validate_cdc_to_batch_table_names(source, source.tables.order_by("pk"))
        checkpoints = list(
            transition.table_checkpoints.order_by("source_table_id")
        )
        table_ids = set(source.tables.values_list("pk", flat=True))
        if not checkpoints or {item.source_table_id for item in checkpoints} != table_ids:
            raise RuntimeError(
                "The source table list changed since the transition was prepared."
            )
        if any(
            item.status not in (
                CdcToBatchTableCheckpoint.Status.PENDING,
                CdcToBatchTableCheckpoint.Status.STAGED,
            )
            for item in checkpoints
        ):
            raise RuntimeError("A transition checkpoint is not eligible for staging.")

        pending = next(
            (item for item in checkpoints
             if cdc_to_batch_checkpoint_needs_staging(item)),
            None,
        )
        if pending:
            stage_cdc_to_batch_table(pending.pk, batch_size=batch_size)
            CdcToBatchTransition.objects.filter(
                pk=transition_pk, status=CdcToBatchTransition.Status.PREPARING
            ).update(staging_verified_at=None)
            return f"Staged checkpoint {pending.pk}", True

        with transaction.atomic():
            locked_checkpoints = list(
                CdcToBatchTableCheckpoint.objects.select_for_update()
                .filter(transition_id=transition_pk).order_by("pk")
            )
            source = SourceProfile.objects.select_for_update().get(pk=source.pk)
            transition = CdcToBatchTransition.objects.select_for_update().get(pk=transition_pk)
            if (
                transition.source_id != source.pk
                or transition.status != CdcToBatchTransition.Status.PREPARING
                or not source.is_active
                or source.ingestion_mode != IngestionMode.CDC
                or source.db_type == DbChoice.MONGODB
                or {item.pk for item in checkpoints} != {item.pk for item in locked_checkpoints}
                or {item.source_table_id for item in locked_checkpoints}
                != set(source.tables.values_list("pk", flat=True))
                or any(cdc_to_batch_checkpoint_needs_staging(item) for item in locked_checkpoints)
            ):
                raise RuntimeError("The transition changed while staging was verified.")
            validate_cdc_to_batch_table_names(source, source.tables.order_by("pk"))
            for checkpoint in locked_checkpoints:
                stage_cdc_to_batch_table(
                    checkpoint.pk, batch_size=batch_size, verification_only=True
                )
            if (
                {item.pk for item in locked_checkpoints}
                != set(transition.table_checkpoints.values_list("pk", flat=True))
                or {item.source_table_id for item in locked_checkpoints}
                != set(source.tables.values_list("pk", flat=True))
            ):
                raise RuntimeError("The transition changed while staging was verified.")
            validate_cdc_to_batch_table_names(source, source.tables.order_by("pk"))
            transition.staging_verified_at = timezone.now()
            transition.last_error = ""
            transition.save(update_fields=["staging_verified_at", "last_error", "updated_at"])
        return (
            f"All {len(checkpoints)} tables staged for transition {transition.pk}; "
            "cutover has not started.", False
        )
    except Exception as exc:
        _record_cdc_to_batch_task_failure(transition_pk, "Staging", exc)
        raise


class CdcToBatchDeploymentFenceUnavailable(RuntimeError):
    pass


def _require_cdc_to_batch_deployment_fence():
    raise CdcToBatchDeploymentFenceUnavailable(
        "CDC-to-batch cutover is blocked: deployment fencing and external "
        "request recovery are not implemented."
    )


@shared_task(queue="queue_backfill")
def execute_cdc_to_batch_transition_task(transition_pk, batch_size=None):
    """Advance a transition by one bounded step; disabled until deployment is safe."""
    from etl.cdc_to_batch import (
        CdcToBatchStepResult,
        _mark_cdc_to_batch_cutover_pending,
        _run_cdc_to_batch_cutover_step,
        _run_cdc_to_batch_generation_step,
    )

    if not getattr(settings, "CDC_TO_BATCH_EXECUTOR_ENABLED", False):
        raise RuntimeError("The CDC-to-batch executor is not enabled.")
    if not getattr(settings, "CDC_EXTERNAL_OPERATION_TRACKING", False):
        raise RuntimeError("CDC external operation tracking is not enabled.")
    try:
        _require_cdc_to_batch_deployment_fence()
        transition = CdcToBatchTransition.objects.get(pk=transition_pk)
        if transition.status == CdcToBatchTransition.Status.PREPARING:
            message, staged_one = _stage_cdc_to_batch_transition_step(
                transition_pk, batch_size
            )
            if not staged_one:
                _mark_cdc_to_batch_cutover_pending(transition_pk)
                message = "Cutover is pending"
        elif transition.status in (
            CdcToBatchTransition.Status.CUTOVER_PENDING,
            CdcToBatchTransition.Status.SUCCEEDED,
        ):
            if getattr(settings, "CDC_TO_BATCH_GENERATION_EXECUTOR_ENABLED", False):
                result = _run_cdc_to_batch_generation_step(transition_pk)
            else:
                result = _run_cdc_to_batch_cutover_step(transition_pk)
            if result == CdcToBatchStepResult.SUCCEEDED:
                return f"CDC-to-batch transition {transition_pk} completed."
            message = "A table cutover completed"
        else:
            raise RuntimeError("The transition cannot be resumed in its current state.")
        execute_cdc_to_batch_transition_task.apply_async(
            args=[transition_pk], kwargs={"batch_size": batch_size}, queue="queue_backfill"
        )
        return f"{message}; queued the next transition step."
    except Exception as exc:
        _record_cdc_to_batch_task_failure(transition_pk, "Transition", exc)
        if isinstance(exc, CdcToBatchDeploymentFenceUnavailable):
            CdcToBatchTransition.objects.filter(
                pk=transition_pk,
                status__in=[
                    CdcToBatchTransition.Status.PREPARING,
                    CdcToBatchTransition.Status.CUTOVER_PENDING,
                ],
            ).update(
                last_error=(
                    "Cutover is blocked: deployment fencing and external request "
                    "recovery are not implemented."
                ),
                updated_at=timezone.now(),
            )
        raise


@shared_task(bind=True, queue="queue_cdc")
def rebuild_cdc_table_task(self, source_table_pk, rebuild_pk=None):
    if not getattr(settings, "CDC_EXTERNAL_OPERATION_TRACKING", False):
        return _rebuild_cdc_table(self, source_table_pk, rebuild_pk)
    try:
        source_pk = SourceTable.objects.values_list("source_id", flat=True).get(
            pk=source_table_pk
        )
    except SourceTable.DoesNotExist as exc:
        raise ValueError(f"Source table {source_table_pk} does not exist.") from exc
    with _track_cdc_external_operation(
        source_pk, CdcSourceExternalOperation.Kind.REBUILD, self.request.id or ""
    ):
        return _rebuild_cdc_table(
            self, source_table_pk, rebuild_pk, expected_source_pk=source_pk
        )


def create_cdc_table_rebuild_record(table):
    generation = uuid4()
    suffix = generation.hex[:12]
    connector_name = f"reporthub-source-{table.source_id}-table-{table.pk}-{suffix}"
    active_table_name = source_table_clickhouse_name(table)
    return CdcTableRebuild.objects.create(
        source_table=table,
        generation=generation,
        connector_name=connector_name,
        topic_prefix=connector_name,
        notification_topic=f"{connector_name}.notifications",
        consumer_group=f"{connector_name}-rebuild",
        staging_table=f"{active_table_name}__rebuild_{suffix}",
        previous_connector_name=table.cdc_connector_name,
    )


def _rebuild_cdc_table(self, source_table_pk, rebuild_pk=None, *, expected_source_pk=None):
    from etl.cdc import (
        _clickhouse_table_uuid,
        _create_cdc_table,
        _ensure_connector_config,
        _required_field_names,
        consume_cdc_table_rebuild_batch,
    )
    from reports.query_builder import quote_clickhouse_identifier

    active_statuses = [
        CdcTableRebuild.Status.SNAPSHOTTING,
        CdcTableRebuild.Status.CUTOVER_PENDING,
    ]
    try:
        table = SourceTable.objects.select_related("source").get(
            pk=source_table_pk
        )
    except SourceTable.DoesNotExist as exc:
        raise ValueError(f"Source table {source_table_pk} does not exist.") from exc

    if expected_source_pk is not None and table.source_id != expected_source_pk:
        raise RuntimeError("The CDC rebuild source changed after operation registration.")
    profile = table.source
    if (
        profile.ingestion_mode != IngestionMode.CDC
        or profile.db_type == DbChoice.MONGODB
        or not table.primary_key_fields
        or not table.cdc_managed
    ):
        raise ValueError(
            "A safe CDC rebuild requires a CDC-managed relational table with "
            "a discovered primary key."
        )

    with transaction.atomic():
        profile = SourceProfile.objects.select_for_update().get(pk=profile.pk)
        if (
            profile.ingestion_mode != IngestionMode.CDC
            or not profile.is_active
        ):
            raise ValueError("A CDC rebuild requires an active CDC source.")
        if CdcToBatchTransition.objects.filter(
            source=profile,
            status__in=[
                CdcToBatchTransition.Status.PREPARING,
                CdcToBatchTransition.Status.CUTOVER_PENDING,
            ],
        ).exists():
            raise RuntimeError(
                "Cannot start or resume a CDC table rebuild while a "
                "CDC-to-batch transition is active."
            )
        if rebuild_pk is None:
            recovery = table.cdc_rebuilds.filter(
                status__in=active_statuses
            ).first()
            if recovery is None:
                recovery = create_cdc_table_rebuild_record(table)
            rebuild_pk = recovery.pk
        else:
            recovery = CdcTableRebuild.objects.get(
                pk=rebuild_pk,
                source_table=table,
            )

    if recovery.status == CdcTableRebuild.Status.SUCCEEDED:
        return f"CDC rebuild {recovery.pk} already succeeded."
    if recovery.status == CdcTableRebuild.Status.FAILED:
        raise RuntimeError(
            f"CDC rebuild {recovery.pk} failed: {recovery.last_error}"
        )

    ch_client = clickhouse_connect.get_client(
        host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
        port=getattr(settings, "CLICKHOUSE_PORT", 8123),
        username=getattr(settings, "CLICKHOUSE_USER", "default"),
        password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
        settings=_clickhouse_ingestion_settings(),
    )
    try:
        database_engine = ch_client.query(
            "SELECT engine FROM system.databases "
            "WHERE name = currentDatabase()"
        ).result_rows
        if not database_engine or database_engine[0][0] != "Atomic":
            raise RuntimeError(
                "Safe CDC table replacement requires an Atomic ClickHouse database."
            )

        active_table_name = source_table_clickhouse_name(table)
        active_uuid = _clickhouse_table_uuid(ch_client, active_table_name)
        if not recovery.active_table_checked:
            if active_uuid and not table.clickhouse_uses_replacing_merge_tree:
                raise RuntimeError(
                    f"{table.schema_name}.{table.table_name} is not stored in a "
                    "CDC-compatible ReplacingMergeTree."
                )
            recovery.active_uuid_before = active_uuid
            recovery.active_table_checked = True
            recovery.save(update_fields=[
                "active_uuid_before",
                "active_table_checked",
                "updated_at",
            ])
        elif (
            active_uuid != recovery.active_uuid_before
            and not (
                recovery.status == CdcTableRebuild.Status.CUTOVER_PENDING
                and active_uuid == recovery.staging_uuid
            )
        ):
            raise RuntimeError(
                "The active ClickHouse table changed while the CDC rebuild "
                "was in progress."
            )
        cutover_already_completed = (
            recovery.status == CdcTableRebuild.Status.CUTOVER_PENDING
            and active_uuid == recovery.staging_uuid
        )
        if not cutover_already_completed:
            field_names = sorted(_required_field_names(profile, table))
            _create_cdc_table(
                ch_client,
                profile,
                table,
                field_names,
                physical_table_name=recovery.staging_table,
                staging=True,
            )
            if not recovery.staging_uuid:
                recovery.staging_uuid = _clickhouse_table_uuid(
                    ch_client, recovery.staging_table
                )
                if not recovery.staging_uuid:
                    raise RuntimeError(
                        "ClickHouse did not expose a UUID for the CDC staging table."
                    )
                recovery.save(update_fields=["staging_uuid", "updated_at"])

            if recovery.status == CdcTableRebuild.Status.SNAPSHOTTING:
                _ensure_connector_config(
                    profile,
                    [table],
                    recovery.connector_name,
                    recovery.topic_prefix,
                    notification_topic=recovery.notification_topic,
                )
            drained = consume_cdc_table_rebuild_batch(
                recovery,
                profile,
                table,
                ch_client,
                batch_size=getattr(settings, "DEFAULT_ETL_BATCH_SIZE", 10000),
            )
            recovery.refresh_from_db()
            if (
                recovery.status == CdcTableRebuild.Status.SNAPSHOTTING
                or not drained
            ):
                rebuild_cdc_table_task.apply_async(
                    args=[source_table_pk, recovery.pk],
                    countdown=5,
                    queue="queue_cdc",
                )
                return (
                    f"CDC rebuild {recovery.pk} is snapshotting "
                    f"{table.schema_name}.{table.table_name}."
                )

        lock_key = f"reporthub:cdc-ingestion:{profile.pk}"
        lock_token = uuid4().hex
        if not cache.add(lock_key, lock_token, timeout=900):
            rebuild_cdc_table_task.apply_async(
                args=[source_table_pk, recovery.pk],
                countdown=10,
                queue="queue_cdc",
            )
            return (
                f"CDC rebuild {recovery.pk} is waiting for source ingestion "
                "to release its lock."
            )

        try:
            profile.refresh_from_db(fields=["ingestion_mode", "is_active"])
            if profile.ingestion_mode != IngestionMode.CDC or not profile.is_active:
                raise RuntimeError(
                    "CDC rebuild cutover cannot continue because the source "
                    "mode or active state changed while waiting for the "
                    "ingestion lock."
                )
            active_uuid = _clickhouse_table_uuid(ch_client, active_table_name)
            staging_uuid = _clickhouse_table_uuid(
                ch_client, recovery.staging_table
            )
            if active_uuid != recovery.staging_uuid:
                if staging_uuid != recovery.staging_uuid:
                    raise RuntimeError(
                        "CDC rebuild staging table changed unexpectedly; "
                        "cutover was not attempted."
                    )
                if active_uuid == recovery.active_uuid_before:
                    if active_uuid:
                        ch_client.command(
                            f"EXCHANGE TABLES "
                            f"{quote_clickhouse_identifier(active_table_name)} "
                            f"AND "
                            f"{quote_clickhouse_identifier(recovery.staging_table)}"
                        )
                    else:
                        ch_client.command(
                            f"RENAME TABLE "
                            f"{quote_clickhouse_identifier(recovery.staging_table)} "
                            f"TO {quote_clickhouse_identifier(active_table_name)}"
                        )
                else:
                    raise RuntimeError(
                        "The active ClickHouse table changed during CDC rebuild; "
                        "cutover was not attempted."
                    )

            table.cdc_connector_name = recovery.connector_name
            table.cdc_topic_prefix = recovery.topic_prefix
            table.cdc_managed = True
            table.clickhouse_uses_replacing_merge_tree = True
            table.save(update_fields=[
                "cdc_connector_name",
                "cdc_topic_prefix",
                "cdc_managed",
                "clickhouse_uses_replacing_merge_tree",
            ])
            recovery.status = CdcTableRebuild.Status.SUCCEEDED
            recovery.last_error = ""
            recovery.save(update_fields=[
                "status",
                "last_error",
                "updated_at",
            ])
        finally:
            if cache.get(lock_key) == lock_token:
                cache.delete(lock_key)

        return (
            f"CDC rebuild {recovery.pk} completed for "
            f"{table.schema_name}.{table.table_name}."
        )
    except Exception as exc:
        recovery.refresh_from_db()
        if recovery.status not in {
            CdcTableRebuild.Status.SUCCEEDED,
            CdcTableRebuild.Status.FAILED,
        }:
            recovery.last_error = f"{type(exc).__name__}: {exc}"
            if recovery.status == CdcTableRebuild.Status.SNAPSHOTTING:
                recovery.status = CdcTableRebuild.Status.FAILED
            recovery.save(update_fields=[
                "status",
                "last_error",
                "updated_at",
            ])
        raise
    finally:
        ch_client.close()


@shared_task(bind=True, queue='queue_periodic')
def backfill_source_to_clickhouse_task(self, source_pk, batch_size=None, origin_mode=None):
    """Rebuild one source's ClickHouse tables using its saved warehouse field selections."""
    source = SourceProfile.objects.get(pk=source_pk)
    _validate_ingestion_task_origin(source, origin_mode)
    if _source_has_primary_key_tables(source_pk):
        logger = get_etl_logger()
        message = (
            f"Skipped ClickHouse backfill for source {source_pk}: "
            "it has primary-key tables managed by CDC."
        )
        logger.warning(message)
        return message

    ingest_source_to_clickhouse_task.apply_async(
        args=[source_pk],
        kwargs={
            'batch_size': batch_size,
            'full_refresh': True,
            'origin_mode': source.ingestion_mode,
            'task_queue': 'queue_backfill',
        },
        queue='queue_backfill',
    )
    return f"Dispatched ClickHouse backfill for source {source_pk}"


def _source_has_primary_key_tables(source_pk):
    return any(
        primary_key_fields
        for primary_key_fields in SourceTable.objects.filter(
            source_id=source_pk, source__ingestion_mode=IngestionMode.CDC
        ).values_list(
            'primary_key_fields', flat=True
        )
    )


@shared_task(bind=True, queue='queue_periodic')
def trigger_all_active_clickhouse_backfill_task(self, batch_size=None):
    """Dispatch bounded full-refresh batches for active sources without CDC tables."""
    logger = get_etl_logger()
    active_sources = SourceProfile.objects.filter(is_active=True)
    dispatched_count = 0
    skipped_count = 0

    for source in active_sources:
        if _source_has_primary_key_tables(source.pk):
            logger.warning(
                f"Skipped ClickHouse backfill for source {source.pk} ({source.name}): "
                "it has primary-key tables managed by CDC."
            )
            skipped_count += 1
            continue
        backfill_source_to_clickhouse_task.delay(
            source.pk, batch_size=batch_size, origin_mode=source.ingestion_mode
        )
        dispatched_count += 1

    return (
        f"Dispatched ClickHouse backfill for {dispatched_count} active source(s); "
        f"skipped {skipped_count} source(s) with primary-key tables managed by CDC."
    )