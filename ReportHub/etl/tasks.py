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
from django.conf import settings
from celery import shared_task, Task
from django.utils import timezone
from pymongo import MongoClient
from control_plane.models import (
    SourceProfile, 
    SourceTable, 
    SourceField, 
    SourceRelationship, 
    WarehouseDatasetMapping,
    SyncStatus, 
    DbChoice
)
from control_plane.utils import set_task_progress
from reports.models import ReportDefinition


def create_source_engine(profile):
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
    return sa.create_engine(url)


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
                if schema not in {"sys", "INFORMATION_SCHEMA"}
            ]
        elif profile.db_type == DbChoice.ORACLE:
            target_schemas = [profile.username.upper()]
        else:
            target_schemas = [profile.database_name]

        for schema_index, schema_name in enumerate(target_schemas, start=1):
            try:
                tables = inspector.get_table_names(
                    schema=schema_name if profile.db_type != DbChoice.MYSQL else None
                )
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
        try:
            ch_client = clickhouse_connect.get_client(
                host=getattr(settings, 'CLICKHOUSE_HOST', 'localhost'),
                port=getattr(settings, 'CLICKHOUSE_PORT', 8123),
                username=getattr(settings, 'CLICKHOUSE_USER', 'default'),
                password=getattr(settings, 'CLICKHOUSE_PASSWORD', ''),
            )
            ch_client.command(f"KILL QUERY WHERE query_id = '{query_id}' SYNC")
        except Exception as e:
            pass


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
    set_task_progress(task_id, percentage=10, status="RUNNING", current_table="Connecting to source...")
    
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
    set_task_progress(task_id, percentage=5, status="RUNNING", current_table="Introspecting Schema...")
    
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


def _ingest_source_to_clickhouse_batch(
    task,
    source_pk,
    batch_size=None,
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
    set_task_progress(task_id, percentage=0, status="RUNNING", current_table="Initializing Ingestion...")

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
        current_table="Preparing ingestion...",
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

    required_fields_by_table = {}
    source_relationships = list(SourceRelationship.objects.filter(source=profile))
    for relationship in source_relationships:
        required_fields_by_table.setdefault(
            (relationship.from_schema, relationship.from_table), set()
        ).add(relationship.from_field)
        required_fields_by_table.setdefault(
            (relationship.to_schema, relationship.to_table), set()
        ).add(relationship.to_field)

    for report in ReportDefinition.objects.filter(source=profile):
        config_columns = (report.config or {}).get('selectedColumns', [])
        for column in config_columns:
            if not isinstance(column, dict):
                continue
            table_id = column.get('table_id')
            field_name = column.get('field_name')
            if not table_id or not field_name:
                continue
            source_table = SourceTable.objects.filter(source=profile, pk=table_id).first()
            if source_table:
                required_fields_by_table.setdefault(
                    (source_table.schema_name, source_table.table_name), set()
                ).add(field_name)

        for field_name in report.selected_fields or []:
            if isinstance(field_name, str):
                required_fields_by_table.setdefault(
                    (report.base_table.schema_name, report.base_table.table_name), set()
                ).add(field_name)

    table = source_tables[table_index]
    profile.refresh_from_db()
    if not profile.is_active:
        profile.last_sync_status = SyncStatus.CANCELLED
        profile.save(update_fields=['last_sync_status'])
        set_task_progress(task_id, percentage=int((table_index / total_tables) * 100), status="CANCELLED")
        if not incremental_run:
            ch_client.command(f"DROP TABLE IF EXISTS {staging_table_name}")
        if source_engine:
            source_engine.dispose()
        ch_client.close()
        return f"Ingestion cancelled for {profile.name}"

    required_field_names = required_fields_by_table.get(
        (table.schema_name, table.table_name), set()
    )
    mappings = WarehouseDatasetMapping.objects.filter(
        source_profile=profile,
        table_name=table.table_name,
    )
    if mappings.exists():
        warehouse_field_names = set(
            mappings.filter(is_enabled=True).values_list('field_name', flat=True)
        )
        selected_fields = list(
            table.fields.filter(
                Q(field_name__in=warehouse_field_names)
                | Q(field_name__in=required_field_names)
            )
        )
    else:
        selected_fields = list(
            table.fields.filter(
                Q(is_selected=True) | Q(field_name__in=required_field_names)
            )
        )

    ch_table_name = f"source_{profile.pk}__{table.schema_name}_{table.table_name}".lower()
    staging_table_name = (
        f"{ch_table_name}__staging_{ingestion_run_id}"
    ).lower()

    if not selected_fields:
        if full_refresh:
            ch_client.command(f"DROP TABLE IF EXISTS {ch_table_name}")
            table.incremental_ready = False
            table.incremental_watermark = None
            table.clickhouse_uses_replacing_merge_tree = False
            table.save(update_fields=[
                'incremental_ready',
                'incremental_watermark',
                'clickhouse_uses_replacing_merge_tree',
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
                    'full_refresh': full_refresh,
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
        table.primary_key_fields
        and incremental_field
        and is_datetime_source_type(incremental_field.data_type)
        and all(key in source_fields for key in table.primary_key_fields)
    )

    target_exists = ch_client.query(
        f"EXISTS TABLE {ch_table_name}",
        transport_settings={"query_id": query_id},
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
                transport_settings={"query_id": query_id},
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

    if not incremental_run and row_offset == 0:
        column_defs = []
        for field in ingestion_fields:
            nullable = field.field_name not in table.primary_key_fields
            ch_type = map_to_clickhouse_type(field.data_type, is_nullable=nullable)
            safe_name = field.field_name.replace('`', '``')
            column_defs.append(f"`{safe_name}` {ch_type}")
        columns_sql = ", ".join(column_defs)

        if incremental_eligible:
            order_by = ", ".join(
                f"`{key.replace('`', '``')}`" for key in table.primary_key_fields
            )
            engine_sql = f"ReplacingMergeTree() ORDER BY ({order_by})"
        else:
            engine_sql = "MergeTree() ORDER BY tuple()"
        ch_client.command(f"DROP TABLE IF EXISTS {staging_table_name}")
        ch_client.command(
            f"CREATE TABLE {staging_table_name} ({columns_sql}) ENGINE = {engine_sql}",
            transport_settings={"query_id": query_id},
        )
    elif not incremental_run:
        stage_exists = ch_client.query(
            f"EXISTS TABLE {staging_table_name}",
            transport_settings={"query_id": query_id},
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
            if (
                not incremental_run
                and last_key_values
                and table.primary_key_fields == ["_id"]
            ):
                key_field = source_fields["_id"]
                last_key = parse_source_key(last_key_values[0], key_field)
                if key_field.data_type == "ObjectId":
                    last_key = ObjectId(str(last_key))
                query_filter["_id"] = {"$gt": last_key}
            chunks = read_mongo_chunks(
                profile,
                table.table_name,
                field_names,
                batch_size,
                query_filter=query_filter,
                skip=(
                    row_offset
                    if incremental_run or table.primary_key_fields != ["_id"]
                    else 0
                ),
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

        chunk = chunk.astype(object).where(pd.notnull(chunk), None)

        # Map every field name to its expected ClickHouse type definition
        field_ch_types = {
            field.field_name: map_to_clickhouse_type(
                field.data_type, 
                is_nullable=(field.field_name not in table.primary_key_fields)
            )
            for field in ingestion_fields
        }

        # Force-coerce every single cell based on the ClickHouse column type
        sanitized_rows = []
        for row in chunk[field_names].values.tolist():
            new_row = []
            for field_name, val in zip(field_names, row):
                ch_type = field_ch_types.get(field_name, '')
                
                # Handle nulls / NaNs
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    new_row.append(None)
                # If ClickHouse column is String or Nullable(String), ensure output is strictly a string or None
                elif 'string' in ch_type.lower():
                    if hasattr(val, 'isoformat'):
                        new_row.append(val.isoformat())
                    elif isinstance(val, float):
                        new_row.append(str(int(val)) if val.is_integer() else str(val))
                    else:
                        new_row.append(str(val))
                # If it's a date/time type
                elif 'date' in ch_type.lower() or 'time' in ch_type.lower():
                    if hasattr(val, 'isoformat'):
                        new_row.append(val.isoformat())
                    else:
                        new_row.append(str(val) if val is not None else None)
                else:
                    new_row.append(val)
            sanitized_rows.append(new_row)

        data_rows = sanitized_rows
        if data_rows:
            ch_client.insert(
                ch_table_name if incremental_run else staging_table_name,
                data_rows,
                column_names=field_names,
                transport_settings={"query_id": query_id},
            )
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
            not incremental_run and table.primary_key_fields
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
                'full_refresh': full_refresh,
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
        if target_exists:
            ch_client.command(
                f"EXCHANGE TABLES {ch_table_name} AND {staging_table_name}",
                transport_settings={"query_id": query_id},
            )
            ch_client.command(
                f"DROP TABLE {staging_table_name}",
                transport_settings={"query_id": query_id},
            )
        else:
            ch_client.command(
                f"RENAME TABLE {staging_table_name} TO {ch_table_name}",
                transport_settings={"query_id": query_id},
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
    table.clickhouse_uses_replacing_merge_tree = incremental_eligible
    table.last_synced_at = timezone.now()
    table.save(update_fields=[
        'incremental_watermark',
        'incremental_ready',
        'clickhouse_uses_replacing_merge_tree',
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
                'full_refresh': full_refresh,
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


@shared_task(bind=True, base=CancellableETLTask, queue='queue_hot')
def ingest_source_to_clickhouse_task(
    self,
    source_pk,
    batch_size=None,
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
    try:
        return _ingest_source_to_clickhouse_batch(
            self,
            source_pk,
            batch_size=batch_size,
            full_refresh=full_refresh,
            table_index=table_index,
            row_offset=row_offset,
            last_key_values=last_key_values,
            ingestion_run_id=ingestion_run_id,
            incremental_run=incremental_run,
            watermark_value=watermark_value,
            watermark_upper=watermark_upper,
            watermark_max=watermark_max,
            task_queue=task_queue,
        )
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
            "ClickHouse ingestion batch failed for source %s: %s",
            source_pk,
            exc,
        )
        raise


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
        ingest_source_to_clickhouse_task.delay(source.pk)
        
    return f"Successfully dispatched ClickHouse ingestion for {active_sources.count()} active sources."


@shared_task(bind=True, queue='queue_periodic')
def backfill_source_to_clickhouse_task(self, source_pk, batch_size=None):
    """Rebuild one source's ClickHouse tables using its saved warehouse field selections."""
    ingest_source_to_clickhouse_task.apply_async(
        args=[source_pk],
        kwargs={
            'batch_size': batch_size,
            'full_refresh': True,
            'task_queue': 'queue_backfill',
        },
        queue='queue_backfill',
    )
    return f"Dispatched ClickHouse backfill for source {source_pk}"


@shared_task(bind=True, queue='queue_periodic')
def trigger_all_active_clickhouse_backfill_task(self, batch_size=None):
    """Dispatch bounded full-refresh batches for all active data sources."""
    active_sources = SourceProfile.objects.filter(is_active=True)
    for source in active_sources:
        backfill_source_to_clickhouse_task.delay(source.pk, batch_size=batch_size)
    return (
        f"Successfully dispatched ClickHouse backfill for "
        f"{active_sources.count()} active sources."
    )