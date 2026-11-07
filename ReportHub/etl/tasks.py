import os
import time
import logging
import pymysql
import psycopg2
import pyodbc
import sqlalchemy as sa
import pandas as pd
import clickhouse_connect
from django.db.models import Q
from django.conf import settings
from celery import shared_task, Task
from django.utils import timezone
from control_plane.models import (
    SourceProfile, 
    SourceTable, 
    SourceField, 
    SourceRelationship, 
    SyncStatus, 
    DbChoice
)
from control_plane.utils import set_task_progress
from reports.models import ReportDefinition


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
        if profile.db_type == DbChoice.MYSQL:
            engine_url = f"mysql+pymysql://{profile.username}:{profile.password}@{profile.host}:{profile.port}/{profile.database_name}"
        elif profile.db_type == DbChoice.POSTGRESQL:
            engine_url = f"postgresql+psycopg2://{profile.username}:{profile.password}@{profile.host}:{profile.port}/{profile.database_name}"
        elif profile.db_type == DbChoice.MSSQL:
            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={profile.host},{profile.port};"
                f"DATABASE={profile.database_name};"
                f"UID={profile.username};"
                f"PWD={profile.password};"
                f"TrustServerCertificate=yes;"
            )
            engine_url = f"mssql+pyodbc:///?odbc_connect={sa.engine.url.quote_plus(conn_str)}"
        else:
            raise ValueError(f"Unsupported database type: {profile.db_type}")

        engine = sa.create_engine(engine_url)
        inspector = sa.inspect(engine)

        schemas = inspector.get_schema_names()
        if profile.db_type == DbChoice.POSTGRESQL:
            target_schemas = [s for s in schemas if s not in ['information_schema', 'pg_catalog', 'pg_toast']]
        elif profile.db_type == DbChoice.MSSQL:
            target_schemas = [s for s in schemas if s not in ['sys', 'INFORMATION_SCHEMA']]
        else:
            target_schemas = [profile.database_name]

        for schema_index, schema_name in enumerate(target_schemas, start=1):
            try:
                tables = inspector.get_table_names(schema=schema_name if profile.db_type != DbChoice.MYSQL else None)
            except Exception as e:
                logger.warning(f"Could not fetch tables for schema {schema_name}: {e}")
                continue

            for table_index, table_name in enumerate(tables, start=1):
                current_schema = schema_name if profile.db_type != DbChoice.MYSQL else profile.database_name
                
                set_task_progress(
                    task_id,
                    percentage=int((schema_index / len(target_schemas)) * 50),
                    status="RUNNING",
                    current_table=f"{current_schema}.{table_name}"
                )
                
                source_table, created = SourceTable.objects.get_or_create(
                    source=profile,
                    schema_name=current_schema,
                    table_name=table_name
                )

                try:
                    columns = inspector.get_columns(table_name, schema=schema_name if profile.db_type != DbChoice.MYSQL else None)
                except Exception as e:
                    logger.warning(f"Could not fetch columns for {current_schema}.{table_name}: {e}")
                    continue

                for col in columns:
                    field_name = col['name']
                    data_type = str(col['type'])

                    SourceField.objects.update_or_create(
                        table=source_table,
                        field_name=field_name,
                        defaults={'data_type': data_type}
                    )

                try:
                    foreign_keys = inspector.get_foreign_keys(table_name, schema=schema_name if profile.db_type != DbChoice.MYSQL else None)
                    for fk in foreign_keys:
                        constrained_cols = fk.get('constrained_columns', [])
                        referred_table = fk.get('referred_table')
                        referred_cols = fk.get('referred_columns', [])
                        referred_schema = fk.get('referred_schema') or schema_name
                        constraint_name = fk.get('name')

                        for c_col, r_col in zip(constrained_cols, referred_cols):
                            SourceRelationship.objects.update_or_create(
                                source=profile,
                                from_schema=current_schema,
                                from_table=table_name,
                                from_field=c_col,
                                to_table=referred_table,
                                to_field=r_col,
                                defaults={
                                    'to_schema': referred_schema if profile.db_type != DbChoice.MYSQL else profile.database_name,
                                    'constraint_name': constraint_name
                                }
                            )
                except Exception as e:
                    logger.warning(f"Could not fetch foreign keys for {current_schema}.{table_name}: {e}")

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


@shared_task(bind=True, base=CancellableETLTask, queue='queue_hot')
def ingest_source_to_clickhouse_task(self, source_pk, batch_size=None):
    """
    Ingests selected tables and fields from a specific SourceProfile 
    into ClickHouse, maintaining strict source/table separation and Redis progress tracking.
    """
    logger = get_etl_logger()
    task_id = self.request.id
    query_id = f"etl_task_{task_id}"
    set_task_progress(task_id, percentage=0, status="RUNNING", current_table="Initializing Ingestion...")
    
    if batch_size is None:
        batch_size = getattr(settings, 'DEFAULT_ETL_BATCH_SIZE', 10000)
    
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
        percentage=0,
        status="RUNNING",
        current_table="Preparing ingestion...",
        source_name=profile.name,
    )
    logger.info(f"Starting ClickHouse data ingestion for source: '{profile.name}' (Batch Size: {batch_size})")

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
        if profile.db_type == DbChoice.MYSQL:
            engine_url = f"mysql+pymysql://{profile.username}:{profile.password}@{profile.host}:{profile.port}/{profile.database_name}"
        elif profile.db_type == DbChoice.POSTGRESQL:
            engine_url = f"postgresql+psycopg2://{profile.username}:{profile.password}@{profile.host}:{profile.port}/{profile.database_name}"
        elif profile.db_type == DbChoice.MSSQL:
            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={profile.host},{profile.port};"
                f"DATABASE={profile.database_name};"
                f"UID={profile.username};"
                f"PWD={profile.password};"
                f"TrustServerCertificate=yes;"
            )
            engine_url = f"mssql+pyodbc:///?odbc_connect={sa.engine.url.quote_plus(conn_str)}"
        else:
            raise ValueError(f"Unsupported database type: {profile.db_type}")

        source_engine = sa.create_engine(engine_url)
    except Exception as e:
        logger.error(f"Failed to create source engine for {profile.name}: {e}")
        profile.last_sync_status = SyncStatus.FAILED
        profile.last_sync_error = str(e)
        profile.save(update_fields=['last_sync_status', 'last_sync_error'])
        set_task_progress(task_id, percentage=0, status="FAILED", error=str(e))
        return f"Engine creation failed: {e}"

    source_tables = list(SourceTable.objects.filter(source=profile, source__is_active=True))
    total_tables = len(source_tables)

    if total_tables == 0:
        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.SUCCESS
        profile.last_sync_error = ''
        profile.save(update_fields=['last_synced', 'last_sync_status', 'last_sync_error'])
        set_task_progress(task_id, percentage=100, status="SUCCESS", current_table="No active tables")
        return f"No active tables to ingest for {profile.name}"

    preparer = source_engine.dialect.identifier_preparer

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

    for index, table in enumerate(source_tables, start=1):
        profile.refresh_from_db()
        if not profile.is_active:
            profile.last_sync_status = SyncStatus.CANCELLED
            profile.save(update_fields=['last_sync_status'])
            set_task_progress(task_id, percentage=int((index / total_tables) * 100), status="CANCELLED")
            logger.warning(f"ETL ingestion for '{profile.name}' was cancelled/deactivated.")
            return f"Ingestion cancelled for {profile.name}"

        current_percentage = int((index / total_tables) * 100)
        set_task_progress(
            task_id,
            percentage=current_percentage,
            status="RUNNING",
            current_table=table.table_name,
            source_name=profile.name
        )

        required_field_names = required_fields_by_table.get(
            (table.schema_name, table.table_name), set()
        )
        selected_fields = list(
            table.fields.filter(
                Q(is_selected=True) | Q(field_name__in=required_field_names)
            )
        )
        if not selected_fields:
            continue  

        field_names = [f.field_name for f in selected_fields]
        ch_table_name = f"source_{profile.pk}__{table.schema_name}_{table.table_name}".lower()
        staging_table_name = (
            f"{ch_table_name}__staging_{str(task_id).replace('-', '_')}"
        ).lower()

        quoted_col_names = [preparer.quote(f.field_name) for f in selected_fields]
        select_cols_sql = ", ".join(quoted_col_names)

        columns_sql = ", ".join([
            f"`{f.field_name.replace('`', '``')}` {map_to_clickhouse_type(f.data_type)}"
            for f in selected_fields
        ])
        create_table_query = f"""
            CREATE TABLE {staging_table_name} (
                {columns_sql}
            ) ENGINE = MergeTree()
            ORDER BY tuple();
        """
        try:
            ch_client.command(
                f"DROP TABLE IF EXISTS {staging_table_name}",
                transport_settings={"query_id": query_id},
            )
            ch_client.command(
                create_table_query,
                transport_settings={"query_id": query_id},
            )
        except Exception as e:
            logger.error(f"Failed to create ClickHouse staging table {staging_table_name}: {e}")
            raise

        quoted_table_name = preparer.quote(table.table_name)
        if profile.db_type == DbChoice.MYSQL:
            source_query = f"SELECT {select_cols_sql} FROM {quoted_table_name}"
        elif profile.db_type == DbChoice.POSTGRESQL:
            quoted_schema_name = preparer.quote(table.schema_name)
            source_query = f"SELECT {select_cols_sql} FROM {quoted_schema_name}.{quoted_table_name}"
        elif profile.db_type == DbChoice.MSSQL:
            quoted_schema_name = preparer.quote(table.schema_name)
            source_query = f"SELECT {select_cols_sql} FROM {quoted_schema_name}.{quoted_table_name}"
        else:
            source_query = f"SELECT {select_cols_sql} FROM {quoted_table_name}"

        try:
            with source_engine.connect() as conn:
                chunk_count = 0
                max_chunks_per_burst = 50

                for chunk in pd.read_sql(source_query, con=conn, chunksize=batch_size):
                    profile.refresh_from_db()
                    if not profile.is_active:
                        ch_client.command(f"KILL QUERY WHERE query_id = '{query_id}' SYNC")
                        ch_client.command(f"DROP TABLE IF EXISTS {staging_table_name}")
                        profile.last_sync_status = SyncStatus.CANCELLED
                        profile.save(update_fields=['last_sync_status'])
                        set_task_progress(
                            task_id,
                            percentage=current_percentage,
                            status="CANCELLED",
                        )
                        logger.warning(
                            f"ETL ingestion for '{profile.name}' was cancelled/deactivated."
                        )
                        return f"Ingestion cancelled for {profile.name}"

                    chunk = chunk.where(pd.notnull(chunk), None)

                    for f in selected_fields:
                        ch_type = map_to_clickhouse_type(f.data_type)
                        if ch_type == 'String' and f.field_name in chunk.columns:
                            chunk[f.field_name] = chunk[f.field_name].apply(
                                lambda x: x.isoformat() if hasattr(x, 'isoformat') and x is not None else (str(x) if x is not None else None)
                            )
                    
                    data_rows = chunk.values.tolist()
                    if data_rows:
                        ch_client.insert(
                            staging_table_name,
                            data_rows,
                            column_names=field_names,
                            transport_settings={"query_id": query_id},
                        )
                    
                    chunk_count += 1
                    if chunk_count >= max_chunks_per_burst:
                        time.sleep(0.1)
                        chunk_count = 0

            target_exists = ch_client.query(
                f"EXISTS TABLE {ch_table_name}",
                transport_settings={"query_id": query_id},
            ).result_rows[0][0]
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

            logger.info(f"Successfully ingested table {table.table_name} into ClickHouse as {ch_table_name}")
        except Exception as e:
            try:
                ch_client.command(f"DROP TABLE IF EXISTS {staging_table_name}")
            except Exception as cleanup_error:
                logger.error(
                    f"Failed to remove ClickHouse staging table {staging_table_name}: "
                    f"{cleanup_error}"
                )
            logger.error(f"Error streaming data for table {table.table_name}: {e}")
            raise

    profile.last_synced = timezone.now()
    profile.last_sync_status = SyncStatus.SUCCESS
    profile.last_sync_error = ''
    profile.save(update_fields=['last_synced', 'last_sync_status', 'last_sync_error'])
    
    set_task_progress(task_id, percentage=100, status="SUCCESS", current_table="Completed")
    return f"ClickHouse ingestion completed for {profile.name}"


def map_to_clickhouse_type(source_type):
    """Simple helper mapper to convert source SQL data types to ClickHouse types."""
    source_type_lower = source_type.lower()
    if 'int' in source_type_lower:
        return 'Int64'
    elif 'char' in source_type_lower or 'text' in source_type_lower or 'varchar' in source_type_lower:
        return 'String'
    elif 'bool' in source_type_lower:
        return 'UInt8'
    elif 'date' in source_type_lower or 'time' in source_type_lower:
        return 'String'
    elif 'float' in source_type_lower or 'double' in source_type_lower or 'decimal' in source_type_lower:
        return 'Float64'
    return 'String'


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