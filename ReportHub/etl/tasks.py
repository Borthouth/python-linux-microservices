import os
import logging
import pymysql
import psycopg2
import pyodbc
import sqlalchemy as sa
from celery import shared_task
from django.utils import timezone
from control_plane.models import SourceProfile, SourceTable, SourceField, SyncStatus, DbChoice

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
    
    try:
        profile = SourceProfile.objects.get(pk=source_pk)
    except SourceProfile.DoesNotExist:
        logger.error(f"ETL Error: SourceProfile with ID {source_pk} not found.")
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
        profile.save()
        
        logger.info(f"ETL sync successfully completed for data source: '{profile.name}'")
        return f"ETL pipeline successfully synced {profile.name}"

    except Exception as e:
        error_message = str(e)
        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.FAILED
        profile.last_sync_error = error_message
        profile.save()
        
        logger.error(f"ETL sync failed for data source '{profile.name}': {error_message}")
        return f"ETL pipeline failed for {profile.name}: {error_message}"

@shared_task(bind=True, queue='queue_ux')
def test_source_connection_task(self, source_pk):
    """Background task wrapper specifically for Save & Test Connection workflow."""
    return sync_data_source_task(source_pk)


@shared_task(bind=True, queue='queue_periodic')
def introspect_source_schema_task(self, source_pk):
    """
    Introspects remote source database tables and columns using SQLAlchemy,
    updating SourceTable and SourceField control plane models automatically.
    """
    logger = get_etl_logger()
    
    try:
        profile = SourceProfile.objects.get(pk=source_pk)
    except SourceProfile.DoesNotExist:
        logger.error(f"Introspection Error: SourceProfile with ID {source_pk} not found.")
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

        for schema_name in target_schemas:
            try:
                tables = inspector.get_table_names(schema=schema_name if profile.db_type != DbChoice.MYSQL else None)
            except Exception as e:
                logger.warning(f"Could not fetch tables for schema {schema_name}: {e}")
                continue

            for table_name in tables:
                source_table, created = SourceTable.objects.get_or_create(
                    source=profile,
                    schema_name=schema_name if profile.db_type != DbChoice.MYSQL else profile.database_name,
                    table_name=table_name
                )

                try:
                    columns = inspector.get_columns(table_name, schema=schema_name if profile.db_type != DbChoice.MYSQL else None)
                except Exception as e:
                    logger.warning(f"Could not fetch columns for {schema_name}.{table_name}: {e}")
                    continue

                for col in columns:
                    field_name = col['name']
                    data_type = str(col['type'])

                    SourceField.objects.update_or_create(
                        table=source_table,
                        field_name=field_name,
                        defaults={'data_type': data_type}
                    )

        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.SUCCESS
        profile.last_sync_error = ''
        profile.save()

        logger.info(f"Schema introspection successfully completed for data source: '{profile.name}'")
        return f"Schema introspection successfully completed for {profile.name}"

    except Exception as e:
        error_message = str(e)
        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.FAILED
        profile.last_sync_error = error_message
        profile.save()

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
        # Dispatch individual source introspection task asynchronously
        introspect_source_schema_task.delay(source.pk)
        
    return f"Successfully dispatched schema introspection for {active_sources.count()} active sources."