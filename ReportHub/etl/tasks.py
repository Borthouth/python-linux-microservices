import os
import logging
import pymysql
import psycopg2
import pyodbc
from celery import shared_task
from django.utils import timezone
from control_plane.models import SourceProfile, SyncStatus, DbChoice

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

@shared_task(bind=True)
def test_external_logging_task(self):
    """A test task to verify host-mounted file logging from inside Docker."""
    logger = get_etl_logger()
    logger.info("TEST_LOG_EVENT: Celery worker successfully wrote to host-mounted log directory!")
    return "Test log entry written to /app/logs/etl_execution.log"

@shared_task(bind=True)
def sync_data_source_task(self, source_pk):
    """
    ETL pipeline task for database connection checks and schema introspection,
    recording detailed execution logs to the host-mounted volume.
    """
    logger = get_etl_logger()
    
    try:
        profile = SourceProfile.objects.get(pk=source_pk)
    except SourceProfile.DoesNotExist:
        logger.error(f"ETL Error: SourceProfile with ID {source_pk} not found.")
        return f"SourceProfile {source_pk} not found."

    logger.info(f"Starting ETL sync pipeline for data source: '{profile.name}' ({profile.db_type}) at {profile.host}:{profile.port}")

    # Mark status as RUNNING so the UI reflects active execution
    profile.last_sync_status = SyncStatus.RUNNING
    profile.last_sync_error = ''
    profile.save(update_fields=['last_sync_status', 'last_sync_error'])

    try:
        # Perform connection test against target containers
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

        # Update tracking fields on success
        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.SUCCESS
        profile.last_sync_error = ''
        profile.save()
        
        logger.info(f"ETL sync successfully completed for data source: '{profile.name}'")
        return f"ETL pipeline successfully synced {profile.name}"

    except Exception as e:
        error_message = str(e)
        # Update tracking fields on failure
        profile.last_synced = timezone.now()
        profile.last_sync_status = SyncStatus.FAILED
        profile.last_sync_error = error_message
        profile.save()
        
        logger.error(f"ETL sync failed for data source '{profile.name}': {error_message}")
        return f"ETL pipeline failed for {profile.name}: {error_message}"

@shared_task(bind=True)
def test_source_connection_task(self, source_pk):
    """
    Background task wrapper specifically for the Save & Test Connection workflow,
    delegating to sync_data_source_task logic.
    """
    return sync_data_source_task(source_pk)

@shared_task(bind=True)
def cleanup_postgres_warehouse_task(self):
    """Cleans up stale warehouse data."""
    # Add your cleanup logic here
    pass