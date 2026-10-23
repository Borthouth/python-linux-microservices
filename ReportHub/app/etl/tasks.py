from app.core.celery_app import celery_app
from app.etl.mysql_inspector import MySQLSchemaInspector
import logging

logger = logging.getLogger(__name__)

@celery_app.task(bind=True, name="etl.extract_mysql_schema")
def extract_mysql_schema_task(self, connection_string: str):
    """
    Celery task to asynchronously inspect a source MySQL database
    and return its table structures for warehouse ingestion.
    """
    logger.info("Starting MySQL schema extraction task...")
    inspector = None
    try:
        inspector = MySQLSchemaInspector(connection_string)
        tables = inspector.get_table_names()
        
        schema_map = {}
        for table in tables:
            schema_map[table] = inspector.get_table_schema(table)
            
        logger.info(f"Successfully extracted schema for {len(tables)} tables.")
        return {"status": "success", "tables": tables, "schemas": schema_map}
        
    except Exception as exc:
        logger.error(f"ETL extraction failed: {exc}")
        self.retry(exc=exc, countdown=60, max_retries=3)
    finally:
        if inspector:
            inspector.close()