from celery import shared_task

@shared_task
def cleanup_postgres_warehouse_task():
    """Background task to clean up warehouse data or run ETL maintenance."""
    # Add your cleaning/ETL logic here
    print("Executing warehouse cleanup task...")
    return "Cleanup completed successfully"