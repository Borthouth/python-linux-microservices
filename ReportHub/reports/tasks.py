import csv
import json
import io
from datetime import datetime
from celery import shared_task
from django.utils import timezone
from minio import Minio
from django.conf import settings
from .models import ReportDefinition, ReportExecution

@shared_task(bind=True)
def execute_report_task(self, report_id, execution_id=None):
    try:
        report = ReportDefinition.objects.get(pk=report_id)
        
        # Get or create execution record
        if execution_id:
            execution = ReportExecution.objects.get(pk=execution_id)
            execution.status = 'running'
            execution.save()
        else:
            execution = ReportExecution.objects.create(report=report, status='running')

        # 1. Simulate or execute dynamic query against base_table
        table_name = report.base_table.table_name if hasattr(report.base_table, 'table_name') else str(report.base_table)
        selected_fields = report.selected_fields or []
        filters = report.filter_config or {}

        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write headers
        writer.writerow(selected_fields)
        
        row_count = 0
        
        csv_data = output.getvalue()
        file_bytes = csv_data.encode('utf-8')

        # 2. Upload artifact to MinIO using correct settings and unique execution path
        endpoint = getattr(settings, 'AWS_S3_ENDPOINT_URL', 'http://minio:9000')
        if endpoint.startswith('http://'):
            endpoint = endpoint[7:]
        elif endpoint.startswith('https://'):
            endpoint = endpoint[8:]

        minio_client = Minio(
            endpoint,
            access_key=getattr(settings, 'AWS_ACCESS_KEY_ID', 'minioadmin'),
            secret_key=getattr(settings, 'AWS_SECRET_ACCESS_KEY', 'minioadminpassword'),
            secure=False
        )
        
        # Align bucket name with reporthub-reports
        bucket_name = getattr(settings, 'AWS_STORAGE_BUCKET_NAME', 'reporthub-reports')
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        # Use the unique execution.pk to guarantee a 1:1 unique path per run
        file_extension = report.export_format.lower() if report.export_format else 'csv'
        file_name = f"reports/executions/{execution.pk}/artifact.{file_extension}"
        
        minio_client.put_object(
            bucket_name,
            file_name,
            io.BytesIO(file_bytes),
            len(file_bytes),
            content_type='text/csv' if file_extension == 'csv' else 'application/octet-stream'
        )

        # 3. Update execution success status
        execution.status = 'completed'
        execution.file_path = file_name
        execution.row_count = row_count
        execution.completed_at = timezone.now()
        execution.save()

    except Exception as e:
        if 'execution' in locals():
            execution.status = 'failed'
            execution.error_message = str(e)
            execution.completed_at = timezone.now()
            execution.save()
        raise e