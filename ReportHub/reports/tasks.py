import csv
import json
import io
import pyodbc
import pymysql
import psycopg2
import logging
from datetime import datetime
from celery import shared_task, Task
from django.utils import timezone
from django.core.mail import EmailMessage
from django.conf import settings
from minio import Minio
from .models import ReportDefinition, ReportExecution
from control_plane.utils import set_task_progress

logger = logging.getLogger(__name__)


class CancellableReportTask(Task):
    """Base Celery task that cleans up execution state or handles revocation for reports."""
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        super().on_failure(exc, task_id, args, kwargs, einfo)


@shared_task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 60})
def send_report_email_task(self, report_id, execution_id=None):
    """
    Asynchronous Celery task to dispatch report completion notifications 
    and file attachments from MinIO storage to configured recipient lists,
    recording an audit log for every attempt.
    """
    from .models import EmailAuditLog, ReportDefinition, ReportExecution
    
    report = None
    execution = None
    recipients = []
    
    try:
        report = ReportDefinition.objects.get(pk=report_id)
        recipients = report.get_recipient_list()
        recipient_str = ", ".join(recipients)

        if not report.is_email_active or not recipients:
            logger.info(f"Email notifications disabled or no recipients configured for report: {report.name}")
            return f"Skipped: Email inactive or empty for {report.name}"

        subject = f"[ReportHub] Automated Report Output: {report.name}"
        body = (
            f"Hello,\n\n"
            f"Your scheduled execution for report '{report.name}' has completed successfully.\n\n"
            f"Description:\n{report.description or 'No description provided.'}\n\n"
            f"Attached is your generated report artifact.\n"
            f"You can also view full results and audit history directly inside the ReportHub Control Plane.\n\n"
            f"Best regards,\nReportHub Automation Engine"
        )

        email = EmailMessage(
            subject=subject,
            body=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=recipients,
        )

        # Retrieve and attach the report file artifact from MinIO if execution_id is provided
        if execution_id:
            try:
                execution = ReportExecution.objects.get(pk=execution_id)
                if execution.file_path:
                    endpoint = getattr(settings, 'AWS_S3_ENDPOINT_URL', 'http://minio:9000')
                    if endpoint.startswith('http://'): endpoint = endpoint[7:]
                    elif endpoint.startswith('https://'): endpoint = endpoint[8:]

                    minio_client = Minio(
                        endpoint,
                        access_key=getattr(settings, 'AWS_ACCESS_KEY_ID', 'minioadmin'),
                        secret_key=getattr(settings, 'AWS_SECRET_ACCESS_KEY', 'minioadminpassword'),
                        secure=False
                    )
                    bucket_name = getattr(settings, 'AWS_STORAGE_BUCKET_NAME', 'reporthub-artifacts')
                    
                    response = minio_client.get_object(bucket_name, execution.file_path)
                    file_data = response.read()
                    response.close()
                    response.release_conn()

                    filename = execution.file_path.split('/')[-1]
                    content_type = 'application/pdf' if filename.endswith('.pdf') else ('application/json' if filename.endswith('.json') else 'text/csv')
                    
                    email.attach(filename, file_data, content_type)
            except Exception as attachment_err:
                logger.warning(f"Could not attach report artifact to email for execution {execution_id}: {attachment_err}")

        # Send Email via SMTP / Mailpit
        email.send(fail_silently=False)

        # Record Successful Audit Log
        EmailAuditLog.objects.create(
            report=report,
            execution=execution,
            recipients=recipient_str,
            status='sent'
        )

        logger.info(f"Successfully dispatched report '{report.name}' to {len(recipients)} recipient(s).")
        return f"Dispatched {report.name} to {recipients}"

    except ReportDefinition.DoesNotExist:
        logger.error(f"ReportDefinition with ID {report_id} does not exist.")
        raise
    except Exception as exc:
        # Record Failed Audit Log
        if report:
            EmailAuditLog.objects.create(
                report=report,
                execution=execution,
                recipients=", ".join(recipients) if recipients else "Unknown",
                status='failed',
                error_message=str(exc)
            )
        logger.error(f"Failed to send email for report ID {report_id}: {exc}")
        raise exc


@shared_task(bind=True, base=CancellableReportTask)
def execute_report_task(self, report_id, execution_id=None):
    task_id = self.request.id
    set_task_progress(task_id, percentage=5, status="RUNNING", current_step="Initializing report execution...")

    try:
        report = ReportDefinition.objects.get(pk=report_id)
        
        if execution_id:
            execution = ReportExecution.objects.get(pk=execution_id)
            execution.status = 'running'
            execution.progress_percentage = 10
            execution.is_cancelled = False
            execution.updated_at = timezone.now()
            execution.save(update_fields=['status', 'progress_percentage', 'is_cancelled', 'updated_at'])
        else:
            execution = ReportExecution.objects.create(
                report=report, 
                status='running', 
                progress_percentage=10
            )

        # Check cancellation early
        execution.refresh_from_db()
        if execution.is_cancelled:
            execution.status = 'cancelled'
            execution.completed_at = timezone.now()
            execution.save(update_fields=['status', 'completed_at'])
            set_task_progress(task_id, percentage=10, status="CANCELLED", error="Cancelled before query")
            return "Execution cancelled before query."

        table_name = report.base_table.table_name if hasattr(report.base_table, 'table_name') else str(report.base_table)
        config = report.config or {}
        selected_columns = config.get('selectedColumns', [])
        show_totals = config.get('showTotals', True)
        summary_position = config.get('summaryPosition', 'bottom')
        
        if selected_columns:
            fields_to_query = [col['field'] for col in selected_columns]
            headers = [col.get('label') or col['field'] for col in selected_columns]
            aggregates = [col.get('aggregate', '').upper() for col in selected_columns]
        else:
            fields_to_query = report.selected_fields or []
            headers = fields_to_query
            aggregates = [''] * len(fields_to_query)

        export_format = report.export_format.lower() if report.export_format else 'csv'
        row_count = 0
        rows = []
        
        # --- Stage 1: Database Query Execution (Progress: 30%) ---
        execution.progress_percentage = 30
        execution.save(update_fields=['progress_percentage'])
        set_task_progress(task_id, percentage=30, status="RUNNING", current_step=f"Querying database for {report.name}...")

        if fields_to_query and table_name and report.source:
            source = report.source
            query = f"SELECT {', '.join(fields_to_query)} FROM {table_name}"
            db_type = str(source.db_type).lower()
            source_name = str(source.name).lower()
            
            if any(k in db_type or k in source_name for k in ['mssql', 'sql server', 'microsoft']):
                conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={source.host},{source.port};DATABASE={source.database_name};UID={source.username};PWD={source.password};TrustServerCertificate=yes;"
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
                cursor.execute(query)
                while True:
                    execution.refresh_from_db()
                    if execution.is_cancelled:
                        cursor.close()
                        conn.close()
                        raise InterruptedError("Report execution cancelled during fetch.")

                    batch = cursor.fetchmany(1000)
                    if not batch:
                        break
                    for row in batch:
                        rows.append(list(row))
                        row_count += 1
                conn.close()

            elif 'mysql' in db_type or 'mysql' in source_name:
                conn = pymysql.connect(host=source.host, port=source.port, user=source.username, password=source.password, database=source.database_name)
                cursor = conn.cursor()
                cursor.execute(query)
                while True:
                    execution.refresh_from_db()
                    if execution.is_cancelled:
                        cursor.close()
                        conn.close()
                        raise InterruptedError("Report execution cancelled during fetch.")

                    batch = cursor.fetchmany(1000)
                    if not batch:
                        break
                    for row in batch:
                        rows.append(list(row))
                        row_count += 1
                conn.close()

            else:
                conn = psycopg2.connect(host=source.host, port=source.port, user=source.username, password=source.password, dbname=source.database_name)
                cursor = conn.cursor()
                cursor.execute(query)
                while True:
                    execution.refresh_from_db()
                    if execution.is_cancelled:
                        cursor.close()
                        conn.close()
                        raise InterruptedError("Report execution cancelled during fetch.")

                    batch = cursor.fetchmany(1000)
                    if not batch:
                        break
                    for row in batch:
                        rows.append(list(row))
                        row_count += 1
                conn.close()

        # Check cancellation after heavy query
        execution.refresh_from_db()
        if execution.is_cancelled:
            execution.status = 'cancelled'
            execution.completed_at = timezone.now()
            execution.save(update_fields=['status', 'completed_at'])
            set_task_progress(task_id, percentage=30, status="CANCELLED", error="Cancelled after query")
            return "Execution cancelled after database query."

        # --- Stage 2: Summary Aggregates & File Build (Progress: 60%) ---
        execution.progress_percentage = 60
        execution.save(update_fields=['progress_percentage'])
        set_task_progress(task_id, percentage=60, status="RUNNING", current_step="Building summaries and formatting file...")

        summary_lines = []
        if show_totals and rows and len(rows) > 0:
            for col_idx, agg_type in enumerate(aggregates):
                if not agg_type: continue 
                col_name = fields_to_query[col_idx]
                col_values = [r[col_idx] for r in rows if r[col_idx] is not None]
                computed_val = ""
                
                if agg_type == 'COUNT':
                    computed_val = len(rows)
                elif agg_type in ['SUM', 'AVG', 'MIN', 'MAX'] and col_values:
                    try:
                        numeric_vals = [float(v) for v in col_values if str(v).replace('.', '', 1).isdigit() or isinstance(v, (int, float))]
                        if numeric_vals:
                            if agg_type == 'SUM': res = sum(numeric_vals)
                            elif agg_type == 'AVG': res = sum(numeric_vals) / len(numeric_vals)
                            elif agg_type == 'MIN': res = min(numeric_vals)
                            elif agg_type == 'MAX': res = max(numeric_vals)
                            computed_val = int(res) if isinstance(numeric_vals[0], int) and agg_type != 'AVG' else round(res, 2)
                    except Exception:
                        pass
                summary_lines.append(f"{col_name} ({agg_type.lower()}): {computed_val}")

        file_bytes = b''
        content_type = 'text/csv'

        if export_format == 'json':
            structured_data = [dict(zip(headers, row)) for row in rows]
            output_payload = {"data": structured_data}
            if show_totals and summary_lines: output_payload["summaries"] = summary_lines
            file_bytes = json.dumps(output_payload, default=str, indent=2).encode('utf-8')
            content_type = 'application/json'

        elif export_format == 'pdf':
            from reportlab.lib.pagesizes import letter
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            from reportlab.lib.styles import getSampleStyleSheet
            from reportlab.lib import colors

            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=36, leftMargin=36, topMargin=36, bottomMargin=36)
            story = []
            styles = getSampleStyleSheet()

            story.append(Paragraph(f"<b>Report:</b> {report.name}", styles['Heading1']))
            story.append(Paragraph(f"<b>Generated at:</b> {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
            story.append(Spacer(1, 10))

            def build_summary_paragraph():
                if not summary_lines: return None
                return Paragraph("<b>Summary Aggregates:</b><br/>" + "<br/>".join(summary_lines), styles['Normal'])

            if show_totals and summary_lines and summary_position == 'top':
                sum_para = build_summary_paragraph()
                if sum_para:
                    story.append(sum_para)
                    story.append(Spacer(1, 20))

            story.append(Paragraph("<b>Detailed Records</b>", styles['Heading2']))
            table_data = [headers]
            for row in rows:
                table_data.append([str(val) if val is not None else '' for val in row])

            col_widths = [doc.width / max(len(headers), 1)] * len(headers)
            t = Table(table_data, colWidths=col_widths)
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4F46E5')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F9FAFB')),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#E5E7EB')),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
            ]))
            story.append(t)

            if show_totals and summary_lines and summary_position == 'bottom':
                story.append(Spacer(1, 15))
                sum_para = build_summary_paragraph()
                if sum_para: story.append(sum_para)

            doc.build(story)
            file_bytes = buffer.getvalue()
            buffer.close()
            content_type = 'application/pdf'

        else: # CSV
            output = io.StringIO()
            writer = csv.writer(output)
            if show_totals and summary_lines and summary_position == 'top':
                for line in summary_lines: writer.writerow([line])
                writer.writerow([])
            writer.writerow(headers)
            for row in rows: writer.writerow(row)
            if show_totals and summary_lines and summary_position == 'bottom':
                writer.writerow([])
                for line in summary_lines: writer.writerow([line])
            file_bytes = output.getvalue().encode('utf-8')
            content_type = 'text/csv'

        # --- Stage 3: MinIO Storage Upload (Progress: 85%) ---
        execution.progress_percentage = 85
        execution.save(update_fields=['progress_percentage'])
        set_task_progress(task_id, percentage=85, status="RUNNING", current_step="Uploading artifact to MinIO...")

        endpoint = getattr(settings, 'AWS_S3_ENDPOINT_URL', 'http://minio:9000')
        if endpoint.startswith('http://'): endpoint = endpoint[7:]
        elif endpoint.startswith('https://'): endpoint = endpoint[8:]

        minio_client = Minio(endpoint, access_key=getattr(settings, 'AWS_ACCESS_KEY_ID', 'minioadmin'), secret_key=getattr(settings, 'AWS_SECRET_ACCESS_KEY', 'minioadminpassword'), secure=False)
        bucket_name = getattr(settings, 'AWS_STORAGE_BUCKET_NAME', 'reporthub-artifacts')
        if not minio_client.bucket_exists(bucket_name): minio_client.make_bucket(bucket_name)

        file_extension = export_format if export_format in ['csv', 'json', 'pdf'] else 'csv'
        file_name = f"reports/executions/{execution.pk}/artifact.{file_extension}"
        minio_client.put_object(bucket_name, file_name, io.BytesIO(file_bytes), len(file_bytes), content_type=content_type)

        execution.status = 'completed'
        execution.progress_percentage = 100
        execution.file_path = file_name
        execution.row_count = row_count
        execution.completed_at = timezone.now()
        execution.save()

        set_task_progress(task_id, percentage=100, status="SUCCESS", current_step="Completed")

        # --- Stage 4: Trigger Asynchronous Email Notifications with Attachment ---
        if report.is_email_active:
            send_report_email_task.delay(report.pk, execution.pk)

        return f"Report {report.name} successfully executed."

    except Exception as e:
        if 'execution' in locals():
            execution.refresh_from_db()
            if execution.is_cancelled or isinstance(e, InterruptedError):
                execution.status = 'cancelled'
                execution.error_message = 'Execution cancelled by user.'
            else:
                execution.status = 'failed'
                execution.error_message = str(e)
            execution.completed_at = timezone.now()
            execution.save()
        if 'task_id' in locals():
            status_val = "CANCELLED" if 'execution' in locals() and execution.status == 'cancelled' else "FAILED"
            set_task_progress(task_id, percentage=100, status=status_val, error=str(e))
        raise e