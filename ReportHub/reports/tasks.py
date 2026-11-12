import csv
import json
import io
import logging
from datetime import datetime
import clickhouse_connect
from celery import shared_task, Task
from django.utils import timezone
from django.core.mail import EmailMessage
from django.conf import settings
from minio import Minio
from .models import ReportDefinition, ReportExecution
from .progress import ReportProgress
from .ai_summary import generate_report_summary
from .charting import build_chart_data
from .exporters import build_json_report, build_xlsx_report
from .query_builder import build_clickhouse_report_query
from .cutover_guard import report_query_guard
from .pdf_export import build_pdf_report
from control_plane.utils import set_task_progress
from config.clickhouse_queries import cancel_clickhouse_query

logger = logging.getLogger(__name__)
PDF_MAX_ROWS = 5000


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
            execution.progress_percentage = ReportProgress.INITIALIZING
            execution.is_cancelled = False
            execution.updated_at = timezone.now()
            execution.save(update_fields=['status', 'progress_percentage', 'is_cancelled', 'updated_at'])
        else:
            execution = ReportExecution.objects.create(
                report=report, 
                status='running', 
                progress_percentage=ReportProgress.INITIALIZING
            )

        # Check cancellation early
        execution.refresh_from_db()
        if execution.is_cancelled:
            execution.status = 'cancelled'
            execution.completed_at = timezone.now()
            execution.save(update_fields=['status', 'completed_at'])
            set_task_progress(task_id, percentage=10, status="CANCELLED", error="Cancelled before query")
            return "Execution cancelled before query."

        config = report.config or {}
        selected_columns = config.get('selectedColumns', [])
        show_totals = config.get('showTotals', True)
        summary_position = config.get('summaryPosition', 'bottom')
        chart_config = config.get('chart')
        include_ai_summary = config.get('includeAiSummary') is True
        fields_to_query = selected_columns or report.selected_fields or []

        export_format = report.export_format.lower() if report.export_format else 'csv'
        row_count = 0
        rows = []
        pdf_truncated = False

        # --- Stage 1: ClickHouse Query Execution (Progress: 30%) ---
        execution.progress_percentage = ReportProgress.QUERYING
        execution.save(update_fields=['progress_percentage'])
        set_task_progress(task_id, percentage=ReportProgress.QUERYING.value, status="RUNNING", current_step=f"Querying ClickHouse for {report.name}...")

        with report_query_guard(report, task_id=task_id or "") as current_report:
            current_config = current_report.config or {}
            if (
                current_config != config
                or current_report.selected_fields != report.selected_fields
                or current_report.base_table_id != report.base_table_id
                or current_report.join_config != report.join_config
                or current_report.export_format != report.export_format
            ):
                raise RuntimeError("The report changed while waiting to run; retry the report.")
            query, headers, aggregates = build_clickhouse_report_query(
                current_report, fields_to_query
            )
            if export_format == 'pdf':
                query += f" LIMIT {PDF_MAX_ROWS + 1}"

            ch_client = clickhouse_connect.get_client(
                host=getattr(settings, 'CLICKHOUSE_HOST', 'localhost'),
                port=getattr(settings, 'CLICKHOUSE_PORT', 8123),
                username=getattr(settings, 'CLICKHOUSE_USER', 'default'),
                password=getattr(settings, 'CLICKHOUSE_PASSWORD', ''),
            )
            try:
                result = ch_client.query(
                    query,
                    settings={
                        "join_use_nulls": 1,
                        "query_id": f"report_{execution.pk}",
                    },
                )
                execution.refresh_from_db()
                if execution.is_cancelled:
                    cancel_clickhouse_query(ch_client, f"report_{execution.pk}")
                    raise InterruptedError("Report execution cancelled during query.")
                rows = [list(row) for row in result.result_rows]
                if export_format == 'pdf' and len(rows) > PDF_MAX_ROWS:
                    pdf_truncated = True
                    rows = rows[:PDF_MAX_ROWS]
                row_count = len(rows)
            finally:
                ch_client.close()

        # Check cancellation after heavy query
        execution.refresh_from_db()
        if execution.is_cancelled:
            execution.status = 'cancelled'
            execution.completed_at = timezone.now()
            execution.save(update_fields=['status', 'completed_at'])
            set_task_progress(task_id, percentage=30, status="CANCELLED", error="Cancelled after query")
            return "Execution cancelled after database query."

        # --- Stage 2: Summary Aggregates & File Build (Progress: 60%) ---
        execution.progress_percentage = ReportProgress.FORMATTING
        execution.save(update_fields=['progress_percentage'])
        set_task_progress(task_id, percentage=ReportProgress.FORMATTING.value, status="RUNNING", current_step="Building summaries and formatting file...")

        summary_lines = []
        if show_totals and rows and len(rows) > 0:
            for col_idx, agg_type in enumerate(aggregates):
                if not agg_type: continue 
                col_name = headers[col_idx]
                col_values = [r[col_idx] for r in rows if r[col_idx] is not None]
                computed_val = ""
                
                if agg_type == 'COUNT':
                    if (config.get('reportType') or 'tabular') == 'grouped_summary':
                        try:
                            computed_val = sum(int(value) for value in col_values)
                        except (TypeError, ValueError):
                            computed_val = ""
                    else:
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

        chart_data = None
        if export_format in {'pdf', 'xlsx'} and chart_config:
            if not isinstance(chart_config, dict):
                raise ValueError("The chart configuration must be an object.")
            if chart_config.get("type"):
                chart_data = build_chart_data(chart_config, headers, rows)

        ai_summary = None
        if include_ai_summary and export_format in {'pdf', 'xlsx', 'json'}:
            ai_summary = (
                generate_report_summary(headers, rows, pdf_truncated)
                if rows else "No records were available to summarize."
            )

        file_bytes = b''
        content_type = 'text/csv'

        if export_format == 'json':
            file_bytes = build_json_report(
                headers,
                rows,
                summary_lines,
                show_totals,
                ai_summary,
            )
            content_type = 'application/json'

        elif export_format == 'xlsx':
            file_bytes = build_xlsx_report(
                headers,
                rows,
                summary_lines,
                show_totals,
                summary_position,
                chart_data,
                ai_summary,
            )
            content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

        elif export_format == 'pdf':
            file_bytes = build_pdf_report(
                report_name=report.name,
                generated_at=timezone.now().strftime('%Y-%m-%d %H:%M:%S'),
                headers=headers,
                rows=rows,
                summary_lines=summary_lines,
                show_totals=show_totals,
                summary_position=summary_position,
                chart_data=chart_data,
                ai_summary=ai_summary,
                pdf_truncated=pdf_truncated,
                max_rows=PDF_MAX_ROWS,
            )
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
        execution.progress_percentage = ReportProgress.UPLOADING
        execution.save(update_fields=['progress_percentage'])
        set_task_progress(task_id, percentage=ReportProgress.UPLOADING.value, status="RUNNING", current_step="Uploading artifact to MinIO...")

        endpoint = getattr(settings, 'AWS_S3_ENDPOINT_URL', 'http://minio:9000')
        if endpoint.startswith('http://'): endpoint = endpoint[7:]
        elif endpoint.startswith('https://'): endpoint = endpoint[8:]

        minio_client = Minio(endpoint, access_key=getattr(settings, 'AWS_ACCESS_KEY_ID', 'minioadmin'), secret_key=getattr(settings, 'AWS_SECRET_ACCESS_KEY', 'minioadminpassword'), secure=False)
        bucket_name = getattr(settings, 'AWS_STORAGE_BUCKET_NAME', 'reporthub-artifacts')
        if not minio_client.bucket_exists(bucket_name): minio_client.make_bucket(bucket_name)

        file_extension = export_format if export_format in ['csv', 'json', 'pdf', 'xlsx'] else 'csv'
        file_name = f"reports/executions/{execution.pk}/artifact.{file_extension}"
        minio_client.put_object(bucket_name, file_name, io.BytesIO(file_bytes), len(file_bytes), content_type=content_type)

        execution.status = 'completed'
        execution.progress_percentage = ReportProgress.COMPLETED
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