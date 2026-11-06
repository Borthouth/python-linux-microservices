import json
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.db.models import OuterRef, Subquery
from .models import (
    ReportDefinition, 
    SourceTable, 
    SourceField, 
    ReportExecution, 
    EmailAuditLog
)
from .forms import ReportDefinitionForm
from datetime import timedelta
from django.http import Http404, HttpResponseRedirect
from django.conf import settings
from minio import Minio
from .tasks import execute_report_task
from django.contrib import messages
from django.template.loader import render_to_string
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt


def is_admin(user):
    return user.is_staff or user.is_superuser


@login_required
@user_passes_test(is_admin)
def report_list(request):
    # Annotate each report with its latest execution status and file path
    latest_execution = ReportExecution.objects.filter(
        report=OuterRef('pk')
    ).order_by('-created_at')

    reports = ReportDefinition.objects.annotate(
        latest_status=Subquery(latest_execution.values('status')[:1]),
        latest_execution_pk=Subquery(latest_execution.values('pk')[:1]),
        latest_file_path=Subquery(latest_execution.values('file_path')[:1]),
        latest_run_time=Subquery(latest_execution.values('created_at')[:1])
    ).order_by('-created_at')

    return render(request, 'reports/report_list.html', {'reports': reports})


@login_required
@user_passes_test(is_admin)
def report_create(request):
    if request.method == 'POST':
        form = ReportDefinitionForm(request.POST)
        if form.is_valid():
            report = form.save(commit=False)
            report.created_by = request.user
            report.is_active = True  # Ensure new reports start active by default
            
            # Capture and save the visual builder configuration JSON payload
            config_raw = request.POST.get('config_json')
            if config_raw:
                try:
                    config_data = json.loads(config_raw)
                    report.config = config_data
                    
                    # Sync active columns into selected_fields for execution tasks
                    active_cols = config_data.get('selectedColumns', [])
                    if active_cols:
                        report.selected_fields = [col['field'] for col in active_cols]
                except json.JSONDecodeError:
                    pass
                    
            report.save()
            return redirect('reports:report_list')
    else:
        form = ReportDefinitionForm()
    return render(request, 'reports/report_form.html', {'form': form, 'title': 'Create New Report', 'button_text': 'Create Report'})


@login_required
@user_passes_test(is_admin)
def report_edit(request, pk):
    report = get_object_or_404(ReportDefinition, pk=pk)
    previous_active_state = report.is_active

    if request.method == 'POST':
        form = ReportDefinitionForm(request.POST, instance=report)
        if form.is_valid():
            report = form.save(commit=False)
            report.is_active = previous_active_state
            
            config_raw = request.POST.get('config_json')
            if config_raw:
                try:
                    config_data = json.loads(config_raw)
                    report.config = config_data
                    
                    active_cols = config_data.get('selectedColumns', [])
                    if active_cols:
                        report.selected_fields = [col['field'] for col in active_cols]
                except json.JSONDecodeError:
                    pass
                    
            report.save()
            return redirect('reports:report_list')
    else:
        form = ReportDefinitionForm(instance=report)
        
    return render(request, 'reports/report_form.html', {
        'form': form, 
        'report': report,
        'title': f'Edit Report: {report.name}', 
        'button_text': 'Update Report'
    })


@login_required
@user_passes_test(is_admin)
def report_run(request, pk):
    report = get_object_or_404(ReportDefinition, pk=pk)
    
    # Create a pending execution record
    execution = ReportExecution.objects.create(report=report, status='pending')
    
    # Dispatch Celery task asynchronously
    execute_report_task.delay(str(report.id), str(execution.id))
    
    # Redirect to history / list page with a success notice
    messages.info(request, f"Report execution started for '{report.name}'. Check history for status updates.")
    return redirect('reports:report_list')


@login_required
@user_passes_test(is_admin)
def report_toggle(request, pk):
    report = get_object_or_404(ReportDefinition, pk=pk)
    report.is_active = not report.is_active
    report.save()
    return redirect('reports:report_list')


@login_required
@user_passes_test(is_admin)
def report_delete(request, pk):
    if request.method == 'POST':
        report = get_object_or_404(ReportDefinition, pk=pk)
        report.delete()
    return redirect('reports:report_list')


@login_required
@user_passes_test(is_admin)
def ajax_load_tables(request):
    source_id = request.GET.get('source_id')
    if not source_id:
        return JsonResponse([], safe=False)
    # Fetch tables linked to this specific SourceProfile
    tables = SourceTable.objects.filter(source_id=source_id).values('id', 'table_name')
    return JsonResponse(list(tables), safe=False)


def ajax_load_table_fields(request):
    table_id = request.GET.get('base_table')
    # Fetch fields belonging to the selected table
    fields = SourceField.objects.filter(table_id=table_id) if table_id else []
    return render(request, 'reports/components/field_checkboxes.html', {'fields': fields})


@login_required
@user_passes_test(is_admin)
def ajax_load_table_columns(request):
    table_id = request.GET.get('table_id')
    if not table_id:
        return JsonResponse([], safe=False)
    try:
        source_table = SourceTable.objects.get(pk=table_id)
        # Fetch the actual introspected field names from SourceField
        fields = source_table.fields.all().values_list('field_name', flat=True)
        columns = list(fields)
        
        if not columns:
            columns = ['id', 'created_at', 'updated_at', 'name', 'status', 'description']
            
        return JsonResponse(columns, safe=False)
    except SourceTable.DoesNotExist:
        return JsonResponse([], safe=False)


@login_required
@user_passes_test(is_admin)
def report_history(request, pk):
    report = get_object_or_404(ReportDefinition, pk=pk)
    executions = report.executions.all().order_by('-created_at')
    return render(request, 'reports/report_history.html', {
        'report': report,
        'executions': executions
    })


def get_minio_client():
    # Pulls your Minio endpoint and credentials from Django settings
    endpoint = getattr(settings, 'MINIO_STORAGE_ENDPOINT', 'minio:9000')
    if endpoint.startswith('http://'):
        endpoint = endpoint[7:]
    elif endpoint.startswith('https://'):
        endpoint = endpoint[8:]
        
    return Minio(
        endpoint,
        access_key=getattr(settings, 'MINIO_STORAGE_ACCESS_KEY', ''),
        secret_key=getattr(settings, 'MINIO_STORAGE_SECRET_KEY', ''),
        secure=getattr(settings, 'MINIO_STORAGE_USE_HTTPS', False)
    )


def get_internal_minio_client():
    endpoint = getattr(settings, 'AWS_S3_ENDPOINT_URL', 'http://minio:9000')
    if endpoint.startswith('http://'):
        endpoint = endpoint[7:]
    elif endpoint.startswith('https://'):
        endpoint = endpoint[8:]
        
    return Minio(
        endpoint,
        access_key=getattr(settings, 'AWS_ACCESS_KEY_ID', 'minioadmin'),
        secret_key=getattr(settings, 'AWS_SECRET_ACCESS_KEY', 'minioadminpassword'),
        secure=False
    )


@login_required
@user_passes_test(is_admin)
def report_download(request, execution_pk):
    execution = get_object_or_404(ReportExecution, pk=execution_pk)
    
    if not execution.file_path:
        raise Http404("Report file path not found.")
        
    bucket_name = getattr(settings, 'AWS_STORAGE_BUCKET_NAME', 'reporthub-reports')
    
    try:
        client = get_internal_minio_client()
        
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            
        response_obj = client.get_object(bucket_name, execution.file_path)
        file_data = response_obj.read()
        response_obj.close()
        response_obj.release_conn()
        
        filename = execution.file_path.split('/')[-1]
        response = HttpResponse(file_data, content_type='application/octet-stream')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response
        
    except Exception as e:
        try:
            objects = client.list_objects(bucket_name, recursive=True)
            existing_keys = [obj.object_name for obj in objects]
            print(f"DEBUG - Requested file_path: {execution.file_path}")
            print(f"DEBUG - Existing objects in bucket: {existing_keys}")
        except Exception as list_err:
            print(f"DEBUG - Could not list bucket objects: {list_err}")

        messages.error(request, f"Failed to download report artifact (File not found in storage): {str(e)}")
        return redirect('reports:report_list')


@login_required
@user_passes_test(is_admin)
def report_status(request, pk):
    report = get_object_or_404(ReportDefinition, pk=pk)
    
    latest_exec = report.executions.order_by('-created_at').first()
    
    status = latest_exec.status if latest_exec else 'PENDING'
    timestamp = latest_exec.completed_at or latest_exec.created_at if latest_exec else None
    error_msg = getattr(latest_exec, 'error_message', None) if latest_exec else None

    if status in ['SUCCESS', 'completed']:
        session_key = f'report_msg_shown_{report.pk}_{timestamp}'
        if not request.session.get(session_key):
            messages.success(request, f"Process complete for '{report.name}'! Artifact generated successfully.")
            request.session[session_key] = True
            
        response = render(request, 'control_plane/components/celery_status.html', {
            'component_id': f'report-status-{report.pk}',
            'status': status,
            'timestamp': timestamp,
            'poll_url': f"/reports/{report.pk}/status/"
        })
        response['HX-Refresh'] = 'true'
        return response

    if status in ['FAILED', 'failed']:
        fail_key = f'report_fail_shown_{report.pk}_{timestamp}'
        if not request.session.get(fail_key):
            messages.error(request, f"Report execution failed for '{report.name}': {error_msg}")
            request.session[fail_key] = True
            
        response = render(request, 'control_plane/components/celery_status.html', {
            'component_id': f'report-status-{report.pk}',
            'status': status,
            'timestamp': timestamp,
            'error_message': error_msg,
            'poll_url': f"/reports/{report.pk}/status/"
        })
        response['HX-Refresh'] = 'true'
        return response

    return render(request, 'control_plane/components/celery_status.html', {
        'component_id': f'report-status-{report.pk}',
        'status': status,
        'timestamp': timestamp,
        'poll_url': f"/reports/{report.pk}/status/"
    })


@csrf_exempt
def email_webhook_receiver(view_request):
    if view_request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    try:
        events = json.loads(view_request.body)
        for event in events:
            msg_id = event.get('sg_message_id') or event.get('message-id')
            event_type = event.get('event') # e.g., 'bounce', 'delivered', 'dropped'
            
            if msg_id:
                log_entry = EmailAuditLog.objects.filter(message_id__icontains=msg_id).first()
                if log_entry:
                    if event_type in ['bounce', 'dropped']:
                        log_entry.status = 'bounced'
                        log_entry.error_message = event.get('reason', 'Unknown bounce reason')
                    elif event_type == 'delivered':
                        log_entry.status = 'delivered'
                    log_entry.save()

        return JsonResponse({'status': 'success'}, status=200)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)