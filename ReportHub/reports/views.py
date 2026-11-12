import json
import logging
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.db.models import OuterRef, Subquery
from django.core.paginator import Paginator
from .models import (
    ReportDefinition, 
    SourceTable, 
    SourceField, 
    ReportExecution
)
from .forms import ReportDefinitionForm
from datetime import timedelta
from django.http import Http404, HttpResponseRedirect
from django.conf import settings
from django.db.models import Q
from minio import Minio
from .tasks import execute_report_task
from django.contrib import messages
from django.template.loader import render_to_string
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from control_plane.models import SourceRelationship
from .query_builder import relationship_groups
from .ai_design import OllamaUnavailable, generate_report_design
from .email_webhooks import handle_email_webhook
from .metadata_views import guard_report_creation, guard_report_mutation


logger = logging.getLogger(__name__)


def is_admin(user):
    return user.is_staff or user.is_superuser


@login_required
@user_passes_test(is_admin)
def report_list(request):
    search_query = request.GET.get('q', '').strip()
    status_filter = request.GET.get('status', '')
    # Only expose artifacts generated after the current report configuration was saved.
    latest_execution = ReportExecution.objects.filter(
        report=OuterRef('pk')
    ).order_by('-created_at')
    downloadable_execution = ReportExecution.objects.filter(
        report=OuterRef('pk'),
        status='completed',
        completed_at__gte=OuterRef('updated_at'),
    ).exclude(file_path__isnull=True).exclude(file_path='').order_by('-completed_at')

    reports = ReportDefinition.objects.annotate(
        latest_status=Subquery(latest_execution.values('status')[:1]),
        latest_execution_pk=Subquery(latest_execution.values('pk')[:1]),
        latest_file_path=Subquery(latest_execution.values('file_path')[:1]),
        latest_run_time=Subquery(latest_execution.values('created_at')[:1]),
        downloadable_execution_pk=Subquery(downloadable_execution.values('pk')[:1]),
        downloadable_file_path=Subquery(downloadable_execution.values('file_path')[:1]),
    ).order_by('-created_at')

    if search_query:
        reports = reports.filter(
            Q(name__icontains=search_query) |
            Q(description__icontains=search_query) |
            Q(source__name__icontains=search_query)
        )
    if status_filter == 'active':
        reports = reports.filter(is_active=True)
    elif status_filter == 'inactive':
        reports = reports.filter(is_active=False)

    page_obj = Paginator(reports, 15).get_page(request.GET.get('page'))
    return render(request, 'reports/report_list.html', {
        'reports': page_obj,
        'page_obj': page_obj,
        'search_query': search_query,
        'status_filter': status_filter,
        'filter_options': [
            {'value': 'active', 'label': 'Active'},
            {'value': 'inactive', 'label': 'Inactive'},
        ],
    })


@login_required
@user_passes_test(is_admin)
@guard_report_creation
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
                    report.join_config = config_data.get('selectedRelationships', [])
                    
                    # Sync active columns into selected_fields for execution tasks
                    active_cols = config_data.get('selectedColumns', [])
                    if active_cols:
                        report.selected_fields = [
                            col.get('field_name') or col['field']
                            for col in active_cols
                        ]
                except json.JSONDecodeError:
                    pass
            config = report.config if isinstance(report.config, dict) else {}
            config['includeAiSummary'] = (
                form.cleaned_data['include_ai_summary']
                and report.export_format in {'pdf', 'xlsx', 'json'}
            )
            report.config = config
                    
            report.save()
            return redirect('reports:report_list')
    else:
        form = ReportDefinitionForm()
    return render(request, 'reports/report_form.html', {'form': form, 'title': 'Create New Report', 'button_text': 'Create Report'})


@login_required
@user_passes_test(is_admin)
@guard_report_mutation(post_only=True, may_change_source=True)
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
                    report.join_config = config_data.get('selectedRelationships', [])
                    
                    active_cols = config_data.get('selectedColumns', [])
                    if active_cols:
                        report.selected_fields = [
                            col.get('field_name') or col['field']
                            for col in active_cols
                        ]
                except json.JSONDecodeError:
                    pass
            config = report.config if isinstance(report.config, dict) else {}
            config['includeAiSummary'] = (
                form.cleaned_data['include_ai_summary']
                and report.export_format in {'pdf', 'xlsx', 'json'}
            )
            report.config = config
                    
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
@guard_report_mutation()
def report_toggle(request, pk):
    report = get_object_or_404(ReportDefinition, pk=pk)
    report.is_active = not report.is_active
    report.save()
    return redirect('reports:report_list')


@login_required
@user_passes_test(is_admin)
@guard_report_mutation(post_only=True)
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
            
        return JsonResponse(columns, safe=False)
    except SourceTable.DoesNotExist:
        return JsonResponse([], safe=False)


@login_required
@user_passes_test(is_admin)
def ajax_load_related_tables(request):
    source_id = request.GET.get('source_id')
    table_id = request.GET.get('table_id')
    if not source_id or not table_id:
        return JsonResponse([], safe=False)

    try:
        base_table = SourceTable.objects.get(pk=table_id, source_id=source_id)
    except (SourceTable.DoesNotExist, ValueError, TypeError):
        return JsonResponse([], safe=False)

    relationships = SourceRelationship.objects.filter(source_id=source_id).filter(
        Q(from_schema=base_table.schema_name, from_table=base_table.table_name)
        | Q(to_schema=base_table.schema_name, to_table=base_table.table_name)
    )

    result = []
    for group in relationship_groups(relationships):
        first = group[0]
        from_is_base = (
            first.from_schema == base_table.schema_name
            and first.from_table == base_table.table_name
        )
        related_schema = first.to_schema if from_is_base else first.from_schema
        related_name = first.to_table if from_is_base else first.from_table
        related_table = SourceTable.objects.filter(
            source_id=source_id,
            schema_name=related_schema,
            table_name=related_name,
        ).first()
        if related_table is None:
            continue

        relationship_ids = sorted(item.pk for item in group)
        pairs = [
            {
                'base_field': item.from_field if from_is_base else item.to_field,
                'related_field': item.to_field if from_is_base else item.from_field,
            }
            for item in group
        ]
        result.append({
            'key': ','.join(str(item) for item in relationship_ids),
            'relationship_ids': relationship_ids,
            'table_id': related_table.pk,
            'table_name': related_table.table_name,
            'schema_name': related_table.schema_name,
            'label': f"{related_table.schema_name}.{related_table.table_name}",
            'join_pairs': pairs,
            'fields': list(related_table.fields.values_list('field_name', flat=True)),
        })

    return JsonResponse(result, safe=False)


@login_required
@user_passes_test(is_admin)
@require_POST
def ai_report_design(request):
    try:
        payload = json.loads(request.body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JsonResponse({"error": "The request body must be valid JSON."}, status=400)
    if not isinstance(payload, dict):
        return JsonResponse({"error": "The request body must be a JSON object."}, status=400)

    description = payload.get("description")
    if not isinstance(description, str) or not description.strip():
        return JsonResponse({"error": "Describe the report you want to create."}, status=400)
    if len(description) > 2000:
        return JsonResponse({"error": "Keep the report description under 2,000 characters."}, status=400)

    try:
        base_table = SourceTable.objects.get(
            pk=payload.get("table_id"),
            source_id=payload.get("source_id"),
        )
    except (SourceTable.DoesNotExist, ValueError, TypeError):
        return JsonResponse({"error": "Select a valid data source and base table first."}, status=400)

    base_label = f"{base_table.schema_name}.{base_table.table_name}"
    schema = {
        "base_table": {
            "table_id": base_table.pk,
            "label": base_label,
            "fields": [
                {"name": field.field_name, "type": field.data_type}
                for field in base_table.fields.order_by("field_name")
            ],
        },
        "related_tables": [],
    }

    relationships = SourceRelationship.objects.filter(source_id=base_table.source_id).filter(
        Q(from_schema=base_table.schema_name, from_table=base_table.table_name)
        | Q(to_schema=base_table.schema_name, to_table=base_table.table_name)
    )
    for group in relationship_groups(relationships):
        first = group[0]
        from_is_base = (
            first.from_schema == base_table.schema_name
            and first.from_table == base_table.table_name
        )
        related_schema = first.to_schema if from_is_base else first.from_schema
        related_name = first.to_table if from_is_base else first.from_table
        related_table = SourceTable.objects.filter(
            source_id=base_table.source_id,
            schema_name=related_schema,
            table_name=related_name,
        ).first()
        if related_table is None:
            continue
        relationship_ids = sorted(item.pk for item in group)
        schema["related_tables"].append({
            "key": ",".join(str(item) for item in relationship_ids),
            "table_id": related_table.pk,
            "label": f"{related_table.schema_name}.{related_table.table_name}",
            "join_pairs": [
                {
                    "base_field": item.from_field if from_is_base else item.to_field,
                    "related_field": item.to_field if from_is_base else item.from_field,
                }
                for item in group
            ],
            "fields": [
                {"name": field.field_name, "type": field.data_type}
                for field in related_table.fields.order_by("field_name")
            ],
        })

    try:
        config = generate_report_design(description.strip(), schema)
    except OllamaUnavailable:
        return JsonResponse(
            {"error": "The local AI service is unavailable. Check that Ollama is running and the configured model is installed."},
            status=503,
        )
    except ValueError:
        logger.exception("AI report design generation or validation failed")
        return JsonResponse(
            {"error": "The AI returned an invalid report configuration. Please try again."},
            status=422,
        )
    return JsonResponse({"config": config})


@login_required
@user_passes_test(is_admin)
def report_history(request, pk):
    report = get_object_or_404(ReportDefinition, pk=pk)
    search_query = request.GET.get('q', '').strip()
    status_filter = request.GET.get('status', '')
    executions = report.executions.all().order_by('-created_at')
    if search_query:
        executions = executions.filter(
            Q(status__icontains=search_query) |
            Q(file_path__icontains=search_query)
        )
    if status_filter in dict(ReportExecution.STATUS_CHOICES):
        executions = executions.filter(status=status_filter)
    page_obj = Paginator(executions, 20).get_page(request.GET.get('page'))
    return render(request, 'reports/report_history.html', {
        'report': report,
        'executions': page_obj,
        'page_obj': page_obj,
        'search_query': search_query,
        'status_filter': status_filter,
        'filter_options': [
            {'value': value, 'label': label}
            for value, label in ReportExecution.STATUS_CHOICES
        ],
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
    return handle_email_webhook(view_request)