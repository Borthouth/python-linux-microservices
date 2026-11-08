import json
import logging
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, get_object_or_404, redirect
from django.utils.dateparse import parse_datetime
from django.core.paginator import Paginator
from django.db.models import Case, CharField, F, Q, Value, When
from django.http import Http404, HttpResponseBadRequest, FileResponse, JsonResponse
from control_plane.models import (
    CdcExecutionFailure,
    SourceProfile, 
    UserSourceAccess, 
    SyncStatus,
    UserWidgetPreference
)
from control_plane.utils import get_redis_client, get_task_progress
from etl.progress import PIPELINE_STEPS
from reports.models import ReportExecution
from reports.progress import REPORT_RUNNING_STEPS

logger = logging.getLogger(__name__)


def is_admin(user):
    return user.is_staff or user.is_superuser


def _active_pipeline_executions(redis_client):
    executions = []
    for key in redis_client.keys("task:progress:*"):
        key_text = key.decode("utf-8") if isinstance(key, bytes) else key
        task_id = key_text.rsplit(":", 1)[-1]
        data = get_task_progress(task_id)
        if data.get("status") != "RUNNING" or (
            "current_step" in data and "current_table" not in data
        ):
            continue
        started_at = parse_datetime(data["started_at"]) if data.get("started_at") else None
        executions.append({
            "task_id": task_id,
            "current_table": data.get("current_table", "Processing..."),
            "name": data.get("source_name", "Running Pipeline"),
            "db_type": data.get("db_type", "ETL"),
            "started_at": started_at,
        })
    executions.sort(
        key=lambda item: (
            item["started_at"].timestamp() if item["started_at"] else float("-inf"),
            item["task_id"],
        ),
        reverse=True,
    )
    return executions


def _active_report_generations():
    return (
        ReportExecution.objects.filter(status="running")
        .select_related("report")
        .order_by(F("created_at").desc(nulls_last=True), "-pk")
    )


@login_required
def dashboard_view(request):
    prefs, _ = UserWidgetPreference.objects.get_or_create(user=request.user)
    total_sources = SourceProfile.objects.count()
    user_access_count = UserSourceAccess.objects.filter(user=request.user).count()
    
    # Pipelines: Scan Redis DB for active running tasks
    r = get_redis_client()
    active_running_sources = _active_pipeline_executions(r)

    recent_jobs = SourceProfile.objects.exclude(last_sync_status=SyncStatus.RUNNING).order_by('-updated_at')[:5]

    # Active Report Generations
    active_running_reports = _active_report_generations()[:5]

    # Dead Letter Queue Tasks & Count
    dlq_keys = r.keys("dlq:task:*")
    dlq_tasks = []
    for key in dlq_keys:
        data = r.get(key)
        if data:
            try:
                task_dict = json.loads(data.decode('utf-8') if isinstance(data, bytes) else data)
                task_dict['id'] = task_dict.get('id') or task_dict.get('task_id') or key.decode('utf-8').split(':')[-1]
                task_dict['display_name'] = task_dict.get('task_name') or task_dict.get('name') or task_dict['id']
                task_dict['error_message'] = task_dict.get('error_summary') or task_dict.get('traceback') or 'Exception raised during execution'
                
                raw_time = task_dict.get('failed_at') or task_dict.get('timestamp')
                task_dict['failed_timestamp'] = parse_datetime(raw_time) if raw_time else None
                
                dlq_tasks.append(task_dict)
            except Exception:
                pass

    # Sort newest failures first
    dlq_tasks.sort(key=lambda x: x.get('failed_timestamp') or '', reverse=True)

    context = {
        'stats': {
            'total_connections': total_sources,
            'active_pipelines': len(active_running_sources),
            'reports_generated': ReportExecution.objects.filter(status='completed').count(),
            'dlq_count': len(dlq_tasks),
        },
        'total_sources': total_sources,
        'user_access_count': user_access_count,
        'active_running_sources': active_running_sources[:5],
        'active_running_reports': active_running_reports,
        'recent_jobs': recent_jobs,
        'widget_prefs': prefs,
        'dlq_count': len(dlq_tasks),
        'dlq_tasks': dlq_tasks[:5],  # Pass recent failed tasks to the dashboard widget
        'cdc_failures': (
            CdcExecutionFailure.objects.select_related("source").order_by("-failed_at", "-pk")[:5]
            if is_admin(request.user) else []
        ),
    }
    return render(request, 'control_plane/dashboard.html', context)


@login_required
def active_pipelines_fragment(request):
    """HTMX polling fragment for active pipelines widget."""
    r = get_redis_client()
    active_running_sources = _active_pipeline_executions(r)
    context = {"active_running_sources": active_running_sources[:5]}
    # Updated path to match your dashboard/components/ structure
    return render(request, 'control_plane/dashboard/components/active_pipelines.html', context)


@login_required
def active_pipeline_executions_view(request):
    search_query = request.GET.get("q", "").strip()
    step_filter = request.GET.get("step", "")
    executions = _active_pipeline_executions(get_redis_client())
    if step_filter:
        executions = [
            execution for execution in executions
            if execution["current_table"] == step_filter
        ]
    if search_query:
        query = search_query.casefold()
        executions = [
            execution for execution in executions
            if any(
                query in str(execution[field]).casefold()
                for field in ("name", "task_id", "current_table", "db_type")
            )
        ]
    page_obj = Paginator(executions, 10).get_page(request.GET.get("page"))
    return render(request, "control_plane/active_pipeline_executions.html", {
        "page_obj": page_obj,
        "search_query": search_query,
        "step_filter": step_filter,
        "unlisted_step": bool(step_filter and step_filter not in PIPELINE_STEPS),
        "filter_options": [
            {"value": step, "label": step}
            for step in PIPELINE_STEPS
        ],
    })


@login_required
def active_reports_fragment(request):
    """HTMX polling fragment for active reports widget."""
    active_running_reports = _active_report_generations()[:5]
    context = {"active_running_reports": active_running_reports}
    # Updated path to match your dashboard/components/ structure
    return render(request, 'control_plane/dashboard/components/active_reports.html', context)


@login_required
def active_report_generations_view(request):
    search_query = request.GET.get("q", "").strip()
    step_filter = request.GET.get("step", "")
    step_values = {key: value for key, _, value in REPORT_RUNNING_STEPS}
    if step_filter and step_filter not in step_values and step_filter != "unknown":
        return HttpResponseBadRequest("Invalid report progress step.")
    reports = _active_report_generations().annotate(
        step_label=Case(
            *(When(progress_percentage=value, then=Value(label))
              for _, label, value in REPORT_RUNNING_STEPS),
            default=Value("Unknown"),
            output_field=CharField(),
        )
    )
    if step_filter == "unknown":
        reports = reports.exclude(progress_percentage__in=step_values.values())
    elif step_filter:
        reports = reports.filter(progress_percentage=step_values[step_filter])
    if search_query:
        reports = reports.filter(report__name__icontains=search_query)
    page_obj = Paginator(reports, 10).get_page(request.GET.get("page"))
    return render(request, "control_plane/active_report_generations.html", {
        "page_obj": page_obj,
        "search_query": search_query,
        "step_filter": step_filter,
        "filter_options": [
            {"value": key, "label": label} for key, label, _ in REPORT_RUNNING_STEPS
        ] + [{"value": "unknown", "label": "Unknown"}],
    })


@login_required
@user_passes_test(is_admin)
def cancel_pipeline_view(request, source_pk):
    if request.method == 'POST':
        source = get_object_or_404(SourceProfile, pk=source_pk)
        # Deactivating the source triggers the cancellation guard in your Celery task loop
        source.is_active = False
        source.save(update_fields=['is_active'])
    return redirect('control_plane:dashboard')


@login_required
@user_passes_test(is_admin)
def pipeline_history_view(request):
    query = request.GET.get('q', '')
    status_filter = request.GET.get('status', '')
    
    sources = SourceProfile.objects.all().order_by('-updated_at')
    
    if query:
        sources = sources.filter(Q(name__icontains=query) | Q(db_type__icontains=query))
    if status_filter:
        sources = sources.filter(last_sync_status=status_filter)
        
    paginator = Paginator(sources, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    context = {
        'page_obj': page_obj,
        'query': query,
        'search_query': query,
        'status_filter': status_filter,
        'cdc_failures': CdcExecutionFailure.objects.select_related(
            "source"
        ).order_by("-failed_at", "-pk")[:25],
        'filter_options': [
            {'value': 'SUCCESS', 'label': 'Success'},
            {'value': 'RUNNING', 'label': 'Running'},
            {'value': 'FAILED', 'label': 'Failed'},
            {'value': 'CANCELLED', 'label': 'Cancelled'},
        ],
    }
    return render(request, 'control_plane/pipeline_history.html', context)


@login_required
@user_passes_test(is_admin)
def cancel_report_view(request, report_pk):
    if request.method == 'POST':
        report_job = get_object_or_404(ReportExecution, pk=report_pk)
        report_job.is_cancelled = True
        report_job.status = 'CANCELLED'
        report_job.save(update_fields=['is_cancelled', 'status'])
    return redirect('control_plane:dashboard')


@login_required
@user_passes_test(is_admin)
def report_history_view(request):
    query = request.GET.get('q', '')
    status_filter = request.GET.get('status', '')
    
    executions = ReportExecution.objects.select_related('report').all().order_by('-created_at')
    
    if query:
        executions = executions.filter(report__name__icontains=query)
    if status_filter:
        executions = executions.filter(status=status_filter)
        
    paginator = Paginator(executions, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    context = {
        'page_obj': page_obj,
        'query': query,
        'search_query': query,
        'status_filter': status_filter,
        'filter_options': [
            {'value': 'completed', 'label': 'Completed'},
            {'value': 'running', 'label': 'Running'},
            {'value': 'failed', 'label': 'Failed'},
            {'value': 'cancelled', 'label': 'Cancelled'},
        ],
    }
    return render(request, 'control_plane/report_history.html', context)


@login_required
@user_passes_test(is_admin)
def download_report_view(request, report_pk):
    execution = get_object_or_404(ReportExecution, pk=report_pk, status='completed')
    
    if not execution.file_path:
        raise Http404("Report file not found.")
        
    if execution.file_path.startswith('http'):
        return redirect(execution.file_path)
        
    try:
        file_handle = open(execution.file_path, 'rb')
        return FileResponse(file_handle, as_attachment=True)
    except FileNotFoundError:
        raise Http404("Report file missing from storage.")


@login_required
@user_passes_test(is_admin)
def update_widget_preferences(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            prefs, _ = UserWidgetPreference.objects.get_or_create(user=request.user)
            
            widget_key = data.get('widget')
            is_visible = data.get('visible')
            
            if widget_key == 'stats':
                prefs.show_stats = is_visible
            elif widget_key == 'pipelines':
                prefs.show_pipelines = is_visible
            elif widget_key == 'reports':
                prefs.show_reports = is_visible
            elif widget_key == 'history':
                prefs.show_history = is_visible
            elif widget_key == 'dlq':
                prefs.show_dlq = is_visible
            elif widget_key == 'cdc_failures':
                prefs.show_cdc_failures = is_visible
                
            prefs.save()
            return JsonResponse({'status': 'success'})
        except Exception:
            logger.exception("Failed to update widget preferences")
            return JsonResponse(
                {'status': 'error', 'message': 'Unable to update widget preferences.'},
                status=400,
            )
    return JsonResponse({'status': 'invalid method'}, status=405)