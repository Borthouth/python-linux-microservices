import json
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, get_object_or_404, redirect
from django.utils.dateparse import parse_datetime
from django.core.paginator import Paginator
from django.db.models import Q
from django.http import Http404, FileResponse, JsonResponse
from control_plane.models import (
    SourceProfile, 
    UserSourceAccess, 
    SyncStatus,
    UserWidgetPreference
)
from control_plane.utils import get_redis_client, get_task_progress
from reports.models import ReportExecution


def is_admin(user):
    return user.is_staff or user.is_superuser


@login_required
def dashboard_view(request):
    prefs, _ = UserWidgetPreference.objects.get_or_create(user=request.user)
    total_sources = SourceProfile.objects.count()
    user_access_count = UserSourceAccess.objects.filter(user=request.user).count()
    
    # Pipelines: Scan Redis DB for active running tasks
    r = get_redis_client()
    keys = r.keys("task:progress:*")
    active_running_sources = []
    
    for key in keys:
        task_id = key.decode('utf-8').split(":")[-1]
        data = get_task_progress(task_id)
        if data.get("status") == "RUNNING":
            active_running_sources.append({
                "task_id": task_id,
                "percentage": int(data.get("percentage", 0)),
                "current_table": data.get("current_table", "Processing..."),
                "name": data.get("source_name", "Running Pipeline"),
                "db_type": data.get("db_type", "ETL"),
                "started_at": parse_datetime(data["started_at"]) if data.get("started_at") else None,
            })

    recent_jobs = SourceProfile.objects.exclude(last_sync_status=SyncStatus.RUNNING).order_by('-updated_at')[:5]

    # Active Report Generations
    active_running_reports = ReportExecution.objects.filter(status='running')

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
        'active_running_sources': active_running_sources,
        'active_running_reports': active_running_reports,
        'recent_jobs': recent_jobs,
        'widget_prefs': prefs,
        'dlq_count': len(dlq_tasks),
        'dlq_tasks': dlq_tasks[:5],  # Pass recent failed tasks to the dashboard widget
    }
    return render(request, 'control_plane/dashboard.html', context)


@login_required
def active_pipelines_fragment(request):
    """HTMX polling fragment for active pipelines widget."""
    r = get_redis_client()
    keys = r.keys("task:progress:*")
    active_running_sources = []
    
    for key in keys:
        task_id = key.decode('utf-8').split(":")[-1]
        data = get_task_progress(task_id)
        if data.get("status") == "RUNNING":
            active_running_sources.append({
                "task_id": task_id,
                "percentage": int(data.get("percentage", 0)),
                "current_table": data.get("current_table", "Processing..."),
                "name": data.get("source_name", "Running Pipeline"),
                "started_at": parse_datetime(data["started_at"]) if data.get("started_at") else None
            })
            
    context = {"active_running_sources": active_running_sources}
    # Updated path to match your dashboard/components/ structure
    return render(request, 'control_plane/dashboard/components/active_pipelines.html', context)


@login_required
def active_reports_fragment(request):
    """HTMX polling fragment for active reports widget."""
    active_running_reports = ReportExecution.objects.filter(status='running').select_related('report')
    context = {"active_running_reports": active_running_reports}
    # Updated path to match your dashboard/components/ structure
    return render(request, 'control_plane/dashboard/components/active_reports.html', context)


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
                
            prefs.save()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=400)
    return JsonResponse({'status': 'invalid method'}, status=405)