import json
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, get_object_or_404, redirect
from django.core.paginator import Paginator
from django.db.models import Q
from django.http import Http404, FileResponse, JsonResponse
from control_plane.models import (
    SourceProfile, 
    UserSourceAccess, 
    SyncStatus,
    UserWidgetPreference
)
from reports.models import ReportExecution


def is_admin(user):
    return user.is_staff or user.is_superuser

@login_required
def dashboard_view(request):
    prefs, _ = UserWidgetPreference.objects.get_or_create(user=request.user)
    total_sources = SourceProfile.objects.count()
    user_access_count = UserSourceAccess.objects.filter(user=request.user).count()
    
    # Pipelines
    active_running_sources = SourceProfile.objects.filter(last_sync_status=SyncStatus.RUNNING)
    recent_jobs = SourceProfile.objects.exclude(last_sync_status=SyncStatus.RUNNING).order_by('-updated_at')[:5]

    # Active Report Generations
    active_running_reports = ReportExecution.objects.filter(status='RUNNING')

    context = {
        'total_sources': total_sources,
        'user_access_count': user_access_count,
        'active_running_sources': active_running_sources,
        'active_running_reports': active_running_reports,
        'recent_jobs': recent_jobs,
        'widget_prefs': prefs,
    }
    return render(request, 'control_plane/dashboard.html', context)


@login_required
@user_passes_test(is_admin)
def cancel_pipeline_view(request, source_pk):
    if request.method == 'POST':
        source = get_object_or_404(SourceProfile, pk=source_pk)
        source.is_cancelled = True
        source.save(update_fields=['is_cancelled'])
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
            
            # Update fields dynamically based on incoming payload
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
                
            prefs.save()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=400)
    return JsonResponse({'status': 'invalid method'}, status=405)