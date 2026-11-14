import json
import logging
from django.contrib import messages
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, get_object_or_404, redirect
from django.utils.dateparse import parse_datetime
from django.core.paginator import Paginator
from django.db.models import Case, CharField, F, Q, Value, When
from django.http import Http404, HttpResponseBadRequest, JsonResponse
from django.views.decorators.http import require_POST
from kombu.exceptions import OperationalError
from control_plane.models import (
    CdcExecutionFailure,
    SourceProfile, 
    UserSourceAccess, 
    SyncStatus,
    UserWidgetPreference
)
from control_plane.utils import get_redis_client, get_task_progress
from etl.progress import PIPELINE_STEPS
from reports.models import (
    EmailAuditLog,
    ReportArtifactAttempt,
    ReportExecution,
    ReportFileDelivery,
)
from reports.progress import REPORT_RUNNING_STEPS
from reports.tasks import reconcile_report_artifact_task

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
        'recent_email_audits': (
            EmailAuditLog.objects.select_related("report").order_by("-created_at", "-pk")[:5]
            if request.user.is_active and request.user.is_staff else []
        ),
        'recent_file_deliveries': (
            ReportFileDelivery.objects.select_related(
                "report", "execution__report", "destination"
            ).order_by("-created_at", "-pk")[:5]
            if request.user.is_active and request.user.is_staff else []
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
def cdc_failure_history_view(request):
    query = request.GET.get("q", "").strip()
    failures = CdcExecutionFailure.objects.select_related("source").order_by(
        "-failed_at", "-pk"
    )
    if query:
        failures = failures.filter(
            Q(source__name__icontains=query)
            | Q(source__db_type__icontains=query)
            | Q(task_id__icontains=query)
            | Q(error_message__icontains=query)
        )
    page_obj = Paginator(failures, 10).get_page(request.GET.get("page"))
    return render(request, "control_plane/cdc_failure_history.html", {
        "page_obj": page_obj,
        "search_query": query,
    })


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
    page_executions = list(page_obj.object_list)
    attempts_by_id = {
        attempt.pk: attempt
        for attempt in ReportArtifactAttempt.objects.filter(
            pk__in=[
                execution.current_artifact_attempt_id
                for execution in page_executions
                if execution.current_artifact_attempt_id
            ],
            execution_id__in=[execution.pk for execution in page_executions],
        )
    }
    for execution in page_executions:
        execution._current_artifact_attempt_cache = attempts_by_id.get(
            execution.current_artifact_attempt_id
        )
    page_obj.object_list = page_executions
    
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
@require_POST
def reconcile_report_artifact_view(request, report_pk, attempt_pk):
    execution = get_object_or_404(ReportExecution, pk=report_pk)
    attempt = execution.current_artifact_attempt
    if (
        attempt is None
        or attempt.pk != attempt_pk
        or not execution.artifact_reconciliation_available
    ):
        messages.error(request, 'This report artifact is not eligible for reconciliation.')
        return redirect('control_plane:report_history')

    try:
        reconcile_report_artifact_task.delay(str(execution.pk), str(attempt.pk))
    except OperationalError:
        logger.exception('Could not queue artifact reconciliation for %s', attempt.pk)
        messages.error(request, 'Could not queue artifact reconciliation. Check the task broker.')
    else:
        messages.success(
            request,
            'Artifact reconciliation was queued. The stored object will only be '
            'accepted if its size and checksum match.',
        )
    return redirect('control_plane:report_history')


@login_required
@user_passes_test(is_admin)
def download_report_view(request, report_pk):
    execution = get_object_or_404(ReportExecution, pk=report_pk, status='completed')
    if not execution.file_path:
        raise Http404("Report file not found.")
    return redirect('reports:report_download', execution_pk=execution.pk)


@login_required
@user_passes_test(is_admin)
def update_widget_preferences(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            if not isinstance(data, dict):
                return JsonResponse({'status': 'error', 'message': 'Invalid widget preferences.'}, status=400)
            widget_key = data.get('widget')
            is_visible = data.get('visible')
            allowed_widgets = (
                'stats', 'pipelines', 'reports', 'history', 'dlq',
                'cdc_failures', 'email_audit', 'file_deliveries',
            )
            if widget_key not in allowed_widgets or not isinstance(is_visible, bool):
                return JsonResponse({'status': 'error', 'message': 'Invalid widget or visibility.'}, status=400)
            if widget_key == 'file_deliveries' and not (
                request.user.is_active and request.user.is_staff
            ):
                return JsonResponse({'status': 'error', 'message': 'Staff access required.'}, status=403)
            prefs, _ = UserWidgetPreference.objects.get_or_create(user=request.user)

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
            elif widget_key == 'email_audit':
                prefs.show_email_audit = is_visible
            elif widget_key == 'file_deliveries':
                prefs.show_file_deliveries = is_visible
                
            prefs.save()
            return JsonResponse({'status': 'success'})
        except Exception:
            logger.exception("Failed to update widget preferences")
            return JsonResponse(
                {'status': 'error', 'message': 'Unable to update widget preferences.'},
                status=400,
            )
    return JsonResponse({'status': 'invalid method'}, status=405)