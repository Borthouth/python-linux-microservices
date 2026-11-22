import logging
from datetime import datetime, timedelta, timezone as datetime_timezone
from urllib.parse import urlencode

from kombu.exceptions import OperationalError
from django.contrib import messages
from django.core.paginator import EmptyPage, Paginator
from django.db import transaction
from django.db.models import Count, Q
from django.http import Http404, JsonResponse, QueryDict
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.template.loader import render_to_string
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from operations_plane.forms import (
    LOG_SEARCH_WINDOW,
    OperationsLogCursorForm,
    OperationsLogAnalysisForm,
    OperationsLogSearchForm,
    OperationsLiveLogForm,
    OperationsSearchHistoryForm,
    OperationsSavedResultsForm,
    OperationsDeleteSearchForm,
)
from operations_plane.breadcrumbs import get_operations_breadcrumbs
from operations_plane.log_store import MAX_ANALYSIS_SAMPLES, MAX_SEARCH_RESULTS, LogStoreUnavailable, latest_ingestion_time, log_event, recent_logs, search_logs
from operations_plane.models import OperationsLogAnalysis, OperationsLogSearch
from operations_plane.saved_results import SAVED_FILTER_FIELDS, filter_saved_results, saved_result_filters
from operations_plane.source_status import collection_status
from operations_plane.tasks import _filters_for, _serialize_log_rows, run_log_analysis, run_log_search
from operations_plane.views import operations_user_required
from otel.source_policy import SOURCE_CATEGORIES


logger = logging.getLogger(__name__)

FILTER_FIELDS = (
    'source',
    'service',
    'severity',
    'queue',
    'task_name',
    'task_id',
    'worker',
    'text',
)


def _model_filters(filters):
    return {
        'range_start': filters['range_start'],
        'range_end': filters['range_end'],
        'service_filter': filters['service'],
        'source_filter': filters.get('source', ''),
        'severity_filter': filters['severity'],
        'queue_filter': filters['queue'],
        'task_name_filter': filters['task_name'],
        'task_id_filter': filters['task_id'],
        'worker_filter': filters['worker'],
        'text_filter': filters['text'],
    }


def _form_filter_values(filters):
    values = {
        'range_start': timezone.localtime(filters['range_start']).strftime('%Y-%m-%dT%H:%M'),
        'range_end': timezone.localtime(filters['range_end']).strftime('%Y-%m-%dT%H:%M'),
    }
    values.update({field: filters.get(field) or '' for field in FILTER_FIELDS})
    return values


def _task_drilldown_url(filters, task_id):
    parameters = QueryDict('', mutable=True)
    parameters.update({
        'range_start': timezone.localtime(filters['range_start']).strftime('%Y-%m-%dT%H:%M'),
        'range_end': timezone.localtime(filters['range_end']).strftime('%Y-%m-%dT%H:%M'),
        'task_id': task_id,
    })
    return f"{reverse('operations_plane:logs')}?{parameters.urlencode()}"


def _attach_task_links(rows, filters):
    for row in rows:
        if row.get('task_id'):
            row['task_url'] = _task_drilldown_url(filters, row['task_id'])
    return rows


def _display_rows(rows, filters=None, preview_chars=300, preview_lines=3):
    if filters is None:
        filters = {'range_start': timezone.now() - LOG_SEARCH_WINDOW, 'range_end': timezone.now()}
    displayed_rows = []
    for row in rows:
        row = dict(row)
        if isinstance(row.get('event_time'), str):
            row['event_time'] = datetime.fromisoformat(row['event_time'])
        if isinstance(row.get('ingested_at'), str):
            row['ingested_at'] = datetime.fromisoformat(row['ingested_at'])
        if timezone.is_naive(row['event_time']):
            row['event_time'] = timezone.make_aware(row['event_time'], datetime_timezone.utc)
        if row.get('ingested_at') and timezone.is_naive(row['ingested_at']):
            row['ingested_at'] = timezone.make_aware(row['ingested_at'], datetime_timezone.utc)
        message = row['message']
        row['message_preview'] = '\n'.join(message[:preview_chars].splitlines()[:preview_lines])
        row['message_truncated'] = len(message) > preview_chars or len(message.splitlines()) > preview_lines
        row['source_label'] = dict(SOURCE_CATEGORIES).get(row.get('source_category'), row.get('source') or 'Unknown')
        row['detail_url'] = (
            reverse('operations_plane:log_event', args=[row['event_id']])
            + '?' + urlencode({'event_time': row['event_time'].isoformat()})
        )
        displayed_rows.append(row)
    return _attach_task_links(displayed_rows, filters)


def _queue_job(record, task, queue):
    try:
        async_result = task.apply_async(args=[record.pk], queue=queue)
    except OperationalError as exc:
        logger.warning('Could not queue Operations log job: %s', type(exc).__name__)
        record.status = record.Status.FAILED
        record.error_message = 'The Operations task queue is unavailable. Try again shortly.'
        record.finished_at = timezone.now()
        record.save(update_fields=[
            'status',
            'error_message',
            'finished_at',
            'updated_at',
        ])
        return

    record.task_id = async_result.id
    record.save(update_fields=['task_id', 'updated_at'])


def _result_pagination(filters, rows, has_more, before=None, after=None):
    if not rows:
        return {}

    def cursor_url(row, direction):
        parameters = _form_filter_values(filters)
        parameters.update({
            'event_time': row['event_time'].isoformat(timespec='microseconds'),
            'event_id': row['event_id'],
            'direction': direction,
        })
        return f"{reverse('operations_plane:logs')}?{urlencode(parameters)}"

    return {
        'previous_url': cursor_url(rows[0], 'newer') if before or (after and has_more) else '',
        'next_url': cursor_url(rows[-1], 'older') if (after or has_more) else '',
        'summary': f'{len(rows)} matches on this page, newest first',
    }


def _analysis_context(request, search, filters=None):
    analyses = search.analyses.filter(requested_by=request.operations_user)
    applied_filters = saved_result_filters(request.GET if filters is None else filters)
    selected_id = request.GET.get('analysis')
    if selected_id is not None:
        if not selected_id.isdecimal() or len(selected_id) > 18:
            raise Http404('Analysis not found.')
        analysis = get_object_or_404(analyses, pk=int(selected_id))
        if filters is None and not any(field in request.GET for field in SAVED_FILTER_FIELDS):
            applied_filters = saved_result_filters(analysis.saved_result_filters or {})
    else:
        matching = analyses.filter(saved_result_filters=applied_filters)
        analysis = matching.filter(status=OperationsLogAnalysis.Status.COMPLETED).first() or matching.first()
        if analysis is None and not any(applied_filters.values()):
            analysis = analyses.filter(
                saved_result_filters__isnull=True,
                status=OperationsLogAnalysis.Status.COMPLETED,
            ).first()
    return {
        'analysis': analysis,
        'analyses': analyses,
        'saved_result_filters': applied_filters,
        'analysis_sample_limit': MAX_ANALYSIS_SAMPLES,
    }


@operations_user_required
def logs_view(request):
    form = OperationsLogSearchForm(request.GET if request.GET else None)
    rows = []
    search_result = None
    saved_search = None
    filters = None
    error_message = ''
    store_available = True
    status_checked = False
    latest = None
    searched = False
    before = None
    after = None

    if request.GET:
        if form.is_valid():
            filters = form.filters()
            searched = True
        else:
            error_message = 'Please correct the search filters and try again.'
    else:
        filters = form.initial_filters()

    if filters and searched:
        cursor_form = OperationsLogCursorForm(request.GET)
        if any(name in request.GET for name in ('event_time', 'event_id', 'direction')):
            if cursor_form.is_valid():
                cursor = (cursor_form.cleaned_data['event_time'], str(cursor_form.cleaned_data['event_id']))
                if cursor_form.cleaned_data['direction'] == 'newer':
                    after = cursor
                else:
                    before = cursor
            else:
                error_message = 'The log page cursor is invalid. Run the search again.'

        if not error_message and filters['range_start'] < timezone.now() - LOG_SEARCH_WINDOW:
            search_result = OperationsLogSearch.objects.create(
                requested_by=request.operations_user,
                before_event_time=before[0] if before else None,
                before_event_id=before[1] if before else None,
                after_event_time=after[0] if after else None,
                after_event_id=after[1] if after else None,
                **_model_filters(filters),
            )
            _queue_job(search_result, run_log_search, 'queue_operations_logs')
            return redirect('operations_plane:log_search_detail', search_id=search_result.pk)

        if not error_message:
            try:
                latest = latest_ingestion_time()
                status_checked = True
                result = search_logs(filters, before=before, after=after)
                saved_search = OperationsLogSearch.objects.create(
                    requested_by=request.operations_user,
                    status=OperationsLogSearch.Status.COMPLETED,
                    results=_serialize_log_rows(result['rows']),
                    total_events=len(result['rows']),
                    has_more=result['has_more'],
                    before_event_time=before[0] if before else None,
                    before_event_id=before[1] if before else None,
                    after_event_time=after[0] if after else None,
                    after_event_id=after[1] if after else None,
                    started_at=timezone.now(),
                    finished_at=timezone.now(),
                    **_model_filters(filters),
                )
                rows = _display_rows(result['rows'], filters)
                search_result = result
            except LogStoreUnavailable:
                logger.warning('Operations log store is unavailable for the current search.')
                store_available = False

    if filters:
        form_values = _form_filter_values(filters)
    else:
        form_values = {}

    if not status_checked and store_available:
        try:
            latest = latest_ingestion_time()
        except LogStoreUnavailable:
            latest = None
            store_available = False
    context = {
        'operations_user': request.operations_user,
        'form': form,
        'rows': rows,
        'searched': searched,
        'search_result': search_result,
        'search': saved_search,
        **(_analysis_context(request, saved_search, filters={}) if saved_search else {}),
        'filters': filters,
        'form_values': form_values,
        'latest_ingestion': latest,
        'store_available': store_available,
        'error_message': error_message,
        'pagination': _result_pagination(filters, rows, search_result['has_more'], before, after) if search_result else {},
        'messages': messages.get_messages(request),
        'breadcrumbs': get_operations_breadcrumbs(request),
        'source_status': collection_status(),
    }
    return render(request, 'operations_plane/logs/logs_page.html', context)


@operations_user_required
@require_GET
def live_logs_view(request):
    form = OperationsLiveLogForm(request.GET or {'limit': 25})
    valid = form.is_valid()
    return render(request, 'operations_plane/logs/live_logs.html', {
        'operations_user': request.operations_user,
        'form': form,
        'source_status': collection_status(),
        'breadcrumbs': get_operations_breadcrumbs(request),
    }, status=200 if valid else 400)


@never_cache
@operations_user_required
@require_GET
def live_logs_data(request):
    form = OperationsLiveLogForm(request.GET or {'limit': 25})
    if not form.is_valid():
        return JsonResponse({'error': 'Invalid live log filters.', 'fields': form.errors}, status=400)
    filters = form.cleaned_data
    since = filters['since']
    if since and since > timezone.now():
        return JsonResponse({'error': 'The live log cursor cannot be in the future.'}, status=400)
    try:
        result = recent_logs(filters, limit=filters['limit'], since=since)
    except LogStoreUnavailable:
        logger.warning('Operations live log store is unavailable.')
        return JsonResponse({'error': 'Live logs are unavailable. Retrying with backoff; ingestion settings are unchanged.'}, status=503)
    rows = _display_rows(result['rows'])
    cursor = max((row['ingested_at'] for row in rows), default=since)
    sources = collection_status()
    selected = next((source for source in sources['categories'] if source['key'] == filters['source']), None)
    return JsonResponse({
        'html': render_to_string('operations_plane/logs/components/log_rows.html', {'rows': list(reversed(rows))}, request=request),
        'cursor': cursor.isoformat() if cursor else None,
        'has_more': result['has_more'],
        'source_status': render_to_string('operations_plane/logs/components/source_summary.html', {
            'source_status': sources, 'operations_user': request.operations_user,
        }, request=request),
        'selected_source_status': f"{selected['label']}: {selected['state']}" if selected else '',
        'checked_at': timezone.now().isoformat(),
    })


@never_cache
@operations_user_required
@require_GET
def log_event_view(request, event_id):
    cursor = OperationsLogCursorForm({
        'event_id': str(event_id), 'event_time': request.GET.get('event_time', ''),
    })
    if not cursor.is_valid():
        return JsonResponse({'error': 'A valid event time is required.'}, status=400)
    try:
        event = log_event(event_id, cursor.cleaned_data['event_time'])
    except LogStoreUnavailable:
        logger.warning('Operations log detail is unavailable.')
        return JsonResponse({'error': 'The log event could not be loaded. Try again.'}, status=503)
    if event is None:
        return JsonResponse({'error': 'This log event is no longer available.'}, status=404)
    return JsonResponse(_display_rows([event])[0])


@operations_user_required
@require_GET
def log_search_detail_view(request, search_id):
    search = get_object_or_404(
        OperationsLogSearch,
        pk=search_id,
        requested_by=request.operations_user,
    )
    filters = _filters_for(search)
    analysis_context = _analysis_context(request, search)
    parameters = request.GET.copy()
    parameters.update(analysis_context['saved_result_filters'])
    form = OperationsSavedResultsForm(parameters, rows=search.results)
    valid = form.is_valid()
    saved_rows = filter_saved_results(search.results, form.cleaned_data) if valid else []
    paginator = Paginator(saved_rows, MAX_SEARCH_RESULTS)
    try:
        page = paginator.page((form.cleaned_data.get('page') or 1) if valid else 1)
    except EmptyPage as exc:
        raise Http404('Saved results page not found.') from exc

    detail_url = reverse('operations_plane:log_search_detail', args=[search.pk])
    analysis_parameters = {'analysis': analysis_context['analysis'].pk} if analysis_context['analysis'] else {}

    def page_url(number):
        parameters = {**form.cleaned_data, **analysis_parameters, 'page': number}
        return f'{detail_url}?{urlencode(parameters)}'

    clear_url = detail_url
    return render(
        request,
        'operations_plane/logs/log_search_detail.html',
        {
            'operations_user': request.operations_user,
            'breadcrumbs': get_operations_breadcrumbs(request),
            'search': search,
            **analysis_context,
            'form_values': _form_filter_values(filters),
            'results_form': form,
            'results_action_url': detail_url,
            'results_clear_url': clear_url,
            'results_searched': any(field in request.GET for field in SAVED_FILTER_FIELDS) or any(analysis_context['saved_result_filters'].values()),
            'rows': _display_rows(page.object_list, filters),
            'pagination': {
                'previous_url': page_url(page.previous_page_number()) if page.has_previous() else '',
                'next_url': page_url(page.next_page_number()) if page.has_next() else '',
                'summary': f'Page {page.number} of {paginator.num_pages} - {paginator.count} saved matches',
            },
        },
        status=200 if valid else 400,
    )


@operations_user_required
@require_GET
def log_search_history_view(request):
    form = OperationsSearchHistoryForm(request.GET)
    valid = form.is_valid()
    q = form.cleaned_data.get('q', '')
    status = form.cleaned_data.get('status', '')
    searches = OperationsLogSearch.objects.filter(
        requested_by=request.operations_user,
    ).annotate(
        analysis_count=Count('analyses'),
        completed_analysis_count=Count('analyses', filter=Q(analyses__status=OperationsLogAnalysis.Status.COMPLETED)),
        queued_analysis_count=Count('analyses', filter=Q(analyses__status=OperationsLogAnalysis.Status.QUEUED)),
        running_analysis_count=Count('analyses', filter=Q(analyses__status=OperationsLogAnalysis.Status.RUNNING)),
        failed_analysis_count=Count('analyses', filter=Q(analyses__status=OperationsLogAnalysis.Status.FAILED)),
    )
    if not valid:
        searches = searches.none()
    if q:
        conditions = (
            Q(service_filter__icontains=q) | Q(source_filter__icontains=q)
            | Q(task_name_filter__icontains=q) | Q(task_id_filter__icontains=q)
            | Q(text_filter__icontains=q) | Q(error_message__icontains=q)
        )
        if q.isdecimal() and len(q) <= 18:
            conditions |= Q(pk=int(q))
        searches = searches.filter(conditions)
    if status:
        searches = searches.filter(status=status)
    paginator = Paginator(searches.order_by('-created_at', '-pk'), 20)
    try:
        page = paginator.page((form.cleaned_data.get('page') or 1) if valid else 1)
    except EmptyPage as exc:
        raise Http404('Search history page not found.') from exc

    def page_url(number):
        return f"{reverse('operations_plane:log_search_history')}?{urlencode({'q': q, 'status': status, 'page': number})}"

    return render(request, 'operations_plane/logs/search_history.html', {
        'operations_user': request.operations_user,
        'breadcrumbs': get_operations_breadcrumbs(request),
        'form': form,
        'page_obj': page,
        'search_query': q,
        'status_filter': status,
        'filter_options': [{'value': value, 'label': label} for value, label in OperationsLogSearch.Status.choices],
        'search_action_url': reverse('operations_plane:log_search_history'),
        'pagination': {
            'previous_url': page_url(page.previous_page_number()) if page.has_previous() else '',
            'next_url': page_url(page.next_page_number()) if page.has_next() else '',
            'summary': f'Page {page.number} of {paginator.num_pages}',
        },
    }, status=200 if valid else 400)


@require_POST
@operations_user_required
def delete_log_search_view(request, search_id):
    with transaction.atomic():
        search = get_object_or_404(
            OperationsLogSearch.objects.select_for_update(),
            pk=search_id, requested_by=request.operations_user,
        )
        if not OperationsDeleteSearchForm(request.POST).is_valid():
            messages.error(request, 'Confirm deletion of the saved search and its linked AI results.')
        elif search.status in (search.Status.QUEUED, search.Status.RUNNING) or search.analyses.filter(
            status__in=[OperationsLogAnalysis.Status.QUEUED, OperationsLogAnalysis.Status.RUNNING],
        ).exists():
            messages.error(request, 'This search or a linked AI analysis is still active. Wait for it to finish before deleting.')
        else:
            search.analyses.all().delete()
            search.delete()
            messages.success(request, 'Saved search and linked AI results deleted. Original ClickHouse logs are unchanged.')
    return redirect('operations_plane:log_search_history')


@never_cache
@require_POST
@operations_user_required
def analyze_logs_view(request):
    wants_json = request.headers.get('Accept') == 'application/json'
    form = OperationsLogAnalysisForm(request.POST)
    if not form.is_valid():
        error = 'Complete a log search before analyzing logs.'
        if wants_json:
            return JsonResponse({'error': error}, status=400)
        messages.error(request, error)
        return redirect('operations_plane:logs')

    with transaction.atomic():
        search = get_object_or_404(
            OperationsLogSearch.objects.select_for_update(),
            pk=form.cleaned_data['search_id'],
            requested_by=request.operations_user,
            status=OperationsLogSearch.Status.COMPLETED,
        )
        result_form = OperationsSavedResultsForm(request.POST, rows=search.results)
        if not result_form.is_valid():
            error = 'Please correct the saved-result filters before analyzing.'
            if wants_json:
                return JsonResponse({'error': error, 'fields': result_form.errors}, status=400)
            messages.error(request, error)
            return redirect('operations_plane:log_search_detail', search_id=search.pk)
        applied_filters = saved_result_filters(result_form.cleaned_data)
        analysis = search.analyses.filter(
            requested_by=request.operations_user,
            saved_result_filters=applied_filters,
            status__in=[OperationsLogAnalysis.Status.QUEUED, OperationsLogAnalysis.Status.RUNNING],
        ).first()
        if analysis is None:
            analysis = OperationsLogAnalysis.objects.create(
                search=search,
                requested_by=request.operations_user,
                saved_result_filters=applied_filters,
                **_model_filters(_filters_for(search)),
            )
            transaction.on_commit(
                lambda: _queue_job(analysis, run_log_analysis, 'queue_operations_ai'),
            )
    if wants_json:
        return JsonResponse(_analysis_payload(request, analysis))
    return redirect(
        reverse('operations_plane:log_search_detail', args=[search.pk])
        + f'?analysis={analysis.pk}#ai-analysis',
    )


def _analysis_payload(request, analysis):
    return {
        'id': analysis.pk,
        'status': analysis.status,
        'status_label': analysis.get_status_display(),
        'status_url': reverse('operations_plane:log_analysis_status', args=[analysis.pk]),
        'runs_html': render_to_string(
            'operations_plane/logs/components/analysis_runs.html',
            {
                'search': analysis.search,
                'analysis': analysis,
                'analyses': analysis.search.analyses.filter(requested_by=request.operations_user) if analysis.search_id else [],
            }, request=request,
        ),
        'html': render_to_string(
            'operations_plane/logs/components/analysis_result.html',
            {'analysis': analysis, 'analysis_sample_limit': MAX_ANALYSIS_SAMPLES}, request=request,
        ),
    }


@never_cache
@operations_user_required
@require_GET
def log_analysis_status_view(request, analysis_id):
    analysis = get_object_or_404(
        OperationsLogAnalysis, pk=analysis_id, requested_by=request.operations_user,
    )
    return JsonResponse(_analysis_payload(request, analysis))


@operations_user_required
def log_analysis_detail_view(request, analysis_id):
    analysis = get_object_or_404(
        OperationsLogAnalysis,
        pk=analysis_id,
        requested_by=request.operations_user,
    )
    if analysis.search_id:
        return redirect(
            reverse('operations_plane:log_search_detail', args=[analysis.search_id])
            + f'?analysis={analysis.pk}#ai-analysis',
        )
    return render(
        request,
        'operations_plane/logs/log_analysis_detail.html',
        {
            'operations_user': request.operations_user,
            'breadcrumbs': get_operations_breadcrumbs(request),
            'analysis': analysis,
            'analysis_sample_limit': MAX_ANALYSIS_SAMPLES,
        },
    )
