import json
import logging
from functools import wraps
from uuid import uuid4

from django.contrib.auth import authenticate, get_user_model, login, logout
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.http import require_http_methods, require_POST

from operations_plane.breadcrumbs import get_operations_breadcrumbs
from operations_plane.forms import OperationsLoginForm
from operations_plane.log_store import LogStoreUnavailable, latest_ingestion_time
from operations_plane.models import (
    OperationsLogAnalysis,
    OperationsLogSearch,
    OperationsUser,
    OperationsWidgetPreference,
)


logger = logging.getLogger(__name__)


def _redirect_for_request(request, route_name):
    return redirect(reverse(route_name, urlconf=getattr(request, 'urlconf', None)))


def _active_operations_user(user):
    if not user.is_authenticated:
        return None
    return OperationsUser.objects.filter(
        auth_user=user,
        is_active=True,
    ).select_related('auth_user').first()


def operations_user_required(view_func):
    @wraps(view_func)
    def wrapped(request, *args, **kwargs):
        account = _active_operations_user(request.user)
        if account is None:
            return _redirect_for_request(request, 'operations_plane:login')
        request.operations_user = account
        return view_func(request, *args, **kwargs)

    return wrapped


@require_http_methods(['GET', 'POST'])
def login_view(request):
    existing_account = _active_operations_user(request.user)
    if existing_account:
        return _redirect_for_request(request, 'operations_plane:dashboard')

    form = OperationsLoginForm(request.POST or None)
    if request.method == 'POST' and form.is_valid():
        account = (
            OperationsUser.objects
            .filter(username=form.cleaned_data['username'])
            .select_related('auth_user')
            .first()
        )
        auth_username = (
            account.auth_user.username
            if account
            else str(uuid4())
        )
        authenticated_user = authenticate(
            request,
            username=auth_username,
            password=form.cleaned_data['password'],
        )

        if (
            account
            and account.is_active
            and authenticated_user
            and authenticated_user.pk == account.auth_user_id
        ):
            login(request, authenticated_user)
            return _redirect_for_request(request, 'operations_plane:dashboard')

        form.add_error(None, 'Invalid username or password.')

    return render(request, 'operations_plane/login_page.html', {'form': form})


@operations_user_required
def dashboard_view(request):
    operations_user = request.operations_user
    widget_prefs, _ = OperationsWidgetPreference.objects.get_or_create(
        operations_user=operations_user,
    )
    searches = OperationsLogSearch.objects.filter(requested_by=operations_user)
    analyses = OperationsLogAnalysis.objects.filter(requested_by=operations_user)
    active_statuses = [
        OperationsLogSearch.Status.QUEUED,
        OperationsLogSearch.Status.RUNNING,
    ]
    active_searches = searches.filter(status__in=active_statuses).count()
    active_analyses = analyses.filter(
        status__in=[
            OperationsLogAnalysis.Status.QUEUED,
            OperationsLogAnalysis.Status.RUNNING,
        ]
    ).count()
    try:
        latest_ingestion = latest_ingestion_time()
        log_store_available = True
    except LogStoreUnavailable:
        logger.warning('Operations dashboard could not reach the ClickHouse log store.')
        latest_ingestion = None
        log_store_available = False
    return render(
        request,
        'operations_plane/dashboard_page.html',
        {
            'operations_user': operations_user,
            'widget_prefs': widget_prefs,
            'widget_options': [
                ('stats', 'Quick activity stats', widget_prefs.show_stats),
                ('log_store', 'Store connectivity', widget_prefs.show_log_store),
                ('searches', 'Historical searches', widget_prefs.show_searches),
                ('analyses', 'Recent AI analyses', widget_prefs.show_analyses),
            ],
            'stats': {
                'active_jobs': active_searches + active_analyses,
                'completed_searches': searches.filter(
                    status=OperationsLogSearch.Status.COMPLETED,
                ).count(),
                'completed_analyses': analyses.filter(
                    status=OperationsLogAnalysis.Status.COMPLETED,
                ).count(),
                'failed_jobs': (
                    searches.filter(status=OperationsLogSearch.Status.FAILED).count()
                    + analyses.filter(status=OperationsLogAnalysis.Status.FAILED).count()
                ),
            },
            'recent_searches': searches[:5],
            'recent_analyses': analyses[:5],
            'latest_ingestion': latest_ingestion,
            'log_store_available': log_store_available,
            'breadcrumbs': get_operations_breadcrumbs(request),
        },
    )


@operations_user_required
@require_POST
def update_widget_preferences(request):
    try:
        payload = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse(
            {'status': 'error', 'message': 'Invalid widget preferences.'},
            status=400,
        )

    if not isinstance(payload, dict):
        return JsonResponse(
            {'status': 'error', 'message': 'Invalid widget preferences.'},
            status=400,
        )

    widget_fields = {
        'stats': 'show_stats',
        'log_store': 'show_log_store',
        'searches': 'show_searches',
        'analyses': 'show_analyses',
    }
    widget_name = payload.get('widget')
    visible = payload.get('visible')
    if widget_name not in widget_fields or type(visible) is not bool:
        return JsonResponse(
            {'status': 'error', 'message': 'Invalid widget or visibility.'},
            status=400,
        )

    preferences, _ = OperationsWidgetPreference.objects.get_or_create(
        operations_user=request.operations_user,
    )
    field_name = widget_fields[widget_name]
    setattr(preferences, field_name, visible)
    preferences.save(update_fields=[field_name])
    return JsonResponse({'status': 'success'})


@require_POST
@operations_user_required
def logout_view(request):
    logout(request)
    return _redirect_for_request(request, 'operations_plane:login')
