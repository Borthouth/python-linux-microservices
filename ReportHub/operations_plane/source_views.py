import json
import logging
import os
from uuid import uuid4

from django import forms
from django.contrib import messages
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.shortcuts import redirect, render
from django.views.decorators.http import require_http_methods

from operations_plane.breadcrumbs import get_operations_breadcrumbs
from operations_plane.models import OperationsLogSources, OperationsUser
from operations_plane.source_status import collection_status, control_directory
from operations_plane.views import operations_user_required
from otel.source_policy import SOURCE_CHOICES, default_sources


logger = logging.getLogger(__name__)


class LogSourcesForm(forms.Form):
    enabled_sources = forms.MultipleChoiceField(
        choices=[(key, label) for key, label, _ in SOURCE_CHOICES],
        required=False,
        widget=forms.CheckboxSelectMultiple,
    )


def publish_selection(selection):
    directory = control_directory()
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / f'desired-{selection.revision}.tmp'
    try:
        temporary.write_text(json.dumps({
            'revision': str(selection.revision),
            'enabled_sources': selection.enabled_sources,
        }))
        os.replace(temporary, directory / 'desired.json')
    finally:
        temporary.unlink(missing_ok=True)


@operations_user_required
@require_http_methods(['GET', 'POST'])
def log_sources_view(request):
    if request.operations_user.role != OperationsUser.Role.ADMIN:
        raise PermissionDenied

    selection = OperationsLogSources.objects.filter(pk=1).first()
    form = LogSourcesForm(
        request.POST if request.method == 'POST' else None,
        initial={'enabled_sources': selection.enabled_sources if selection else default_sources()},
    )
    if request.method == 'POST' and form.is_valid():
        try:
            with transaction.atomic():
                selection, _ = OperationsLogSources.objects.select_for_update().get_or_create(
                    pk=1, defaults={'revision': uuid4()},
                )
                selection.enabled_sources = form.cleaned_data['enabled_sources']
                selection.revision = uuid4()
                selection.updated_by = request.user
                selection.save()
                publish_selection(selection)
        except OSError:
            logger.exception('Could not publish Operations log source selection.')
            form.add_error(None, 'Could not send choices to the Collector. Your changes were not saved; try again.')
        else:
            messages.info(request, 'Source choices saved. Waiting for the Collector to confirm application.')
            return redirect('operations_plane:log_sources')

    status = collection_status(selection)
    applied = status['applied']
    if applied and request.session.get('operations_log_sources_confirmed_revision') != str(selection.revision):
        messages.success(request, 'Collector confirmed these choices.')
        request.session['operations_log_sources_confirmed_revision'] = str(selection.revision)
    return render(request, 'operations_plane/logs/log_sources.html', {
        'operations_user': request.operations_user,
        'breadcrumbs': get_operations_breadcrumbs(request),
        'form': form,
        'source_fields': list(zip(form['enabled_sources'], SOURCE_CHOICES)),
        'applied': applied,
        'pending': status['pending'],
        'status_error': status['status_error'],
    })
