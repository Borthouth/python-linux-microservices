from datetime import timedelta

from django import forms
from django.utils import timezone
from otel.source_policy import SOURCE_CATEGORIES
from operations_plane.models import OperationsLogSearch


LOG_SEARCH_WINDOW = timedelta(hours=24)
LOG_INPUT_CLASS = (
    'w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2.5 '
    'text-sm text-slate-100 focus:border-indigo-500 focus:outline-none '
    'focus:ring-4 focus:ring-indigo-500/20'
)


class OperationsLoginForm(forms.Form):
    username = forms.CharField(
        max_length=150,
        strip=True,
        widget=forms.TextInput(attrs={
            'class': (
                'w-full rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 '
                'text-sm text-slate-100 placeholder:text-slate-500 '
                'focus:border-indigo-500 focus:outline-none focus:ring-4 '
                'focus:ring-indigo-500/20'
            ),
            'autocomplete': 'username',
            'autofocus': True,
            'placeholder': 'Operations username',
        }),
    )
    password = forms.CharField(
        strip=False,
        widget=forms.PasswordInput(attrs={
            'class': (
                'w-full rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 '
                'text-sm text-slate-100 placeholder:text-slate-500 '
                'focus:border-indigo-500 focus:outline-none focus:ring-4 '
                'focus:ring-indigo-500/20'
            ),
            'autocomplete': 'current-password',
            'placeholder': 'Password',
        }),
    )


class OperationsLogSearchForm(forms.Form):
    source = forms.ChoiceField(
        required=False,
        choices=[('', 'All sources'), *SOURCE_CATEGORIES],
        widget=forms.Select(attrs={'class': LOG_INPUT_CLASS}),
    )
    range_start = forms.DateTimeField(
        input_formats=['%Y-%m-%dT%H:%M'],
        widget=forms.DateTimeInput(
            attrs={'type': 'datetime-local', 'class': LOG_INPUT_CLASS},
            format='%Y-%m-%dT%H:%M',
        ),
    )
    range_end = forms.DateTimeField(
        input_formats=['%Y-%m-%dT%H:%M'],
        widget=forms.DateTimeInput(
            attrs={'type': 'datetime-local', 'class': LOG_INPUT_CLASS},
            format='%Y-%m-%dT%H:%M',
        ),
    )
    service = forms.CharField(
        max_length=100,
        required=False,
        strip=True,
        widget=forms.TextInput(attrs={'class': LOG_INPUT_CLASS}),
    )
    severity = forms.ChoiceField(
        required=False,
        widget=forms.Select(attrs={'class': LOG_INPUT_CLASS}),
        choices=[
            ('', 'All levels'),
            ('DEBUG', 'Debug'),
            ('INFO', 'Info'),
            ('WARNING', 'Warning'),
            ('ERROR', 'Error'),
            ('CRITICAL', 'Critical'),
        ],
    )
    queue = forms.CharField(
        max_length=100,
        required=False,
        strip=True,
        widget=forms.TextInput(attrs={'class': LOG_INPUT_CLASS}),
    )
    task_name = forms.CharField(
        max_length=255,
        required=False,
        strip=True,
        widget=forms.TextInput(attrs={'class': LOG_INPUT_CLASS}),
    )
    task_id = forms.CharField(
        max_length=255,
        required=False,
        strip=True,
        widget=forms.TextInput(attrs={'class': LOG_INPUT_CLASS}),
    )
    worker = forms.CharField(
        max_length=255,
        required=False,
        strip=True,
        widget=forms.TextInput(attrs={'class': LOG_INPUT_CLASS}),
    )
    text = forms.CharField(
        max_length=200,
        required=False,
        strip=True,
        widget=forms.TextInput(attrs={'class': LOG_INPUT_CLASS}),
    )

    def __init__(self, *args, **kwargs):
        data = args[0] if args else kwargs.get('data')
        if data is None and 'initial' not in kwargs:
            now = timezone.localtime().replace(second=0, microsecond=0)
            kwargs['initial'] = {
                'range_start': now - LOG_SEARCH_WINDOW,
                'range_end': now,
            }
        super().__init__(*args, **kwargs)

    def clean(self):
        cleaned_data = super().clean()
        range_start = cleaned_data.get('range_start')
        range_end = cleaned_data.get('range_end')
        now = timezone.now()
        if range_start and range_end:
            if range_end <= range_start:
                self.add_error('range_end', 'End time must be after the start time.')
            if range_end > now:
                self.add_error('range_end', 'End time cannot be in the future.')
        return cleaned_data

    def filters(self):
        return {
            'range_start': self.cleaned_data['range_start'],
            'range_end': self.cleaned_data['range_end'],
            'service': self.cleaned_data['service'],
            'source': self.cleaned_data['source'],
            'severity': self.cleaned_data['severity'],
            'queue': self.cleaned_data['queue'],
            'task_name': self.cleaned_data['task_name'],
            'task_id': self.cleaned_data['task_id'],
            'worker': self.cleaned_data['worker'],
            'text': self.cleaned_data['text'],
        }

    def initial_filters(self):
        return {
            field_name: self.get_initial_for_field(self.fields[field_name], field_name)
            for field_name in (
                'range_start',
                'range_end',
                'service',
                'source',
                'severity',
                'queue',
                'task_name',
                'task_id',
                'worker',
                'text',
            )
        }

    def is_within_recent_window(self):
        return self.cleaned_data['range_start'] >= timezone.now() - LOG_SEARCH_WINDOW


class OperationsLogCursorForm(forms.Form):
    event_time = forms.DateTimeField(
        input_formats=['%Y-%m-%dT%H:%M:%S.%f%z', '%Y-%m-%dT%H:%M:%S%z'],
    )
    event_id = forms.UUIDField()
    direction = forms.ChoiceField(required=False, choices=[('older', 'Older'), ('newer', 'Newer')])


class OperationsSearchHistoryForm(forms.Form):
    q = forms.CharField(required=False, max_length=200)
    status = forms.ChoiceField(required=False, choices=[('', 'All statuses'), *OperationsLogSearch.Status.choices])
    page = forms.IntegerField(required=False, min_value=1)


class OperationsLogAnalysisForm(forms.Form):
    search_id = forms.IntegerField(min_value=1, max_value=9223372036854775807)


class OperationsSavedResultsForm(forms.Form):
    q = forms.CharField(
        label='Search saved events', required=False, max_length=200,
        widget=forms.TextInput(attrs={
            'class': LOG_INPUT_CLASS,
            'placeholder': 'Messages, services, tasks or other saved values...',
        }),
    )
    severity = forms.ChoiceField(
        label='Level', required=False,
        widget=forms.Select(attrs={'class': LOG_INPUT_CLASS}),
    )
    source = forms.ChoiceField(
        required=False, widget=forms.Select(attrs={'class': LOG_INPUT_CLASS}),
    )
    page = forms.IntegerField(required=False, min_value=1)

    def __init__(self, *args, rows, **kwargs):
        super().__init__(*args, **kwargs)
        severities = sorted({row['severity'] for row in rows if row.get('severity')})
        sources = sorted({row.get('source_category') or row.get('source') for row in rows} - {None, ''})
        labels = dict(SOURCE_CATEGORIES)
        self.fields['severity'].choices = [('', 'All levels'), *((value, value) for value in severities)]
        self.fields['source'].choices = [
            ('', 'All sources'), *((value, labels.get(value, value)) for value in sources),
        ]


class OperationsDeleteSearchForm(forms.Form):
    confirm = forms.BooleanField()


class OperationsLiveLogForm(forms.Form):
    source = OperationsLogSearchForm.base_fields['source']
    service = OperationsLogSearchForm.base_fields['service']
    severity = OperationsLogSearchForm.base_fields['severity']
    text = OperationsLogSearchForm.base_fields['text']
    limit = forms.TypedChoiceField(
        choices=[(25, '25 entries'), (50, '50 entries'), (100, '100 entries')],
        coerce=int, initial=25, required=False, empty_value=25,
        widget=forms.Select(attrs={'class': LOG_INPUT_CLASS}),
    )
    since = forms.DateTimeField(required=False, widget=forms.HiddenInput())
