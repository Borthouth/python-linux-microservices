import json
from django import forms
from django.db.models import Q
from .models import ReportDefinition
from control_plane.models import SourceProfile, SourceTable, TransferDestination


class DestinationChoiceField(forms.ModelMultipleChoiceField):
    def label_from_instance(self, obj):
        return f"{obj.name} (SFTP{'' if obj.is_active else ', inactive'})"

class ReportDefinitionForm(forms.ModelForm):
    transfer_destinations = DestinationChoiceField(
        queryset=TransferDestination.objects.none(), required=False,
        widget=forms.CheckboxSelectMultiple(attrs={
            "class": "w-4 h-4 rounded border-slate-700 bg-slate-950 text-indigo-600 focus:ring-indigo-500"
        }),
    )
    # Declare hidden field so it is properly captured in cleaned_data
    selected_fields_json = forms.CharField(
        required=False,
        widget=forms.HiddenInput()
    )
    include_ai_summary = forms.BooleanField(
        required=False,
        label="Include AI summary",
        widget=forms.CheckboxInput(attrs={
            'class': 'mt-0.5 rounded bg-slate-900 border-slate-700 text-indigo-600 focus:ring-0'
        }),
    )

    class Meta:
        model = ReportDefinition
        fields = [
            'name', 'description', 'source', 'base_table', 
            'selected_fields_json',
            'export_format', 'include_ai_summary', 'email_recipients',
            'is_email_active', 'is_file_delivery_active', 'transfer_destinations', 'is_active'
        ]
        widgets = {
            'name': forms.TextInput(attrs={
                'class': 'bg-gray-900 text-white placeholder-gray-500 shadow-sm focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-700 rounded-md px-3 py-2 border'
            }),
            'description': forms.Textarea(attrs={
                'rows': 2,
                'class': 'bg-gray-900 text-white placeholder-gray-500 shadow-sm focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-700 rounded-md px-3 py-2 border'
            }),
            'source': forms.Select(attrs={
                'id': 'id_source',
                'class': 'bg-gray-900 text-white shadow-sm focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-700 rounded-md px-3 py-2 border'
            }),
            'base_table': forms.Select(attrs={
                'id': 'id_base_table',
                'class': 'bg-gray-900 text-white shadow-sm focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-700 rounded-md px-3 py-2 border'
            }),
            'export_format': forms.Select(attrs={
                'id': 'id_export_format',
                'class': 'bg-slate-900 text-white block w-full border border-slate-800 rounded-lg px-2.5 py-1.5 text-xs focus:outline-none focus:border-indigo-500'
            }),
            'email_recipients': forms.Textarea(attrs={
                'rows': 2,
                'placeholder': 'admin@example.com, reporting-team@example.com',
                'class': 'w-full bg-slate-950 border border-slate-800 text-white px-4 py-2.5 rounded-xl text-sm focus:outline-none focus:border-indigo-500'
            }),
            'is_email_active': forms.CheckboxInput(attrs={
                'class': 'w-4 h-4 text-indigo-600 bg-slate-950 border-slate-700 rounded focus:ring-indigo-500'
            }),
            'is_file_delivery_active': forms.CheckboxInput(attrs={
                'class': 'w-4 h-4 text-indigo-600 bg-slate-950 border-slate-700 rounded focus:ring-indigo-500'
            }),
            'is_active': forms.CheckboxInput(attrs={
                'class': 'h-4 w-4 text-indigo-600 focus:ring-indigo-500 border-gray-700 rounded bg-gray-900'
            }),
        }

    def __init__(self, *args, **kwargs):
        self.user = kwargs.pop("user", None)
        super().__init__(*args, **kwargs)
        destinations = Q(is_active=True)
        if not self.instance._state.adding:
            destinations |= Q(reports=self.instance)
        self.fields["transfer_destinations"].queryset = TransferDestination.objects.filter(
            destinations
        ).distinct().defer("password", "private_key", "private_key_passphrase")
        if self.user is not None and not (self.user.is_active and self.user.is_staff):
            self.fields["transfer_destinations"].queryset = TransferDestination.objects.none()
        
        # Pre-populate JSON inputs if instance exists
        if self.instance and self.instance.pk:
            if self.instance.selected_fields:
                self.initial['selected_fields_json'] = json.dumps(self.instance.selected_fields)
            config = self.instance.config if isinstance(self.instance.config, dict) else {}
            self.initial['include_ai_summary'] = bool(config.get('includeAiSummary', False))

        # Handle dependent dropdown filtering for base_table
        source_id = None
        if 'source' in self.data:
            try:
                source_id = int(self.data.get('source'))
            except (ValueError, TypeError):
                pass
        elif self.instance and self.instance.pk:
            try:
                if self.instance.source_id:
                    source_id = self.instance.source_id
            except Exception:
                pass

        if source_id:
            self.fields['base_table'].queryset = SourceTable.objects.filter(source_id=source_id)
        else:
            self.fields['base_table'].queryset = SourceTable.objects.none()

    def clean(self):
        cleaned_data = super().clean()
        if cleaned_data.get("is_file_delivery_active"):
            if self.user is not None and not (self.user.is_active and self.user.is_staff):
                self.add_error("is_file_delivery_active", "Only staff can configure file delivery.")
            destinations = cleaned_data.get("transfer_destinations")
            if not destinations:
                self.add_error("transfer_destinations", "Select at least one active transfer destination.")
            elif any(not destination.is_active for destination in destinations):
                self.add_error("transfer_destinations", "Remove inactive destinations or activate them before enabling delivery.")
        
        # 1. Validate selected_fields from hidden JSON input
        fields_input = cleaned_data.get('selected_fields_json')
        if fields_input:
            try:
                parsed = json.loads(fields_input)
                if not isinstance(parsed, list):
                    raise forms.ValidationError("Selected fields must be a list of column names.")
                cleaned_data['selected_fields'] = [str(col) for col in parsed]
            except json.JSONDecodeError as e:
                raise forms.ValidationError({'selected_fields_json': f'Invalid JSON format: {e}'})
        else:
            cleaned_data['selected_fields'] = []

        return cleaned_data

    def save(self, commit=True):
        instance = super().save(commit=False)
        instance.selected_fields = self.cleaned_data.get('selected_fields', [])
        if commit:
            instance.save()
            self.save_m2m()
        return instance