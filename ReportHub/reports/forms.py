import json
from django import forms
from .models import ReportDefinition
from control_plane.models import SourceProfile, SourceTable

class ReportDefinitionForm(forms.ModelForm):
    # Declare hidden field so it is properly captured in cleaned_data
    selected_fields_json = forms.CharField(
        required=False,
        widget=forms.HiddenInput()
    )
    
    filter_config_input = forms.CharField(
        widget=forms.Textarea(attrs={
            'rows': 3,
            'placeholder': '{"status": "active"}',
            'class': 'bg-gray-900 text-white font-mono placeholder-gray-500 shadow-sm focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-700 rounded-md px-3 py-2 border'
        }),
        required=False,
        label="Filter Rules (JSON)"
    )

    class Meta:
        model = ReportDefinition
        fields = ['name', 'description', 'source', 'base_table', 'selected_fields_json', 'filter_config_input', 'export_format', 'is_active']
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
                'class': 'bg-gray-900 text-white shadow-sm focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-700 rounded-md px-3 py-2 border'
            }),
            'is_active': forms.CheckboxInput(attrs={
                'class': 'h-4 w-4 text-indigo-600 focus:ring-indigo-500 border-gray-700 rounded bg-gray-900'
            }),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Pre-populate JSON inputs if instance exists
        if self.instance and self.instance.pk:
            if self.instance.selected_fields:
                self.initial['selected_fields_json'] = json.dumps(self.instance.selected_fields)
            if self.instance.filter_config:
                self.initial['filter_config_input'] = json.dumps(self.instance.filter_config, indent=2)

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

        # 2. Validate filter_config
        filter_str = cleaned_data.get('filter_config_input')
        if filter_str:
            try:
                parsed_filter = json.loads(filter_str)
                if not isinstance(parsed_filter, dict):
                    raise forms.ValidationError("Filter rules must be a JSON dictionary object.")
                cleaned_data['filter_config'] = parsed_filter
            except json.JSONDecodeError as e:
                raise forms.ValidationError({'filter_config_input': f'Invalid JSON format: {e}'})
        else:
            cleaned_data['filter_config'] = {}

        return cleaned_data

    def save(self, commit=True):
        instance = super().save(commit=False)
        instance.selected_fields = self.cleaned_data.get('selected_fields', [])
        instance.filter_config = self.cleaned_data.get('filter_config', {})
        if commit:
            instance.save()
        return instance