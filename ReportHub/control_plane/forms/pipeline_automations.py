from django import forms
import re

# Simple regex for standard 5-part cron expressions (minute, hour, day of month, month, day of week)
CRON_REGEX = r"^(\*|([0-9]|[1-5][0-9])) (\*|([0-9]|1[0-9]|2[0-3])) (\*|([1-9]|[1-2][0-9]|3[0-1])) (\*|([1-9]|1[0-2])) (\*|([0-6]))$"
# Or a more relaxed general cron pattern if you want to support step values like */5:
RELAXED_CRON_REGEX = r"^(\S+\s+){4}\S+$"

class PipelineAutomationForm(forms.Form):
    task_name = forms.CharField(
        max_length=255,
        widget=forms.TextInput(attrs={
            'class': 'w-full bg-slate-900 border border-slate-800 text-white px-4 py-2 rounded-xl focus:outline-none focus:border-indigo-500 text-sm',
            'placeholder': 'e.g., Nightly Warehouse Cleanup'
        }),
        label="Automation Name"
    )
    
    task_path = forms.ChoiceField(
        choices=[
            ('etl.tasks.cleanup_postgres_warehouse_task', 'Cleanup PostgreSQL Warehouse'),
            ('etl.tasks.run_batch_ingestion_task', 'Run Batch Ingestion Task'),
            ('etl.tasks.sync_datasource_schemas_mock_task', 'Sync Data Source Schemas (Mock)'),
        ],
        widget=forms.Select(attrs={
            'class': 'w-full bg-slate-900 border border-slate-800 text-white px-4 py-2 rounded-xl focus:outline-none focus:border-indigo-500 text-sm'
        }),
        label="Target Celery Task"
    )

    cron_expression = forms.CharField(
        max_length=64,
        help_text="Format: Minute Hour Day(Month) Month Day(Week) (e.g., 0 0 * * *)",
        widget=forms.TextInput(attrs={
            'class': 'w-full bg-slate-900 border border-slate-800 text-white px-4 py-2 rounded-xl focus:outline-none focus:border-indigo-500 text-sm font-mono',
            'placeholder': '0 0 * * *'
        }),
        label="Cron Schedule / Timing"
    )

    def clean_cron_expression(self):
        cron_expr = self.cleaned_data.get('cron_expression', '').strip()
        parts = cron_expr.split()
        if len(parts) != 5:
            raise forms.ValidationError("A valid cron expression must consist of exactly 5 parts (e.g., * * * * *).")
        return cron_expr