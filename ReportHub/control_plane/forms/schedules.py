from django import forms

class TaskScheduleForm(forms.Form):
    task_name = forms.CharField(
        max_length=255, 
        label="Task Schedule Name",
        widget=forms.TextInput(attrs={
            'class': 'w-full px-4 py-2.5 bg-slate-800 border border-slate-700 rounded-xl text-white text-sm focus:outline-none focus:border-indigo-500 transition-all',
            'placeholder': 'Enter schedule name...'
        })
    )
    frequency = forms.ChoiceField(
        choices=[
            ('every_minute', 'Every Minute (For Testing)'),
            ('hourly', 'Every Hour'),
            ('daily', 'Every Day at Midnight'),
        ],
        label="Run Frequency",
        widget=forms.Select(attrs={
            'class': 'w-full px-4 py-2.5 bg-slate-800 border border-slate-700 rounded-xl text-white text-sm focus:outline-none focus:border-indigo-500 transition-all'
        })
    )