from django import forms
from control_plane.models import SourceProfile

class SourceProfileForm(forms.ModelForm):
    DEFAULT_PORTS = {
        'mysql': 3306,
        'postgresql': 5432,
        'mssql': 1433,
        'mongodb': 27017,
        'oracle': 1521,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['port'].help_text = (
            'Default ports: MySQL 3306, PostgreSQL 5432, SQL Server 1433, '
            'MongoDB 27017, Oracle 1521.'
        )
        self.fields['database_name'].help_text = (
            'For MongoDB, enter the database name. For Oracle, enter the service name '
            '(for example, FREEPDB1).'
        )
        if not self.is_bound and not self.instance.pk:
            db_type = self.initial.get('db_type', self.instance.db_type)
            self.initial['port'] = self.DEFAULT_PORTS.get(db_type, 3306)

    class Meta:
        model = SourceProfile
        fields = ['name', 'db_type', 'host', 'port', 'database_name', 'username', 'password', 'is_active']
        widgets = {
            'name': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'db_type': forms.Select(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'host': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'port': forms.NumberInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'database_name': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'username': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'password': forms.PasswordInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}, render_value=True),
            'is_active': forms.CheckboxInput(attrs={'class': 'w-4 h-4 text-indigo-600 bg-slate-900 border-slate-700 rounded focus:ring-indigo-500'}),
        }