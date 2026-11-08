from django import forms
from control_plane.models import DbChoice, IngestionMode, SourceProfile

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
        self._original_ingestion_mode = self.instance.ingestion_mode
        self.fields['port'].help_text = (
            'Default ports: MySQL 3306, PostgreSQL 5432, SQL Server 1433, '
            'MongoDB 27017, Oracle 1521.'
        )
        self.fields['database_name'].help_text = (
            'For MongoDB, enter the database name. For Oracle, enter the service name '
            '(for example, FREEPDB1). Keep this service name for source connections; '
            'configure the CDB/PDB fields below separately for Oracle CDC.'
        )
        self.fields['oracle_cdb_name'].help_text = (
            'Oracle CDC only. Optional CDB name for Debezium; leave both CDB/PDB '
            'blank to retain the existing service-name connector configuration.'
        )
        self.fields['oracle_pdb_name'].help_text = (
            'Oracle CDC only. PDB captured by Debezium; configure together with '
            'the CDB name.'
        )
        self.fields['oracle_logminer_username'].help_text = (
            'Oracle CDC only. Optional LogMiner account; when blank, the source '
            'username is used.'
        )
        self.fields['oracle_logminer_password'].help_text = (
            'Oracle CDC only. Optional LogMiner password; when blank, the source '
            'password is used. Leave blank when editing to keep the saved value.'
        )
        if not self.is_bound and not self.instance.pk:
            db_type = self.initial.get('db_type', self.instance.db_type)
            self.initial['port'] = self.DEFAULT_PORTS.get(db_type, 3306)

    def clean(self):
        cleaned_data = super().clean()
        if cleaned_data.get("db_type") == DbChoice.ORACLE:
            cdb_name = cleaned_data.get("oracle_cdb_name")
            pdb_name = cleaned_data.get("oracle_pdb_name")
            if bool(cdb_name) != bool(pdb_name):
                message = "Configure both the Oracle CDB and PDB names, or leave both blank."
                self.add_error("oracle_cdb_name", message)
                self.add_error("oracle_pdb_name", message)

            logminer_username = cleaned_data.get("oracle_logminer_username")
            logminer_password = cleaned_data.get("oracle_logminer_password")
            if bool(logminer_username) != bool(logminer_password):
                message = (
                    "Configure both LogMiner credentials, or leave both blank "
                    "to use the source credentials."
                )
                self.add_error("oracle_logminer_username", message)
                self.add_error("oracle_logminer_password", message)

        mode = cleaned_data.get("ingestion_mode")
        if mode == IngestionMode.CDC and cleaned_data.get("db_type") == DbChoice.MONGODB:
            self.add_error(
                "ingestion_mode",
                "MongoDB CDC ingestion is not supported yet; use batch refresh.",
            )
        if self.instance.pk and mode and mode != self._original_ingestion_mode:
            self.add_error(
                "ingestion_mode",
                "Changing ingestion mode is not available until coordinated "
                "CDC snapshot and cutover are implemented.",
            )
        return cleaned_data

    class Meta:
        model = SourceProfile
        fields = [
            'name', 'db_type', 'host', 'port', 'database_name',
            'oracle_cdb_name', 'oracle_pdb_name',
            'oracle_logminer_username', 'oracle_logminer_password',
            'username', 'password', 'ingestion_mode', 'is_active',
        ]
        widgets = {
            'name': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'db_type': forms.Select(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'host': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'port': forms.NumberInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'database_name': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'oracle_cdb_name': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm', 'data-oracle-config': 'true'}),
            'oracle_pdb_name': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm', 'data-oracle-config': 'true'}),
            'oracle_logminer_username': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm', 'data-oracle-config': 'true'}),
            'oracle_logminer_password': forms.PasswordInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm', 'data-oracle-config': 'true'}, render_value=False),
            'username': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'password': forms.PasswordInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}, render_value=True),
            'ingestion_mode': forms.Select(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'is_active': forms.CheckboxInput(attrs={'class': 'w-4 h-4 text-indigo-600 bg-slate-900 border-slate-700 rounded focus:ring-indigo-500'}),
        }

    def clean_oracle_logminer_password(self):
        password = self.cleaned_data.get("oracle_logminer_password")
        if not password and self.instance.pk:
            return self.instance.oracle_logminer_password
        return password