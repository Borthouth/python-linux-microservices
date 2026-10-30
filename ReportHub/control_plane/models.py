from django.db import models
from django.urls import reverse
from django.conf import settings
from django.contrib.auth.models import User
from encrypted_model_fields.fields import EncryptedCharField
from simple_history.models import HistoricalRecords
from django_celery_beat.models import PeriodicTask


class TimeStampedAuditedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)
    
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        related_name="%(class)s_created"
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        related_name="%(class)s_updated"
    )

    # Automatically gives history tracking to any model inheriting from this base
    history = HistoricalRecords(inherit=True)

    class Meta:
        abstract = True  # Tells Django not to create a database table for this model

class Role(models.TextChoices):
    VIEWER = 'VIEWER', 'Viewer'
    ANALYST = 'ANALYST', 'Analyst/Builder'
    ADMIN = 'ADMIN', 'System Admin'

class DbChoice(models.TextChoices):
    MYSQL = 'mysql', 'MySQL'
    POSTGRESQL = 'postgresql', 'PostgreSQL'
    MSSQL = 'mssql', 'Microsoft SQL Server'

class SyncStatus(models.TextChoices):
    PENDING = 'PENDING', 'Pending'
    RUNNING = 'RUNNING', 'Running'
    SUCCESS = 'SUCCESS', 'Success'
    FAILED = 'FAILED', 'Failed'

class SourceProfile(TimeStampedAuditedModel):
    """Represents a remote data source (e.g., client MySQL database) to be introspected."""
    name = models.CharField(max_length=100, unique=True)

    db_type = models.CharField(
        max_length=20, 
        choices=DbChoice.choices, 
        default=DbChoice.MYSQL
    )

    host = models.CharField(max_length=255)
    port = models.IntegerField(default=3306)
    database_name = models.CharField(max_length=100)
    username = models.CharField(max_length=100)
    password = EncryptedCharField(max_length=255, blank=True, null=True)
    is_active = models.BooleanField(default=True)

    # Schema Synchronization Tracking Fields
    last_synced = models.DateTimeField(null=True, blank=True)
    last_sync_status = models.CharField(
        max_length=20,
        choices=SyncStatus.choices,
        default=SyncStatus.PENDING
    )
    last_sync_error = models.TextField(blank=True, null=True)

    def __str__(self):
        return f"{self.name} ({self.database_name})"

    def get_status_url(self):
        return reverse('control_plane:source_status', kwargs={'pk': self.pk})

class UserSourceAccess(TimeStampedAuditedModel):
    """Layer 1 Security: Maps users to specific Source Profiles with assigned roles."""
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='source_access')
    source = models.ForeignKey(SourceProfile, on_delete=models.CASCADE, related_name='user_access')
    role = models.CharField(max_length=20, choices=Role.choices, default=Role.VIEWER)
    assigned_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user', 'source')

    def __str__(self):
        return f"{self.user.username} -> {self.source.name} [{self.role}]"

    def get_audit_display_name(self):
        return f"Access: {self.user.username} &rarr; {self.source.name}"

class TrackedUser(User):
    history = HistoricalRecords()

    class Meta:
        proxy = True
        verbose_name = 'Tracked User'
        verbose_name_plural = 'Tracked Users'

class PipelineAutomationAuditLog(models.Model):
    """Standalone audit log that mimics history records for pipeline automations."""
    name = models.CharField(max_length=200)
    action_type = models.CharField(max_length=5)  # '+' for create, '~' for edit/toggle, '-' for delete
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    change_summary = models.CharField(max_length=255, blank=True)
    changes_data = models.JSONField(default=dict, blank=True)  # Stores field-level diffs

    @property
    def history_date(self):
        return self.timestamp
            
    @property
    def history_user(self):
        return self.user
                
    @property
    def history_type(self):
        return self.action_type
                
    @property
    def prev_record(self):
        return None

    def get_history_type_display(self):
        if self.action_type == '+':
            return 'Created'
        elif self.action_type == '~':
            return 'Updated'
        return 'Deleted'

class SourceTable(TimeStampedAuditedModel):
    """Stores introspected tables linked to a specific SourceProfile data source."""
    
    source = models.ForeignKey(
        SourceProfile, 
        on_delete=models.CASCADE, 
        related_name='tables'
    )
    schema_name = models.CharField(max_length=100, default='public')
    table_name = models.CharField(max_length=255)
    
    # Retention & Tiering Configuration
    enable_tiering = models.BooleanField(default=False, help_text="Enable hot/warm/cold tiering rules based on partition dates.")
    hot_retention_days = models.IntegerField(default=365, help_text="Number of days to keep data in the hot tier.")
    date_partition_field_name = models.CharField(
        max_length=255, 
        blank=True, 
        null=True, 
        help_text="Column name used for time-based partitioning and tiering."
    )
    
    last_synced_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        verbose_name = "Source Table"
        verbose_name_plural = "Source Tables"
        unique_together = ('source', 'schema_name', 'table_name')

    def __str__(self):
        return f"{self.source.name}.{self.schema_name}.{self.table_name}"

    def get_audit_display_name(self):
        return f"Table: {self.schema_name}.{self.table_name}"


class SourceField(TimeStampedAuditedModel):
    """Stores individual columns/fields for each source table, toggleable via UI."""
    
    table = models.ForeignKey(
        SourceTable, 
        on_delete=models.CASCADE, 
        related_name='fields'
    )
    field_name = models.CharField(max_length=255)
    data_type = models.CharField(max_length=100, help_text="Original data type from source database.")
    is_selected = models.BooleanField(default=True, help_text="Toggled via HTMX UI to include/exclude field from ingestion.")
    
    class Meta:
        verbose_name = "Source Field"
        verbose_name_plural = "Source Fields"
        unique_together = ('table', 'field_name')

    def __str__(self):
        status = "Included" if self.is_selected else "Excluded"
        return f"{self.field_name} ({self.data_type}) - {status}"


class SourceRelationship(TimeStampedAuditedModel):
    """Tracks foreign key relationships discovered during introspection for report joining."""
    source = models.ForeignKey(SourceProfile, on_delete=models.CASCADE, related_name='relationships')
    
    # Origin of the relationship (e.g., orders.customer_id)
    from_schema = models.CharField(max_length=100, default='public')
    from_table = models.CharField(max_length=255)
    from_field = models.CharField(max_length=255)
    
    # Destination of the relationship (e.g., customers.id)
    to_schema = models.CharField(max_length=100, default='public')
    to_table = models.CharField(max_length=255)
    to_field = models.CharField(max_length=255)
    
    constraint_name = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        verbose_name = "Source Relationship"
        verbose_name_plural = "Source Relationships"
        unique_together = ('source', 'from_table', 'from_field', 'to_table', 'to_field')

    def __str__(self):
        return f"{self.from_table}.{self.from_field} -> {self.to_table}.{self.to_field}"


class WarehouseDatasetMapping(models.Model):
    source_profile = models.ForeignKey('SourceProfile', on_delete=models.CASCADE, related_name='warehouse_mappings')
    table_name = models.CharField(max_length=255)
    field_name = models.CharField(max_length=255)
    is_enabled = models.BooleanField(default=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('source_profile', 'table_name', 'field_name')