import uuid

from django.db import models
from django.db.models import Q
from django.urls import reverse
from django.conf import settings
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.validators import MaxLengthValidator, MaxValueValidator, MinValueValidator
from encrypted_model_fields.fields import EncryptedCharField, EncryptedTextField
from simple_history.models import HistoricalRecords
from django_celery_beat.models import PeriodicTask

from control_plane.transfer_validation import (
    validate_host_key_sha256,
    validate_remote_directory,
    validate_ssh_private_key,
    validate_transfer_host,
)


class TransferDestination(models.Model):
    class TestStatus(models.TextChoices):
        NOT_TESTED = "", "Not tested"
        QUEUED = "QUEUED", "Queued"
        RUNNING = "RUNNING", "Testing"
        SUCCESS = "SUCCESS", "Passed"
        FAILED = "FAILED", "Failed"

    class Authentication(models.TextChoices):
        PASSWORD = "password", "Password"
        PRIVATE_KEY = "private_key", "SSH private key"

    name = models.CharField(max_length=100, unique=True)
    protocol = models.CharField(max_length=10, choices=[("sftp", "SFTP")], default="sftp")
    host = models.CharField(max_length=253, validators=[validate_transfer_host])
    port = models.PositiveIntegerField(
        default=22, validators=[MinValueValidator(1), MaxValueValidator(65535)]
    )
    username = models.CharField(max_length=100)
    authentication = models.CharField(
        max_length=20, choices=Authentication.choices, default=Authentication.PASSWORD
    )
    password = EncryptedCharField(max_length=1024, blank=True, default="")
    private_key = EncryptedTextField(
        blank=True, default="", validators=[MaxLengthValidator(32768)]
    )
    private_key_passphrase = EncryptedCharField(max_length=1024, blank=True, default="")
    host_key_sha256 = models.CharField(
        "Server host-key fingerprint",
        max_length=50,
        validators=[validate_host_key_sha256],
        help_text="Obtain the SHA256 fingerprint from the server administrator over a trusted channel.",
    )
    remote_directory = models.CharField(
        max_length=1024, default="/", validators=[validate_remote_directory]
    )
    is_active = models.BooleanField(
        "Active for report transfers",
        default=False,
        help_text="Allow reports with file delivery enabled to transfer files to this destination.",
    )
    last_tested_at = models.DateTimeField(null=True, blank=True, editable=False)
    last_test_status = models.CharField(
        max_length=10, choices=TestStatus.choices, default="", blank=True, editable=False,
    )
    last_test_error = models.TextField(blank=True, default="", editable=False)
    last_test_task_id = models.CharField(max_length=36, blank=True, default="", editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True,
        related_name="transfer_destinations_created", editable=False,
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True,
        related_name="transfer_destinations_updated", editable=False,
    )
    history = HistoricalRecords(
        excluded_fields=[
            "password", "private_key", "private_key_passphrase",
            "last_tested_at", "last_test_status", "last_test_error", "last_test_task_id",
        ]
    )

    class Meta:
        ordering = ["name"]

    def __str__(self):
        return self.name

    def clean(self):
        super().clean()
        if self.authentication == self.Authentication.PASSWORD:
            if not self.password:
                raise ValidationError({"password": "A password is required for password authentication."})
            if self.private_key or self.private_key_passphrase:
                raise ValidationError({"authentication": "Password authentication cannot include an SSH key or passphrase."})
        elif self.authentication == self.Authentication.PRIVATE_KEY:
            if self.password:
                raise ValidationError({"password": "Use only an SSH key and optional passphrase for key authentication."})
            if not self.private_key:
                raise ValidationError({"private_key": "An SSH private key is required."})
            try:
                validate_ssh_private_key(self.private_key, self.private_key_passphrase)
            except ValidationError as error:
                raise ValidationError({"private_key": error.messages}) from error


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
    MONGODB = 'mongodb', 'MongoDB'
    ORACLE = 'oracle', 'Oracle'


class IngestionMode(models.TextChoices):
    BATCH = 'batch', 'Batch refresh'
    CDC = 'cdc', 'Change data capture (CDC)'


class SyncStatus(models.TextChoices):
    PENDING = 'PENDING', 'Pending'
    RUNNING = 'RUNNING', 'Running'
    SUCCESS = 'SUCCESS', 'Success'
    FAILED = 'FAILED', 'Failed'
    CANCELLED = 'CANCELLED', 'Cancelled'

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
    oracle_cdb_name = models.CharField(max_length=100, blank=True, default="")
    oracle_pdb_name = models.CharField(max_length=100, blank=True, default="")
    oracle_logminer_username = models.CharField(
        max_length=100, blank=True, default=""
    )
    oracle_logminer_password = EncryptedCharField(
        max_length=255, blank=True, null=True
    )
    username = models.CharField(max_length=100)
    password = EncryptedCharField(max_length=255, blank=True, null=True)
    is_active = models.BooleanField(default=True)
    ingestion_mode = models.CharField(
        max_length=10,
        choices=IngestionMode.choices,
        default=IngestionMode.BATCH,
        help_text=(
            "Batch refresh does not require Kafka. CDC requires Kafka, "
            "Debezium and source database CDC configuration."
        ),
    )

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


class CdcExecutionFailure(models.Model):
    source = models.ForeignKey(
        SourceProfile, on_delete=models.CASCADE, related_name="cdc_failures"
    )
    task_id = models.CharField(max_length=255, blank=True)
    failed_at = models.DateTimeField(auto_now_add=True, db_index=True)
    error_message = models.TextField()


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
    incremental_field_name = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Date/time column used to detect new and updated source rows.",
    )
    incremental_watermark = models.TextField(blank=True, null=True)
    primary_key_fields = models.JSONField(default=list, blank=True)
    incremental_ready = models.BooleanField(default=False)
    clickhouse_uses_replacing_merge_tree = models.BooleanField(default=False)
    clickhouse_physical_table = models.CharField(max_length=255, blank=True, default="", editable=False)
    cdc_managed = models.BooleanField(default=False)
    cdc_connector_name = models.CharField(max_length=255, blank=True, default="")
    cdc_topic_prefix = models.CharField(max_length=255, blank=True, default="")
    
    last_synced_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        verbose_name = "Source Table"
        verbose_name_plural = "Source Tables"
        unique_together = ('source', 'schema_name', 'table_name')

    def __str__(self):
        return f"{self.source.name}.{self.schema_name}.{self.table_name}"

    def get_audit_display_name(self):
        return f"Table: {self.schema_name}.{self.table_name}"


class CdcTableRebuild(models.Model):
    class Status(models.TextChoices):
        SNAPSHOTTING = "SNAPSHOTTING", "Snapshotting"
        CUTOVER_PENDING = "CUTOVER_PENDING", "Cutover pending"
        SUCCEEDED = "SUCCEEDED", "Succeeded"
        FAILED = "FAILED", "Failed"

    source_table = models.ForeignKey(
        SourceTable,
        on_delete=models.CASCADE,
        related_name="cdc_rebuilds",
    )
    generation = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    connector_name = models.CharField(max_length=255, unique=True)
    topic_prefix = models.CharField(max_length=255, unique=True)
    notification_topic = models.CharField(max_length=255, unique=True)
    consumer_group = models.CharField(max_length=255, unique=True)
    staging_table = models.CharField(max_length=255, unique=True)
    previous_connector_name = models.CharField(max_length=255, blank=True, default="")
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.SNAPSHOTTING,
    )
    snapshot_end_offsets = models.JSONField(default=dict, blank=True)
    staging_uuid = models.CharField(max_length=64, blank=True, default="")
    active_uuid_before = models.CharField(max_length=64, blank=True, default="")
    active_table_checked = models.BooleanField(default=False)
    last_error = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["source_table"],
                condition=Q(
                    status__in=["SNAPSHOTTING", "CUTOVER_PENDING"]
                ),
                name="uniq_active_cdc_table_rebuild",
            ),
        ]


class CdcResourceCleanup(TimeStampedAuditedModel):
    class Status(models.TextChoices):
        SCANNING = "SCANNING", "Scanning"
        AWAITING_CONFIRMATION = "AWAITING_CONFIRMATION", "Awaiting confirmation"
        QUEUED = "QUEUED", "Queued"
        RUNNING = "RUNNING", "Running"
        COMPLETED = "COMPLETED", "Completed"
        FAILED = "FAILED", "Failed"

    rebuild = models.ForeignKey(
        CdcTableRebuild,
        on_delete=models.CASCADE,
        related_name="resource_cleanups",
    )
    status = models.CharField(max_length=32, choices=Status.choices)
    task_id = models.CharField(max_length=255, blank=True, default="")
    last_error = models.TextField(blank=True, default="")
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["rebuild"],
                condition=Q(
                    status__in=[
                        "SCANNING",
                        "AWAITING_CONFIRMATION",
                        "QUEUED",
                        "RUNNING",
                    ]
                ),
                name="uniq_active_cdc_resource_cleanup",
            ),
        ]


class CdcResourceCleanupItem(TimeStampedAuditedModel):
    class ResourceType(models.TextChoices):
        CONNECTOR = "CONNECTOR", "Debezium connector"
        POSTGRES_SLOT = "POSTGRES_SLOT", "PostgreSQL replication slot"
        POSTGRES_PUBLICATION = "POSTGRES_PUBLICATION", "PostgreSQL publication"
        KAFKA_TOPIC = "KAFKA_TOPIC", "Kafka topic"
        KAFKA_GROUP = "KAFKA_GROUP", "Kafka consumer group"
        CLICKHOUSE_TABLE = "CLICKHOUSE_TABLE", "ClickHouse table"

    class Status(models.TextChoices):
        PENDING = "PENDING", "Pending"
        RUNNING = "RUNNING", "Running"
        COMPLETED = "COMPLETED", "Completed"
        FAILED = "FAILED", "Failed"

    cleanup = models.ForeignKey(
        CdcResourceCleanup,
        on_delete=models.CASCADE,
        related_name="items",
    )
    resource_type = models.CharField(max_length=32, choices=ResourceType.choices)
    resource_name = models.CharField(max_length=512)
    exists_at_scan = models.BooleanField(null=True, blank=True)
    scan_error = models.TextField(blank=True, default="")
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.PENDING,
    )
    outcome = models.CharField(max_length=255, blank=True, default="")
    last_error = models.TextField(blank=True, default="")
    attempts = models.PositiveIntegerField(default=0)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["cleanup", "resource_type", "resource_name"],
                name="uniq_cdc_cleanup_resource",
            ),
        ]


class CdcSourceExternalOperation(models.Model):
    class Kind(models.TextChoices):
        INGESTION = "INGESTION", "CDC ingestion"
        REFRESH = "REFRESH", "CDC-mode refresh"
        REBUILD = "REBUILD", "CDC table rebuild"
        REPORT = "REPORT", "Report query"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    source = models.ForeignKey(
        SourceProfile, on_delete=models.PROTECT, related_name="cdc_external_operations"
    )
    kind = models.CharField(max_length=20, choices=Kind.choices)
    task_id = models.CharField(max_length=255, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)


class CdcToBatchTransition(models.Model):
    class Status(models.TextChoices):
        PREPARING = "PREPARING", "Preparing"
        CUTOVER_PENDING = "CUTOVER_PENDING", "Cutover pending"
        SUCCEEDED = "SUCCEEDED", "Succeeded"
        FAILED = "FAILED", "Failed"

    source = models.ForeignKey(
        SourceProfile,
        on_delete=models.CASCADE,
        related_name="cdc_to_batch_transitions",
    )
    generation = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PREPARING,
    )
    last_error = models.TextField(blank=True, default="")
    staging_verified_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["source"],
                condition=Q(status__in=["PREPARING", "CUTOVER_PENDING"]),
                name="uniq_active_cdc_batch_transition",
            ),
        ]


class CdcToBatchTableCheckpoint(models.Model):
    class Status(models.TextChoices):
        PENDING = "PENDING", "Pending"
        STAGED = "STAGED", "Staged"
        CUTOVER = "CUTOVER", "Cutover complete"

    transition = models.ForeignKey(
        CdcToBatchTransition,
        on_delete=models.CASCADE,
        related_name="table_checkpoints",
    )
    source_table = models.ForeignKey(
        SourceTable,
        on_delete=models.CASCADE,
        related_name="cdc_to_batch_checkpoints",
    )
    staging_table = models.CharField(max_length=255)
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
    )
    active_uuid_before = models.CharField(max_length=64, blank=True, default="")
    active_table_checked = models.BooleanField(default=False)
    staging_uuid = models.CharField(max_length=64, blank=True, default="")
    staging_schema = models.JSONField(default=dict, blank=True)
    staging_row_count = models.PositiveBigIntegerField(null=True, blank=True)
    cutover_command_inflight = models.BooleanField(default=False)
    generation_table = models.CharField(max_length=255, blank=True, default="")
    generation_uuid = models.CharField(max_length=64, blank=True, default="")
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["transition", "source_table"],
                name="uniq_cdc_batch_table_checkpoint",
            ),
        ]


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


class WarehouseDatasetMapping(TimeStampedAuditedModel):
    source_profile = models.ForeignKey('SourceProfile', on_delete=models.CASCADE, related_name='warehouse_mappings')
    table_name = models.CharField(max_length=255)
    field_name = models.CharField(max_length=255)
    is_enabled = models.BooleanField(default=True)

    class Meta:
        unique_together = ('source_profile', 'table_name', 'field_name')


class BackgroundJob(TimeStampedAuditedModel):
    class Status(models.TextChoices):
        PENDING = 'PENDING', 'Pending'
        RUNNING = 'RUNNING', 'Running'
        COMPLETED = 'COMPLETED', 'Completed'
        FAILED = 'FAILED', 'Failed'
        CANCELLED = 'CANCELLED', 'Cancelled'

    task_id = models.CharField(max_length=255, unique=True)
    name = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    progress_percentage = models.PositiveIntegerField(default=0)  # 0 to 100
    is_cancelled = models.BooleanField(default=False)


class UserWidgetPreference(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='widget_preferences')
    show_stats = models.BooleanField(default=True)
    show_pipelines = models.BooleanField(default=True)
    show_reports = models.BooleanField(default=True)
    show_history = models.BooleanField(default=True)
    show_dlq = models.BooleanField(default=True)
    show_cdc_failures = models.BooleanField(default=True)
    show_email_audit = models.BooleanField(default=True)
    show_file_deliveries = models.BooleanField(default=True)

    def __str__(self):
        return f"Widget Preferences for {self.user.username}"