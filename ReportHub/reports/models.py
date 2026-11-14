import uuid
from django.db import models
from django.conf import settings
from django.core.exceptions import ValidationError
from django_celery_beat.models import PeriodicTask
from simple_history.models import HistoricalRecords
from control_plane.models import SourceProfile, SourceTable, SourceField
from .validators import validate_comma_separated_emails

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

    history = HistoricalRecords(inherit=True)

    class Meta:
        abstract = True

class ReportDefinition(TimeStampedAuditedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255, unique=True)
    description = models.TextField(blank=True, null=True)
    
    # Target Data Source & Base Table
    source = models.ForeignKey(SourceProfile, on_delete=models.CASCADE, related_name='reports')
    base_table = models.ForeignKey(SourceTable, on_delete=models.CASCADE, related_name='primary_reports')
    
    # Query configuration
    selected_fields = models.JSONField(default=list)
    join_config = models.JSONField(default=list, blank=True)
    filter_config = models.JSONField(default=dict, blank=True)
    config = models.JSONField(default=dict, blank=True)
    
    # Export format selection
    export_format = models.CharField(
        max_length=10,
        choices=[
            ('csv', 'CSV'),
            ('xlsx', 'Excel (XLSX)'),
            ('json', 'JSON'),
            ('pdf', 'PDF'),
        ],
        default='csv'
    )
    
    # Report Email Lists / Subscriptions
    email_recipients = models.TextField(
        blank=True, 
        validators=[validate_comma_separated_emails],
        help_text="Comma-separated list of email addresses for automated report distribution (e.g., user1@example.com, user2@example.com)."
    )
    is_email_active = models.BooleanField(default=False)
    is_file_delivery_active = models.BooleanField(default=False)
    transfer_destinations = models.ManyToManyField(
        "control_plane.TransferDestination", blank=True, related_name="reports"
    )
    
    # Celery Beat Periodic Task integration
    periodic_task = models.OneToOneField(
        PeriodicTask, 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        related_name='report_definition'
    )
    
    is_active = models.BooleanField(default=True)
    
    # Execution tracking fields
    last_run_at = models.DateTimeField(blank=True, null=True)
    last_run_status = models.CharField(max_length=50, blank=True, null=True)

    def get_recipient_list(self):
        """Helper to safely parse comma-separated emails into a clean Python list."""
        if not self.email_recipients:
            return []
        return [email.strip() for email in self.email_recipients.split(',') if email.strip()]

    def clean(self):
        super().clean()
        if self.is_email_active and not self.get_recipient_list():
            raise ValidationError({
                'email_recipients': 'At least one valid recipient email address is required when email activation is enabled.'
            })

    def __str__(self):
        return self.name

class ReportExecution(TimeStampedAuditedModel):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    report = models.ForeignKey(ReportDefinition, on_delete=models.CASCADE, related_name='executions')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    file_path = models.CharField(max_length=512, blank=True, null=True)  # MinIO object path/URL
    error_message = models.TextField(blank=True, null=True)
    row_count = models.IntegerField(default=0)
    completed_at = models.DateTimeField(blank=True, null=True)
    progress_percentage = models.PositiveIntegerField(default=0)
    is_cancelled = models.BooleanField(default=False)
    current_artifact_attempt_id = models.UUIDField(
        blank=True, null=True, editable=False
    )

    def __str__(self):
        date_str = self.created_at.strftime('%Y-%m-%d %H:%M') if self.created_at else 'Unknown date'
        return f"{self.report.name} - {self.status} ({date_str})"

    @property
    def current_artifact_attempt(self):
        if not hasattr(self, "_current_artifact_attempt_cache"):
            self._current_artifact_attempt_cache = (
                ReportArtifactAttempt.objects.filter(
                    pk=self.current_artifact_attempt_id,
                    execution_id=self.pk,
                ).first()
                if self.current_artifact_attempt_id
                else None
            )
        return self._current_artifact_attempt_cache

    @property
    def artifact_reconciliation_available(self):
        return (
            self.status in {"running", "failed"}
            and not self.is_cancelled
            and self.current_artifact_attempt is not None
            and self.current_artifact_attempt.status in {
                ReportArtifactAttempt.Status.UPLOADING,
                ReportArtifactAttempt.Status.VERIFIED,
            }
        )


class ReportArtifactAttempt(models.Model):
    class Status(models.TextChoices):
        PREPARING = "preparing", "Preparing"
        UPLOADING = "uploading", "Upload outcome uncertain"
        VERIFIED = "verified", "Stored object verified"
        COMPLETED = "completed", "Execution completed"
        FAILED = "failed", "Failed before upload"
        SUPERSEDED = "superseded", "Superseded"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    execution = models.ForeignKey(
        ReportExecution, on_delete=models.CASCADE, related_name="artifact_attempts"
    )
    file_path = models.CharField(max_length=512, blank=True, null=True, unique=True)
    sha256 = models.CharField(max_length=64, blank=True)
    size = models.PositiveBigIntegerField(blank=True, null=True)
    content_type = models.CharField(max_length=150, blank=True)
    row_count = models.PositiveIntegerField(default=0)
    status = models.CharField(
        max_length=20, choices=Status.choices, default=Status.PREPARING
    )
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]


class ReportFileDelivery(models.Model):
    class Status(models.TextChoices):
        QUEUED = "queued", "Queued"
        RUNNING = "running", "Running"
        SUCCEEDED = "succeeded", "Delivered"
        FAILED = "failed", "Failed"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    report = models.ForeignKey(
        ReportDefinition, on_delete=models.SET_NULL, null=True, related_name="file_deliveries"
    )
    execution = models.ForeignKey(
        ReportExecution, on_delete=models.SET_NULL, null=True, related_name="file_deliveries"
    )
    destination = models.ForeignKey(
        "control_plane.TransferDestination", on_delete=models.SET_NULL,
        null=True, related_name="file_deliveries",
    )
    destination_name = models.CharField(max_length=100)
    destination_signature = models.CharField(max_length=64)
    file_path = models.CharField(max_length=512)
    remote_path = models.CharField(max_length=2048)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.QUEUED)
    task_id = models.CharField(max_length=255, blank=True)
    retry_count = models.PositiveIntegerField(default=0)
    attempt_count = models.PositiveIntegerField(default=0)
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        constraints = [
            models.UniqueConstraint(
                fields=["execution", "destination"], name="unique_execution_file_destination"
            ),
        ]


class EmailAuditLog(models.Model):
    STATUS_CHOICES = [
        ('sent', 'Sent'),
        ('failed', 'Failed'),
        ('bounced', 'Bounced'),
        ('delivered', 'Delivered'),
    ]

    report = models.ForeignKey('ReportDefinition', on_delete=models.SET_NULL, null=True, related_name='email_logs')
    execution = models.ForeignKey('ReportExecution', on_delete=models.SET_NULL, null=True, related_name='email_logs')
    recipients = models.TextField(help_text="Comma-separated list of recipients")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='sent')
    error_message = models.TextField(blank=True, null=True)
    message_id = models.CharField(max_length=255, blank=True, null=True, help_text="SMTP or provider message ID")
    created_at = models.DateTimeField(auto_now_add=True)
    retry_count = models.PositiveIntegerField(default=0)

    def __index__(self):
        return f"Email for {self.report.name if self.report else 'Deleted'} - {self.status} ({self.created_at})"