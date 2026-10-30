import uuid
from django.db import models
from django.conf import settings
from django_celery_beat.models import PeriodicTask
from simple_history.models import HistoricalRecords
from control_plane.models import SourceProfile, SourceTable

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
    
    # Export format selection
    export_format = models.CharField(
        max_length=10,
        choices=[
            ('csv', 'CSV'),
            ('xlsx', 'Excel (XLSX)'),
            ('json', 'JSON'),
        ],
        default='csv'
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

    def __str__(self):
        return self.name

class ReportExecution(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    report = models.ForeignKey(ReportDefinition, on_delete=models.CASCADE, related_name='executions')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    file_path = models.CharField(max_length=512, blank=True, null=True)  # MinIO object path/URL
    error_message = models.TextField(blank=True, null=True)
    row_count = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(blank=True, null=True)

    def __str__(self):
            return f"{self.report.name} - {self.status} ({self.created_at.strftime('%Y-%m-%d %H:%M')})"