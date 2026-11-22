from django.conf import settings
from django.db import models

from control_plane.models import TimeStampedAuditedModel
from otel.source_policy import default_sources


class OperationsLogSources(TimeStampedAuditedModel):
    enabled_sources = models.JSONField(default=default_sources)
    revision = models.UUIDField()


class OperationsUser(TimeStampedAuditedModel):
    class Role(models.TextChoices):
        ADMIN = 'ADMIN', 'Operations Admin'
        SUPPORT = 'SUPPORT', 'Support Staff'

    auth_user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='operations_account',
    )
    username = models.CharField(max_length=150, unique=True)
    email = models.EmailField(blank=True, default='')
    first_name = models.CharField(max_length=150, blank=True, default='')
    last_name = models.CharField(max_length=150, blank=True, default='')
    role = models.CharField(max_length=10, choices=Role.choices, default=Role.SUPPORT)
    is_active = models.BooleanField(default=True)

    class Meta:
        ordering = ['username']
        verbose_name = 'Operations user'
        verbose_name_plural = 'Operations users'

    def __str__(self):
        return self.username

    @property
    def full_name(self):
        return f'{self.first_name} {self.last_name}'.strip() or self.username


class OperationsWidgetPreference(models.Model):
    operations_user = models.OneToOneField(
        OperationsUser,
        on_delete=models.CASCADE,
        related_name='widget_preferences',
    )
    show_stats = models.BooleanField(default=True)
    show_log_store = models.BooleanField(default=True)
    show_searches = models.BooleanField(default=True)
    show_analyses = models.BooleanField(default=True)

    def __str__(self):
        return f'Widget preferences for {self.operations_user.username}'


class OperationsLogAnalysis(TimeStampedAuditedModel):
    class Status(models.TextChoices):
        QUEUED = 'QUEUED', 'Queued'
        RUNNING = 'RUNNING', 'Running'
        COMPLETED = 'COMPLETED', 'Completed'
        FAILED = 'FAILED', 'Failed'

    requested_by = models.ForeignKey(
        OperationsUser,
        on_delete=models.CASCADE,
        related_name='log_analyses',
    )
    search = models.ForeignKey(
        'OperationsLogSearch',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='analyses',
    )
    task_id = models.CharField(max_length=255, blank=True, default='')
    status = models.CharField(
        max_length=10,
        choices=Status.choices,
        default=Status.QUEUED,
    )
    range_start = models.DateTimeField()
    range_end = models.DateTimeField()
    service_filter = models.CharField(max_length=100, blank=True, default='')
    source_filter = models.CharField(max_length=20, blank=True, default='')
    severity_filter = models.CharField(max_length=10, blank=True, default='')
    queue_filter = models.CharField(max_length=100, blank=True, default='')
    task_name_filter = models.CharField(max_length=255, blank=True, default='')
    task_id_filter = models.CharField(max_length=255, blank=True, default='')
    worker_filter = models.CharField(max_length=255, blank=True, default='')
    text_filter = models.CharField(max_length=200, blank=True, default='')
    total_events = models.PositiveBigIntegerField(null=True, blank=True)
    sampled_events = models.PositiveIntegerField(null=True, blank=True)
    saved_result_filters = models.JSONField(null=True, blank=True, default=None)
    summary = models.TextField(blank=True, default='')
    breakdown = models.JSONField(default=list, blank=True)
    error_message = models.CharField(max_length=255, blank=True, default='')
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Operations log analysis'
        verbose_name_plural = 'Operations log analyses'


class OperationsLogSearch(TimeStampedAuditedModel):
    class Status(models.TextChoices):
        QUEUED = 'QUEUED', 'Queued'
        RUNNING = 'RUNNING', 'Running'
        COMPLETED = 'COMPLETED', 'Completed'
        FAILED = 'FAILED', 'Failed'

    requested_by = models.ForeignKey(
        OperationsUser,
        on_delete=models.CASCADE,
        related_name='log_searches',
    )
    task_id = models.CharField(max_length=255, blank=True, default='')
    status = models.CharField(
        max_length=10,
        choices=Status.choices,
        default=Status.QUEUED,
    )
    range_start = models.DateTimeField()
    range_end = models.DateTimeField()
    service_filter = models.CharField(max_length=100, blank=True, default='')
    source_filter = models.CharField(max_length=20, blank=True, default='')
    severity_filter = models.CharField(max_length=10, blank=True, default='')
    queue_filter = models.CharField(max_length=100, blank=True, default='')
    task_name_filter = models.CharField(max_length=255, blank=True, default='')
    task_id_filter = models.CharField(max_length=255, blank=True, default='')
    worker_filter = models.CharField(max_length=255, blank=True, default='')
    text_filter = models.CharField(max_length=200, blank=True, default='')
    results = models.JSONField(default=list, blank=True)
    before_event_time = models.DateTimeField(null=True, blank=True)
    before_event_id = models.UUIDField(null=True, blank=True)
    after_event_time = models.DateTimeField(null=True, blank=True)
    after_event_id = models.UUIDField(null=True, blank=True)
    next_event_time = models.DateTimeField(null=True, blank=True)
    next_event_id = models.UUIDField(null=True, blank=True)
    total_events = models.PositiveBigIntegerField(null=True, blank=True)
    has_more = models.BooleanField(default=False)
    error_message = models.CharField(max_length=255, blank=True, default='')
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Operations log search'
        verbose_name_plural = 'Operations log searches'
