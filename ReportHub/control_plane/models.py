from django.db import models
from django.conf import settings
from django.contrib.auth.models import User
from encrypted_model_fields.fields import EncryptedCharField
from simple_history.models import HistoricalRecords


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
    MSSQL = 'mssql', 'SQL Server'

class SourceProfile(TimeStampedAuditedModel):
    """Represents a remote data source (e.g., client MySQL database) to be introspected."""
    name = models.CharField(max_length=100, unique=True)

    # Update this field to use choices
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

    def __str__(self):
        return f"{self.name} ({self.database_name})"

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

