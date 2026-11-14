import uuid

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("reports", "0014_historicalreportdefinition_is_file_delivery_active_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="ReportArtifactAttempt",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4, editable=False, primary_key=True, serialize=False
                    ),
                ),
                ("file_path", models.CharField(blank=True, max_length=512, null=True, unique=True)),
                ("sha256", models.CharField(blank=True, max_length=64)),
                ("size", models.PositiveBigIntegerField(blank=True, null=True)),
                ("content_type", models.CharField(blank=True, max_length=150)),
                ("row_count", models.PositiveIntegerField(default=0)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("preparing", "Preparing"),
                            ("uploading", "Upload outcome uncertain"),
                            ("verified", "Stored object verified"),
                            ("completed", "Execution completed"),
                            ("failed", "Failed before upload"),
                            ("superseded", "Superseded"),
                        ],
                        default="preparing",
                        max_length=20,
                    ),
                ),
                ("error_message", models.TextField(blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "execution",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="artifact_attempts",
                        to="reports.reportexecution",
                    ),
                ),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
        migrations.AddField(
            model_name="reportexecution",
            name="current_artifact_attempt_id",
            field=models.UUIDField(blank=True, editable=False, null=True),
        ),
        migrations.AddField(
            model_name="historicalreportexecution",
            name="current_artifact_attempt_id",
            field=models.UUIDField(blank=True, editable=False, null=True),
        ),
    ]
