from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("control_plane", "0032_cdctobatchtablecheckpoint_staging_row_count"),
    ]

    operations = [
        migrations.AddField(
            model_name="cdctobatchtablecheckpoint",
            name="cutover_command_inflight",
            field=models.BooleanField(default=False),
        ),
    ]
