from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("control_plane", "0031_cdctobatchtablecheckpoint_active_table_checked"),
    ]

    operations = [
        migrations.AddField(
            model_name="cdctobatchtablecheckpoint",
            name="staging_row_count",
            field=models.PositiveBigIntegerField(blank=True, null=True),
        ),
    ]
