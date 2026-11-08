from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('control_plane', '0029_cdctobatchtablecheckpoint_staging_schema'),
    ]

    operations = [
        migrations.AddField(
            model_name='userwidgetpreference',
            name='show_email_audit',
            field=models.BooleanField(default=True),
        ),
    ]
