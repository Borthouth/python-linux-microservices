from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('control_plane', '0030_userwidgetpreference_show_email_audit'),
    ]

    operations = [
        migrations.AddField(
            model_name='cdctobatchtablecheckpoint',
            name='active_table_checked',
            field=models.BooleanField(default=False),
        ),
    ]
