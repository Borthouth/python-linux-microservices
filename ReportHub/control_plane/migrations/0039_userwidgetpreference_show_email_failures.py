from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("control_plane", "0038_cdc_batch_generation_copy"),
    ]

    operations = [
        migrations.AddField(
            model_name="userwidgetpreference",
            name="show_email_failures",
            field=models.BooleanField(default=True),
        ),
    ]
