from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("control_plane", "0043_userwidgetpreference_show_file_deliveries"),
    ]

    operations = [
        migrations.AlterField(
            model_name="transferdestination",
            name="is_active",
            field=models.BooleanField(
                default=False,
                help_text="Allow reports with file delivery enabled to transfer files to this destination.",
                verbose_name="Enabled for report transfers",
            ),
        ),
        migrations.AlterField(
            model_name="historicaltransferdestination",
            name="is_active",
            field=models.BooleanField(
                default=False,
                help_text="Allow reports with file delivery enabled to transfer files to this destination.",
                verbose_name="Enabled for report transfers",
            ),
        ),
    ]
