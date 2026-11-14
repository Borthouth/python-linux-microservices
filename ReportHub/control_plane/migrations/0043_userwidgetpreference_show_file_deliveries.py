from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("control_plane", "0042_historicaltransferdestination_transferdestination"),
    ]

    operations = [
        migrations.AddField(
            model_name="userwidgetpreference",
            name="show_file_deliveries",
            field=models.BooleanField(default=True),
        ),
    ]
