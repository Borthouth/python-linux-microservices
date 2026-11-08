from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("control_plane", "0021_source_table_clickhouse_engine_state"),
    ]

    operations = [
        migrations.AddField(
            model_name="historicalsourcetable",
            name="cdc_managed",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="sourcetable",
            name="cdc_managed",
            field=models.BooleanField(default=False),
        ),
    ]
