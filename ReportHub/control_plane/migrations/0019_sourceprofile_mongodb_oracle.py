from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('control_plane', '0018_backgroundjob_created_by_backgroundjob_updated_by_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='historicalsourceprofile',
            name='db_type',
            field=models.CharField(
                choices=[
                    ('mysql', 'MySQL'),
                    ('postgresql', 'PostgreSQL'),
                    ('mssql', 'Microsoft SQL Server'),
                    ('mongodb', 'MongoDB'),
                    ('oracle', 'Oracle'),
                ],
                default='mysql',
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name='sourceprofile',
            name='db_type',
            field=models.CharField(
                choices=[
                    ('mysql', 'MySQL'),
                    ('postgresql', 'PostgreSQL'),
                    ('mssql', 'Microsoft SQL Server'),
                    ('mongodb', 'MongoDB'),
                    ('oracle', 'Oracle'),
                ],
                default='mysql',
                max_length=20,
            ),
        ),
    ]
