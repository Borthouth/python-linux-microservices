from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("control_plane", "0039_userwidgetpreference_show_email_failures"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="userwidgetpreference",
            name="show_email_failures",
        ),
    ]
