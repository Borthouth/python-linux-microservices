from django.apps import AppConfig
from simple_history.models import HistoricalRecords


class ControlPlaneConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'control_plane'

    def ready(self):
        from django.contrib.auth import get_user_model
        User = get_user_model()
        # Register history on User if not already registered
        if not hasattr(User, 'history'):
            HistoricalRecords().contribute_to_class(User, 'history')
