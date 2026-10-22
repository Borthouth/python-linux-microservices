from django.conf import settings


class WatchdogSettings:
    @property
    def TIME_THRESHOLD(self):
        return getattr(settings, "WATCHDOG_TIME_THRESHOLD", 0.5) # 500ms default

    @property
    def QUERY_COUNT_THRESHOLD(self):
        return getattr(settings, "WATCHDOG_QUERY_COUNT_THRESHOLD", 30)

    @property
    def ENABLED(self):
        return getattr(settings, "WATCHDOG_ENABLED", True)


watchdog_settings = WatchdogSettings()

