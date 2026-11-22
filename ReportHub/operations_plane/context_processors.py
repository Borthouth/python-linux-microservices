from django.conf import settings


def tailwind_cdn(request):
    return {'operations_tailwind_cdn_enabled': settings.DEBUG}
