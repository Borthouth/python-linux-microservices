from urllib.parse import urlsplit

from django.conf import settings
from django.http import Http404

from operations_plane.models import OperationsUser


class OperationsRealmMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        self.operations_host = settings.OPERATIONS_HOST.strip().lower().rstrip('.')

    def __call__(self, request):
        host = (urlsplit(f'//{request.get_host()}').hostname or '').lower().rstrip('.')
        is_operations_host = bool(self.operations_host) and host == self.operations_host

        if self.operations_host:
            if is_operations_host:
                request.urlconf = 'operations_plane.host_urls'
            elif request.path_info.startswith('/operations/'):
                raise Http404

        is_operations_area = is_operations_host or (
            not self.operations_host
            and request.path_info.startswith('/operations/')
        )
        if request.user.is_authenticated:
            is_operations_user = OperationsUser.objects.filter(
                auth_user_id=request.user.pk
            ).exists()
            if is_operations_user and not is_operations_area:
                raise Http404

        return self.get_response(request)
