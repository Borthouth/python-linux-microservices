from django.shortcuts import redirect
from django.urls import reverse

class PasswordRotationMiddleware:
    """
    Intercepts authenticated requests and forces a password change 
    if the user's account is flagged for password rotation.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.user.is_authenticated:
            # Resolve your password change/rotation route name safely
            try:
                change_password_url = reverse('control_plane:password_change')
            except Exception:
                change_password_url = None

            # Skip interception if already on the password change page or if it's an AJAX/static route
            if change_password_url and request.path != change_password_url:
                # Check if the user is forced to rotate their password
                # You can check a user attribute, a related profile flag, or a session value
                if getattr(request.user, 'must_change_password', False):
                    return redirect(change_password_url)

        response = self.get_response(request)
        return response