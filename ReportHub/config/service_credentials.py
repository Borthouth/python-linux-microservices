from django.core.exceptions import ImproperlyConfigured


def service_credentials(environ, prefix, fallback_user, fallback_password):
    user_key = f"{prefix}_USER"
    password_key = f"{prefix}_PASSWORD"
    if user_key not in environ and password_key not in environ:
        return fallback_user, fallback_password
    if (
        user_key not in environ
        or password_key not in environ
        or not environ[user_key].strip()
        or not environ[password_key]
    ):
        raise ImproperlyConfigured(
            f"{user_key} and {password_key} must both be set and nonempty."
        )
    return environ[user_key], environ[password_key]
