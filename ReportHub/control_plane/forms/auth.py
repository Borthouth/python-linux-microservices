from uuid import uuid4

from django.contrib.auth import authenticate
from django.contrib.auth.forms import AuthenticationForm, UsernameField
from django.core.exceptions import ValidationError
from django.db.models import Q
from django.db.models.functions import Lower, Trim

from control_plane.models import ControlPlaneUser
from operations_plane.models import OperationsUser


class ControlPlaneAuthenticationForm(AuthenticationForm):
    username = UsernameField(max_length=254)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["username"].max_length = 254
        self.fields["username"].widget.attrs["maxlength"] = 254

    def clean(self):
        username = self.cleaned_data.get('username')
        password = self.cleaned_data.get('password')

        if username and password:
            accounts = list(
                ControlPlaneUser.objects
                .filter(archived_at__isnull=True)
                .annotate(normalized_email=Lower(Trim("email")))
                .filter(Q(username=username) | Q(normalized_email__iexact=username))
                .select_related('auth_user')
                [:2]
            )
            account = accounts[0] if len(accounts) == 1 else None
            auth_username = (
                account.auth_user.username
                if account and account.is_active
                else username if not accounts
                else str(uuid4())
            )
            user = authenticate(
                self.request,
                username=auth_username,
                password=password,
            )

            if len(accounts) > 1:
                user = None
            elif account and (
                not account.is_active
                or user is None
                or user.pk != account.auth_user_id
            ):
                user = None
            elif account is None and user and (
                OperationsUser.objects.filter(auth_user_id=user.pk).exists()
                or ControlPlaneUser.objects.filter(auth_user_id=user.pk).exists()
            ):
                user = None

            if user is None:
                raise self.get_invalid_login_error()

            self.confirm_login_allowed(user)
            self.user_cache = user

        return self.cleaned_data

    def confirm_login_allowed(self, user):
        super().confirm_login_allowed(user)
        if OperationsUser.objects.filter(auth_user_id=user.pk).exists():
            raise ValidationError(
                'Invalid username or password.',
                code='invalid_login',
            )
