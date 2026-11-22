from uuid import uuid4

from django import forms
from django.contrib.auth import get_user_model, password_validation
from django.core.exceptions import ValidationError
from django.db import transaction

from control_plane.models import (
    ControlPlanePermission,
    ControlPlaneProfile,
    ControlPlaneProfileMembership,
    ControlPlaneProfilePermission,
    ControlPlaneProfileSourceAccess,
    ControlPlaneUser,
)
from control_plane.utils import generate_control_plane_username, validate_control_plane_email


_INPUT_CLASS = (
    "w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl "
    "text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm"
)


class ControlPlaneUserForm(forms.Form):
    username = forms.CharField(max_length=150, label="Control Plane username")
    email = forms.EmailField(max_length=254)
    first_name = forms.CharField(max_length=150)
    last_name = forms.CharField(max_length=150)
    password = forms.CharField(
        required=False,
        widget=forms.PasswordInput,
        help_text="Required when creating an account; leave blank to keep the current password.",
    )
    is_staff = forms.BooleanField(
        required=False,
        label="System administrator",
        help_text="Grants global Control Plane administration privileges.",
    )
    is_active = forms.BooleanField(required=False, initial=True)
    profiles = forms.ModelMultipleChoiceField(
        queryset=ControlPlaneProfile.objects.all(),
        required=False,
        widget=forms.CheckboxSelectMultiple,
        label="Shared access profiles",
    )

    def __init__(self, *args, account=None, **kwargs):
        self.account = account
        super().__init__(*args, **kwargs)
        if account:
            self.initial.update({
                "username": account.username,
                "email": account.email,
                "first_name": account.first_name,
                "last_name": account.last_name,
                "is_staff": account.auth_user.is_staff,
                "is_active": account.is_active,
                "profiles": account.profiles.all(),
            })
        else:
            self.fields.pop("username")
            self.fields["password"].required = True
        for field in self.fields.values():
            if not isinstance(field.widget, forms.CheckboxSelectMultiple):
                field.widget.attrs["class"] = _INPUT_CLASS

    def clean_username(self):
        username = self.cleaned_data["username"].strip()
        if not username:
            raise ValidationError("Username cannot be empty.")
        accounts = ControlPlaneUser.objects.filter(username=username)
        if self.account:
            accounts = accounts.exclude(pk=self.account.pk)
        if accounts.exists():
            raise ValidationError("This Control Plane username is already in use.")
        return username

    def clean_email(self):
        return validate_control_plane_email(
            self.cleaned_data["email"], self.account.pk if self.account else None,
        )

    def clean_password(self):
        password = self.cleaned_data.get("password")
        if password:
            auth_user = get_user_model()(
                username=self.cleaned_data.get("username", ""),
                email=self.cleaned_data.get("email", ""),
                first_name=self.cleaned_data.get("first_name", ""),
                last_name=self.cleaned_data.get("last_name", ""),
            )
            try:
                password_validation.validate_password(password, auth_user)
            except ValidationError as error:
                raise ValidationError(error.messages) from error
        return password

    @transaction.atomic
    def save(self, actor):
        auth_user = self.account.auth_user if self.account else get_user_model()(
            username=str(uuid4()),
        )
        auth_user.email = self.cleaned_data["email"].strip()
        auth_user.first_name = self.cleaned_data["first_name"].strip()
        auth_user.last_name = self.cleaned_data["last_name"].strip()
        auth_user.is_staff = self.cleaned_data["is_staff"]
        auth_user.is_active = self.cleaned_data["is_active"]
        auth_user.is_superuser = False if self.account is None else auth_user.is_superuser
        if self.cleaned_data["password"]:
            auth_user.set_password(self.cleaned_data["password"])
        auth_user.save()

        account = self.account or ControlPlaneUser(auth_user=auth_user)
        account.username = (
            self.cleaned_data["username"] if self.account else
            generate_control_plane_username(self.cleaned_data["first_name"], self.cleaned_data["last_name"])
        )
        account.email = self.cleaned_data["email"].strip()
        account.first_name = self.cleaned_data["first_name"].strip()
        account.last_name = self.cleaned_data["last_name"].strip()
        account.is_active = self.cleaned_data["is_active"]
        if self.account is None:
            account.created_by = actor
        account.updated_by = actor
        account.save()
        selected_profiles = list(self.cleaned_data["profiles"])
        memberships = ControlPlaneProfileMembership.objects.filter(user=account)
        if selected_profiles:
            memberships.exclude(profile__in=selected_profiles).delete()
        else:
            memberships.delete()
        for profile in selected_profiles:
            ControlPlaneProfileMembership.objects.get_or_create(
                user=account,
                profile=profile,
                defaults={
                    "created_by": actor,
                    "updated_by": actor,
                },
            )
        return account


class ControlPlaneProfileForm(forms.ModelForm):
    announcement_permissions = forms.ModelMultipleChoiceField(
        queryset=ControlPlanePermission.objects.filter(is_active=True),
        label="Feature permissions",
        required=False,
        widget=forms.CheckboxSelectMultiple,
        help_text=(
            "Grants combine across active profiles. These permissions prepare future "
            "announcement workflows; they do not grant source access or system "
            "administration. Publishing across planes also requires Publish announcements."
        ),
    )

    class Meta:
        model = ControlPlaneProfile
        fields = ["name", "description", "is_active"]
        widgets = {
            "name": forms.TextInput(attrs={"class": _INPUT_CLASS}),
            "description": forms.Textarea(attrs={"class": _INPUT_CLASS, "rows": 3}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance.pk:
            self.initial["announcement_permissions"] = list(
                self.instance.announcement_permissions.values_list("permission_id", flat=True)
            )

    @transaction.atomic
    def save(self, actor):
        profile = super().save(commit=False)
        if profile._state.adding:
            profile.created_by = actor
        profile.updated_by = actor
        profile.save()
        selected = self.cleaned_data["announcement_permissions"]
        profile.announcement_permissions.filter(
            permission__is_active=True,
        ).exclude(permission__in=selected).delete()
        for permission in selected:
            ControlPlaneProfilePermission.objects.get_or_create(
                profile=profile,
                permission=permission,
                defaults={"created_by": actor, "updated_by": actor},
            )
        return profile


class ControlPlaneProfileSourceAccessForm(forms.ModelForm):
    class Meta:
        model = ControlPlaneProfileSourceAccess
        fields = ["source", "role"]
        widgets = {
            "source": forms.Select(attrs={"class": _INPUT_CLASS}),
            "role": forms.Select(attrs={"class": _INPUT_CLASS}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["source"].queryset = self.fields["source"].queryset.order_by("name")


class ControlPlaneSelfProfileForm(forms.ModelForm):
    class Meta:
        model = ControlPlaneUser
        fields = ["first_name", "last_name", "email"]
        widgets = {
            "first_name": forms.TextInput(attrs={"class": _INPUT_CLASS}),
            "last_name": forms.TextInput(attrs={"class": _INPUT_CLASS}),
            "email": forms.EmailInput(attrs={"class": _INPUT_CLASS}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for name in ("first_name", "last_name", "email"):
            self.fields[name].required = True

    def clean_email(self):
        return validate_control_plane_email(self.cleaned_data["email"], self.instance.pk)
