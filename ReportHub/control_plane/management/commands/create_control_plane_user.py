from getpass import getpass
from uuid import uuid4

from django.contrib.auth import get_user_model, password_validation
from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError, transaction

from control_plane.forms.accounts import ControlPlaneUserForm
from control_plane.models import ControlPlaneProfile, ControlPlaneUser
from control_plane.utils import USER_DETAILS_ERROR, validate_control_plane_email


class Command(BaseCommand):
    help = "Create a Control Plane account."

    def add_arguments(self, parser):
        parser.add_argument("--username", required=True)
        parser.add_argument("--email", default="", help="Required email address.")
        parser.add_argument("--first-name", default="", help="Required first name.")
        parser.add_argument("--last-name", default="", help="Required last name.")
        parser.add_argument(
            "--profile",
            action="append",
            default=[],
            help="Name of a shared access profile to assign. May be repeated.",
        )
        parser.add_argument(
            "--create-profile",
            action="store_true",
            help="Create any missing requested profiles as empty profiles.",
        )
        parser.add_argument(
            "--staff",
            action="store_true",
            help="Grant global Control Plane administrator privileges.",
        )

    def handle(self, *args, **options):
        username = options["username"].strip()
        if not username:
            raise CommandError("Username cannot be empty.")
        if len(username) > 150:
            raise CommandError("Username cannot exceed 150 characters.")
        if ControlPlaneUser.objects.filter(username=username).exists():
            raise CommandError("A Control Plane account with this username already exists.")

        profile_names = []
        for requested_name in options["profile"]:
            name = requested_name.strip()
            if not name:
                raise CommandError("Profile names cannot be empty.")
            if len(name) > 100:
                raise CommandError("Profile names cannot exceed 100 characters.")
            if name not in profile_names:
                profile_names.append(name)
        if options["create_profile"] and not profile_names:
            raise CommandError("--create-profile requires at least one --profile.")

        details = {}
        for field_name in ("email", "first_name", "last_name"):
            try:
                details[field_name] = ControlPlaneUserForm.base_fields[field_name].clean(
                    options[field_name]
                )
            except ValidationError as error:
                label = field_name.replace("_", " ").capitalize()
                raise CommandError(f"{label}: {' '.join(error.messages)}") from error
        try:
            validate_control_plane_email(details["email"])
        except ValidationError as error:
            raise CommandError(USER_DETAILS_ERROR) from error

        profiles_by_name = {
            profile.name: profile
            for profile in ControlPlaneProfile.objects.filter(name__in=profile_names)
        }
        inactive_profiles = [
            name for name, profile in profiles_by_name.items()
            if not profile.is_active
        ]
        if inactive_profiles:
            raise CommandError(
                "Requested profiles are inactive: " + ", ".join(sorted(inactive_profiles))
            )
        missing_profiles = [
            name for name in profile_names
            if name not in profiles_by_name
        ]
        if missing_profiles and not options["create_profile"]:
            raise CommandError(
                "Profiles do not exist: " + ", ".join(missing_profiles)
            )

        password = getpass("Password: ")
        confirmation = getpass("Password (again): ")
        if not password:
            raise CommandError("Password cannot be empty.")
        if password != confirmation:
            raise CommandError("The two password fields did not match.")

        auth_user = get_user_model()(
            username=username,
            email=details["email"],
            first_name=details["first_name"],
            last_name=details["last_name"],
            is_active=True,
            is_staff=options["staff"],
            is_superuser=False,
        )
        try:
            password_validation.validate_password(password, auth_user)
        except ValidationError as error:
            raise CommandError(" ".join(error.messages)) from error

        try:
            with transaction.atomic():
                for name in missing_profiles:
                    profiles_by_name[name] = ControlPlaneProfile.objects.create(name=name)
                auth_user.username = str(uuid4())
                auth_user.set_password(password)
                auth_user.save()
                account = ControlPlaneUser.objects.create(
                    auth_user=auth_user,
                    username=username,
                    email=auth_user.email,
                    first_name=auth_user.first_name,
                    last_name=auth_user.last_name,
                )
                account.profiles.set(
                    profiles_by_name[name]
                    for name in profile_names
                )
        except IntegrityError as error:
            try:
                validate_control_plane_email(details["email"])
            except ValidationError:
                raise CommandError(USER_DETAILS_ERROR) from error
            raise

        account_type = "administrator" if options["staff"] else "user"
        self.stdout.write(
            self.style.SUCCESS(
                f'Created Control Plane {account_type} account "{account.username}".'
            )
        )
