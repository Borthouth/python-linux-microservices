from getpass import getpass
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.contrib.auth.password_validation import validate_password

from operations_plane.models import OperationsUser


class Command(BaseCommand):
    help = 'Create an Operations Plane account.'

    def add_arguments(self, parser):
        parser.add_argument('--username', required=True)
        parser.add_argument('--email', default='')
        parser.add_argument('--first-name', default='')
        parser.add_argument('--last-name', default='')
        parser.add_argument(
            '--role',
            choices=[role.value for role in OperationsUser.Role],
            default=OperationsUser.Role.SUPPORT,
        )

    def handle(self, *args, **options):
        username = options['username'].strip()
        if not username:
            raise CommandError('Username cannot be empty.')
        if OperationsUser.objects.filter(username=username).exists():
            raise CommandError('An Operations account with this username already exists.')

        password = getpass('Password: ')
        confirmation = getpass('Password (again): ')
        if not password:
            raise CommandError('Password cannot be empty.')
        if password != confirmation:
            raise CommandError('The two password fields did not match.')

        user_model = get_user_model()
        auth_user = user_model(
            username=str(uuid4()),
            is_active=True,
            is_staff=False,
            is_superuser=False,
        )
        try:
            validate_password(password, auth_user)
        except ValidationError as error:
            raise CommandError(' '.join(error.messages)) from error

        with transaction.atomic():
            auth_user.set_password(password)
            auth_user.save()
            operations_user = OperationsUser.objects.create(
                auth_user=auth_user,
                username=username,
                email=options['email'].strip(),
                first_name=options['first_name'].strip(),
                last_name=options['last_name'].strip(),
                role=options['role'],
            )

        self.stdout.write(
            self.style.SUCCESS(
                f'Created {operations_user.get_role_display()} account '
                f'"{operations_user.username}".'
            )
        )
