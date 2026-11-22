from uuid import UUID, uuid4
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import IntegrityError, connection, transaction
from django.db.migrations.executor import MigrationExecutor
from django.template.loader import render_to_string
from django.test import Client, RequestFactory, TestCase, TransactionTestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from control_plane.access import (
    authorized_source_ids,
    effective_announcement_permissions,
    effective_source_roles,
    has_announcement_permission,
)
from control_plane.forms.accounts import ControlPlaneSelfProfileForm, ControlPlaneUserForm
from control_plane.forms.users import UserEditForm, UserRegistrationWithAccessForm
from control_plane.models import (
    AnnouncementPermission,
    ControlPlaneProfile,
    ControlPlaneProfileMembership,
    ControlPlaneProfilePermission,
    ControlPlanePermission,
    ControlPlaneProfileSourceAccess,
    ControlPlaneUser,
    DbChoice,
    Role,
    SourceProfile,
    UserSourceAccess,
)
from operations_plane.models import OperationsUser
from control_plane.utils import USER_DETAILS_ERROR


@override_settings(CACHES={
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}
})
class ControlPlaneAccountTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.admin = self.user_model.objects.create_user(
            username="legacy-admin",
            password="Legacy-admin-password-92!",
            is_staff=True,
        )

    def create_control_plane_account(self, username, password="Control-password-92!"):
        auth_user = self.user_model.objects.create_user(
            username=str(uuid4()),
            password=password,
            is_staff=False,
            is_superuser=False,
        )
        account = ControlPlaneUser.objects.create(
            auth_user=auth_user,
            username=username,
            email=f"{username}@example.test",
        )
        return account

    def test_control_plane_login_maps_app_username_to_uuid_user(self):
        account = self.create_control_plane_account("keith")

        response = self.client.post(
            reverse("control_plane:login"),
            {"username": "keith", "password": "Control-password-92!"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            self.client.session["_auth_user_id"],
            str(account.auth_user_id),
        )
        UUID(account.auth_user.username)

    def test_onboarding_generates_account_and_allows_email_login(self):
        self.client.force_login(self.admin)
        url = reverse("control_plane:account_list")
        response = self.client.get(url)
        self.assertNotIn("username", response.context["form"].fields)
        response = self.client.post(url, {
            "email": " Person@Example.test ", "first_name": "Example", "last_name": "Person",
            "password": "Strong-onboard-password-92!", "is_active": "on",
        })
        self.assertRedirects(response, url)
        account = ControlPlaneUser.objects.get(email__iexact="Person@example.test")
        UUID(account.auth_user.username)
        self.assertNotEqual(account.username, account.auth_user.username)
        self.assertTrue(account.auth_user.check_password("Strong-onboard-password-92!"))
        response = Client().post(reverse("control_plane:login"), {
            "username": "person@example.TEST", "password": "Strong-onboard-password-92!",
        })
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.client.session["_auth_user_id"], str(account.auth_user_id))

    def test_long_email_can_be_used_to_sign_in(self):
        account = self.create_control_plane_account("long-email")
        account.email = f"{'a' * 60}@{'b' * 60}.{'c' * 60}.{'d' * 50}.test"
        account.save(update_fields=["email"])
        response = Client().post(reverse("control_plane:login"), {
            "username": account.email, "password": "Control-password-92!",
        })
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.client.session["_auth_user_id"], str(account.auth_user_id))

    def test_email_login_preserves_plane_isolation_and_rejects_ambiguous_identifier(self):
        account = self.create_control_plane_account("email-login")
        operations_auth = self.user_model.objects.create_user(username=str(uuid4()), password="Operations-password-92!")
        OperationsUser.objects.create(
            auth_user=operations_auth, username="ops-email-login", email=account.email,
        )
        response = Client().post(reverse("control_plane:login"), {
            "username": account.email.upper(), "password": "Operations-password-92!",
        })
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("_auth_user_id", response.client.session)
        other_auth = self.user_model.objects.create_user(username=str(uuid4()))
        ControlPlaneUser.objects.create(
            auth_user=other_auth, username=account.email, email="other@example.test",
        )
        response = Client().post(reverse("control_plane:login"), {
            "username": account.email, "password": "Control-password-92!",
        })
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("_auth_user_id", response.client.session)

    def test_control_plane_email_is_unique_and_conflicts_are_generic(self):
        existing = self.create_control_plane_account("existing-email")
        self.client.force_login(self.admin)
        data = {
            "email": f" {existing.email.upper()} ", "first_name": "Test", "last_name": "Person",
            "password": "Strong-onboard-password-92!", "role": Role.VIEWER,
        }
        response = self.client.post(reverse("control_plane:account_list"), {
            **data, "username": "duplicate-email",
        })
        self.assertContains(response, USER_DETAILS_ERROR)
        self.assertEqual(ControlPlaneUser.objects.count(), 1)

        other = self.create_control_plane_account("other-email")
        response = self.client.post(reverse("control_plane:account_edit", args=[other.pk]), {
            **data, "username": other.username, "is_active": "on",
        })
        self.assertContains(response, USER_DETAILS_ERROR)
        self.client.force_login(other.auth_user)
        response = self.client.post(reverse("control_plane:user_profile"), data)
        self.assertContains(response, USER_DETAILS_ERROR)
        other.refresh_from_db()
        other.auth_user.refresh_from_db()
        self.assertEqual(other.email, "other-email@example.test")
        self.assertNotEqual(other.auth_user.email, existing.email)

        auth_user = self.user_model.objects.create_user(username=str(uuid4()))
        with self.assertRaises(IntegrityError), transaction.atomic():
            ControlPlaneUser.objects.create(
                auth_user=auth_user, username="db-duplicate", email=f" {existing.email.upper()} ",
            )

    def test_onboarding_handles_email_conflict_at_database_save(self):
        account = self.create_control_plane_account("race-email")
        self.client.force_login(self.admin)
        with patch("control_plane.forms.accounts.validate_control_plane_email", return_value=account.email):
            response = self.client.post(reverse("control_plane:account_list"), {
                "email": account.email, "first_name": "Test", "last_name": "Person",
                "password": "Strong-onboard-password-92!", "role": Role.VIEWER,
            })
        self.assertContains(response, USER_DETAILS_ERROR)
        self.assertEqual(ControlPlaneUser.objects.count(), 1)
        self.assertEqual(self.user_model.objects.count(), 2)

    def test_onboarding_allows_email_used_by_operations_account(self):
        operations_auth = self.user_model.objects.create_user(username=str(uuid4()))
        OperationsUser.objects.create(
            auth_user=operations_auth, username="ops-email", email="shared@example.test",
        )
        self.client.force_login(self.admin)
        response = self.client.post(reverse("control_plane:account_list"), {
            "email": "shared@example.test", "first_name": "Test", "last_name": "Person",
            "password": "Strong-onboard-password-92!", "role": Role.VIEWER,
        })
        self.assertRedirects(response, reverse("control_plane:account_list"))
        self.assertTrue(ControlPlaneUser.objects.filter(email="shared@example.test").exists())

    @patch("control_plane.management.commands.create_control_plane_user.getpass")
    def test_create_command_rejects_duplicate_email_before_password_prompt(self, getpass):
        account = self.create_control_plane_account("command-existing")
        with self.assertRaisesMessage(CommandError, USER_DETAILS_ERROR):
            call_command(
                "create_control_plane_user", username="duplicate-cli",
                email=account.email.upper(), first_name="First", last_name="Last",
            )
        getpass.assert_not_called()

    def test_legacy_user_edit_redirects_to_account_edit(self):
        account = self.create_control_plane_account("sync-edit")
        self.client.force_login(self.admin)
        response = self.client.get(reverse("control_plane:user_edit", args=[account.auth_user_id]))
        self.assertRedirects(response, reverse("control_plane:account_edit", args=[account.pk]))

    def test_control_plane_login_rejects_internal_uuid_but_keeps_legacy_login(self):
        account = self.create_control_plane_account("new-account")
        response = self.client.post(
            reverse("control_plane:login"),
            {
                "username": account.auth_user.username,
                "password": "Control-password-92!",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Invalid username or password.")
        self.assertNotIn("_auth_user_id", self.client.session)

        legacy = self.user_model.objects.create_user(
            username="legacy-user",
            password="Legacy-user-password-92!",
        )
        legacy_response = Client().post(
            reverse("control_plane:login"),
            {"username": legacy.username, "password": "Legacy-user-password-92!"},
        )
        self.assertEqual(legacy_response.status_code, 302)

    def test_operations_credentials_cannot_sign_in_to_control_plane(self):
        auth_user = self.user_model.objects.create_user(
            username=str(uuid4()),
            password="Operations-password-92!",
        )
        OperationsUser.objects.create(
            auth_user=auth_user,
            username="operations-only",
            role=OperationsUser.Role.SUPPORT,
        )

        response = Client().post(
            reverse("control_plane:login"),
            {
                "username": auth_user.username,
                "password": "Operations-password-92!",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Invalid username or password.")

    def test_control_plane_username_can_match_a_legacy_auth_username(self):
        self.user_model.objects.create_user(
            username="shared-name",
            password="Legacy-password-92!",
        )
        account = self.create_control_plane_account("shared-name")

        response = Client().post(
            reverse("control_plane:login"),
            {"username": "shared-name", "password": "Control-password-92!"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response.client.session["_auth_user_id"],
            str(account.auth_user_id),
        )

    def test_multiple_users_share_profile_source_permissions(self):
        source = SourceProfile.objects.create(
            name="Shared profile source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            database_name="source",
            username="reader",
        )
        profile = ControlPlaneProfile.objects.create(name="Shared analysts")
        ControlPlaneProfileSourceAccess.objects.create(
            profile=profile,
            source=source,
            role=Role.ADMIN,
        )
        first = self.create_control_plane_account("profile-user-a")
        second = self.create_control_plane_account("profile-user-b")
        ControlPlaneProfileMembership.objects.create(user=first, profile=profile)
        ControlPlaneProfileMembership.objects.create(user=second, profile=profile)
        UserSourceAccess.objects.create(
            user=first.auth_user,
            source=source,
            role=Role.VIEWER,
        )

        for account in (first, second):
            with self.subTest(username=account.username):
                self.assertEqual(
                    effective_source_roles(account.auth_user),
                    {source.pk: Role.ADMIN},
                )
                self.assertEqual(
                    authorized_source_ids(account.auth_user, role=Role.ADMIN),
                    [source.pk],
                )

    def test_shared_profile_permissions_are_used_by_cdc_authorization(self):
        from control_plane.views.cdc_rebuilds import _can_resume_rebuild, _visible_sources

        source = SourceProfile.objects.create(
            name="CDC profile source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            database_name="source",
            username="reader",
        )
        profile = ControlPlaneProfile.objects.create(name="CDC operators")
        ControlPlaneProfileSourceAccess.objects.create(
            profile=profile,
            source=source,
            role=Role.VIEWER,
        )
        account = self.create_control_plane_account("cdc-profile-user")
        ControlPlaneProfileMembership.objects.create(user=account, profile=profile)

        self.assertEqual(list(_visible_sources(account.auth_user)), [source])
        self.assertFalse(_can_resume_rebuild(account.auth_user, source.pk))

        grant = profile.source_access.get(source=source)
        grant.role = Role.ADMIN
        grant.save(update_fields=["role"])
        self.assertTrue(_can_resume_rebuild(account.auth_user, source.pk))

        profile.is_active = False
        profile.save(update_fields=["is_active"])
        self.assertEqual(list(_visible_sources(account.auth_user)), [])
        self.assertFalse(_can_resume_rebuild(account.auth_user, source.pk))

    def test_account_management_creates_app_user_with_uuid_auth(self):
        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("control_plane:account_list"),
            {
                "username": "new-control-user",
                "email": "new-control-user@example.test",
                "first_name": "New",
                "last_name": "User",
                "password": "Strong-Cp-login-92!",
                "is_active": "on",
            },
        )

        self.assertRedirects(response, reverse("control_plane:account_list"))
        account = ControlPlaneUser.objects.get(email="new-control-user@example.test")
        UUID(account.auth_user.username)
        self.assertTrue(account.auth_user.check_password("Strong-Cp-login-92!"))
        self.assertEqual(account.auth_user.email, account.email)

    def test_users_page_searches_linked_accounts_without_a_user_column(self):
        account = self.create_control_plane_account("shared-identity")
        account.first_name = "Control"
        account.last_name = "Person"
        account.save()
        operations_auth = self.user_model.objects.create_user(username=str(uuid4()))
        OperationsUser.objects.create(
            auth_user=operations_auth, username="shared-identity",
            first_name="Operations", last_name="Person",
        )
        self.client.force_login(self.admin)
        response = self.client.get(reverse("control_plane:account_list"), {"q": "shared-identity"})
        self.assertContains(response, account.email)
        self.assertNotContains(response, ">User</th>")
        self.assertNotContains(response, account.auth_user.username)
        self.assertNotContains(response, operations_auth.username)
        response = self.client.get(reverse("control_plane:account_list"), {"q": "Control"})
        self.assertContains(response, account.email)
        self.assertNotContains(response, "Operations Person")

    def test_user_access_edit_and_status_messages_use_readable_identity(self):
        account = self.create_control_plane_account("friendly-user")
        account.first_name = "Friendly"
        account.last_name = "Person"
        account.save()
        identity = f"Friendly Person (friendly-user) [User ID: {account.auth_user_id}]"
        self.client.force_login(self.admin)
        response = self.client.get(reverse("control_plane:account_source_access", args=[account.pk]))
        self.assertContains(response, identity)
        source = SourceProfile.objects.create(
            name="Direct test source", db_type=DbChoice.POSTGRESQL, host="localhost",
            database_name="synthetic", username="reader",
        )
        response = self.client.post(reverse("control_plane:account_source_access", args=[account.pk]), {
            "action": "save", "source": source.pk, "role": Role.VIEWER,
        }, follow=True)
        self.assertContains(response, f"Data source access updated for {identity}.")
        response = self.client.post(
            reverse("control_plane:user_toggle_active", args=[account.auth_user_id]), follow=True,
        )
        self.assertContains(response, f"User {identity} has been deactivated.")

    def test_direct_source_permissions_change_only_the_selected_grant(self):
        account = self.create_control_plane_account("direct-access")
        first = SourceProfile.objects.create(
            name="Direct first", db_type=DbChoice.POSTGRESQL, host="localhost",
            database_name="synthetic", username="reader",
        )
        second = SourceProfile.objects.create(
            name="Direct second", db_type=DbChoice.POSTGRESQL, host="localhost",
            database_name="synthetic", username="reader",
        )
        existing = UserSourceAccess.objects.create(user=account.auth_user, source=second, role=Role.ADMIN)
        self.client.force_login(self.admin)
        url = reverse("control_plane:account_source_access", args=[account.pk])
        self.client.post(url, {"action": "save", "source": first.pk, "role": Role.ANALYST})
        first_grant = UserSourceAccess.objects.get(user=account.auth_user, source=first)
        self.assertEqual(first_grant.role, Role.ANALYST)
        existing.refresh_from_db()
        self.assertEqual(existing.role, Role.ADMIN)
        self.client.post(url, {"action": "remove", "grant_id": first_grant.pk})
        self.assertFalse(UserSourceAccess.objects.filter(pk=first_grant.pk).exists())
        self.assertTrue(UserSourceAccess.objects.filter(pk=existing.pk, role=Role.ADMIN).exists())
        old_url = reverse("control_plane:user_edit_access", args=[account.auth_user_id])
        self.assertRedirects(self.client.get(old_url), url)
        self.client.post(old_url, {"sources": []})
        self.assertTrue(UserSourceAccess.objects.filter(pk=existing.pk, role=Role.ADMIN).exists())

    def test_legacy_user_list_redirects_to_accounts(self):
        user = self.user_model.objects.create_user(
            username=str(uuid4()), email="unlinked-identity@example.test",
        )
        self.client.force_login(self.admin)
        response = self.client.get(reverse("control_plane:user_management"))
        self.assertRedirects(response, reverse("control_plane:account_list"))
        response = self.client.get(reverse("control_plane:account_list"), {"q": user.email})
        self.assertNotContains(response, user.username)

    def test_account_creation_and_edit_messages_include_name_and_user_id(self):
        self.client.force_login(self.admin)
        data = {
            "username": "message-user", "email": "message@example.test",
            "first_name": "Message", "last_name": "User",
            "password": "Strong-message-password-92!", "is_active": "on",
        }
        response = self.client.post(reverse("control_plane:account_list"), data, follow=True)
        account = ControlPlaneUser.objects.get(email="message@example.test")
        identity = f"Message User ({account.username}) [User ID: {account.auth_user_id}]"
        self.assertContains(response, f"Created Control Plane account {identity}.")
        data.update(first_name="Updated", password="", username=account.username)
        response = self.client.post(
            reverse("control_plane:account_edit", args=[account.pk]), data, follow=True,
        )
        self.assertContains(
            response, f"Updated Control Plane account Updated User ({account.username}) [User ID: {account.auth_user_id}].",
        )

    def test_account_pagination_uses_separate_shared_component_with_filters(self):
        for index in range(16):
            self.create_control_plane_account(f"paging-account-{index:02d}")
        self.client.force_login(self.admin)
        url = reverse("control_plane:account_list")
        response = self.client.get(url, {"q": "paging-account", "status": "active"})
        self.assertRegex(
            response.content.decode(),
            r'</table>\s*</div>\s*</section>\s*<div class="mt-6 flex items-center justify-between bg-gray-900',
        )
        self.assertContains(response, "?q=paging-account&amp;status=active&amp;page=2")
        response = self.client.get(
            url, {"q": "paging-account", "status": "active", "page": 2}
        )
        self.assertContains(response, "?q=paging-account&amp;status=active&amp;page=1")
        self.assertEqual(len(response.context["page_obj"]), 1)

    def test_control_plane_user_profile_edits_sync_to_auth_user(self):
        account = self.create_control_plane_account("self-profile")
        self.client.force_login(account.auth_user)

        response = self.client.post(
            reverse("control_plane:user_profile"),
            {
                "first_name": "Updated",
                "last_name": "Account",
                "email": "updated@example.test",
            },
        )

        self.assertRedirects(response, reverse("control_plane:user_profile"))
        account.refresh_from_db()
        account.auth_user.refresh_from_db()
        self.assertEqual(account.first_name, "Updated")
        self.assertEqual(account.auth_user.first_name, "Updated")
        self.assertEqual(account.email, "updated@example.test")
        self.assertEqual(account.auth_user.email, account.email)

    def test_archive_reuse_email_and_restore_preserve_identity_and_audit(self):
        account = self.create_control_plane_account("archive-user")
        old_auth_id = account.auth_user_id
        old_email = account.email
        profile = ControlPlaneProfile.objects.create(name="Archived user profile")
        ControlPlaneProfileMembership.objects.create(user=account, profile=profile)
        self.client.force_login(self.admin)
        archive_url = reverse("control_plane:account_archive", args=[account.pk])
        response = self.client.post(archive_url, follow=True)
        self.assertContains(response, "Deactivate the account before archiving")
        account.refresh_from_db()
        self.assertIsNone(account.archived_at)

        self.client.post(reverse("control_plane:user_toggle_active", args=[old_auth_id]))
        account.refresh_from_db()
        account.auth_user.refresh_from_db()
        self.assertFalse(account.is_active)
        self.assertFalse(account.auth_user.is_active)
        self.client.post(archive_url)
        account.refresh_from_db()
        self.assertIsNotNone(account.archived_at)
        self.assertEqual(account.history.first().history_user_id, self.admin.pk)
        self.assertTrue(account.profiles.filter(pk=profile.pk).exists())
        accounts = self.client.get(reverse("control_plane:account_list")).context["page_obj"]
        self.assertNotIn(account.pk, [item.pk for item in accounts])
        self.assertRedirects(
            self.client.get(reverse("control_plane:user_management")),
            reverse("control_plane:account_list"),
        )
        self.assertContains(self.client.get(reverse("control_plane:archived_accounts")), account.username)
        for route, arg in (
            ("control_plane:account_edit", account.pk),
            ("control_plane:user_edit", old_auth_id),
            ("control_plane:user_edit_access", old_auth_id),
        ):
            self.assertEqual(self.client.get(reverse(route, args=[arg])).status_code, 404)
        self.assertEqual(self.client.post(reverse("control_plane:user_toggle_active", args=[old_auth_id])).status_code, 404)

        response = self.client.post(reverse("control_plane:account_list"), {
            "email": old_email.upper(), "first_name": "New", "last_name": "Owner",
            "password": "Strong-replacement-password-92!", "is_active": "on",
        })
        self.assertRedirects(response, reverse("control_plane:account_list"))
        replacement = ControlPlaneUser.objects.exclude(pk=account.pk).get(email__iexact=old_email)
        self.assertNotEqual(replacement.auth_user_id, old_auth_id)
        self.assertFalse(replacement.profiles.exists())
        login = Client().post(reverse("control_plane:login"), {
            "username": old_email, "password": "Strong-replacement-password-92!",
        })
        self.assertEqual(login.status_code, 302)
        self.assertEqual(login.client.session["_auth_user_id"], str(replacement.auth_user_id))
        old_login = Client().post(reverse("control_plane:login"), {
            "username": account.username, "password": "Control-password-92!",
        })
        self.assertEqual(old_login.status_code, 200)

        restore_url = reverse("control_plane:account_restore", args=[account.pk])
        response = self.client.post(restore_url, follow=True)
        self.assertContains(response, "Unable to restore the account")
        account.refresh_from_db()
        self.assertIsNotNone(account.archived_at)
        replacement.email = "replacement@example.test"
        replacement.save(update_fields=["email"])
        self.client.post(restore_url)
        account.refresh_from_db()
        account.auth_user.refresh_from_db()
        self.assertIsNone(account.archived_at)
        self.assertFalse(account.is_active)
        self.assertFalse(account.auth_user.is_active)
        self.assertEqual(account.auth_user_id, old_auth_id)
        self.assertTrue(account.profiles.filter(pk=profile.pk).exists())

    def test_archive_and_restore_require_administrator_and_post(self):
        account = self.create_control_plane_account("archive-permission")
        self.client.force_login(account.auth_user)
        self.assertEqual(self.client.post(reverse("control_plane:account_archive", args=[account.pk])).status_code, 302)
        self.client.force_login(self.admin)
        self.assertEqual(self.client.get(reverse("control_plane:account_archive", args=[account.pk])).status_code, 405)
        self.assertEqual(self.client.get(reverse("control_plane:account_restore", args=[account.pk])).status_code, 405)

    def test_user_forms_require_contact_details_and_validate_email(self):
        data = {
            "username": "contact-user",
            "password": "Strong-contact-password-92!",
            "email": "valid@example.test",
            "first_name": "First",
            "last_name": "Last",
            "role": Role.VIEWER,
        }
        cases = [
            ("email", ""), ("email", "   "), ("email", "not-an-email"),
            ("first_name", ""), ("first_name", "  "),
            ("last_name", ""), ("last_name", "  "),
        ]
        for form_class in (
            ControlPlaneUserForm, ControlPlaneSelfProfileForm,
            UserRegistrationWithAccessForm, UserEditForm,
        ):
            for field, value in cases:
                with self.subTest(form=form_class.__name__, field=field, value=value):
                    form = form_class({**data, field: value})
                    self.assertFalse(form.is_valid())
                    self.assertIn(field, form.errors)
            form = form_class({
                **data,
                "email": " valid@example.test ",
                "first_name": " First ",
                "last_name": " Last ",
            })
            self.assertTrue(form.is_valid(), form.errors)
            self.assertEqual(form.cleaned_data["email"], "valid@example.test")
            self.assertEqual(form.cleaned_data["first_name"], "First")
            self.assertEqual(form.cleaned_data["last_name"], "Last")

    def test_account_create_edit_and_profile_reject_invalid_details(self):
        account = self.create_control_plane_account("validation-account")
        original_email = account.email
        self.client.force_login(self.admin)
        response = self.client.post(reverse("control_plane:account_list"), {
            "username": "invalid-new-account", "password": "Strong-contact-password-92!",
            "email": "invalid", "first_name": " ", "last_name": "",
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(set(response.context["form"].errors), {"email", "first_name", "last_name"})
        self.assertFalse(ControlPlaneUser.objects.filter(username="invalid-new-account").exists())

        response = self.client.post(reverse("control_plane:account_edit", args=[account.pk]), {
            "username": account.username, "email": "", "first_name": " ", "last_name": "",
            "is_active": "on",
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(set(response.context["form"].errors), {"email", "first_name", "last_name"})
        account.refresh_from_db()
        self.assertEqual(account.email, original_email)

        response = self.client.post(reverse("control_plane:account_edit", args=[account.pk]), {
            "username": account.username, "email": " new@example.test ",
            "first_name": " New ", "last_name": " Name ", "is_active": "on",
        })
        self.assertRedirects(response, reverse("control_plane:account_list"))
        account.refresh_from_db()
        account.auth_user.refresh_from_db()
        self.assertEqual((account.email, account.first_name, account.last_name),
                         ("new@example.test", "New", "Name"))
        self.assertEqual(account.auth_user.email, account.email)

        self.client.force_login(account.auth_user)
        response = self.client.post(reverse("control_plane:user_profile"), {
            "email": "invalid", "first_name": "", "last_name": " ",
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(set(response.context["form"].errors), {"email", "first_name", "last_name"})
        account.refresh_from_db()
        self.assertEqual(account.email, "new@example.test")

    def test_legacy_user_create_edit_and_self_profile_validation(self):
        self.client.force_login(self.admin)
        invalid = {
            "email": "bad-email", "first_name": " ", "last_name": "",
            "role": Role.VIEWER,
        }
        response = self.client.post(reverse("control_plane:account_list"), {
            **invalid, "username": "legacy-invalid", "password": "Strong-contact-password-92!",
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(set(response.context["form"].errors), {"email", "first_name", "last_name"})
        self.assertFalse(self.user_model.objects.filter(username="legacy-invalid").exists())

        legacy = self.user_model.objects.create_user(
            username="legacy-edit", email="old@example.test", first_name="Old", last_name="Name",
        )
        response = self.client.post(reverse("control_plane:user_edit", args=[legacy.pk]), invalid)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(set(response.context["form"].errors), {"email", "first_name", "last_name"})
        legacy.refresh_from_db()
        self.assertEqual(legacy.email, "old@example.test")

        self.client.force_login(legacy)
        response = self.client.post(reverse("control_plane:user_profile"), {
            **invalid, "update_profile": "1",
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(set(response.context["form"].errors), {"email", "first_name", "last_name"})
        legacy.refresh_from_db()
        self.assertEqual(legacy.email, "old@example.test")
        response = self.client.post(reverse("control_plane:user_profile"), {
            "email": " updated@example.test ", "first_name": " New ", "last_name": " Name ",
            "role": Role.VIEWER, "update_profile": "1",
        })
        self.assertRedirects(response, reverse("control_plane:user_profile"))
        legacy.refresh_from_db()
        self.assertEqual((legacy.email, legacy.first_name, legacy.last_name),
                         ("updated@example.test", "New", "Name"))

    def test_account_and_profile_management_pages_render(self):
        self.client.force_login(self.admin)
        profile = ControlPlaneProfile.objects.create(name="Render profile")

        for route in (
            reverse("control_plane:account_list"),
            reverse("control_plane:profile_list"),
            reverse("control_plane:profile_create"),
            reverse("control_plane:profile_edit", args=[profile.pk]),
            reverse("control_plane:permission_list"),
        ):
            with self.subTest(route=route):
                self.assertEqual(self.client.get(route).status_code, 200)

    def test_profile_and_permission_lists_search_and_paginate(self):
        self.client.force_login(self.admin)
        for index in range(16):
            ControlPlaneProfile.objects.create(name=f"Matching profile {index:02}", is_active=True)
        ControlPlaneProfile.objects.create(name="Matching inactive", is_active=False)
        url = reverse("control_plane:profile_list")
        response = self.client.get(url, {"q": "Matching", "status": "active"})
        self.assertEqual(response.context["page_obj"].paginator.count, 16)
        self.assertContains(response, "q=Matching&amp;status=active&amp;page=2")
        self.assertNotContains(response, "Matching inactive")
        response = self.client.get(url, {"q": "Matching", "status": "active", "page": 2})
        self.assertEqual(len(response.context["page_obj"]), 1)

        for index in range(16):
            ControlPlanePermission.objects.create(
                code=f"test_permission_{index}", name=f"Test permission {index:02}",
                category="Test catalogue",
            )
        url = reverse("control_plane:permission_list")
        response = self.client.get(url, {"q": "Test catalogue"})
        self.assertEqual(response.context["page_obj"].paginator.count, 16)
        self.assertContains(response, "q=Test+catalogue&amp;page=2")
        self.assertEqual(len(self.client.get(url, {"q": "Test catalogue", "page": 2}).context["page_obj"]), 1)

    def test_archived_accounts_search_and_pagination(self):
        self.client.force_login(self.admin)
        for index in range(16):
            account = self.create_control_plane_account(f"archive-match-{index:02}")
            account.archived_at = timezone.now()
            account.save(update_fields=["archived_at"])
        url = reverse("control_plane:archived_accounts")
        response = self.client.get(url, {"q": "archive-match"})
        self.assertEqual(response.context["page_obj"].paginator.count, 16)
        self.assertContains(response, "q=archive-match&amp;page=2")
        self.assertEqual(len(self.client.get(url, {"q": "archive-match", "page": 2}).context["page_obj"]), 1)

    def test_create_profile_with_announcement_permissions_and_assign_account(self):
        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("control_plane:profile_create"),
            {
                "name": "Announcement publishers",
                "is_active": "on",
                "announcement_permissions": [
                    AnnouncementPermission.PUBLISH,
                    AnnouncementPermission.PIN,
                ],
            },
        )
        self.assertRedirects(response, reverse("control_plane:profile_list"))
        profile = ControlPlaneProfile.objects.get(name="Announcement publishers")
        for grant in profile.announcement_permissions.all():
            self.assertEqual(grant.created_by, self.admin)
            self.assertEqual(grant.history.first().history_user, self.admin)

        response = self.client.post(
            reverse("control_plane:account_list"),
            {
                "username": "announcement-user",
                "email": "announcement-user@example.test",
                "first_name": "Announcement",
                "last_name": "User",
                "password": "Strong-Cp-login-92!",
                "is_active": "on",
                "profiles": [profile.pk],
            },
        )
        self.assertRedirects(response, reverse("control_plane:account_list"))
        account = ControlPlaneUser.objects.get(email="announcement-user@example.test")
        self.assertEqual(
            effective_announcement_permissions(account.auth_user),
            {AnnouncementPermission.PUBLISH, AnnouncementPermission.PIN},
        )
        self.assertFalse(account.auth_user.is_staff)
        self.assertEqual(effective_source_roles(account.auth_user), {})
        response = self.client.get(
            reverse("control_plane:account_edit", args=[account.pk])
        )
        self.assertContains(response, "Publish announcements")
        self.assertContains(response, "Pin notices")

    def test_announcement_grants_combine_and_revocation_takes_effect(self):
        account = self.create_control_plane_account("announcement-grants")
        publisher = ControlPlaneProfile.objects.create(name="Publishers")
        cross_plane = ControlPlaneProfile.objects.create(name="Cross-plane publishers")
        membership = ControlPlaneProfileMembership.objects.create(
            user=account, profile=publisher
        )
        ControlPlaneProfileMembership.objects.create(user=account, profile=cross_plane)
        ControlPlaneProfilePermission.objects.create(
            profile=publisher, permission_id=AnnouncementPermission.PUBLISH
        )
        ControlPlaneProfilePermission.objects.create(
            profile=cross_plane, permission_id=AnnouncementPermission.PUBLISH_CROSS_PLANE
        )
        self.assertTrue(
            has_announcement_permission(
                account.auth_user, AnnouncementPermission.PUBLISH_CROSS_PLANE
            )
        )
        publisher.is_active = False
        publisher.save(update_fields=["is_active"])
        self.assertFalse(
            has_announcement_permission(
                account.auth_user, AnnouncementPermission.PUBLISH_CROSS_PLANE
            )
        )
        publisher.is_active = True
        publisher.save(update_fields=["is_active"])
        membership.delete()
        self.assertEqual(
            effective_announcement_permissions(account.auth_user),
            {AnnouncementPermission.PUBLISH_CROSS_PLANE},
        )
        self.assertFalse(
            has_announcement_permission(
                account.auth_user, AnnouncementPermission.PUBLISH_CROSS_PLANE
            )
        )
        account.is_active = False
        account.save(update_fields=["is_active"])
        self.assertEqual(effective_announcement_permissions(account.auth_user), set())
        account.is_active = True
        account.save(update_fields=["is_active"])
        account.auth_user.is_active = False
        self.assertEqual(effective_announcement_permissions(account.auth_user), set())

    def test_announcement_permissions_do_not_follow_admin_flags_or_operations_identity(self):
        self.admin.is_superuser = True
        self.admin.save(update_fields=["is_superuser"])
        for user in (AnonymousUser(), self.admin):
            self.assertEqual(effective_announcement_permissions(user), set())

        account = self.create_control_plane_account("shared-announcement-name")
        profile = ControlPlaneProfile.objects.create(name="Announcement boundary")
        ControlPlaneProfileMembership.objects.create(user=account, profile=profile)
        ControlPlaneProfilePermission.objects.create(
            profile=profile, permission_id=AnnouncementPermission.VIEW_AUDIT
        )
        operations_auth = self.user_model.objects.create_user(username=str(uuid4()))
        OperationsUser.objects.create(
            auth_user=operations_auth,
            username=account.username,
            email=account.email,
            role=OperationsUser.Role.ADMIN,
        )
        self.assertEqual(effective_announcement_permissions(operations_auth), set())
        self.assertTrue(
            has_announcement_permission(account.auth_user, AnnouncementPermission.VIEW_AUDIT)
        )
        self.assertFalse(has_announcement_permission(account.auth_user, "unknown"))

    def test_profile_permission_edits_preserve_source_grants_and_audit_revocation(self):
        self.client.force_login(self.admin)
        profile = ControlPlaneProfile.objects.create(name="Mixed grants")
        source = SourceProfile.objects.create(
            name="Mixed grant source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            database_name="source",
            username="reader",
        )
        source_grant = ControlPlaneProfileSourceAccess.objects.create(
            profile=profile, source=source, role=Role.ANALYST
        )
        grant = ControlPlaneProfilePermission.objects.create(
            profile=profile, permission_id=AnnouncementPermission.PIN
        )
        account = self.create_control_plane_account("mixed-grant-user")
        ControlPlaneProfileMembership.objects.create(user=account, profile=profile)
        url = reverse("control_plane:profile_edit", args=[profile.pk])
        response = self.client.post(
            url,
            {"action": "save_profile", "name": profile.name, "is_active": "on"},
        )
        self.assertRedirects(response, url)
        self.assertFalse(profile.announcement_permissions.exists())
        deleted = ControlPlaneProfilePermission.history.get(
            id=grant.pk, history_type="-"
        )
        self.assertEqual(deleted.history_user, self.admin)
        source_grant.refresh_from_db()
        self.assertEqual(effective_source_roles(account.auth_user), {source.pk: Role.ANALYST})
        self.assertEqual(effective_announcement_permissions(account.auth_user), set())

    def test_inactive_account_cannot_use_source_grants_or_edit_its_profile(self):
        account = self.create_control_plane_account("inactive-access")
        source = SourceProfile.objects.create(
            name="Inactive account source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            database_name="source",
            username="reader",
        )
        UserSourceAccess.objects.create(user=account.auth_user, source=source, role=Role.ANALYST)
        self.client.force_login(account.auth_user)
        self.assertEqual(effective_source_roles(account.auth_user), {source.pk: Role.ANALYST})

        account.is_active = False
        account.save(update_fields=["is_active"])
        self.assertEqual(effective_source_roles(account.auth_user), {})
        response = self.client.post(reverse("control_plane:user_profile"), {
            "email": "changed@example.test", "first_name": "Changed", "last_name": "User",
        })
        self.assertEqual(response.status_code, 404)
        account.refresh_from_db()
        self.assertEqual(account.email, "inactive-access@example.test")

    def test_invalid_or_unauthorized_profile_permission_edit_changes_nothing(self):
        profile = ControlPlaneProfile.objects.create(name="Protected permissions")
        grant = ControlPlaneProfilePermission.objects.create(
            profile=profile, permission_id=AnnouncementPermission.PIN
        )
        url = reverse("control_plane:profile_edit", args=[profile.pk])
        data = {
            "action": "save_profile",
            "name": "Should not save",
            "is_active": "on",
            "announcement_permissions": ["invalid_permission"],
        }
        self.client.force_login(self.admin)
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Select a valid choice")
        account = self.create_control_plane_account("non-admin-publisher")
        ControlPlaneProfileMembership.objects.create(user=account, profile=profile)
        self.client.force_login(account.auth_user)
        data["announcement_permissions"] = [AnnouncementPermission.PUBLISH]
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302)
        profile.refresh_from_db()
        grant.refresh_from_db()
        self.assertEqual(profile.name, "Protected permissions")
        self.assertEqual(grant.permission_id, AnnouncementPermission.PIN)
        self.assertEqual(profile.announcement_permissions.count(), 1)

    def test_permission_catalogue_drives_labels_categories_and_profile_choices(self):
        permission = ControlPlanePermission.objects.get(code=AnnouncementPermission.PIN)
        permission.name = "Pin operational notices"
        permission.category = "Notice management"
        permission.description = "Display a notice prominently."
        permission.save()
        self.client.force_login(self.admin)
        profile = ControlPlaneProfile.objects.create(name="Catalogue-backed profile")
        for route in (
            reverse("control_plane:permission_list"),
            reverse("control_plane:profile_create"),
            reverse("control_plane:profile_edit", args=[profile.pk]),
        ):
            with self.subTest(route=route):
                response = self.client.get(route)
                self.assertContains(response, permission.name)
                self.assertContains(response, permission.category)
                self.assertContains(response, permission.description)
        account = self.create_control_plane_account("catalogue-label-user")
        ControlPlaneProfileMembership.objects.create(user=account, profile=profile)
        ControlPlaneProfilePermission.objects.create(profile=profile, permission=permission)
        response = self.client.get(reverse("control_plane:account_edit", args=[account.pk]))
        self.assertContains(response, permission.name)

    def test_profile_create_is_separate_and_account_pages_have_breadcrumbs(self):
        self.client.force_login(self.admin)
        profile = ControlPlaneProfile.objects.create(name="Breadcrumb profile")
        account = self.create_control_plane_account("breadcrumb-user")
        profile_list = reverse("control_plane:profile_list")
        create = reverse("control_plane:profile_create")

        response = self.client.get(profile_list)
        self.assertContains(response, f'href="{create}"')
        self.assertNotContains(response, 'name="announcement_permissions"')
        response = self.client.get(create)
        self.assertContains(response, 'name="announcement_permissions"')
        self.assertContains(response, f'href="{profile_list}"')
        self.assertContains(response, 'aria-current="page" class="text-slate-400 font-medium">Create Profile')

        for route, parent in (
            (reverse("control_plane:account_edit", args=[account.pk]),
             reverse("control_plane:account_list")),
            (reverse("control_plane:profile_edit", args=[profile.pk]), profile_list),
            (reverse("control_plane:permission_list"), profile_list),
        ):
            with self.subTest(route=route):
                response = self.client.get(route)
                self.assertContains(response, f'href="{parent}"')
                self.assertNotContains(response, "← Control Plane accounts")
                self.assertNotContains(response, "← Shared access profiles")

    def test_profile_create_invalid_or_non_admin_does_not_save(self):
        url = reverse("control_plane:profile_create")
        self.client.force_login(self.admin)
        response = self.client.post(url, {
            "name": "Invalid profile",
            "is_active": "on",
            "announcement_permissions": ["not-a-permission"],
        })
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Select a valid choice")
        self.assertFalse(ControlPlaneProfile.objects.filter(name="Invalid profile").exists())

        account = self.create_control_plane_account("profile-create-non-admin")
        self.client.force_login(account.auth_user)
        response = self.client.post(url, {"name": "Unauthorized", "is_active": "on"})
        self.assertEqual(response.status_code, 302)
        self.assertFalse(ControlPlaneProfile.objects.filter(name="Unauthorized").exists())

    def test_inactive_catalogue_permission_is_not_selectable_or_effective(self):
        account = self.create_control_plane_account("inactive-definition-user")
        profile = ControlPlaneProfile.objects.create(name="Inactive definition profile")
        ControlPlaneProfileMembership.objects.create(user=account, profile=profile)
        permission = ControlPlanePermission.objects.get(code=AnnouncementPermission.PIN)
        grant = ControlPlaneProfilePermission.objects.create(
            profile=profile, permission=permission
        )
        permission.is_active = False
        permission.save(update_fields=["is_active"])
        self.assertEqual(effective_announcement_permissions(account.auth_user), set())
        self.client.force_login(self.admin)
        url = reverse("control_plane:profile_edit", args=[profile.pk])
        response = self.client.get(url)
        self.assertNotContains(response, permission.name)
        data = {"action": "save_profile", "name": profile.name, "is_active": "on"}
        response = self.client.post(
            url, {**data, "announcement_permissions": [permission.code]}
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Select a valid choice")
        response = self.client.post(url, data)
        self.assertRedirects(response, url)
        self.assertTrue(ControlPlaneProfilePermission.objects.filter(pk=grant.pk).exists())
        permission.is_active = True
        permission.save(update_fields=["is_active"])
        self.assertEqual(
            effective_announcement_permissions(account.auth_user),
            {AnnouncementPermission.PIN},
        )
        self.client.force_login(account.auth_user)
        self.assertEqual(
            self.client.get(reverse("control_plane:permission_list")).status_code, 302
        )

    def test_dashboard_welcome_uses_app_identity_and_keeps_legacy_fallback(self):
        account = self.create_control_plane_account("welcome-user")
        request = RequestFactory().get("/control-plane/")
        request.user = account.auth_user
        template = "control_plane/dashboard/components/welcome_banner.html"
        content = render_to_string(template, request=request)
        self.assertIn("Welcome back, welcome-user", content)
        self.assertNotIn(account.auth_user.username, content)
        account.first_name = "Welcome"
        account.save(update_fields=["first_name"])
        request.user = self.user_model.objects.get(pk=account.auth_user_id)
        self.assertIn("Welcome back, Welcome", render_to_string(template, request=request))
        request.user = self.admin
        self.assertIn(
            "Welcome back, legacy-admin", render_to_string(template, request=request)
        )
        self.admin.first_name = "Legacy"
        self.assertIn("Welcome back, Legacy", render_to_string(template, request=request))

    @patch("control_plane.management.commands.create_control_plane_user.getpass")
    def test_create_command_assigns_uuid_user_and_shared_profiles(self, getpass):
        profile = ControlPlaneProfile.objects.create(name="CLI profile")
        getpass.side_effect = ["Strong-cli-password-92!", "Strong-cli-password-92!"]

        call_command(
            "create_control_plane_user",
            "--username",
            "cli-control-user",
            "--email",
            "cli@example.test",
            "--first-name",
            "CLI",
            "--last-name",
            "User",
            "--profile",
            profile.name,
        )

        account = ControlPlaneUser.objects.get(username="cli-control-user")
        UUID(account.auth_user.username)
        self.assertTrue(account.auth_user.check_password("Strong-cli-password-92!"))
        self.assertEqual(list(account.profiles.all()), [profile])
        self.assertFalse(account.auth_user.is_staff)
        self.assertFalse(account.auth_user.is_superuser)

    @patch("control_plane.management.commands.create_control_plane_user.getpass")
    def test_create_command_can_grant_global_staff_access(self, getpass):
        getpass.side_effect = ["Strong-staff-password-92!", "Strong-staff-password-92!"]

        call_command(
            "create_control_plane_user",
            "--username",
            "cli-control-admin",
            "--email",
            "admin@example.test",
            "--first-name",
            "Admin",
            "--last-name",
            "User",
            "--staff",
        )

        account = ControlPlaneUser.objects.get(username="cli-control-admin")
        self.assertTrue(account.auth_user.is_staff)
        self.assertFalse(account.auth_user.is_superuser)

    @patch("control_plane.management.commands.create_control_plane_user.getpass")
    def test_create_command_can_create_a_missing_empty_profile(self, getpass):
        getpass.side_effect = ["Strong-profile-password-92!", "Strong-profile-password-92!"]

        call_command(
            "create_control_plane_user",
            "--username",
            "profile-cli-user",
            "--email",
            "profile@example.test",
            "--first-name",
            "Profile",
            "--last-name",
            "User",
            "--profile",
            "New profile",
            "--create-profile",
        )

        account = ControlPlaneUser.objects.get(username="profile-cli-user")
        profile = ControlPlaneProfile.objects.get(name="New profile")
        self.assertEqual(list(account.profiles.all()), [profile])
        self.assertFalse(profile.source_access.exists())

    @patch("control_plane.management.commands.create_control_plane_user.getpass")
    def test_create_command_rejects_incomplete_email_before_password_prompt(self, getpass):
        with self.assertRaises(CommandError):
            call_command(
                "create_control_plane_user",
                "--username",
                "invalid-email-user",
                "--email",
                "keith.williams",
            )

        getpass.assert_not_called()
        self.assertFalse(
            ControlPlaneUser.objects.filter(username="invalid-email-user").exists()
        )

    @patch("control_plane.management.commands.create_control_plane_user.getpass")
    def test_create_command_requires_contact_details_before_password_prompt(self, getpass):
        valid = {
            "email": "cli@example.test",
            "first_name": "CLI",
            "last_name": "User",
        }
        for field in valid:
            for value in ("", "  "):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(CommandError):
                        call_command(
                            "create_control_plane_user", username="incomplete-cli-user",
                            **{**valid, field: value},
                        )
        getpass.assert_not_called()
        self.assertFalse(ControlPlaneUser.objects.filter(username="incomplete-cli-user").exists())

    def test_shared_profiles_are_visible_in_account_navigation(self):
        account = self.create_control_plane_account("profile-nav")
        profile = ControlPlaneProfile.objects.create(name="Navigation profile")
        ControlPlaneProfileMembership.objects.create(user=account, profile=profile)
        source = SourceProfile.objects.create(
            name="Navigation profile source",
            db_type=DbChoice.POSTGRESQL,
            host="localhost",
            database_name="source",
            username="reader",
        )
        ControlPlaneProfileSourceAccess.objects.create(
            profile=profile,
            source=source,
            role=Role.VIEWER,
        )
        self.client.force_login(account.auth_user)

        response = self.client.get(reverse("control_plane:user_profile"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response,
            reverse("control_plane:cdc_rebuild_list"),
        )


class PermissionCatalogueMigrationTests(TransactionTestCase):
    def test_upgrade_preserves_existing_grants_and_history_without_automatic_grants(self):
        previous = [("control_plane", "0052_profile_announcement_permissions")]
        current = [("control_plane", "0055_archive_control_plane_users")]
        executor = MigrationExecutor(connection)
        try:
            executor.migrate(previous)
            old_apps = executor.loader.project_state(previous).apps
            profile = old_apps.get_model(
                "control_plane", "ControlPlaneProfile"
            ).objects.create(name="Existing permission profile")
            grant = old_apps.get_model(
                "control_plane", "ControlPlaneProfilePermission"
            ).objects.create(profile_id=profile.pk, permission="pin_announcements")
            history = old_apps.get_model(
                "control_plane", "HistoricalControlPlaneProfilePermission"
            ).objects.create(
                id=grant.pk,
                profile_id=profile.pk,
                permission="pin_announcements",
                history_date=timezone.now(),
                history_type="+",
            )
            executor = MigrationExecutor(connection)
            executor.migrate(current)
            new_apps = executor.loader.project_state(current).apps
            permission_model = new_apps.get_model("control_plane", "ControlPlanePermission")
            new_grants = new_apps.get_model("control_plane", "ControlPlaneProfilePermission")
            new_history = new_apps.get_model(
                "control_plane", "HistoricalControlPlaneProfilePermission"
            )
            self.assertEqual(
                new_grants.objects.get(pk=grant.pk).permission_id, "pin_announcements"
            )
            self.assertEqual(
                new_history.objects.get(pk=history.pk).permission_id, "pin_announcements"
            )
            self.assertEqual(permission_model.objects.count(), 5)
            self.assertEqual(new_grants.objects.count(), 1)
        finally:
            executor = MigrationExecutor(connection)
            executor.migrate(executor.loader.graph.leaf_nodes())
