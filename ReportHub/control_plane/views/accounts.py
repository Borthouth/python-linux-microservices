from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required, user_passes_test
from django.core.exceptions import ValidationError
from django.core.paginator import Paginator
from django.db import IntegrityError, transaction
from django.db.models import Q
from django.http import Http404, HttpResponseBadRequest
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from control_plane.access import effective_announcement_permissions
from control_plane.forms.accounts import (
    ControlPlaneProfileForm,
    ControlPlaneProfileSourceAccessForm,
    ControlPlaneSelfProfileForm,
    ControlPlaneUserForm,
)
from control_plane.models import (
    ControlPlanePermission,
    ControlPlaneProfile,
    ControlPlaneProfileSourceAccess,
    ControlPlaneUser,
    UserSourceAccess,
)
from control_plane.utils import add_email_conflict_error, user_identity, validate_control_plane_email


def _is_admin(user):
    return user.is_staff or user.is_superuser


@login_required
@user_passes_test(_is_admin)
def account_list_view(request):
    if request.method == "POST":
        form = ControlPlaneUserForm(request.POST)
        if form.is_valid():
            try:
                account = form.save(request.user)
            except IntegrityError:
                if not add_email_conflict_error(form):
                    raise
            else:
                messages.success(request, f"Created Control Plane account {user_identity(account.auth_user)['label']}.")
                return redirect("control_plane:account_list")
    else:
        form = ControlPlaneUserForm()

    accounts = ControlPlaneUser.objects.filter(archived_at__isnull=True).select_related("auth_user").prefetch_related(
        "profiles"
    )
    query = request.GET.get("q", "").strip()
    status = request.GET.get("status", "")
    if query:
        accounts = accounts.filter(
            Q(username__icontains=query)
            | Q(email__icontains=query)
            | Q(first_name__icontains=query)
            | Q(last_name__icontains=query)
        )
    if status == "active":
        accounts = accounts.filter(is_active=True)
    elif status == "inactive":
        accounts = accounts.filter(is_active=False)

    page_obj = Paginator(accounts, 15).get_page(request.GET.get("page"))
    return render(request, "control_plane/accounts/account_list.html", {
        "form": form,
        "page_obj": page_obj,
        "search_query": query,
        "status_filter": status,
        "filter_options": [
            {"value": "active", "label": "Active"},
            {"value": "inactive", "label": "Inactive"},
        ],
    })


@login_required
@user_passes_test(_is_admin)
@transaction.atomic
def account_edit_view(request, account_id):
    account = get_object_or_404(
        ControlPlaneUser.objects.select_for_update().filter(
            archived_at__isnull=True,
        ).select_related("auth_user").prefetch_related("profiles"),
        pk=account_id,
    )
    form = ControlPlaneUserForm(
        request.POST or None,
        account=account,
    )
    if request.method == "POST" and form.is_valid():
        if (
            account.auth_user.is_active
            and (not form.cleaned_data["is_active"] or not form.cleaned_data["is_staff"])
            and account.auth_user.is_staff
        ):
            other_admin_exists = (
                get_user_model().objects.filter(is_active=True)
                .filter(Q(is_staff=True) | Q(is_superuser=True))
                .exclude(pk=account.auth_user_id)
                .exists()
            )
            if not other_admin_exists:
                form.add_error(
                    None,
                    "You cannot deactivate or remove staff access from the last active system administrator.",
                )

        if not form.errors:
            try:
                form.save(request.user)
            except IntegrityError:
                if not add_email_conflict_error(form, account.pk):
                    raise
            else:
                messages.success(request, f"Updated Control Plane account {user_identity(account.auth_user)['label']}.")
                return redirect("control_plane:account_list")

    permissions = effective_announcement_permissions(account.auth_user)
    return render(request, "control_plane/accounts/account_edit.html", {
        "account": account,
        "form": form,
        "announcement_permissions": ControlPlanePermission.objects.filter(
            code__in=permissions,
        ),
    })


@login_required
@user_passes_test(_is_admin)
@transaction.atomic
def account_source_access_view(request, account_id):
    account = get_object_or_404(
        ControlPlaneUser.objects.select_for_update().select_related("auth_user"),
        pk=account_id, archived_at__isnull=True,
    )
    form = ControlPlaneProfileSourceAccessForm()
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "save":
            form = ControlPlaneProfileSourceAccessForm(request.POST)
            if form.is_valid():
                grant, created = UserSourceAccess.objects.get_or_create(
                    user=account.auth_user,
                    source=form.cleaned_data["source"],
                    defaults={
                        "role": form.cleaned_data["role"],
                        "created_by": request.user,
                        "updated_by": request.user,
                    },
                )
                if not created:
                    grant.role = form.cleaned_data["role"]
                    grant.updated_by = request.user
                    grant._history_user = request.user
                    grant.save(update_fields=["role", "updated_by", "updated_at"])
                messages.success(request, f"Data source access updated for {user_identity(account.auth_user)['label']}.")
                return redirect("control_plane:account_source_access", account_id=account.pk)
        elif action == "remove":
            if not request.POST.get("grant_id", "").isdigit():
                return HttpResponseBadRequest("Invalid source access grant.")
            grant = get_object_or_404(
                UserSourceAccess, user=account.auth_user, pk=request.POST.get("grant_id"),
            )
            grant._history_user = request.user
            grant.delete()
            messages.success(request, f"Data source access removed for {user_identity(account.auth_user)['label']}.")
            return redirect("control_plane:account_source_access", account_id=account.pk)
        else:
            messages.error(request, "Invalid source access action.")
    return render(request, "control_plane/accounts/account_source_access.html", {
        "account": account,
        "form": form,
        "grants": account.auth_user.source_access.select_related("source").order_by("source__name"),
    })


@login_required
@user_passes_test(_is_admin)
@require_GET
def archived_accounts_view(request):
    query = request.GET.get("q", "").strip()
    accounts = ControlPlaneUser.objects.filter(archived_at__isnull=False).select_related(
        "auth_user",
    ).order_by("-archived_at", "-pk")
    if query:
        accounts = accounts.filter(
            Q(username__icontains=query) | Q(email__icontains=query)
            | Q(first_name__icontains=query) | Q(last_name__icontains=query)
        )
    return render(request, "control_plane/accounts/archived_accounts.html", {
        "page_obj": Paginator(accounts, 15).get_page(request.GET.get("page")),
        "search_query": query,
    })


@login_required
@user_passes_test(_is_admin)
@require_POST
@transaction.atomic
def account_archive_view(request, account_id):
    account = get_object_or_404(
        ControlPlaneUser.objects.select_for_update().select_related("auth_user"),
        pk=account_id, archived_at__isnull=True,
    )
    if account.is_active or account.auth_user.is_active:
        messages.error(request, "Deactivate the account before archiving it.")
    else:
        account.archived_at = timezone.now()
        account.updated_by = request.user
        account._history_user = request.user
        account.save(update_fields=["archived_at", "updated_by", "updated_at"])
        messages.success(request, f"Archived {user_identity(account.auth_user)['label']}.")
    return redirect("control_plane:account_list")


@login_required
@user_passes_test(_is_admin)
@require_POST
def account_restore_view(request, account_id):
    try:
        with transaction.atomic():
            account = get_object_or_404(
                ControlPlaneUser.objects.select_for_update().select_related("auth_user"),
                pk=account_id, archived_at__isnull=False,
            )
            validate_control_plane_email(account.email, account.pk)
            account.archived_at = None
            account.is_active = False
            account.updated_by = request.user
            account._history_user = request.user
            account.save(update_fields=["archived_at", "is_active", "updated_by", "updated_at"])
            account.auth_user.is_active = False
            account.auth_user.save(update_fields=["is_active"])
    except ValidationError:
        messages.error(request, "Unable to restore the account. Check its email address or contact an administrator.")
    except IntegrityError:
        try:
            validate_control_plane_email(account.email, account.pk)
        except ValidationError:
            messages.error(request, "Unable to restore the account. Check its email address or contact an administrator.")
        else:
            raise
    else:
        messages.success(request, f"Restored {user_identity(account.auth_user)['label']} as inactive.")
    return redirect("control_plane:archived_accounts")


@login_required
@transaction.atomic
def account_profile_view(request):
    account = ControlPlaneUser.objects.select_for_update().filter(auth_user=request.user).first()
    if account is None:
        from control_plane.views.users import user_profile_view

        return user_profile_view(request)
    if account.archived_at or not account.is_active:
        raise Http404

    form = ControlPlaneSelfProfileForm(request.POST or None, instance=account)
    if request.method == "POST" and form.is_valid():
        try:
            with transaction.atomic():
                account = form.save(commit=False)
                account.updated_by = request.user
                account.save()
                auth_user = request.user
                auth_user.email = account.email
                auth_user.first_name = account.first_name
                auth_user.last_name = account.last_name
                auth_user.save(update_fields=["email", "first_name", "last_name"])
        except IntegrityError:
            if not add_email_conflict_error(form, account.pk):
                raise
        else:
            messages.success(request, "Your profile details were updated.")
            return redirect("control_plane:user_profile")

    return render(request, "control_plane/user_profile.html", {"form": form})


@login_required
@user_passes_test(_is_admin)
def profile_list_view(request):
    if request.method == "POST":
        return profile_create_view(request)
    profiles = ControlPlaneProfile.objects.prefetch_related("memberships").order_by("name")
    query = request.GET.get("q", "").strip()
    status = request.GET.get("status", "")
    if query:
        profiles = profiles.filter(Q(name__icontains=query) | Q(description__icontains=query))
    if status in ("active", "inactive"):
        profiles = profiles.filter(is_active=status == "active")
    return render(request, "control_plane/accounts/profile_list.html", {
        "page_obj": Paginator(profiles, 15).get_page(request.GET.get("page")),
        "search_query": query,
        "status_filter": status,
        "filter_options": [
            {"value": "active", "label": "Active"},
            {"value": "inactive", "label": "Inactive"},
        ],
    })


@login_required
@user_passes_test(_is_admin)
def profile_create_view(request):
    if request.method == "POST":
        form = ControlPlaneProfileForm(request.POST)
        if form.is_valid():
            profile = form.save(request.user)
            messages.success(request, f'Created access profile "{profile.name}".')
            return redirect("control_plane:profile_list")
    else:
        form = ControlPlaneProfileForm()
    return render(request, "control_plane/accounts/profile_create.html", {
        "form": form,
    })


@login_required
@user_passes_test(_is_admin)
def profile_edit_view(request, profile_id):
    profile = get_object_or_404(ControlPlaneProfile, pk=profile_id)
    grant_form = ControlPlaneProfileSourceAccessForm()
    save_profile = request.method == "POST" and request.POST.get("action") == "save_profile"
    form = ControlPlaneProfileForm(
        request.POST if save_profile else None,
        instance=profile,
    )
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "save_profile" and form.is_valid():
            form.save(request.user)
            messages.success(request, f'Updated access profile "{profile.name}".')
            return redirect("control_plane:profile_edit", profile_id=profile.pk)
        if action == "add_source":
            grant_form = ControlPlaneProfileSourceAccessForm(request.POST)
            if grant_form.is_valid():
                grant, created = ControlPlaneProfileSourceAccess.objects.get_or_create(
                    profile=profile,
                    source=grant_form.cleaned_data["source"],
                    defaults={
                        "role": grant_form.cleaned_data["role"],
                        "created_by": request.user,
                        "updated_by": request.user,
                    },
                )
                if not created:
                    grant.role = grant_form.cleaned_data["role"]
                    grant.updated_by = request.user
                    grant.save(update_fields=["role", "updated_by"])
                messages.success(request, "Profile source access saved.")
                return redirect("control_plane:profile_edit", profile_id=profile.pk)

    grants = profile.source_access.select_related("source").order_by("source__name")
    return render(request, "control_plane/accounts/profile_edit.html", {
        "profile": profile,
        "form": form,
        "grant_form": grant_form,
        "grants": grants,
        "members": profile.memberships.select_related("user").order_by("user__username"),
    })


@login_required
@user_passes_test(_is_admin)
@require_POST
def profile_source_remove_view(request, profile_id, grant_id):
    grant = get_object_or_404(
        ControlPlaneProfileSourceAccess,
        pk=grant_id,
        profile_id=profile_id,
    )
    grant.delete()
    messages.success(request, "Profile source access removed.")
    return redirect("control_plane:profile_edit", profile_id=profile_id)


@login_required
@user_passes_test(_is_admin)
@require_GET
def permission_list_view(request):
    query = request.GET.get("q", "").strip()
    permissions = ControlPlanePermission.objects.all()
    if query:
        permissions = permissions.filter(
            Q(name__icontains=query) | Q(code__icontains=query)
            | Q(category__icontains=query) | Q(description__icontains=query)
        )
    return render(request, "control_plane/accounts/permission_list.html", {
        "page_obj": Paginator(permissions, 15).get_page(request.GET.get("page")),
        "search_query": query,
    })
