from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import get_object_or_404, redirect, render
from django.http import Http404
from django.views.decorators.http import require_POST
from django.contrib.auth.models import User
from control_plane.models import ControlPlaneUser, TrackedUser
from control_plane.forms import UserEditForm, CustomPasswordChangeForm
from control_plane.utils import add_email_conflict_error, user_identity
from django.db import IntegrityError, transaction
from django.db.models import Q
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth import update_session_auth_hash
from django.contrib import messages
from django.urls import reverse


def is_admin(user):
    return user.is_staff or user.is_superuser


@login_required
@user_passes_test(is_admin)
def user_management_view(request):
    if request.method == 'POST':
        messages.error(request, "User management has moved. Please submit the account form again.")
    url = reverse('control_plane:account_list')
    if request.method == 'GET' and request.META.get('QUERY_STRING'):
        url += '?' + request.META['QUERY_STRING']
    return redirect(url)


@login_required
@user_passes_test(is_admin)
@require_POST
@transaction.atomic
def user_toggle_active_view(request, user_id):
    target_user = get_object_or_404(TrackedUser, id=user_id)
    account = ControlPlaneUser.objects.select_for_update().filter(auth_user_id=user_id).first()
    if account and account.archived_at:
        raise Http404
    deactivate = target_user.is_active

    if deactivate and target_user.pk == request.user.pk:
        messages.error(request, "You cannot deactivate your own account.")
        return redirect('control_plane:account_list')

    if deactivate and (target_user.is_staff or target_user.is_superuser):
        another_active_admin_exists = User.objects.filter(
            is_active=True,
        ).filter(
            Q(is_staff=True) | Q(is_superuser=True)
        ).exclude(pk=target_user.pk).exists()
        if not another_active_admin_exists:
            messages.error(request, "You cannot deactivate the last active system administrator.")
            return redirect('control_plane:account_list')

    target_user.is_active = not target_user.is_active
    target_user._history_user = request.user
    target_user._change_reason = (
        f"Account {'deactivated' if deactivate else 'reactivated'} by {request.user.username}"
    )
    target_user.save(update_fields=['is_active'])
    if account:
        account.is_active = target_user.is_active
        account.updated_by = request.user
        account._history_user = request.user
        account.save(update_fields=['is_active', 'updated_by', 'updated_at'])

    status_text = "deactivated" if deactivate else "reactivated"
    messages.success(request, f"User {user_identity(target_user)['label']} has been {status_text}.")
    return redirect('control_plane:account_list')


@login_required
@user_passes_test(is_admin)
@transaction.atomic
def user_edit_access_view(request, user_id):
    account = get_object_or_404(
        ControlPlaneUser.objects.select_for_update(),
        auth_user_id=user_id, archived_at__isnull=True,
    )
    if request.method == 'POST':
        messages.error(request, "Source access management has moved. Please submit the permission again.")
    return redirect('control_plane:account_source_access', account_id=account.pk)


@login_required
@user_passes_test(is_admin)
@transaction.atomic
def user_edit_view(request, user_id):
    target_user = get_object_or_404(TrackedUser, id=user_id)
    account = ControlPlaneUser.objects.select_for_update().filter(auth_user_id=user_id).first()
    if account and account.archived_at:
        raise Http404
    if account:
        if request.method == 'POST':
            messages.error(request, "Account editing has moved. Please submit the account form again.")
        return redirect('control_plane:account_edit', account_id=account.pk)
    
    if request.method == 'POST':
        form = UserEditForm(request.POST, instance=target_user, actor=request.user)
        if form.is_valid():
            try:
                user = form.save()
            except IntegrityError:
                account = getattr(target_user, 'control_plane_account', None)
                if not account or not add_email_conflict_error(form, account.pk):
                    raise
            else:
                messages.success(request, f"User details updated successfully for {user_identity(user)['label']}.")
                return redirect('control_plane:account_list')
    else:
        form = UserEditForm(instance=target_user)

    return render(request, 'control_plane/user_edit.html', {
        'form': form,
        'target_user': target_user,
        'target_identity': user_identity(target_user),
    })


@login_required
def password_change_view(request):
    if request.method == 'POST':
        form = PasswordChangeForm(request.user, request.POST)
        if form.is_valid():
            user = form.save()
            
            # Clear the rotation flag if present
            if hasattr(user, 'must_change_password'):
                user.must_change_password = False
                user.save()

            # Record audit entry for password change
            if hasattr(user, 'history'):
                user.history.create(
                    instance=user,
                    history_user=request.user,
                    history_type='~'
                )
                
            # Keep the user logged in after password change
            update_session_auth_hash(request, user)
            messages.success(request, 'Your password has been successfully updated.')
            return redirect('control_plane:dashboard')
    else:
        form = PasswordChangeForm(request.user)

    return render(request, 'control_plane/password_change.html', {'form': form})


@login_required
def user_profile_view(request):
    form = UserEditForm(instance=request.user)
    password_form = CustomPasswordChangeForm(request.user)

    if request.method == 'POST':
        if 'update_profile' in request.POST:
            form = UserEditForm(request.POST, instance=request.user)
            if form.is_valid():
                user = form.save()

                # Record audit entry for self-service profile update
                if hasattr(user, 'history'):
                    user.history.create(
                        instance=user,
                        history_user=request.user,
                        history_type='~'
                    )

                messages.success(request, "Your profile details were successfully updated.")
                return redirect('control_plane:user_profile')
        elif 'change_password' in request.POST:
            password_form = CustomPasswordChangeForm(request.user, request.POST)
            if password_form.is_valid():
                user = password_form.save()

                # Record audit entry for password update
                if hasattr(user, 'history'):
                    user.history.create(
                        instance=user,
                        history_user=request.user,
                        history_type='~'
                    )

                update_session_auth_hash(request, user)
                messages.success(request, "Your password has been successfully updated.")
                return redirect('control_plane:user_profile')

    return render(request, 'control_plane/user_profile.html', {
        'form': form,
        'password_form': password_form,
    })