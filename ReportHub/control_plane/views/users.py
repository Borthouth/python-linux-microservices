from django.core.paginator import Paginator
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import get_object_or_404, redirect, render
from django.contrib.auth.models import User, Group
from control_plane.models import SourceProfile, UserSourceAccess, Role, TrackedUser
from control_plane.forms import UserRegistrationWithAccessForm, UserEditForm, CustomPasswordChangeForm
from django.db.models import Q
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth import update_session_auth_hash
from django.contrib import messages


def is_admin(user):
    return user.is_staff or user.is_superuser


@login_required
@user_passes_test(is_admin)
def user_management_view(request):
    search_query = request.GET.get('q', '').strip()
    users_qs = User.objects.all().order_by('-date_joined')

    if search_query:
        users_qs = users_qs.filter(
            Q(username__icontains=search_query) | 
            Q(email__icontains=search_query) |
            Q(first_name__icontains=search_query) |
            Q(last_name__icontains=search_query)
        )

    if request.method == 'POST':
        form = UserRegistrationWithAccessForm(request.POST)
        if form.is_valid():
            user = form.save()
            
            # Record audit entry for new user creation
            if hasattr(user, 'history'):
                user.history.create(
                    instance=user,
                    history_user=request.user,
                    history_type='+'
                )

            messages.success(request, "New platform user onboarded successfully.")
            return redirect('control_plane:user_management')
    else:
        form = UserRegistrationWithAccessForm()

    paginator = Paginator(users_qs, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    return render(request, 'control_plane/user_management.html', {
        'form': form,
        'page_obj': page_obj,
        'search_query': search_query,
    })


@login_required
@user_passes_test(is_admin)
def user_edit_access_view(request, user_id):
    target_user = get_object_or_404(User, id=user_id)
    all_sources = SourceProfile.objects.all().order_by('name')

    # Capture search query for data sources
    source_search = request.GET.get('q', '').strip()
    if source_search:
        all_sources = all_sources.filter(
            Q(name__icontains=source_search) | 
            Q(db_type__icontains=source_search) | 
            Q(database_name__icontains=source_search)
        )

    if request.method == 'POST':
        # Handle data source permission checkboxes
        granted_source_ids = request.POST.getlist('sources')
        
        # Standard model deletes and creates automatically fire django-simple-history signals for UserSourceAccess
        UserSourceAccess.objects.filter(user=target_user).delete()
        
        for source_id in granted_source_ids:
            source = get_object_or_404(SourceProfile, id=source_id)
            UserSourceAccess.objects.create(
                user=target_user,
                source=source
            )

        messages.success(request, f"Data source access updated successfully for {target_user.username}.")
        return redirect('control_plane:user_management')

    # Paginate sources
    paginator = Paginator(all_sources, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # Get current user's assigned sources
    assigned_source_ids = list(UserSourceAccess.objects.filter(user=target_user).values_list('source_id', flat=True))

    return render(request, 'control_plane/user_edit_access.html', {
        'target_user': target_user,
        'page_obj': page_obj,
        'source_search': source_search,
        'user_assigned_source_ids': assigned_source_ids,
    })


@login_required
@user_passes_test(is_admin)
def user_edit_view(request, user_id):
    target_user = get_object_or_404(TrackedUser, id=user_id)
    
    if request.method == 'POST':
        form = UserEditForm(request.POST, instance=target_user)
        if form.is_valid():
            user = form.save() # Proxy model save automatically triggers history tracking!

            messages.success(request, f"User details updated successfully for {target_user.username}.")
            return redirect('control_plane:user_management')
    else:
        form = UserEditForm(instance=target_user)

    return render(request, 'control_plane/user_edit.html', {
        'form': form,
        'target_user': target_user,
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