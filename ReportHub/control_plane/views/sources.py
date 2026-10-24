import pymysql
import psycopg2
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, get_object_or_404, redirect
from django.core.paginator import Paginator
from django.utils import timezone
from django.contrib import messages
from control_plane.models import SourceProfile, DbChoice, SyncStatus
from control_plane.forms import SourceProfileForm

from etl.tasks import test_source_connection_task


def is_admin(user):
    return user.is_staff or user.is_superuser

@login_required
@user_passes_test(is_admin)
def source_list_view(request):
    sources_qs = SourceProfile.objects.all().order_by('name')
    
    paginator = Paginator(sources_qs, 15) # 15 items per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
    }
    return render(request, 'control_plane/source_list.html', context)

@login_required
@user_passes_test(is_admin)
def source_create_view(request):
    if request.method == 'POST':
        form = SourceProfileForm(request.POST)
        if form.is_valid():
            saved_profile = form.save()
            
            if 'save_and_test' in request.POST:
                test_source_connection_task.delay(saved_profile.pk)
                messages.success(request, f"Data source '{saved_profile.name}' created. Connection test is running in the background.")
            else:
                messages.success(request, f"Data source '{saved_profile.name}' created successfully.")
                
            return redirect('control_plane:source_list')
    else:
        form = SourceProfileForm()
    return render(request, 'control_plane/source_edit.html', {'form': form})

@login_required
@user_passes_test(is_admin)
def source_edit_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk)
    if request.method == 'POST':
        form = SourceProfileForm(request.POST, instance=profile)
        if form.is_valid():
            saved_profile = form.save()
            
            if 'save_and_test' in request.POST:
                saved_profile.last_sync_status = SyncStatus.RUNNING
                saved_profile.save()
                test_source_connection_task.delay(saved_profile.pk)
                messages.info(request, f"Data source '{saved_profile.name}' saved. Connection test is running in the background.")
            else:
                messages.success(request, f"Data source '{saved_profile.name}' saved successfully.")
                
            return redirect('control_plane:source_list')
    else:
        form = SourceProfileForm(instance=profile)
    
    return render(request, 'control_plane/source_edit.html', {'form': form, 'profile': profile})

@login_required
@user_passes_test(is_admin)
def source_sync_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk)
    
    if not profile.is_active:
        messages.error(request, f"Cannot sync inactive data source: {profile.name}")
        return redirect('control_plane:source_list')

    profile.last_sync_status = SyncStatus.RUNNING
    profile.save()
    test_source_connection_task.delay(profile.pk)
    messages.info(request, f"Connection test started in the background for '{profile.name}'.")

    return redirect('control_plane:source_list')

@login_required
@user_passes_test(is_admin)
def source_delete_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk)
    if request.method == 'POST':
        profile.delete()
        messages.success(request, f"Data source '{profile.name}' deleted successfully.")
        return redirect('control_plane:source_list')
    
    return render(request, 'control_plane/source_confirm_delete.html', {'profile': profile})

@login_required
@user_passes_test(is_admin)
def source_toggle_active_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk)
    profile.is_active = not profile.is_active
    profile.save()
    status_text = "activated" if profile.is_active else "deactivated"
    messages.success(request, f"Data source '{profile.name}' has been {status_text}.")
    return redirect('control_plane:source_list')

@login_required
@user_passes_test(is_admin)
def source_sync_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk)
    
    if not profile.is_active:
        messages.error(request, f"Cannot sync inactive data source: {profile.name}")
        return redirect('control_plane:source_list')

    # Trigger asynchronous connection test/sync via Celery
    test_source_connection_task.delay(profile.pk)
    messages.success(request, f"Connection test started in the background for '{profile.name}'.")

    return redirect('control_plane:source_list')

@login_required
@user_passes_test(is_admin)
def source_status_view(request, pk):
    source = get_object_or_404(SourceProfile, pk=pk)
    
    # If the background task has completed successfully
    if source.last_sync_status == SyncStatus.SUCCESS:
        # Check if we already added a message for this sync timestamp to prevent duplicates
        session_key = f'sync_msg_shown_{source.pk}_{source.last_synced}'
        if not request.session.get(session_key):
            messages.success(request, f"Connection test complete for '{source.name}'! The background verification finished successfully.")
            request.session[session_key] = True
            
        # Render the component and force an HTMX full page refresh so the message banner appears
        response = render(request, 'control_plane/components/celery_status.html', {
            'component_id': f'source-status-{source.pk}',
            'status': source.last_sync_status,
            'timestamp': source.last_synced,
            'poll_url': source.get_status_url()
        })
        response['HX-Refresh'] = 'true'
        return response

    # If it failed
    if source.last_sync_status == SyncStatus.FAILED:
        fail_key = f'sync_fail_shown_{source.pk}_{source.last_synced}'
        if not request.session.get(fail_key):
            messages.error(request, f"Connection test failed for '{source.name}': {source.last_sync_error}")
            request.session[fail_key] = True
            
        response = render(request, 'control_plane/components/celery_status.html', {
            'component_id': f'source-status-{source.pk}',
            'status': source.last_sync_status,
            'timestamp': source.last_synced,
            'error_message': source.last_sync_error,
            'poll_url': source.get_status_url()
        })
        response['HX-Refresh'] = 'true'
        return response

    # Otherwise, keep polling while running
    return render(request, 'control_plane/components/celery_status.html', {
        'component_id': f'source-status-{source.pk}',
        'status': source.last_sync_status,
        'timestamp': source.last_synced,
        'poll_url': source.get_status_url()
    })