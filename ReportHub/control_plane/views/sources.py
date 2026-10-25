import pymysql
import psycopg2
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, get_object_or_404, redirect
from django.core.paginator import Paginator
from django.utils import timezone
from django.contrib import messages
from control_plane.models import SourceProfile, DbChoice, SyncStatus
from control_plane.forms import SourceProfileForm

from etl.tasks import test_source_connection_task, introspect_source_schema_task


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
                saved_profile.last_sync_status = SyncStatus.RUNNING
                saved_profile.save(update_fields=['last_sync_status'])
                test_source_connection_task.delay(saved_profile.pk)
                
                # Mark in session that this profile is doing a connection test, not a sync
                request.session[f'is_connection_test_{saved_profile.pk}'] = True
                
                messages.info(request, f"Data source '{saved_profile.name}' created. Connection test is running...")
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
                saved_profile.save(update_fields=['last_sync_status'])
                test_source_connection_task.delay(saved_profile.pk)
                
                # Explicitly set the test flag so status view knows to use Blue/Info
                request.session[f'is_connection_test_{saved_profile.pk}'] = True
                
                messages.info(request, f"Data source '{saved_profile.name}' saved. Connection test is running...")
            else:
                messages.success(request, f"Data source '{saved_profile.name}' saved successfully.")
                
            return redirect('control_plane:source_list')
    else:
        form = SourceProfileForm(instance=profile)
    
    return render(request, 'control_plane/source_edit.html', {'form': form, 'profile': profile})

@login_required
@user_passes_test(is_admin)
def source_delete_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk)
    if request.method == 'POST':
        profile_name = profile.name  # Fixed from saved_profile to profile
        profile.delete()
        messages.success(request, f"Data source '{profile_name}' deleted successfully.")
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

    profile.last_sync_status = SyncStatus.RUNNING
    profile.save(update_fields=['last_sync_status'])

    request.session.pop(f'is_connection_test_{profile.pk}', None)

    introspect_source_schema_task.apply_async(args=[profile.pk], queue='queue_ux')
    
    # Blue start for sync
    messages.info(request, f"Schema synchronization started in the background (UX Queue) for '{profile.name}'.")

    return redirect('control_plane:source_list')

@login_required
@user_passes_test(is_admin)
def source_status_view(request, pk):
    source = get_object_or_404(SourceProfile, pk=pk)
    
    is_conn_test = request.session.pop(f'is_connection_test_{source.pk}', False)
    
    # If the background task has completed successfully -> BOTH GREEN
    if source.last_sync_status == SyncStatus.SUCCESS:
        session_key = f'sync_msg_shown_{source.pk}_{source.last_synced}'
        if not request.session.get(session_key):
            if is_conn_test:
                messages.success(request, f"Connection test successful for '{source.name}'! Database is reachable and settings are verified.")
            else:
                messages.success(request, f"Sync complete for '{source.name}'! Schema introspection finished successfully.")
            request.session[session_key] = True
            
        response = render(request, 'control_plane/components/celery_status.html', {
            'component_id': f'source-status-{source.pk}',
            'status': source.last_sync_status,
            'timestamp': source.last_synced,
            'poll_url': source.get_status_url()
        })
        response['HX-Refresh'] = 'true'
        return response

    # If it failed -> BOTH RED
    if source.last_sync_status == SyncStatus.FAILED:
        fail_key = f'sync_fail_shown_{source.pk}_{source.last_synced}'
        if not request.session.get(fail_key):
            if is_conn_test:
                messages.error(request, f"Connection test failed for '{source.name}': {source.last_sync_error}")
            else:
                messages.error(request, f"Sync failed for '{source.name}': {source.last_sync_error}")
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