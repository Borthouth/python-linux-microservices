from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django_celery_beat.models import PeriodicTask, CrontabSchedule
from control_plane.forms.schedules import TaskScheduleForm
from control_plane.models import ScheduleAuditLog
from django.core.paginator import Paginator
from django.core.exceptions import ValidationError
from django.db import IntegrityError

def is_system_admin(user):
    return user.is_authenticated and (user.is_superuser or getattr(user, 'role', None) == 'System Admin')

@login_required
@user_passes_test(is_system_admin)
def schedule_list_view(request):
    query = request.GET.get('q', '').strip()
    tasks_list = PeriodicTask.objects.all().order_by('name')
    
    if query:
        tasks_list = tasks_list.filter(name__icontains=query)
        
    paginator = Paginator(tasks_list, 10)  # Show 10 schedules per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    return render(request, 'control_plane/schedule_list.html', {
        'page_obj': page_obj,
        'search_query': query
    })

@login_required
@user_passes_test(is_system_admin)
def create_schedule_view(request):
    if request.method == 'POST':
        form = TaskScheduleForm(request.POST)
        if form.is_valid():
            name = form.cleaned_data['task_name']
            frequency = form.cleaned_data['frequency']
            
            if frequency == 'every_minute':
                crontab, _ = CrontabSchedule.objects.get_or_create(
                    minute='*', hour='*', day_of_week='*', day_of_month='*', month_of_year='*'
                )
            elif frequency == 'hourly':
                crontab, _ = CrontabSchedule.objects.get_or_create(
                    minute='0', hour='*', day_of_week='*', day_of_month='*', month_of_year='*'
                )
            elif frequency == 'daily':
                crontab, _ = CrontabSchedule.objects.get_or_create(
                    minute='0', hour='0', day_of_week='*', day_of_month='*', month_of_year='*'
                )

            try:
                PeriodicTask.objects.create(
                    crontab=crontab,
                    name=name,
                    task='etl.tasks.cleanup_postgres_warehouse_task',
                )

                # Log schedule creation with detailed action data
                changes_dict = {
                    "action": {"from": "None", "to": f"Created new scheduled task '{name}'"}
                }
                ScheduleAuditLog.objects.create(
                    name=name,
                    action_type='+',
                    user=request.user,
                    change_summary=f"Created new scheduled task '{name}'",
                    changes_data=changes_dict
                )

                messages.success(request, f"Schedule '{name}' created successfully!")
                return redirect('control_plane:schedule_list')
                
            except (ValidationError, IntegrityError) as e:
                if hasattr(e, 'message_dict'):
                    for field, error_list in e.message_dict.items():
                        for err in error_list:
                            form.add_error('task_name' if field == 'name' else field, err)
                else:
                    form.add_error('task_name', 'A scheduled task with this name already exists.')
    else:
        form = TaskScheduleForm()
    return render(request, 'control_plane/create_schedule.html', {'form': form, 'is_edit': False})

@login_required
@user_passes_test(is_system_admin)
def schedule_edit_view(request, task_id):
    task = get_object_or_404(PeriodicTask, id=task_id)
    
    # Capture old values before changes
    old_name = task.name
    old_freq = 'every_minute'
    if task.crontab:
        if task.crontab.minute == '0' and task.crontab.hour == '0':
            old_freq = 'daily'
        elif task.crontab.minute == '0' and task.crontab.hour == '*':
            old_freq = 'hourly'

    if request.method == 'POST':
        form = TaskScheduleForm(request.POST)
        if form.is_valid():
            new_name = form.cleaned_data['task_name']
            new_freq = form.cleaned_data['frequency']
            
            if new_freq == 'every_minute':
                crontab, _ = CrontabSchedule.objects.get_or_create(
                    minute='*', hour='*', day_of_week='*', day_of_month='*', month_of_year='*'
                )
            elif new_freq == 'hourly':
                crontab, _ = CrontabSchedule.objects.get_or_create(
                    minute='0', hour='*', day_of_week='*', day_of_month='*', month_of_year='*'
                )
            elif new_freq == 'daily':
                crontab, _ = CrontabSchedule.objects.get_or_create(
                    minute='0', hour='0', day_of_week='*', day_of_month='*', month_of_year='*'
                )

            try:
                task.name = new_name
                task.crontab = crontab
                task.save()

                # Build exact field-level changes dictionary for the card UI
                changes_dict = {}
                if old_name != new_name:
                    changes_dict['name'] = {"from": old_name, "to": new_name}
                if old_freq != new_freq:
                    changes_dict['frequency'] = {"from": old_freq, "to": new_freq}
                if not changes_dict:
                    changes_dict['status'] = {"from": "Existing", "to": "Saved with no field modifications"}

                ScheduleAuditLog.objects.create(
                    name=new_name,
                    action_type='~',
                    user=request.user,
                    change_summary=f"Updated scheduled task settings for '{new_name}'",
                    changes_data=changes_dict
                )

                messages.success(request, f"Schedule '{new_name}' updated successfully!")
                return redirect('control_plane:schedule_list')
                
            except (ValidationError, IntegrityError) as e:
                if hasattr(e, 'message_dict'):
                    for field, error_list in e.message_dict.items():
                        for err in error_list:
                            form.add_error('task_name' if field == 'name' else field, err)
                else:
                    form.add_error('task_name', 'A scheduled task with this name already exists.')
    else:
        form = TaskScheduleForm(initial={
            'task_name': task.name,
            'frequency': old_freq
        })
    return render(request, 'control_plane/create_schedule.html', {'form': form, 'is_edit': True, 'task': task})

@login_required
@user_passes_test(is_system_admin)
def schedule_toggle_active_view(request, task_id):
    """Toggle a scheduled task between enabled and disabled explicitly."""
    task = get_object_or_404(PeriodicTask, id=task_id)
    old_status = "Enabled" if task.enabled else "Disabled"
    task.enabled = not task.enabled
    task.save()
    new_status = "Enabled" if task.enabled else "Disabled"

    changes_dict = {
        'enabled': {"from": old_status, "to": new_status}
    }

    ScheduleAuditLog.objects.create(
        name=task.name,
        action_type='~',
        user=request.user,
        change_summary=f"Task status changed to {new_status.lower()}",
        changes_data=changes_dict
    )

    messages.success(request, f"Schedule '{task.name}' has been {new_status.lower()}.")
    return redirect('control_plane:schedule_list')

@login_required
@user_passes_test(is_system_admin)
def run_task_manually_view(request, task_id):
    task = get_object_or_404(PeriodicTask, id=task_id)
    from celery import current_app
    current_app.send_task(task.task)
    messages.success(request, f"Task '{task.name}' triggered manually.")
    return redirect('control_plane:schedule_list')

@login_required
@user_passes_test(is_system_admin)
def delete_schedule_view(request, task_id):
    if request.method == 'POST':
        task = get_object_or_404(PeriodicTask, id=task_id)
        name = task.name
        
        changes_dict = {
            "action": {"from": f"Active scheduled task '{name}'", "to": "Deleted"}
        }

        ScheduleAuditLog.objects.create(
            name=name,
            action_type='-',
            user=request.user,
            change_summary=f"Deleted scheduled task '{name}'",
            changes_data=changes_dict
        )

        task.delete()
        messages.success(request, f"Schedule '{name}' deleted successfully.")
    return redirect('control_plane:schedule_list')