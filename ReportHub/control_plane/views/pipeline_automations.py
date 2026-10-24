from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django_celery_beat.models import PeriodicTask, CrontabSchedule
from control_plane.forms.pipeline_automations import PipelineAutomationForm
from control_plane.models import PipelineAutomationAuditLog
from django.core.paginator import Paginator
from django.core.exceptions import ValidationError
from django.db import IntegrityError

def is_system_admin(user):
    return user.is_authenticated and (user.is_superuser or getattr(user, 'role', None) == 'System Admin')

@login_required
@user_passes_test(is_system_admin)
def pipeline_automation_list_view(request):
    query = request.GET.get('q', '').strip()
    tasks_list = PeriodicTask.objects.all().order_by('name')
    
    if query:
        tasks_list = tasks_list.filter(name__icontains=query)
        
    paginator = Paginator(tasks_list, 10)  # Show 10 automations per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    return render(request, 'control_plane/pipeline_automation_list.html', {
        'page_obj': page_obj,
        'search_query': query
    })

@login_required
@user_passes_test(is_system_admin)
def pipeline_automation_create_view(request):
    if request.method == 'POST':
        form = PipelineAutomationForm(request.POST)
        if form.is_valid():
            name = form.cleaned_data['task_name']
            task_path = form.cleaned_data['task_path']
            cron_expr = form.cleaned_data['cron_expression']
            
            minute, hour, day_of_month, month, day_of_week = cron_expr.split()
            
            crontab, _ = CrontabSchedule.objects.get_or_create(
                minute=minute,
                hour=hour,
                day_of_month=day_of_month,
                month_of_year=month,
                day_of_week=day_of_week
            )

            try:
                PeriodicTask.objects.create(
                    crontab=crontab,
                    name=name,
                    task=task_path,
                )

                changes_dict = {
                    "action": {"from": "None", "to": f"Created pipeline automation '{name}' running [{task_path}] with cron [{cron_expr}]"}
                }
                PipelineAutomationAuditLog.objects.create(
                    name=name,
                    action_type='+',
                    user=request.user,
                    change_summary=f"Created pipeline automation '{name}'",
                    changes_data=changes_dict
                )

                messages.success(request, f"Pipeline automation '{name}' created successfully!")
                return redirect('control_plane:pipeline_automation_list')
                
            except (ValidationError, IntegrityError) as e:
                form.add_error('task_name', 'A pipeline automation with this name already exists.')
    else:
        form = PipelineAutomationForm()
    return render(request, 'control_plane/create_pipeline_automation.html', {'form': form, 'is_edit': False})

@login_required
@user_passes_test(is_system_admin)
def pipeline_automation_edit_view(request, task_id):
    task = get_object_or_404(PeriodicTask, id=task_id)
    
    old_name = task.name
    old_task_path = task.task
    old_cron = ""
    if task.crontab:
        old_cron = f"{task.crontab.minute} {task.crontab.hour} {task.crontab.day_of_month} {task.crontab.month_of_year} {task.crontab.day_of_week}"

    if request.method == 'POST':
        form = PipelineAutomationForm(request.POST)
        if form.is_valid():
            new_name = form.cleaned_data['task_name']
            new_task_path = form.cleaned_data['task_path']
            cron_expr = form.cleaned_data['cron_expression']
            
            minute, hour, day_of_month, month, day_of_week = cron_expr.split()
            
            crontab, _ = CrontabSchedule.objects.get_or_create(
                minute=minute,
                hour=hour,
                day_of_month=day_of_month,
                month_of_year=month,
                day_of_week=day_of_week
            )

            try:
                task.name = new_name
                task.task = new_task_path
                task.crontab = crontab
                task.save()

                changes_dict = {}
                if old_name != new_name:
                    changes_dict['name'] = {"from": old_name, "to": new_name}
                if old_task_path != new_task_path:
                    changes_dict['task_path'] = {"from": old_task_path, "to": new_task_path}
                if old_cron != cron_expr:
                    changes_dict['cron_expression'] = {"from": old_cron, "to": cron_expr}
                if not changes_dict:
                    changes_dict['status'] = {"from": "Existing", "to": "Saved with no field modifications"}

                PipelineAutomationAuditLog.objects.create(
                    name=new_name,
                    action_type='~',
                    user=request.user,
                    change_summary=f"Updated pipeline automation settings for '{new_name}'",
                    changes_data=changes_dict
                )

                messages.success(request, f"Pipeline automation '{new_name}' updated successfully!")
                return redirect('control_plane:pipeline_automation_list')
                
            except (ValidationError, IntegrityError) as e:
                form.add_error('task_name', 'A pipeline automation with this name already exists.')
    else:
        form = PipelineAutomationForm(initial={
            'task_name': task.name,
            'task_path': task.task,
            'cron_expression': old_cron
        })
        
    return render(request, 'control_plane/create_pipeline_automation.html', {'form': form, 'is_edit': True, 'task': task})

@login_required
@user_passes_test(is_system_admin)
def pipeline_automation_toggle_active_view(request, task_id):
    task = get_object_or_404(PeriodicTask, id=task_id)
    old_status = "Enabled" if task.enabled else "Disabled"
    task.enabled = not task.enabled
    task.save()
    new_status = "Enabled" if task.enabled else "Disabled"

    changes_dict = {
        'enabled': {"from": old_status, "to": new_status}
    }

    PipelineAutomationAuditLog.objects.create(
        name=task.name,
        action_type='~',
        user=request.user,
        change_summary=f"Pipeline automation status changed to {new_status.lower()}",
        changes_data=changes_dict
    )

    messages.success(request, f"Pipeline automation '{task.name}' has been {new_status.lower()}.")
    return redirect('control_plane:pipeline_automation_list')

@login_required
@user_passes_test(is_system_admin)
def run_pipeline_automation_manually_view(request, task_id):
    task = get_object_or_404(PeriodicTask, id=task_id)
    from celery import current_app
    current_app.send_task(task.task)
    messages.success(request, f"Pipeline automation '{task.name}' triggered manually.")
    return redirect('control_plane:pipeline_automation_list')

@login_required
@user_passes_test(is_system_admin)
def delete_pipeline_automation_view(request, task_id):
    if request.method == 'POST':
        task = get_object_or_404(PeriodicTask, id=task_id)
        name = task.name
        
        changes_dict = {
            "action": {"from": f"Active pipeline automation '{name}'", "to": "Deleted"}
        }

        PipelineAutomationAuditLog.objects.create(
            name=name,
            action_type='-',
            user=request.user,
            change_summary=f"Deleted pipeline automation '{name}'",
            changes_data=changes_dict
        )

        task.delete()
        messages.success(request, f"Pipeline automation '{name}' deleted successfully.")
    return redirect('control_plane:pipeline_automation_list')