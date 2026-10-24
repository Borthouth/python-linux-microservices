import json
from operator import attrgetter
from django.apps import apps
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.contrib.auth import get_user_model
from control_plane.models import PipelineAutomationAuditLog

@login_required
def audit_log_view(request):
    cp_config = apps.get_app_config('control_plane')
    all_history_entries = []
    
    # 1. Grab history from control_plane models
    for model in cp_config.get_models():
        if hasattr(model, 'history'):
            all_history_entries.extend(list(model.history.all()))
            
    # 2. Explicitly grab history from Django's User/TrackedUser model
    User = get_user_model()
    if hasattr(User, 'history'):
        all_history_entries.extend(list(User.history.all()))

    # 3. Include standalone pipeline automation audit logs
    all_history_entries.extend(list(PipelineAutomationAuditLog.objects.all()))

    # Sort all combined history entries globally by date descending
    audit_list = sorted(
        all_history_entries,
        key=attrgetter('history_date'),
        reverse=True
    )
    
    q = request.GET.get('q', '').strip()
    if q:
        filtered_list = []
        for entry in audit_list:
            name_val = getattr(entry, 'name', getattr(entry, 'username', ''))
            user_val = getattr(entry.history_user, 'username', '') if entry.history_user else ''
            if q.lower() in str(name_val).lower() or q.lower() in str(user_val).lower():
                filtered_list.append(entry)
        audit_list = filtered_list
        
    action_type = request.GET.get('action', '').strip()
    if action_type in ['+', '~', '-']:
        audit_list = [entry for entry in audit_list if entry.history_type == action_type]

    paginator = Paginator(audit_list, 15)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    audit_data = []
    for entry in page_obj:
        prev = entry.prev_record
        changes = {}
        
        # Normalize model name
        raw_model_name = entry.instance.__class__.__name__ if hasattr(entry, 'instance') else entry._meta.model.__name__
        if raw_model_name in ['User', 'TrackedUser']:
            model_name = 'User'
        elif raw_model_name == 'PipelineAutomationAuditLog':
            model_name = 'PeriodicTask'
        else:
            model_name = raw_model_name
        
        if entry.history_type == '+':
            changes = {"action": {"from": "None", "to": f"Created new {model_name}"}}
        elif entry.history_type == '-':
            changes = {"action": {"from": "Active Record", "to": f"Deleted {model_name}"}}
        elif prev:
            delta = entry.diff_against(prev)
            if delta and delta.changes:
                for change in delta.changes:
                    old_val = "********" if 'password' in change.field.lower() else change.old
                    new_val = "********" if 'password' in change.field.lower() else change.new
                    changes[change.field] = {"from": old_val, "to": new_val}
            else:
                changes = {"status": {"from": "Existing", "to": "Saved with no field modifications"}}
        else:
            # Handle AutomationAuditLog changes_data or fall back to summary
            if hasattr(entry, 'changes_data') and entry.changes_data:
                changes = entry.changes_data
            elif hasattr(entry, 'change_summary') and entry.change_summary:
                changes = {"status": {"from": "Previous state", "to": entry.change_summary}}
            else:
                changes = {"status": {"from": None, "to": "Historical entry"}}
            
        # Separate primary record identifier from context details
        if model_name == 'UserSourceAccess':
            record_name = str(getattr(entry, 'user', 'User'))
            source_str = str(getattr(entry, 'source', 'Source'))
            role_str = str(getattr(entry, 'role', 'Viewer'))
            context_label = f"→ {source_str} ({role_str})"
        elif model_name == 'SourceProfile':
            record_name = getattr(entry, 'name', 'Unnamed Source')
            context_label = getattr(entry, 'db_type', 'DATABASE')
        elif model_name == 'User':
            record_name = getattr(entry, 'username', 'User')
            context_label = "PLATFORM USER"
        elif model_name == 'PeriodicTask' or raw_model_name == 'AutomationAuditLog':
            record_name = getattr(entry, 'name', 'Pipeline Automation')
            context_label = "PIPELINE AUTOMATION"
        else:
            if hasattr(entry, 'name'):
                record_name = entry.name
            elif hasattr(entry, 'username'):
                record_name = entry.username
            else:
                record_name = f"Record #{entry.id}"
            context_label = model_name
        
        audit_data.append({
            'timestamp': entry.history_date,
            'action': entry.get_history_type_display(),
            'action_code': entry.history_type,
            'model_name': model_name,
            'record_name': record_name,
            'context_label': context_label,
            'user': entry.history_user,
            'changes_json': json.dumps(changes, default=str),
        })

    context = {
        'page_obj': page_obj,
        'audit_data': audit_data,
        'search_query': q,
        'selected_action': action_type,
    }
    return render(request, 'control_plane/audit_list.html', context)