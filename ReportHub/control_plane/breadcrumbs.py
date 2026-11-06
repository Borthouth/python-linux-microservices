# control_plane/breadcrumbs.py
from django.urls import reverse

BREADCRUMB_REGISTRY = {
    # --- Dashboard & General ---
    'dashboard': [
        {'name': 'Dashboard', 'url': ''}
    ],
    'pipeline_history': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Pipeline History', 'url': ''}
    ],
    'report_history': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Reports', 'url': 'reports:report_list'},
        {'name': 'History', 'url': ''}
    ],

    # --- Reports App ---
    'report_list': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Reports', 'url': ''}
    ],
    'report_create': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Reports', 'url': 'reports:report_list'},
        {'name': 'Create Report', 'url': ''}
    ],
    'report_edit': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Reports', 'url': 'reports:report_list'},
        {'name': 'Edit Report', 'url': ''}
    ],

    # --- User Management ---
    'user_management': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Users & Access', 'url': ''}
    ],
    'user_edit': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Users & Access', 'url': 'control_plane:user_management'},
        {'name': 'Edit User', 'url': ''}
    ],
    'user_edit_access': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Users & Access', 'url': 'control_plane:user_management'},
        {'name': 'Edit User Access', 'url': ''}
    ],
    'password_change': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'User Profile', 'url': 'control_plane:user_profile'},
        {'name': 'Change Password', 'url': ''}
    ],
    'user_profile': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'User Profile', 'url': ''}
    ],

    # --- Source Management ---
    'source_list': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Data Sources', 'url': ''}
    ],
    'source_add': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Data Sources', 'url': 'control_plane:source_list'},
        {'name': 'Add Data Source', 'url': ''}
    ],
    'source_create': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Data Sources', 'url': 'control_plane:source_list'},
        {'name': 'Add Data Source', 'url': ''}
    ],
    'source_edit': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Data Sources', 'url': 'control_plane:source_list'},
        {'name': 'Edit Data Source', 'url': ''}
    ],
    'source_delete': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Data Sources', 'url': 'control_plane:source_list'},
        {'name': 'Delete Data Source', 'url': ''}
    ],

    # --- Audit Logs ---
    'audit_log': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Audit Logs', 'url': ''}
    ],
    'email_audit_logs': [
        {'name': 'Administration', 'url': ''},
        {'name': 'Email Delivery Audit', 'url': ''}
    ],

    # --- Pipeline Automations ---
    'pipeline_automation_list': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Pipeline Automations', 'url': ''}
    ],
    'pipeline_automation_create': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Pipeline Automations', 'url': 'control_plane:pipeline_automation_list'},
        {'name': 'Create Automation', 'url': ''}
    ],
    'pipeline_automation_edit': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Pipeline Automations', 'url': 'control_plane:pipeline_automation_list'},
        {'name': 'Edit Automation', 'url': ''}
    ],

    # --- Warehouse Field Mapping ---
    'warehouse_list': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Warehouse Mapping', 'url': ''}
    ],
    'warehouse_field_selection': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Warehouse Mapping', 'url': 'control_plane:warehouse_list'},
        {'name': 'Field Selection', 'url': ''}
    ],

    # --- Dead Letter Queue (DLQ) ---
    'dlq_list': [
        {'name': 'Dashboard', 'url': 'control_plane:dashboard'},
        {'name': 'Administration', 'url': ''},
        {'name': 'Task Health & DLQ', 'url': ''}
    ],
}


def get_breadcrumbs_for_request(request):
    """
    Automatically resolves breadcrumbs based on the active request's resolver match.
    Safely handles cross-app reversing for both control_plane and reports namespaces.
    """
    resolver_match = getattr(request, 'resolver_match', None)
    if not resolver_match:
        return []
        
    url_name = resolver_match.url_name
    raw_crumbs = BREADCRUMB_REGISTRY.get(url_name, [])
    
    resolved_crumbs = []
    for crumb in raw_crumbs:
        url = crumb['url']
        if url and not url.startswith('/') and not url.startswith('http'):
            try:
                # If the URL string doesn't already specify a namespace, 
                # default to the current request's namespace if available.
                if ':' not in url and resolver_match.namespace:
                    url = f"{resolver_match.namespace}:{url}"
                url = reverse(url)
            except Exception:
                # Fallback: try reversing without namespace if namespaced reverse fails
                try:
                    url = reverse(crumb['url'])
                except Exception:
                    url = ''
                
        resolved_crumbs.append({
            'name': crumb['name'],
            'url': url
        })
        
    return resolved_crumbs