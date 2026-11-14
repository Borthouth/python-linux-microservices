from django.urls import reverse


BREADCRUMB_REGISTRY = {
    "dashboard": [
        {"name": "Dashboard", "url": ""},
    ],
    "report_list": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Reports", "url": ""},
    ],
    "report_create": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Reports", "url": "reports:report_list"},
        {"name": "Create Report", "url": ""},
    ],
    "report_edit": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Reports", "url": "reports:report_list"},
        {"name": "Edit Report", "url": ""},
    ],
    "reports:report_history": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Reports", "url": "reports:report_list"},
        {"name": "Report Runs", "url": ""},
    ],
    "user_management": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Users & Access", "url": ""},
    ],
    "user_edit": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Users & Access", "url": "control_plane:user_management"},
        {"name": "Edit User", "url": ""},
    ],
    "user_edit_access": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Users & Access", "url": "control_plane:user_management"},
        {"name": "Edit User Access", "url": ""},
    ],
    "password_change": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "User Profile", "url": "control_plane:user_profile"},
        {"name": "Change Password", "url": ""},
    ],
    "user_profile": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "User Profile", "url": ""},
    ],
    "source_list": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Data Sources", "url": ""},
    ],
    "source_add": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Data Sources", "url": "control_plane:source_list"},
        {"name": "Add Data Source", "url": ""},
    ],
    "source_create": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Data Sources", "url": "control_plane:source_list"},
        {"name": "Add Data Source", "url": ""},
    ],
    "source_edit": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Data Sources", "url": "control_plane:source_list"},
        {"name": "Edit Data Source", "url": ""},
    ],
    "source_delete": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Data Sources", "url": "control_plane:source_list"},
        {"name": "Delete Data Source", "url": ""},
    ],
    "source_sync": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Data Sources", "url": "control_plane:source_list"},
        {"name": "Sync Data Source", "url": ""},
    ],
    "source_status": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Data Sources", "url": "control_plane:source_list"},
        {"name": "Data Source Status", "url": ""},
    ],
    "source_toggle_active": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Data Sources", "url": "control_plane:source_list"},
        {"name": "Update Data Source Status", "url": ""},
    ],
    "source_cdc_to_batch_status": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Data Sources", "url": "control_plane:source_list"},
        {"name": "CDC-to-Batch Status", "url": ""},
    ],
    "source_cdc_to_batch_staging": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Data Sources", "url": "control_plane:source_list"},
        {"name": "CDC-to-Batch Staging", "url": ""},
    ],
    "transfer_destination_list": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Transfer Destinations", "url": ""},
    ],
    "transfer_destination_create": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "Transfer Destinations",
            "url": "control_plane:transfer_destination_list",
        },
        {"name": "Add SFTP Destination", "url": ""},
    ],
    "transfer_destination_edit": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "Transfer Destinations",
            "url": "control_plane:transfer_destination_list",
        },
        {"name": "Edit SFTP Destination", "url": ""},
    ],
    "transfer_destination_delete": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "Transfer Destinations",
            "url": "control_plane:transfer_destination_list",
        },
        {"name": "Delete SFTP Destination", "url": ""},
    ],
    "transfer_destination_toggle": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "Transfer Destinations",
            "url": "control_plane:transfer_destination_list",
        },
        {"name": "Update SFTP Destination", "url": ""},
    ],
    "transfer_destination_test": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "Transfer Destinations",
            "url": "control_plane:transfer_destination_list",
        },
        {"name": "Test SFTP Destination", "url": ""},
    ],
    "transfer_destination_test_status": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "Transfer Destinations",
            "url": "control_plane:transfer_destination_list",
        },
        {"name": "SFTP Test Status", "url": ""},
    ],
    "file_delivery_audit": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "File Delivery Audit", "url": ""},
    ],
    "audit_log": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Audit Logs", "url": ""},
    ],
    "email_audit_logs": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Email Audit Logs", "url": ""},
    ],
    "pipeline_automation_list": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Pipeline Automations", "url": ""},
    ],
    "pipeline_automation_create": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "Pipeline Automations",
            "url": "control_plane:pipeline_automation_list",
        },
        {"name": "Create Automation", "url": ""},
    ],
    "pipeline_automation_edit": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "Pipeline Automations",
            "url": "control_plane:pipeline_automation_list",
        },
        {"name": "Edit Automation", "url": ""},
    ],
    "pipeline_automation_toggle": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "Pipeline Automations",
            "url": "control_plane:pipeline_automation_list",
        },
        {"name": "Update Automation Status", "url": ""},
    ],
    "pipeline_automation_run": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "Pipeline Automations",
            "url": "control_plane:pipeline_automation_list",
        },
        {"name": "Run Automation", "url": ""},
    ],
    "pipeline_automation_delete": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "Pipeline Automations",
            "url": "control_plane:pipeline_automation_list",
        },
        {"name": "Delete Automation", "url": ""},
    ],
    "warehouse_list": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Warehouse Mapping", "url": ""},
    ],
    "warehouse_field_selection": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Warehouse Mapping", "url": "control_plane:warehouse_list"},
        {"name": "Field Selection", "url": ""},
    ],
    "pipeline_history": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Pipeline History", "url": ""},
    ],
    "cdc_failure_history": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "CDC Execution Failures", "url": ""},
    ],
    "active_pipeline_executions": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Active Pipeline Executions", "url": ""},
    ],
    "active_report_generations": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Active Report Generations", "url": ""},
    ],
    "report_history": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "All Report Runs", "url": ""},
    ],
    "cdc_rebuild_list": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "CDC Table Rebuilds", "url": ""},
    ],
    "cdc_rebuild_start": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "CDC Table Rebuilds", "url": "control_plane:cdc_rebuild_list"},
        {"name": "Start CDC Rebuild", "url": ""},
    ],
    "cdc_rebuild_resume": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "CDC Table Rebuilds", "url": "control_plane:cdc_rebuild_list"},
        {"name": "Resume Cutover", "url": ""},
    ],
    "cdc_cleanup_list": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "CDC Resource Cleanup", "url": ""},
    ],
    "cdc_cleanup_scan": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "CDC Resource Cleanup",
            "url": "control_plane:cdc_cleanup_list",
        },
        {"name": "Inventory Resources", "url": ""},
    ],
    "cdc_cleanup_confirm": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "CDC Resource Cleanup",
            "url": "control_plane:cdc_cleanup_list",
        },
        {"name": "Confirm Resource Cleanup", "url": ""},
    ],
    "cdc_cleanup_recover": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {
            "name": "CDC Resource Cleanup",
            "url": "control_plane:cdc_cleanup_list",
        },
        {"name": "Recover Cleanup", "url": ""},
    ],
    "dlq_list": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Dead Letter Queue", "url": ""},
    ],
    "dlq_requeue": [
        {"name": "Dashboard", "url": "control_plane:dashboard"},
        {"name": "Dead Letter Queue", "url": "control_plane:dlq_list"},
        {"name": "Requeue Task", "url": ""},
    ],
}


def get_breadcrumbs_for_request(request):
    """Resolve breadcrumb links for the active view."""
    resolver_match = getattr(request, "resolver_match", None)
    if not resolver_match:
        return []

    url_name = resolver_match.url_name
    namespaced_url_name = (
        f"{resolver_match.namespace}:{url_name}"
        if resolver_match.namespace
        else url_name
    )
    raw_crumbs = BREADCRUMB_REGISTRY.get(
        namespaced_url_name,
        BREADCRUMB_REGISTRY.get(url_name, []),
    )

    resolved_crumbs = []
    for crumb in raw_crumbs:
        url = crumb["url"]
        if url and not url.startswith(("/", "http")):
            if ":" not in url and resolver_match.namespace:
                url = f"{resolver_match.namespace}:{url}"
            url = reverse(url)
        resolved_crumbs.append({"name": crumb["name"], "url": url})

    return resolved_crumbs
