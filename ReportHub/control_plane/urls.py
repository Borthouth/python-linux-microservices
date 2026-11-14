from django.urls import path
from django.contrib.auth import views as auth_views
from .views import (
    sources, 
    dashboard, 
    users, 
    audit, 
    pipeline_automations,
    warehouse,
    dlq,
    email_audit,
    file_deliveries,
    cdc_cleanup,
    cdc_transition,
    cdc_rebuilds,
    transfer_destinations,
)

app_name = 'control_plane'

urlpatterns = [
    path('login/', auth_views.LoginView.as_view(template_name='control_plane/login.html'), name='login'),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),
    
    # dashboard URL
    path('', dashboard.dashboard_view, name='dashboard'),
    path('api/widgets/update/', dashboard.update_widget_preferences, name='update_widget_preferences'),
    path('audit/file-deliveries/', file_deliveries.file_delivery_audit_view, name='file_delivery_audit'),
    path('audit/file-deliveries/<uuid:delivery_id>/resend/', file_deliveries.file_delivery_resend_view, name='file_delivery_resend'),
    path('transfer-destinations/', transfer_destinations.transfer_destination_list_view, name='transfer_destination_list'),
    path('transfer-destinations/create/', transfer_destinations.transfer_destination_edit_view, name='transfer_destination_create'),
    path('transfer-destinations/<int:pk>/edit/', transfer_destinations.transfer_destination_edit_view, name='transfer_destination_edit'),
    path('transfer-destinations/<int:pk>/delete/', transfer_destinations.transfer_destination_delete_view, name='transfer_destination_delete'),
    path('transfer-destinations/<int:pk>/toggle/', transfer_destinations.transfer_destination_toggle_view, name='transfer_destination_toggle'),
    path('transfer-destinations/<int:pk>/test/', transfer_destinations.transfer_destination_test_view, name='transfer_destination_test'),
    path('transfer-destinations/<int:pk>/test-status/', transfer_destinations.transfer_destination_test_status_view, name='transfer_destination_test_status'),
    path('pipelines/history/', dashboard.pipeline_history_view, name='pipeline_history'),
    path('cdc/failures/', dashboard.cdc_failure_history_view, name='cdc_failure_history'),
    path('cdc/rebuilds/', cdc_rebuilds.cdc_rebuild_list_view, name='cdc_rebuild_list'),
    path('cdc/rebuilds/tables/<int:source_table_pk>/start/', cdc_rebuilds.cdc_rebuild_start_view, name='cdc_rebuild_start'),
    path('cdc/rebuilds/<int:rebuild_pk>/resume/', cdc_rebuilds.cdc_rebuild_resume_view, name='cdc_rebuild_resume'),
    path('cdc/cleanup/', cdc_cleanup.cdc_cleanup_list_view, name='cdc_cleanup_list'),
    path('cdc/cleanup/<int:rebuild_pk>/scan/', cdc_cleanup.cdc_cleanup_scan_view, name='cdc_cleanup_scan'),
    path('cdc/cleanup/<int:cleanup_pk>/confirm/', cdc_cleanup.cdc_cleanup_confirm_view, name='cdc_cleanup_confirm'),
    path('cdc/cleanup/<int:cleanup_pk>/recover/', cdc_cleanup.cdc_cleanup_recover_view, name='cdc_cleanup_recover'),
    path('pipelines/active/', dashboard.active_pipeline_executions_view, name='active_pipeline_executions'),
    path('reports/active/', dashboard.active_report_generations_view, name='active_report_generations'),
    path('sources/<int:source_pk>/cancel/', dashboard.cancel_pipeline_view, name='cancel_pipeline'),
    path('reports/<uuid:report_pk>/cancel/', dashboard.cancel_report_view, name='cancel_report'),
    path('reports/history/', dashboard.report_history_view, name='report_history'),
    path(
        'reports/<uuid:report_pk>/artifacts/<uuid:attempt_pk>/reconcile/',
        dashboard.reconcile_report_artifact_view,
        name='report_artifact_reconcile',
    ),
    path('reports/<uuid:report_pk>/download/', dashboard.download_report_view, name='download_report'),
    path('dashboard/fragment/pipelines/', dashboard.active_pipelines_fragment, name='active_pipelines_fragment'),
    path('dashboard/fragment/reports/', dashboard.active_reports_fragment, name='active_reports_fragment'),

    # User management URLs
    path('users/', users.user_management_view, name='user_management'),
    path('users/<int:user_id>/toggle-active/', users.user_toggle_active_view, name='user_toggle_active'),
    path('users/<int:user_id>/edit/', users.user_edit_view, name='user_edit'),  
    path('users/<int:user_id>/access/', users.user_edit_access_view, name='user_edit_access'),
    path('password-change/', users.password_change_view, name='password_change'),
    path('profile/', users.user_profile_view, name='user_profile'),
    
    # Source management URLs
    path('sources/', sources.source_list_view, name='source_list'),
    path('sources/add/', sources.source_create_view, name='source_add'),
    path('sources/create/', sources.source_create_view, name='source_create'),
    path('sources/<int:pk>/edit/', cdc_transition.source_edit_with_metadata_guard, name='source_edit'),
    path('sources/<int:pk>/delete/', cdc_transition.source_delete_with_metadata_guard, name='source_delete'),
    path('sources/<int:pk>/toggle/', cdc_transition.source_toggle_with_metadata_guard, name='source_toggle_active'),
    path('sources/<int:pk>/sync/', sources.source_sync_view, name='source_sync'),
    path('sources/<int:pk>/status/', sources.source_status_view, name='source_status'),
    path('sources/<int:pk>/cdc-to-batch/status/', cdc_transition.source_cdc_to_batch_status_view, name='source_cdc_to_batch_status'),
    path('sources/<int:pk>/cdc-to-batch/staging/', cdc_transition.source_cdc_to_batch_staging_view, name='source_cdc_to_batch_staging'),

    # Audit log URL
    path('audit/', audit.audit_log_view, name='audit_log'),

    # Email audit log URLs
    path('audit/emails/', email_audit.email_audit_log_view, name='email_audit_logs'),
    path('api/webhooks/email/', email_audit.email_webhook_receiver, name='email_webhook_receiver'),
    path('audit/emails/<int:log_id>/requeue/', email_audit.email_audit_requeue_view, name='email_audit_requeue'),
    path('audit/emails/<int:log_id>/delete/', email_audit.email_audit_delete_view, name='email_audit_delete'),

    # Pipeline Automation URLs
    path('pipeline-automations/', pipeline_automations.pipeline_automation_list_view, name='pipeline_automation_list'),
    path('pipeline-automations/create/', pipeline_automations.pipeline_automation_create_view, name='pipeline_automation_create'),
    path('pipeline-automations/<int:task_id>/edit/', pipeline_automations.pipeline_automation_edit_view, name='pipeline_automation_edit'),
    path('pipeline-automations/<int:task_id>/toggle/', pipeline_automations.pipeline_automation_toggle_active_view, name='pipeline_automation_toggle'),
    path('pipeline-automations/<int:task_id>/run/', pipeline_automations.run_pipeline_automation_manually_view, name='pipeline_automation_run'),
    path('pipeline-automations/<int:task_id>/delete/', pipeline_automations.delete_pipeline_automation_view, name='pipeline_automation_delete'),

    # Warehouse Field Mapping Routes
    path('warehouse/', warehouse.warehouse_list_view, name='warehouse_list'),
    path('warehouse/<int:source_pk>/fields/', warehouse.warehouse_field_selection_view, name='warehouse_field_selection'),

    # Dead Letter Queue (DLQ) URLs
    path('dlq/', dlq.dlq_list_view, name='dlq_list'),
    path('dlq/<str:task_id>/requeue/', dlq.dlq_requeue_view, name='dlq_requeue'),
]
