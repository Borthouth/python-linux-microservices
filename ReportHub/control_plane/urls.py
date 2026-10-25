from django.urls import path
from django.contrib.auth import views as auth_views
from .views import (
    sources, 
    dashboard, 
    users, 
    audit, 
    pipeline_automations,
    warehouse,
)

app_name = 'control_plane'

urlpatterns = [
    path('login/', auth_views.LoginView.as_view(template_name='control_plane/login.html'), name='login'),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),
    
    # dashboard URL
    path('', dashboard.dashboard_view, name='dashboard'),

    # User management URLs
    path('users/', users.user_management_view, name='user_management'),
    path('users/<int:user_id>/edit/', users.user_edit_view, name='user_edit'),  
    path('users/<int:user_id>/access/', users.user_edit_access_view, name='user_edit_access'),
    path('password-change/', users.password_change_view, name='password_change'),
    path('profile/', users.user_profile_view, name='user_profile'),
    
    # Source management URLs
    path('sources/', sources.source_list_view, name='source_list'),
    path('sources/add/', sources.source_create_view, name='source_add'),
    path('sources/create/', sources.source_create_view, name='source_create'),
    path('sources/<int:pk>/edit/', sources.source_edit_view, name='source_edit'),
    path('sources/<int:pk>/delete/', sources.source_delete_view, name='source_delete'),
    path('sources/<int:pk>/toggle/', sources.source_toggle_active_view, name='source_toggle_active'),
    path('sources/<int:pk>/sync/', sources.source_sync_view, name='source_sync'),
    path('sources/<int:pk>/status/', sources.source_status_view, name='source_status'),

    # Audit log URL
    path('audit/', audit.audit_log_view, name='audit_log'),

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
]

