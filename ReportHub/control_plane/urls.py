from django.urls import path
from django.contrib.auth import views as auth_views
from .views import sources, dashboard, users, audit  # Import your view modules explicitly

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

    # Audit log URL
    path('audit/', audit.audit_log_view, name='audit_log'),
]