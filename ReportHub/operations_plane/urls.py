from django.urls import path

from operations_plane import log_views, source_views, views


app_name = 'operations_plane'

urlpatterns = [
    path('', views.dashboard_view, name='dashboard'),
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    path(
        'widgets/preferences/',
        views.update_widget_preferences,
        name='update_widget_preferences',
    ),
    path('logs/', log_views.logs_view, name='logs'),
    path('logs/live/', log_views.live_logs_view, name='live_logs'),
    path('logs/live/data/', log_views.live_logs_data, name='live_logs_data'),
    path('logs/events/<uuid:event_id>/', log_views.log_event_view, name='log_event'),
    path('logs/searches/', log_views.log_search_history_view, name='log_search_history'),
    path(
        'logs/searches/<int:search_id>/delete/',
        log_views.delete_log_search_view,
        name='delete_log_search',
    ),
    path('administration/log-sources/', source_views.log_sources_view, name='log_sources'),
    path(
        'logs/searches/<int:search_id>/',
        log_views.log_search_detail_view,
        name='log_search_detail',
    ),
    path('logs/analyze/', log_views.analyze_logs_view, name='analyze_logs'),
    path(
        'logs/analyses/<int:analysis_id>/status/',
        log_views.log_analysis_status_view,
        name='log_analysis_status',
    ),
    path(
        'logs/analyses/<int:analysis_id>/',
        log_views.log_analysis_detail_view,
        name='log_analysis_detail',
    ),
]
