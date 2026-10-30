from django.urls import path
from . import views

app_name = 'reports'

urlpatterns = [
    path('', views.report_list, name='report_list'),
    path('create/', views.report_create, name='report_create'),
    path('<uuid:pk>/edit/', views.report_edit, name='report_edit'),
    path('<uuid:pk>/run/', views.report_run, name='report_run'),
    path('<uuid:pk>/toggle/', views.report_toggle, name='report_toggle'),
    path('<uuid:pk>/delete/', views.report_delete, name='report_delete'),
    path('<uuid:pk>/history/', views.report_history, name='report_history'),
    path('execution/<uuid:execution_pk>/download/', views.report_download, name='report_download'),
    path('ajax/load-tables/', views.ajax_load_tables, name='ajax_load_tables'),
    path('ajax/load-columns/', views.ajax_load_table_columns, name='ajax_load_table_columns'),
    path('<uuid:pk>/status/', views.report_status, name='report_status'),
]