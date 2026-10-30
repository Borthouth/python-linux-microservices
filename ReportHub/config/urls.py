from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('control-plane/', include('control_plane.urls')),
    path('reports/', include('reports.urls')),
]