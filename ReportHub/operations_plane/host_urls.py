from django.urls import include, path


urlpatterns = [
    path('', include('operations_plane.urls')),
]
