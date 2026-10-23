from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from control_plane.models import SourceProfile, UserSourceAccess

@login_required
def dashboard_view(request):
    total_sources = SourceProfile.objects.count()
    user_access_count = UserSourceAccess.objects.filter(user=request.user).count()
    sources = SourceProfile.objects.all()[:5]
    
    running_jobs = [] 
    recent_jobs = []

    context = {
        'total_sources': total_sources,
        'user_access_count': user_access_count,
        'sources': sources,
        'running_jobs': running_jobs,
        'recent_jobs': recent_jobs,
    }
    return render(request, 'control_plane/dashboard.html', context)