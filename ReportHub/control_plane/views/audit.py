from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from control_plane.models import SourceProfile

@login_required
def audit_log_view(request):
    audit_list = SourceProfile.history.all().order_by('-history_date')
    print("TOTAL HISTORY RECORDS:", audit_list.count()) # Check your terminal output!
    
    q = request.GET.get('q', '').strip()
    if q:
        audit_list = audit_list.filter(name__icontains=q) | audit_list.filter(history_user__username__icontains=q)
        
    action_type = request.GET.get('action', '').strip()
    if action_type in ['+', '~', '-']:
        audit_list = audit_list.filter(history_type=action_type)

    paginator = Paginator(audit_list, 15)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    print("RECORDS ON THIS PAGE:", len(page_obj)) # Check your terminal output!

    context = {
        'page_obj': page_obj,
        'search_query': q,
        'selected_action': action_type,
    }
    return render(request, 'control_plane/audit_list.html', context)