from control_plane.breadcrumbs import get_breadcrumbs_for_request

def breadcrumbs_processor(request):
    return {
        'breadcrumbs': get_breadcrumbs_for_request(request)
    }