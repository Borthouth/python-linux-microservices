from django.urls import reverse


BREADCRUMB_REGISTRY = {
    'log_sources': [
        {'name': 'Dashboard', 'url': 'operations_plane:dashboard'},
        {'name': 'Log Sources', 'url': ''},
    ],
    'dashboard': [
        {'name': 'Dashboard', 'url': ''},
    ],
    'logs': [
        {'name': 'Dashboard', 'url': 'operations_plane:dashboard'},
        {'name': 'Log Search', 'url': ''},
    ],
    'live_logs': [
        {'name': 'Dashboard', 'url': 'operations_plane:dashboard'},
        {'name': 'Live Logs', 'url': ''},
    ],
    'log_search_detail': [
        {'name': 'Dashboard', 'url': 'operations_plane:dashboard'},
        {'name': 'Search History', 'url': 'operations_plane:log_search_history'},
        {'name': 'Historical Search', 'url': ''},
    ],
    'log_search_history': [
        {'name': 'Dashboard', 'url': 'operations_plane:dashboard'},
        {'name': 'Search History', 'url': ''},
    ],
    'log_analysis_detail': [
        {'name': 'Dashboard', 'url': 'operations_plane:dashboard'},
        {'name': 'Log Search', 'url': 'operations_plane:logs'},
        {'name': 'AI Analysis', 'url': ''},
    ],
}


def get_operations_breadcrumbs(request):
    resolver_match = getattr(request, 'resolver_match', None)
    if not resolver_match:
        return []

    namespaced_name = (
        f'{resolver_match.namespace}:{resolver_match.url_name}'
        if resolver_match.namespace
        else resolver_match.url_name
    )
    raw_crumbs = BREADCRUMB_REGISTRY.get(
        namespaced_name,
        BREADCRUMB_REGISTRY.get(resolver_match.url_name, []),
    )

    urlconf = getattr(request, 'urlconf', None)
    return [
        {
            'name': crumb['name'],
            'url': reverse(crumb['url'], urlconf=urlconf) if crumb['url'] else '',
        }
        for crumb in raw_crumbs
    ]
