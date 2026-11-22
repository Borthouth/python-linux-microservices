from django import template
from django.http import QueryDict

register = template.Library()


@register.simple_tag
def page_url(request, page_number):
    params = request.GET.copy() if request else QueryDict(mutable=True)
    params["page"] = page_number
    return f"?{params.urlencode()}"
