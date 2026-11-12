from django import template

from control_plane.cdc_transition_status import cdc_to_batch_status_context


register = template.Library()


@register.inclusion_tag("control_plane/source/cdc_to_batch_status.html")
def cdc_to_batch_status(profile):
    return cdc_to_batch_status_context(profile)
