import logging
from uuid import UUID

from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator
from django.db.models import Q
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_POST

from reports.delivery import DeliveryError, queue_delivery
from reports.models import ReportFileDelivery


logger = logging.getLogger(__name__)


@staff_member_required(login_url="control_plane:login")
def file_delivery_audit_view(request):
    status_filter = request.GET.get("status", "")
    search_query = request.GET.get("q", "").strip()
    deliveries = ReportFileDelivery.objects.select_related(
        "report", "execution__report", "destination"
    ).order_by("-created_at", "-pk")
    if status_filter:
        deliveries = deliveries.filter(status=status_filter)
    if search_query:
        search_filter = (
            Q(report__name__icontains=search_query)
            | Q(execution__report__name__icontains=search_query)
            | Q(destination_name__icontains=search_query)
            | Q(destination__name__icontains=search_query)
            | Q(remote_path__icontains=search_query)
            | Q(error_message__icontains=search_query)
        )
        try:
            execution_id = UUID(search_query)
        except ValueError:
            pass
        else:
            search_filter |= Q(execution_id=execution_id)
        deliveries = deliveries.filter(search_filter)
    page_obj = Paginator(deliveries, 20).get_page(request.GET.get("page"))
    return render(request, "control_plane/file_deliveries.html", {
        "page_obj": page_obj,
        "search_query": search_query,
        "status_filter": status_filter,
        "filter_options": [
            {"value": value, "label": label}
            for value, label in ReportFileDelivery._meta.get_field("status").choices
        ],
    })


@staff_member_required(login_url="control_plane:login")
@require_POST
def file_delivery_resend_view(request, delivery_id):
    delivery = get_object_or_404(ReportFileDelivery, pk=delivery_id)
    if delivery.status != "failed":
        messages.error(request, "Only failed file deliveries can be resent.")
        return redirect("control_plane:file_delivery_audit")
    try:
        queue_delivery(delivery.pk, retry=True)
    except DeliveryError as exc:
        logger.warning(
            "File delivery resend rejected: delivery_id=%s error_type=%s",
            delivery.pk, type(exc).__name__,
        )
        messages.error(request, str(exc))
    else:
        messages.success(request, "File delivery queued for resend.")
    return redirect("control_plane:file_delivery_audit")
