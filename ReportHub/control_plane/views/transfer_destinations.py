from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.cache import never_cache
from django.views.decorators.debug import sensitive_post_parameters
from django.views.decorators.http import require_http_methods

from control_plane.forms.transfer_destinations import TransferDestinationForm
from control_plane.models import TransferDestination
from control_plane.tasks import expire_destination_test, queue_destination_test
from reports.sftp import DeliveryError


@staff_member_required(login_url="control_plane:login")
@require_http_methods(["GET"])
def transfer_destination_list_view(request):
    destinations = TransferDestination.objects.defer(
        "password", "private_key", "private_key_passphrase"
    )
    query = request.GET.get("q", "").strip()
    if query:
        destinations = destinations.filter(Q(name__icontains=query) | Q(host__icontains=query))
    status_filter = request.GET.get("status", "")
    if status_filter == "active":
        destinations = destinations.filter(is_active=True)
    elif status_filter == "inactive":
        destinations = destinations.filter(is_active=False)
    return render(request, "control_plane/transfers/destination_list.html", {
        "page_obj": Paginator(destinations, 15).get_page(request.GET.get("page")),
        "search_query": query,
        "status_filter": status_filter,
        "filter_options": [
            {"value": "active", "label": "Active"},
            {"value": "inactive", "label": "Inactive"},
        ],
    })


@staff_member_required(login_url="control_plane:login")
@require_http_methods(["POST"])
def transfer_destination_toggle_view(request, pk):
    with transaction.atomic():
        destination = get_object_or_404(TransferDestination.objects.select_for_update(), pk=pk)
        destination.is_active = not destination.is_active
        destination.updated_by = request.user
        destination._history_user = request.user
        destination.save(update_fields=["is_active", "updated_by", "updated_at"])
    state = "activated" if destination.is_active else "deactivated"
    message = f"SFTP destination '{destination.name}' {state}."
    if not destination.is_active:
        message += " Transfers already in progress are not cancelled."
    messages.success(request, message)
    return redirect("control_plane:transfer_destination_list")


@staff_member_required(login_url="control_plane:login")
@require_http_methods(["POST"])
def transfer_destination_test_view(request, pk):
    try:
        with transaction.atomic():
            destination = get_object_or_404(TransferDestination.objects.select_for_update(), pk=pk)
            queue_destination_test(destination)
    except DeliveryError as error:
        messages.error(request, str(error))
    else:
        messages.info(request, "Connection test queued. No report files will be transferred.")
    return redirect("control_plane:transfer_destination_list")


@staff_member_required(login_url="control_plane:login")
@never_cache
@require_http_methods(["GET"])
def transfer_destination_test_status_view(request, pk):
    destination = get_object_or_404(TransferDestination, pk=pk)
    expire_destination_test(destination)
    return render(request, "control_plane/transfers/destination_test_status.html", {
        "destination": destination,
    })


@staff_member_required(login_url="control_plane:login")
@sensitive_post_parameters("password", "private_key", "private_key_passphrase")
@never_cache
@require_http_methods(["GET", "POST"])
def transfer_destination_edit_view(request, pk=None):
    saved = False
    with transaction.atomic():
        destination = (
            get_object_or_404(TransferDestination.objects.select_for_update(), pk=pk)
            if pk is not None else TransferDestination()
        )
        form = TransferDestinationForm(
            request.POST if request.method == "POST" else None, instance=destination
        )
        if request.method == "POST" and form.is_valid():
            destination = form.save(commit=False)
            if pk is None:
                destination.created_by = request.user
            if set(form.changed_data) - {"name", "is_active"}:
                destination.last_test_status = TransferDestination.TestStatus.NOT_TESTED
                destination.last_test_error = ""
                destination.last_test_task_id = ""
                destination.last_tested_at = None
            destination.updated_by = request.user
            destination._history_user = request.user
            destination.save()
            saved = True
    if saved:
        messages.success(request, f"SFTP destination '{destination.name}' saved. Saving a destination does not transfer files.")
        if "save_and_test" in request.POST:
            try:
                with transaction.atomic():
                    destination = get_object_or_404(
                        TransferDestination.objects.select_for_update(), pk=destination.pk,
                    )
                    queue_destination_test(destination)
            except DeliveryError as error:
                messages.error(request, f"Destination saved, but the connection test could not be queued: {error}")
            else:
                messages.info(request, "Connection test queued. No report files will be transferred.")
        return redirect("control_plane:transfer_destination_list")
    return render(request, "control_plane/transfers/destination_form.html", {
        "form": form, "destination": destination,
    })


@staff_member_required(login_url="control_plane:login")
@require_http_methods(["GET", "POST"])
def transfer_destination_delete_view(request, pk):
    destination = get_object_or_404(TransferDestination, pk=pk)
    if request.method == "POST":
        name = destination.name
        destination._history_user = request.user
        destination.delete()
        messages.success(request, f"SFTP destination '{name}' deleted.")
        return redirect("control_plane:transfer_destination_list")
    return render(request, "control_plane/transfers/destination_confirm_delete.html", {
        "destination": destination,
    })
