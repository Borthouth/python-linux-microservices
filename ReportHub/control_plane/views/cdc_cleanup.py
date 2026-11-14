import logging
from uuid import uuid4

from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.db import transaction
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_http_methods, require_POST
from kombu.exceptions import OperationalError

from control_plane.models import (
    CdcResourceCleanup,
    CdcResourceCleanupItem,
    CdcTableRebuild,
    SourceProfile,
)
from control_plane.tasks import (
    cleanup_cdc_resources_task,
    scan_cdc_resources_task,
)
from etl.cdc_cleanup import (
    CdcCleanupIneligible,
    _ensure_rebuild_eligible,
    _resource_specs,
    cleanup_candidates,
)


logger = logging.getLogger(__name__)


def _queue_cleanup_task(request, cleanup, task):
    task_id = str(uuid4())
    try:
        task.apply_async(
            args=[cleanup.pk],
            task_id=task_id,
            queue="queue_maintenance",
        )
    except OperationalError:
        logger.exception(
            "Could not queue CDC resource cleanup task for cleanup %s.",
            cleanup.pk,
        )
        cleanup.refresh_from_db()
        cleanup.status = CdcResourceCleanup.Status.FAILED
        cleanup.last_error = (
            "The maintenance task could not be queued. Check the broker and try again."
        )
        cleanup.completed_at = timezone.now()
        cleanup.updated_by = request.user
        cleanup._history_user = request.user
        cleanup.save(update_fields=[
            "status", "last_error", "completed_at", "updated_by", "updated_at",
        ])
        messages.error(request, cleanup.last_error)
        return False

    with transaction.atomic():
        cleanup = CdcResourceCleanup.objects.select_for_update().get(pk=cleanup.pk)
        cleanup.task_id = task_id
        cleanup.updated_by = request.user
        cleanup._history_user = request.user
        cleanup.save(update_fields=["task_id", "updated_by", "updated_at"])
    return True


@staff_member_required(login_url="control_plane:login")
@require_http_methods(["GET"])
def cdc_cleanup_list_view(request):
    jobs = CdcResourceCleanup.objects.select_related(
        "rebuild__source_table__source",
        "created_by",
    ).prefetch_related("items").order_by("-created_at", "-pk")[:20]
    return render(request, "control_plane/cdc_cleanup.html", {
        "candidates": cleanup_candidates(),
        "cleanup_jobs": jobs,
    })


@staff_member_required(login_url="control_plane:login")
@require_POST
def cdc_cleanup_scan_view(request, rebuild_pk):
    try:
        with transaction.atomic():
            rebuild = get_object_or_404(
                CdcTableRebuild.objects.select_for_update().select_related(
                    "source_table__source"
                ),
                pk=rebuild_pk,
            )
            SourceProfile.objects.select_for_update().get(
                pk=rebuild.source_table.source_id
            )
            rebuild = _ensure_rebuild_eligible(rebuild)
            cleanup = CdcResourceCleanup(
                rebuild=rebuild,
                status=CdcResourceCleanup.Status.SCANNING,
                created_by=request.user,
                updated_by=request.user,
            )
            cleanup._history_user = request.user
            cleanup.save()
            for resource_type, resource_name in _resource_specs(rebuild):
                item = CdcResourceCleanupItem(
                    cleanup=cleanup,
                    resource_type=resource_type,
                    resource_name=resource_name,
                    created_by=request.user,
                    updated_by=request.user,
                )
                item._history_user = request.user
                item.save()
    except CdcCleanupIneligible as error:
        messages.error(request, str(error))
        return redirect("control_plane:cdc_cleanup_list")

    if _queue_cleanup_task(request, cleanup, scan_cdc_resources_task):
        messages.info(
            request,
            "Resource inventory queued. No resources will be deleted until you review and confirm it.",
        )
    return redirect("control_plane:cdc_cleanup_list")


@staff_member_required(login_url="control_plane:login")
@require_POST
def cdc_cleanup_confirm_view(request, cleanup_pk):
    if request.POST.get("confirm_cleanup") != "yes":
        messages.error(request, "Confirm the resource cleanup before submitting it.")
        return redirect("control_plane:cdc_cleanup_list")

    try:
        with transaction.atomic():
            cleanup = get_object_or_404(
                CdcResourceCleanup.objects.select_for_update().select_related(
                    "rebuild__source_table__source"
                ),
                pk=cleanup_pk,
            )
            if cleanup.status != CdcResourceCleanup.Status.AWAITING_CONFIRMATION:
                raise CdcCleanupIneligible(
                    "Only a completed inventory awaiting confirmation can be cleaned up."
                )
            SourceProfile.objects.select_for_update().get(
                pk=cleanup.rebuild.source_table.source_id
            )
            _ensure_rebuild_eligible(cleanup.rebuild, cleanup_id=cleanup.pk)
            if cleanup.items.filter(
                exists_at_scan__isnull=True
            ).exists() or cleanup.items.filter(scan_error__gt="").exists():
                raise CdcCleanupIneligible(
                    "Every resource must be verified before cleanup can be confirmed."
                )
            if not cleanup.items.filter(exists_at_scan=True).exists():
                raise CdcCleanupIneligible(
                    "The inventory contains no resources to clean up."
                )
            cleanup.status = CdcResourceCleanup.Status.QUEUED
            cleanup.task_id = ""
            cleanup.started_at = None
            cleanup.last_error = ""
            cleanup.updated_by = request.user
            cleanup._history_user = request.user
            cleanup.save(update_fields=[
                "status", "task_id", "started_at", "last_error",
                "updated_by", "updated_at",
            ])
    except CdcCleanupIneligible as error:
        messages.error(request, str(error))
        return redirect("control_plane:cdc_cleanup_list")

    if _queue_cleanup_task(request, cleanup, cleanup_cdc_resources_task):
        messages.success(
            request,
            "Confirmed cleanup queued. Eligibility and ownership will be rechecked before each operation.",
        )
    return redirect("control_plane:cdc_cleanup_list")


@staff_member_required(login_url="control_plane:login")
@require_POST
def cdc_cleanup_recover_view(request, cleanup_pk):
    if request.POST.get("acknowledge_uncertain") != "yes":
        messages.error(
            request,
            "Acknowledge the uncertain external operation before requesting a fresh inventory.",
        )
        return redirect("control_plane:cdc_cleanup_list")
    with transaction.atomic():
        cleanup = get_object_or_404(
            CdcResourceCleanup.objects.select_for_update(),
            pk=cleanup_pk,
        )
        if cleanup.status not in {
            CdcResourceCleanup.Status.SCANNING,
            CdcResourceCleanup.Status.RUNNING,
        }:
            messages.error(
                request,
                "Only a scanning or running cleanup can be marked uncertain.",
            )
            return redirect("control_plane:cdc_cleanup_list")
        was_running = cleanup.status == CdcResourceCleanup.Status.RUNNING
        cleanup.status = CdcResourceCleanup.Status.FAILED
        cleanup.last_error = (
            "An administrator marked this attempt interrupted. Its external request "
            "may still finish; start a fresh inventory before retrying."
            if was_running
            else "An administrator marked this inventory interrupted. It was read-only; "
            "start a fresh inventory before retrying."
        )
        cleanup.completed_at = timezone.now()
        cleanup.updated_by = request.user
        cleanup._history_user = request.user
        cleanup.save(update_fields=[
            "status", "last_error", "completed_at", "updated_by", "updated_at",
        ])
        running_items = cleanup.items.filter(status=CdcResourceCleanupItem.Status.RUNNING)
        for item in running_items:
            item.status = CdcResourceCleanupItem.Status.FAILED
            item.last_error = (
                "External result is uncertain; a fresh inventory is required."
            )
            item.updated_by = request.user
            item._history_user = request.user
            item.save(update_fields=[
                "status", "last_error", "updated_by", "updated_at",
            ])
    messages.warning(
        request,
        "Attempt marked uncertain. No retry was queued; run a fresh resource inventory and review it first.",
    )
    return redirect("control_plane:cdc_cleanup_list")
