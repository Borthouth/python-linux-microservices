import logging
from uuid import uuid4

from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import Exists, OuterRef, Q
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_http_methods, require_POST
from kombu.exceptions import OperationalError

from control_plane.breadcrumbs import get_breadcrumbs_for_request
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


def _cleanup_redirect(request, *, rebuild_pk=None, cleanup_pk=None):
    return_to = request.POST.get("return_to")
    if return_to == "attempt" and cleanup_pk is not None:
        return redirect("control_plane:cdc_cleanup_attempt", cleanup_pk=cleanup_pk)
    if return_to in {"source", "attempt"}:
        if rebuild_pk is not None:
            rebuild = get_object_or_404(
                CdcTableRebuild.objects.select_related("source_table"), pk=rebuild_pk,
            )
        else:
            cleanup = get_object_or_404(
                CdcResourceCleanup.objects.select_related("rebuild__source_table"), pk=cleanup_pk,
            )
            rebuild = cleanup.rebuild
        return redirect("control_plane:cdc_cleanup_source", source_pk=rebuild.source_table.source_id)
    return redirect("control_plane:cdc_cleanup_list")


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
    query = request.GET.get("q", "").strip()
    status_filter = request.GET.get("status", "")
    rebuilds = CdcTableRebuild.objects.filter(
        source_table__source_id=OuterRef("pk"),
        status__in=[CdcTableRebuild.Status.FAILED, CdcTableRebuild.Status.SUCCEEDED],
    )
    cleanups = CdcResourceCleanup.objects.filter(rebuild__source_table__source_id=OuterRef("pk"))
    uncertain = cleanups.filter(
        external_operation_uncertain=True,
        uncertainty_override_at__isnull=True,
    )
    sources = SourceProfile.objects.annotate(
        has_rebuild=Exists(rebuilds),
        has_cleanup=Exists(cleanups),
        cleanup_blocked=Exists(uncertain),
    ).filter(Q(has_rebuild=True) | Q(has_cleanup=True))
    blocked_source_count = sources.filter(cleanup_blocked=True).count()
    if query:
        sources = sources.filter(
            Q(name__icontains=query)
            | Q(db_type__icontains=query)
            | Q(host__icontains=query)
            | Q(database_name__icontains=query)
        )
    if status_filter == "active":
        sources = sources.filter(is_active=True)
    elif status_filter == "inactive":
        sources = sources.filter(is_active=False)
    page_obj = Paginator(sources.order_by("name", "pk"), 20).get_page(request.GET.get("page"))
    return render(request, "control_plane/cdc_cleanup.html", {
        "view_level": "sources",
        "page_obj": page_obj,
        "search_query": query,
        "status_filter": status_filter,
        "filter_options": [
            {"value": "active", "label": "Active"},
            {"value": "inactive", "label": "Inactive"},
        ],
        "blocked_source_count": blocked_source_count,
    })


@staff_member_required(login_url="control_plane:login")
@require_http_methods(["GET"])
def cdc_cleanup_source_view(request, source_pk):
    source = get_object_or_404(SourceProfile, pk=source_pk)
    jobs = CdcResourceCleanup.objects.select_related(
        "rebuild__source_table__source",
        "created_by",
    ).filter(rebuild__source_table__source=source).order_by("-created_at", "-pk")[:20]
    return render(request, "control_plane/cdc_cleanup.html", {
        "view_level": "source",
        "source": source,
        "candidates": cleanup_candidates(source_id=source.pk),
        "cleanup_jobs": jobs,
        "uncertain_cleanups": CdcResourceCleanup.objects.filter(
            rebuild__source_table__source=source,
            external_operation_uncertain=True,
            uncertainty_override_at__isnull=True,
        ).select_related("rebuild__source_table__source").order_by("-created_at", "-pk"),
    })


@staff_member_required(login_url="control_plane:login")
@require_http_methods(["GET"])
def cdc_cleanup_attempt_view(request, cleanup_pk):
    cleanup = get_object_or_404(
        CdcResourceCleanup.objects.select_related(
            "rebuild__source_table__source", "created_by", "updated_by",
        ).prefetch_related("items"),
        pk=cleanup_pk,
    )
    breadcrumbs = get_breadcrumbs_for_request(request)
    breadcrumbs.insert(-1, {
        "name": "Source Resources",
        "url": reverse(
            "control_plane:cdc_cleanup_source",
            args=[cleanup.rebuild.source_table.source_id],
        ),
    })
    return render(request, "control_plane/cdc_cleanup_attempt.html", {
        "cleanup": cleanup,
        "breadcrumbs": breadcrumbs,
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
        return _cleanup_redirect(request, rebuild_pk=rebuild_pk)

    if _queue_cleanup_task(request, cleanup, scan_cdc_resources_task):
        messages.info(
            request,
            "Resource inventory queued. No resources will be deleted until you review and confirm it.",
        )
    if request.POST.get("return_to") == "attempt":
        return _cleanup_redirect(request, cleanup_pk=cleanup.pk)
    return _cleanup_redirect(request, rebuild_pk=rebuild_pk)


@staff_member_required(login_url="control_plane:login")
@require_POST
def cdc_cleanup_confirm_view(request, cleanup_pk):
    if request.POST.get("confirm_cleanup") != "yes":
        messages.error(request, "Confirm the resource cleanup before submitting it.")
        return _cleanup_redirect(request, cleanup_pk=cleanup_pk)

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
        return _cleanup_redirect(request, cleanup_pk=cleanup_pk)

    if _queue_cleanup_task(request, cleanup, cleanup_cdc_resources_task):
        messages.success(
            request,
            "Confirmed cleanup queued. Eligibility and ownership will be rechecked before each operation.",
        )
    return _cleanup_redirect(request, cleanup_pk=cleanup_pk)


@staff_member_required(login_url="control_plane:login")
@require_POST
def cdc_cleanup_recover_view(request, cleanup_pk):
    if request.POST.get("acknowledge_uncertain") != "yes":
        messages.error(
            request,
            "Acknowledge the interrupted attempt before marking it interrupted.",
        )
        return _cleanup_redirect(request, cleanup_pk=cleanup_pk)
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
            return _cleanup_redirect(request, cleanup_pk=cleanup_pk)
        was_running = cleanup.status == CdcResourceCleanup.Status.RUNNING
        cleanup.status = CdcResourceCleanup.Status.FAILED
        if was_running:
            cleanup.external_operation_uncertain = True
        cleanup.last_error = (
            "An administrator marked this attempt interrupted. Its external request "
            "may still finish; further cleanup is blocked unless an administrator accepts the risk."
            if was_running
            else "An administrator marked this inventory interrupted. It was read-only; "
            "start a fresh inventory before retrying."
        )
        cleanup.completed_at = timezone.now()
        cleanup.updated_by = request.user
        cleanup._history_user = request.user
        cleanup.save(update_fields=[
            "status", "last_error", "external_operation_uncertain", "completed_at", "updated_by", "updated_at",
        ])
        running_items = cleanup.items.filter(status=CdcResourceCleanupItem.Status.RUNNING)
        for item in running_items:
            item.status = CdcResourceCleanupItem.Status.FAILED
            item.last_error = (
                "External result is uncertain; further cleanup is blocked."
                if was_running else "The read-only inventory was interrupted."
            )
            item.updated_by = request.user
            item._history_user = request.user
            item.save(update_fields=[
                "status", "last_error", "updated_by", "updated_at",
            ])
    messages.warning(
        request,
        "Attempt marked interrupted. Further cleanup is blocked while its outcome is uncertain."
        if was_running else "Inventory marked interrupted. A fresh inventory can be started.",
    )
    return _cleanup_redirect(request, cleanup_pk=cleanup_pk)


@staff_member_required(login_url="control_plane:login")
@require_POST
def cdc_cleanup_override_view(request, cleanup_pk):
    reason = request.POST.get("reason", "").strip()
    if (
        request.POST.get("accept_risk") != "yes"
        or not reason
        or len(reason) > 1000
    ):
        messages.error(request, "Accept the risk and provide a reason of up to 1000 characters.")
        return _cleanup_redirect(request, cleanup_pk=cleanup_pk)
    with transaction.atomic():
        cleanup = get_object_or_404(
            CdcResourceCleanup.objects.select_for_update().select_related(
                "rebuild__source_table__source"
            ),
            pk=cleanup_pk,
        )
        source = SourceProfile.objects.select_for_update().get(
            pk=cleanup.rebuild.source_table.source_id
        )
        if request.POST.get("source_name", "") != source.name:
            messages.error(request, "Type the source name exactly to accept this risk.")
        elif (
            cleanup.status != CdcResourceCleanup.Status.FAILED
            or not cleanup.external_operation_uncertain
            or cleanup.uncertainty_override_at is not None
        ):
            messages.error(request, "Only a failed, uncertain cleanup can be overridden once.")
        else:
            cleanup.uncertainty_override_at = timezone.now()
            cleanup.uncertainty_override_reason = reason
            cleanup.updated_by = request.user
            cleanup._history_user = request.user
            cleanup._change_reason = "Administrator accepted uncertain cleanup risk"
            cleanup.save(update_fields=[
                "uncertainty_override_at", "uncertainty_override_reason",
                "updated_by", "updated_at",
            ])
            stale_cleanups = CdcResourceCleanup.objects.select_for_update().filter(
                rebuild__source_table__source=source,
                status__in=[
                    CdcResourceCleanup.Status.SCANNING,
                    CdcResourceCleanup.Status.AWAITING_CONFIRMATION,
                    CdcResourceCleanup.Status.QUEUED,
                ],
            ).exclude(pk=cleanup.pk)
            for stale in stale_cleanups:
                stale.status = CdcResourceCleanup.Status.FAILED
                stale.last_error = (
                    "This inventory predates a risk override. Start a fresh inventory."
                )
                stale.completed_at = cleanup.uncertainty_override_at
                stale.updated_by = request.user
                stale._history_user = request.user
                stale.save(update_fields=[
                    "status", "last_error", "completed_at", "updated_by", "updated_at",
                ])
            messages.warning(
                request,
                "Risk accepted and recorded. The old request may still execute; "
                "start a fresh inventory before any further cleanup.",
            )
    return _cleanup_redirect(request, cleanup_pk=cleanup_pk)
