import logging

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db.models import Q
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_POST

from control_plane.models import CdcTableRebuild, Role, UserSourceAccess
from etl.tasks import rebuild_cdc_table_task


logger = logging.getLogger(__name__)


def _is_global_admin(user):
    return user.is_staff or user.is_superuser


def _visible_rebuilds(user):
    rebuilds = CdcTableRebuild.objects.select_related(
        "source_table__source"
    ).order_by("-updated_at", "-pk")
    if _is_global_admin(user):
        return rebuilds
    return rebuilds.filter(
        source_table__source__user_access__user=user
    ).distinct()


def _can_resume_rebuild(user, source_id):
    return _is_global_admin(user) or UserSourceAccess.objects.filter(
        user=user,
        source_id=source_id,
        role=Role.ADMIN,
    ).exists()


@login_required
def cdc_rebuild_list_view(request):
    rebuilds = _visible_rebuilds(request.user)
    global_admin = _is_global_admin(request.user)
    resumable_source_ids = set()
    if not global_admin:
        resumable_source_ids = set(
            UserSourceAccess.objects.filter(
                user=request.user,
                role=Role.ADMIN,
            ).values_list("source_id", flat=True)
        )
    query = request.GET.get("q", "").strip()
    if query:
        rebuilds = rebuilds.filter(
            Q(source_table__source__name__icontains=query)
            | Q(source_table__schema_name__icontains=query)
            | Q(source_table__table_name__icontains=query)
            | Q(status__icontains=query)
            | Q(last_error__icontains=query)
        )
    page_obj = Paginator(rebuilds, 20).get_page(request.GET.get("page"))
    return render(request, "control_plane/cdc_rebuilds.html", {
        "page_obj": page_obj,
        "search_query": query,
        "global_admin": global_admin,
        "resumable_source_ids": resumable_source_ids,
    })


@login_required
@require_POST
def cdc_rebuild_resume_view(request, rebuild_pk):
    rebuild = get_object_or_404(_visible_rebuilds(request.user), pk=rebuild_pk)
    source_id = rebuild.source_table.source_id
    if not _can_resume_rebuild(request.user, source_id):
        messages.error(request, "You are not authorized to resume rebuilds for this source.")
        return redirect("control_plane:cdc_rebuild_list")
    if rebuild.status != CdcTableRebuild.Status.CUTOVER_PENDING:
        messages.error(
            request,
            "Only CDC rebuilds pending cutover can be resumed from this page.",
        )
        return redirect("control_plane:cdc_rebuild_list")

    try:
        rebuild_cdc_table_task.apply_async(
            args=[rebuild.source_table_id, rebuild.pk],
            queue="queue_cdc",
        )
    except Exception:
        logger.exception("Could not queue CDC rebuild %s for resume.", rebuild.pk)
        messages.error(request, "The CDC rebuild could not be queued. Check the worker logs.")
    else:
        messages.success(
            request,
            f"Resume queued for {rebuild.source_table.source.name} "
            f"{rebuild.source_table.schema_name}.{rebuild.source_table.table_name}.",
        )
    return redirect("control_plane:cdc_rebuild_list")
