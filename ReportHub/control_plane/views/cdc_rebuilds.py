import logging

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import OuterRef, Q, Subquery
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.http import require_http_methods, require_POST

from control_plane.breadcrumbs import get_breadcrumbs_for_request
from control_plane.models import (
    CdcTableRebuild,
    CdcToBatchTransition,
    DbChoice,
    IngestionMode,
    Role,
    SourceProfile,
    SourceTable,
    UserSourceAccess,
)
from etl.tasks import (
    create_cdc_table_rebuild_record,
    rebuild_cdc_table_task,
)


logger = logging.getLogger(__name__)
ACTIVE_REBUILD_STATUSES = (
    CdcTableRebuild.Status.SNAPSHOTTING,
    CdcTableRebuild.Status.CUTOVER_PENDING,
)
ACTIVE_TRANSITION_STATUSES = (
    CdcToBatchTransition.Status.PREPARING,
    CdcToBatchTransition.Status.CUTOVER_PENDING,
)


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


def _visible_sources(user):
    sources = SourceProfile.objects.all()
    if _is_global_admin(user):
        return sources
    return sources.filter(user_access__user=user).distinct()


def _visible_rebuild_tables(user):
    return SourceTable.objects.select_related("source").filter(
        source__in=_visible_sources(user),
    ).filter(
        Q(cdc_managed=True) | Q(cdc_rebuilds__isnull=False),
    ).distinct()


def _return_url(request, table):
    location = request.POST.get("return_to", "") if request.method == "POST" else request.GET.get("return_to", "")
    if location == "tables":
        return reverse("control_plane:cdc_rebuild_source_tables", args=[table.source_id])
    if location == "history":
        return reverse("control_plane:cdc_rebuild_table_history", args=[table.pk])
    return reverse("control_plane:cdc_rebuild_list")


def _can_resume_rebuild(user, source_id):
    return _is_global_admin(user) or UserSourceAccess.objects.filter(
        user=user,
        source_id=source_id,
        role=Role.ADMIN,
    ).exists()


def _manageable_source_tables(user):
    tables = SourceTable.objects.select_related("source")
    if _is_global_admin(user):
        return tables
    return tables.filter(
        source__user_access__user=user,
        source__user_access__role=Role.ADMIN,
    ).distinct()


def _startable_source_tables(user, query):
    tables = _manageable_source_tables(user).filter(
        source__ingestion_mode=IngestionMode.CDC,
        source__is_active=True,
        cdc_managed=True,
    ).exclude(
        source__db_type=DbChoice.MONGODB,
    ).exclude(
        primary_key_fields=[],
    ).exclude(
        cdc_rebuilds__status__in=ACTIVE_REBUILD_STATUSES,
    ).exclude(
        source__cdc_to_batch_transitions__status__in=ACTIVE_TRANSITION_STATUSES,
    )
    if query:
        tables = tables.filter(
            Q(source__name__icontains=query)
            | Q(schema_name__icontains=query)
            | Q(table_name__icontains=query)
        )
    return tables.select_related("source").order_by(
        "source__name",
        "schema_name",
        "table_name",
    ).distinct()


def _rebuild_start_block_reason(table):
    source = table.source
    if source.ingestion_mode != IngestionMode.CDC:
        return "CDC table rebuilds require a source in CDC mode."
    if not source.is_active:
        return "The source must be active before a CDC table can be rebuilt."
    if source.db_type == DbChoice.MONGODB:
        return "CDC table rebuilds are not supported for MongoDB."
    if not table.cdc_managed or not table.primary_key_fields:
        return "A CDC rebuild requires a CDC-managed table with a discovered primary key."
    if CdcTableRebuild.objects.filter(
        source_table=table,
        status__in=ACTIVE_REBUILD_STATUSES,
    ).exists():
        return "A rebuild for this table is already in progress or pending cutover."
    if CdcToBatchTransition.objects.filter(
        source_id=source.pk,
        status__in=ACTIVE_TRANSITION_STATUSES,
    ).exists():
        return "A CDC-to-batch transition is active for this source."
    return ""


@login_required
def cdc_rebuild_list_view(request):
    query = request.GET.get("q", "").strip()
    status_filter = request.GET.get("status", "")
    sources = _visible_sources(request.user).filter(
        Q(tables__cdc_managed=True) | Q(tables__cdc_rebuilds__isnull=False),
    )
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
    page_obj = Paginator(sources.distinct().order_by("name", "pk"), 20).get_page(
        request.GET.get("page")
    )
    return render(request, "control_plane/cdc_rebuilds.html", {
        "view_level": "sources",
        "page_obj": page_obj,
        "search_query": query,
        "status_filter": status_filter,
        "filter_options": [
            {"value": "active", "label": "Active"},
            {"value": "inactive", "label": "Inactive"},
        ],
    })


@login_required
def cdc_rebuild_source_tables_view(request, source_pk):
    source = get_object_or_404(_visible_sources(request.user), pk=source_pk)
    query = request.GET.get("q", "").strip()
    tables = _visible_rebuild_tables(request.user).filter(source=source)
    if query:
        tables = tables.filter(Q(schema_name__icontains=query) | Q(table_name__icontains=query))
    latest = CdcTableRebuild.objects.filter(source_table_id=OuterRef("pk")).order_by(
        "-created_at", "-pk",
    )
    tables = tables.annotate(latest_rebuild_id=Subquery(latest.values("pk")[:1])).order_by(
        "schema_name", "table_name", "pk",
    )
    page_obj = Paginator(tables, 20).get_page(request.GET.get("page"))
    page_tables = list(page_obj)
    latest_by_id = CdcTableRebuild.objects.in_bulk(
        [table.latest_rebuild_id for table in page_tables if table.latest_rebuild_id]
    )
    startable_ids = set(
        _startable_source_tables(request.user, "").filter(
            pk__in=[table.pk for table in page_tables],
        ).values_list("pk", flat=True)
    )
    for table in page_tables:
        table.latest_rebuild = latest_by_id.get(table.latest_rebuild_id)
        table.can_start_rebuild = table.pk in startable_ids
    return render(request, "control_plane/cdc_rebuilds.html", {
        "view_level": "tables",
        "source": source,
        "page_obj": page_obj,
        "search_query": query,
    })


@login_required
def cdc_rebuild_table_history_view(request, source_table_pk):
    table = get_object_or_404(_visible_rebuild_tables(request.user), pk=source_table_pk)
    breadcrumbs = get_breadcrumbs_for_request(request)
    breadcrumbs.insert(-1, {
        "name": "Tables",
        "url": reverse("control_plane:cdc_rebuild_source_tables", args=[table.source_id]),
    })
    rebuilds = _visible_rebuilds(request.user).filter(source_table=table).order_by(
        "-created_at", "-pk",
    )
    query = request.GET.get("q", "").strip()
    status_filter = request.GET.get("status", "")
    if query:
        rebuilds = rebuilds.filter(
            Q(source_table__source__name__icontains=query)
            | Q(source_table__schema_name__icontains=query)
            | Q(source_table__table_name__icontains=query)
            | Q(status__icontains=query)
            | Q(last_error__icontains=query)
        )
    if status_filter:
        rebuilds = rebuilds.filter(status=status_filter)
    page_obj = Paginator(rebuilds, 20).get_page(request.GET.get("page"))
    return render(request, "control_plane/cdc_rebuilds.html", {
        "view_level": "history",
        "source_table": table,
        "page_obj": page_obj,
        "search_query": query,
        "status_filter": status_filter,
        "filter_options": [
            {"value": value, "label": label}
            for value, label in CdcTableRebuild.Status.choices
        ],
        "can_resume": _can_resume_rebuild(request.user, table.source_id),
        "breadcrumbs": breadcrumbs,
    })


@login_required
@require_http_methods(["GET", "POST"])
def cdc_rebuild_start_view(request, source_table_pk):
    tables = _manageable_source_tables(request.user)
    table = get_object_or_404(tables, pk=source_table_pk)

    if request.method == "GET":
        block_reason = _rebuild_start_block_reason(table)
        if block_reason:
            messages.error(request, block_reason)
            return redirect(_return_url(request, table))
        return render(
            request,
            "control_plane/cdc_rebuild_confirm.html",
            {
                "source_table": table,
                "return_to": request.GET.get("return_to", ""),
                "cancel_url": _return_url(request, table),
            },
        )

    if request.POST.get("confirm") != "yes":
        messages.error(request, "Confirm the CDC table rebuild before submitting it.")
        return redirect(_return_url(request, table))

    queue_error = False
    with transaction.atomic():
        source = get_object_or_404(
            SourceProfile.objects.select_for_update(),
            pk=table.source_id,
        )
        table = get_object_or_404(_manageable_source_tables(request.user), pk=source_table_pk)
        if table.source_id != source.pk:
            block_reason = "The source table changed while the rebuild was being confirmed."
        else:
            table.source = source
            block_reason = _rebuild_start_block_reason(table)
            if not block_reason:
                rebuild = create_cdc_table_rebuild_record(table)
                try:
                    rebuild_cdc_table_task.apply_async(
                        args=[table.pk, rebuild.pk],
                        queue="queue_cdc",
                    )
                except Exception:
                    logger.exception(
                        "Could not queue CDC rebuild for source table %s.",
                        table.pk,
                    )
                    rebuild.status = CdcTableRebuild.Status.FAILED
                    rebuild.last_error = (
                        "Could not queue the initial CDC rebuild task."
                    )
                    rebuild.save(update_fields=[
                        "status",
                        "last_error",
                        "updated_at",
                    ])
                    queue_error = True

    if block_reason:
        messages.error(request, block_reason)
        return redirect(_return_url(request, table))

    if queue_error:
        messages.error(request, "The CDC rebuild could not be queued. Check the worker logs.")
    else:
        messages.success(
            request,
            f"CDC rebuild queued for {source.name} "
            f"{table.schema_name}.{table.table_name}.",
        )
    return redirect(_return_url(request, table))


@login_required
@require_POST
def cdc_rebuild_resume_view(request, rebuild_pk):
    rebuild = get_object_or_404(_visible_rebuilds(request.user), pk=rebuild_pk)
    source_id = rebuild.source_table.source_id
    if not _can_resume_rebuild(request.user, source_id):
        messages.error(request, "You are not authorized to resume rebuilds for this source.")
        return redirect(_return_url(request, rebuild.source_table))
    if rebuild.status != CdcTableRebuild.Status.CUTOVER_PENDING:
        messages.error(
            request,
            "Only CDC rebuilds pending cutover can be resumed from this page.",
        )
        return redirect(_return_url(request, rebuild.source_table))

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
    return redirect(_return_url(request, rebuild.source_table))
