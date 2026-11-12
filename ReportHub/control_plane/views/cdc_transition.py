import logging
from uuid import UUID

import clickhouse_connect
from django.conf import settings
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.db import DatabaseError, transaction
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from control_plane.cdc_transition_status import cdc_to_batch_status_context
from control_plane.metadata_guard import (
    SourceMetadataCutoverPending,
    acquire_source_metadata_lock,
    source_metadata_write_guard,
)
from control_plane.models import CdcToBatchTableCheckpoint, CdcToBatchTransition, SourceProfile
from control_plane.models import IngestionMode
from control_plane.views import sources
from control_plane.views.sources import is_admin
from etl.cdc_to_batch import prepare_cdc_to_batch_transition
from etl.cdc import _clickhouse_table_uuid
from etl.tasks import stage_cdc_to_batch_transition_task
from reports.cutover_guard import acquire_report_cutover_lock
from reports.query_builder import source_table_clickhouse_name


logger = logging.getLogger(__name__)


@login_required
@user_passes_test(is_admin)
def source_edit_with_metadata_guard(request, pk):
    if request.method != "POST":
        return sources.source_edit_view(request, pk)
    try:
        with source_metadata_write_guard(pk):
            return sources.source_edit_view(request, pk)
    except SourceMetadataCutoverPending as exc:
        messages.error(request, str(exc))
        return redirect("control_plane:source_edit", pk=pk)


@login_required
@user_passes_test(is_admin)
def source_toggle_with_metadata_guard(request, pk):
    try:
        with source_metadata_write_guard(pk):
            return sources.source_toggle_active_view(request, pk)
    except SourceMetadataCutoverPending as exc:
        messages.error(request, str(exc))
        return redirect("control_plane:source_list")


@login_required
@user_passes_test(is_admin)
def source_delete_with_metadata_guard(request, pk):
    if request.method != "POST":
        return sources.source_delete_view(request, pk)
    try:
        with source_metadata_write_guard(pk):
            return sources.source_delete_view(request, pk)
    except SourceMetadataCutoverPending as exc:
        messages.error(request, str(exc))
        return redirect("control_plane:source_edit", pk=pk)


@login_required
@user_passes_test(is_admin)
@require_GET
def source_cdc_to_batch_status_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk)
    return render(
        request, "control_plane/source/cdc_to_batch_status.html",
        cdc_to_batch_status_context(profile),
    )


@login_required
@user_passes_test(is_admin)
@require_POST
def source_cdc_to_batch_staging_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk)
    action = request.POST.get("action")
    if action not in ("start", "retry", "cancel", "abandon_pending"):
        messages.error(request, "Invalid staging action.")
        return redirect("control_plane:source_edit", pk=pk)
    transition_generation = None
    if action in ("retry", "cancel", "abandon_pending"):
        try:
            transition_generation = UUID(request.POST.get("transition", ""))
        except (ValueError, AttributeError):
            messages.error(request, "The staging transition changed or is missing. Reload and try again.")
            return redirect("control_plane:source_edit", pk=pk)
    if request.POST.get("confirm") != "yes":
        return render(request, "control_plane/source/cdc_to_batch_confirm.html", {
            "profile": profile, "action": action, "transition_generation": transition_generation,
        })

    if action == "cancel":
        return _cancel_staging(request, profile, transition_generation)
    if action == "abandon_pending":
        return _abandon_pending(request, profile, transition_generation)

    try:
        with transaction.atomic():
            source = SourceProfile.objects.select_for_update().get(pk=pk)
            existing = source.cdc_to_batch_transitions.filter(
                status__in=[
                    CdcToBatchTransition.Status.PREPARING,
                    CdcToBatchTransition.Status.CUTOVER_PENDING,
                ]
            ).first()
            if action == "start" and existing:
                raise ValueError("A transition is already active for this source.")
            if action == "retry":
                if (
                    not existing or existing.status != CdcToBatchTransition.Status.PREPARING
                    or existing.generation != transition_generation
                ):
                    raise ValueError("The staging transition changed. Reload and try again.")
                transition = prepare_cdc_to_batch_transition(pk)
                if transition.pk != existing.pk:
                    raise RuntimeError("The transition changed before staging could be retried.")
            else:
                transition = prepare_cdc_to_batch_transition(pk)
    except (ValueError, RuntimeError) as exc:
        messages.error(request, str(exc))
        return redirect("control_plane:source_edit", pk=pk)

    try:
        stage_cdc_to_batch_transition_task.apply_async(
            args=[transition.pk], queue="queue_backfill"
        )
    except Exception:
        logger.exception("Could not confirm staging dispatch for transition %s", transition.pk)
        CdcToBatchTransition.objects.filter(
            pk=transition.pk, status=CdcToBatchTransition.Status.PREPARING
        ).update(
            last_error="Staging dispatch could not be confirmed. Check worker logs before retrying.",
            updated_at=timezone.now(),
        )
        messages.error(
            request, "Staging dispatch could not be confirmed. Check worker logs before retrying."
        )
    else:
        messages.info(request, "Batch staging queued. The source remains in CDC mode.")
    return redirect("control_plane:source_edit", pk=pk)


def _cancel_staging(request, profile, transition_generation):
    try:
        with transaction.atomic():
            transition = profile.cdc_to_batch_transitions.filter(
                status=CdcToBatchTransition.Status.PREPARING,
                generation=transition_generation,
            ).first()
            if transition is None:
                raise ValueError("The staging transition changed. Reload and try again.")
            list(
                CdcToBatchTableCheckpoint.objects.select_for_update(nowait=True)
                .filter(transition=transition).order_by("pk")
            )
            source = SourceProfile.objects.select_for_update().get(pk=profile.pk)
            transition = CdcToBatchTransition.objects.select_for_update().get(pk=transition.pk)
            if (
                transition.source_id != source.pk
                or transition.status != CdcToBatchTransition.Status.PREPARING
            ):
                raise ValueError("The transition can no longer be cancelled as staging.")
            transition.status = CdcToBatchTransition.Status.FAILED
            transition.last_error = "Staging cancelled by an administrator. Staging tables were retained."
            transition.staging_verified_at = None
            transition.save(update_fields=["status", "last_error", "staging_verified_at", "updated_at"])
    except ValueError as exc:
        messages.error(request, str(exc))
    except DatabaseError:
        logger.exception("Could not cancel staging for source %s", profile.pk)
        messages.error(
            request, "Staging could not be cancelled. A table may still be staging; check logs and retry."
        )
    else:
        messages.success(
            request, "Staging cancelled. The source remains in CDC mode; staging tables were retained."
        )
    return redirect("control_plane:source_edit", pk=profile.pk)


def _abandon_pending(request, profile, transition_generation):
    try:
        with transaction.atomic():
            acquire_source_metadata_lock(profile.pk)
            acquire_report_cutover_lock(profile.pk)
            transition = profile.cdc_to_batch_transitions.filter(
                generation=transition_generation,
            ).first()
            if transition is None:
                raise ValueError("The transition changed. Reload and try again.")
            checkpoints = list(
                CdcToBatchTableCheckpoint.objects.select_for_update()
                .filter(transition=transition).order_by("pk")
            )
            source = SourceProfile.objects.select_for_update().get(pk=profile.pk)
            transition = CdcToBatchTransition.objects.select_for_update().get(pk=transition.pk)
            if (
                transition.status != CdcToBatchTransition.Status.CUTOVER_PENDING
                or source.ingestion_mode != IngestionMode.CDC
                or not checkpoints
                or {item.pk for item in checkpoints}
                != set(transition.table_checkpoints.values_list("pk", flat=True))
            ):
                raise ValueError("The transition changed. Reload and try again.")
            if any(
                checkpoint.status != CdcToBatchTableCheckpoint.Status.STAGED
                or checkpoint.cutover_command_inflight
                or not checkpoint.active_table_checked
                for checkpoint in checkpoints
            ):
                raise ValueError(
                    "This cutover cannot be abandoned because a table exchange may have occurred."
                )
            tables = list(source.tables.select_for_update().order_by("pk"))
            if {table.pk for table in tables} != {
                checkpoint.source_table_id for checkpoint in checkpoints
            }:
                raise ValueError("The transition tables changed; cutover cannot be abandoned.")
            by_table = {checkpoint.source_table_id: checkpoint for checkpoint in checkpoints}
            try:
                client = clickhouse_connect.get_client(
                    host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
                    port=getattr(settings, "CLICKHOUSE_PORT", 8123),
                    username=getattr(settings, "CLICKHOUSE_USER", "default"),
                    password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
                )
                try:
                    identities = {
                        table.pk: _clickhouse_table_uuid(client, source_table_clickhouse_name(table))
                        for table in tables
                    }
                finally:
                    client.close()
            except Exception:
                logger.exception("Could not verify active tables for transition %s", transition.pk)
                raise RuntimeError("Could not verify active tables. Check logs and retry.")
            if any(
                identities[table.pk] != by_table[table.pk].active_uuid_before
                for table in tables
            ):
                raise ValueError(
                    "This cutover cannot be abandoned because an active table changed."
                )
            transition.status = CdcToBatchTransition.Status.FAILED
            transition.staging_verified_at = None
            transition.last_error = (
                "Pending cutover abandoned by an administrator. The source remains in CDC mode; "
                "staging and generation tables were retained."
            )
            transition.save(update_fields=["status", "staging_verified_at", "last_error", "updated_at"])
    except (ValueError, RuntimeError) as exc:
        messages.error(request, str(exc))
    except DatabaseError:
        logger.exception("Could not abandon pending cutover for source %s", profile.pk)
        messages.error(request, "Could not abandon pending cutover. Check logs and retry.")
    else:
        messages.success(
            request, "Pending cutover abandoned. The source remains in CDC mode; staging and generation tables were retained."
        )
    return redirect("control_plane:source_edit", pk=profile.pk)
