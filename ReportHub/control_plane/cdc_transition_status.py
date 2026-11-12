from control_plane.models import CdcToBatchTableCheckpoint, CdcToBatchTransition


def cdc_to_batch_status_context(profile):
    transition = profile.cdc_to_batch_transitions.order_by("-created_at", "-pk").first()
    checkpoints = []
    if transition:
        checkpoints = list(
            transition.table_checkpoints.select_related("source_table").order_by("pk")
        )
    return {
        "profile": profile,
        "transition": transition,
        "checkpoints": checkpoints,
        "staged_count": sum(
            checkpoint.status in (
                CdcToBatchTableCheckpoint.Status.STAGED,
                CdcToBatchTableCheckpoint.Status.CUTOVER,
            )
            for checkpoint in checkpoints
        ),
        "cutover_count": sum(
            checkpoint.status == CdcToBatchTableCheckpoint.Status.CUTOVER
            for checkpoint in checkpoints
        ),
        "generation_copy_count": sum(bool(checkpoint.generation_uuid) for checkpoint in checkpoints),
        "generation_copies_started": any(checkpoint.generation_table for checkpoint in checkpoints),
        "pre_synchronous_staging_count": sum(
            checkpoint.status == CdcToBatchTableCheckpoint.Status.STAGED
            and (
                not isinstance(checkpoint.staging_schema, dict)
                or checkpoint.staging_schema.get("synchronous_inserts") is not True
            )
            for checkpoint in checkpoints
        ),
        "external_operations": list(profile.cdc_external_operations.order_by("created_at", "pk")),
        "transition_active": bool(
            transition and transition.status in (
                CdcToBatchTransition.Status.PREPARING,
                CdcToBatchTransition.Status.CUTOVER_PENDING,
            )
        ),
    }
