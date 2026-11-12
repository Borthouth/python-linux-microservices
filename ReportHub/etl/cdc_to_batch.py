from enum import Enum
import re
from uuid import uuid4

import clickhouse_connect
import pandas as pd
import sqlalchemy as sa
from django.conf import settings
from django.db import connection, transaction

from control_plane.models import (
    CdcTableRebuild,
    CdcToBatchTableCheckpoint,
    CdcToBatchTransition,
    DbChoice,
    IngestionMode,
    SourceProfile,
    SourceTable,
)
from control_plane.metadata_guard import acquire_source_metadata_lock
from etl.cdc import _clickhouse_table_uuid
from etl.cdc_external_operations import _assert_no_cdc_external_operations
from etl.tasks import (
    _batch_required_fields_by_table,
    _batch_sanitized_rows,
    _batch_selected_fields,
    create_source_engine,
    map_to_clickhouse_type,
)
from reports.query_builder import (
    clickhouse_table_name,
    quote_clickhouse_identifier,
    source_table_clickhouse_name,
)
from reports.cutover_guard import acquire_report_cutover_lock


class CdcToBatchCutoverAction(Enum):
    EXCHANGE = "exchange"
    RENAME = "rename"
    COMPLETE = "complete"


class CdcToBatchStepResult(Enum):
    TABLE_CUTOVER = "table_cutover"
    SUCCEEDED = "succeeded"


def validate_cdc_to_batch_table_names(source, tables):
    seen = {}
    physical_names = {}
    for table in tables:
        name = clickhouse_table_name(source.pk, table.schema_name, table.table_name)
        if name in seen:
            previous = seen[name]
            raise RuntimeError(
                f"Source tables {previous.schema_name}.{previous.table_name} and "
                f"{table.schema_name}.{table.table_name} map to the same "
                f"ClickHouse table {name}."
            )
        seen[name] = table
        physical_name = source_table_clickhouse_name(table)
        if physical_name in physical_names:
            raise RuntimeError(f"Source tables map to the same ClickHouse table {physical_name}.")
        physical_names[physical_name] = table


def plan_cdc_to_batch_table_cutover(checkpoint, active_uuid, staging_uuid):
    """Classify a checkpoint using UUIDs observed under the future cutover fence.

    The caller must persist the initial active-table observation before any
    ClickHouse operation. This function neither establishes a fence nor changes
    a table or checkpoint.
    """
    if checkpoint.status not in (
        CdcToBatchTableCheckpoint.Status.STAGED,
        CdcToBatchTableCheckpoint.Status.CUTOVER,
    ):
        raise RuntimeError("The checkpoint is not staged for cutover.")
    if not checkpoint.active_table_checked or not checkpoint.staging_uuid:
        raise RuntimeError("Cutover UUID checkpoint is incomplete.")

    previous_uuid = checkpoint.active_uuid_before
    expected_uuid = checkpoint.staging_uuid
    if previous_uuid and previous_uuid == expected_uuid:
        raise RuntimeError("Active and staging checkpoint UUIDs must be distinct.")

    if active_uuid == expected_uuid and staging_uuid == previous_uuid:
        return CdcToBatchCutoverAction.COMPLETE

    if checkpoint.status == CdcToBatchTableCheckpoint.Status.CUTOVER:
        raise RuntimeError("Completed cutover tables changed unexpectedly.")

    if active_uuid == previous_uuid and staging_uuid == expected_uuid:
        return (
            CdcToBatchCutoverAction.EXCHANGE
            if previous_uuid else CdcToBatchCutoverAction.RENAME
        )

    raise RuntimeError("Cutover table UUIDs changed unexpectedly.")


def prepare_cdc_to_batch_transition(source_pk):
    with transaction.atomic():
        source = SourceProfile.objects.select_for_update().get(pk=source_pk)
        if (
            not source.is_active
            or source.ingestion_mode != IngestionMode.CDC
            or source.db_type == DbChoice.MONGODB
        ):
            raise ValueError(
                "A CDC-to-batch transition requires an active relational CDC source."
            )

        if CdcTableRebuild.objects.filter(
            source_table__source=source,
            status__in=[
                CdcTableRebuild.Status.SNAPSHOTTING,
                CdcTableRebuild.Status.CUTOVER_PENDING,
            ],
        ).exists():
            raise RuntimeError(
                "Finish or resolve active CDC table rebuilds before starting "
                "a CDC-to-batch transition."
            )

        tables = list(source.tables.order_by("pk"))
        if not tables:
            raise ValueError(
                "A CDC-to-batch transition requires discovered source tables."
            )
        validate_cdc_to_batch_table_names(source, tables)

        transition = source.cdc_to_batch_transitions.filter(
            status__in=[
                CdcToBatchTransition.Status.PREPARING,
                CdcToBatchTransition.Status.CUTOVER_PENDING,
            ],
        ).first()
        if transition:
            return transition

        transition = CdcToBatchTransition.objects.create(source=source)
        CdcToBatchTableCheckpoint.objects.bulk_create([
            CdcToBatchTableCheckpoint(
                transition=transition,
                source_table=table,
                staging_table=f"rh_batch_{transition.generation.hex}_{table.pk}",
            )
            for table in tables
        ])
        return transition


def _stage_fields(profile, table):
    required = _batch_required_fields_by_table(profile)
    fields = _batch_selected_fields(profile, table, required)
    fields_by_name = {field.field_name: field for field in table.fields.all()}
    missing = required.get((table.schema_name, table.table_name), set()) - fields_by_name.keys()
    if missing:
        raise RuntimeError(f"Required source fields are missing from metadata: {sorted(missing)}.")
    for key in table.primary_key_fields:
        if key not in fields_by_name:
            raise RuntimeError(f"Primary key field {key!r} is missing from source metadata.")
        if all(field.field_name != key for field in fields):
            fields.append(fields_by_name[key])
    if not fields:
        raise RuntimeError(
            f"No batch fields selected for {table.schema_name}.{table.table_name}."
        )
    return sorted(fields, key=lambda field: field.pk)


def _field_signature(profile, fields, table):
    return {
        "db_type": profile.db_type,
        "host": profile.host,
        "port": profile.port,
        "database_name": profile.database_name,
        "username": profile.username,
        "schema_name": table.schema_name,
        "table_name": table.table_name,
        "primary_key_fields": list(table.primary_key_fields),
        "fields": [[field.field_name, field.data_type] for field in fields],
        "synchronous_inserts": True,
    }


def _validate_staging_table(ch_client, staging_table, fields, primary_keys):
    table_metadata = ch_client.query(
        "SELECT engine, sorting_key, partition_key, primary_key, sampling_key "
        "FROM system.tables WHERE database = currentDatabase() "
        "AND name = {table_name:String}",
        parameters={"table_name": staging_table},
    ).result_rows
    if len(table_metadata) != 1 or tuple(table_metadata[0]) != (
        "MergeTree", "", "", "", ""
    ):
        raise RuntimeError("The staged ClickHouse table engine or keys changed.")
    expected = [
        (
            field.field_name,
            map_to_clickhouse_type(field.data_type, field.field_name not in primary_keys),
            "",
            "",
        )
        for field in fields
    ]
    actual = [
        tuple(row[:4])
        for row in ch_client.query(
            f"DESCRIBE TABLE {quote_clickhouse_identifier(staging_table)}"
        ).result_rows
    ]
    if actual != expected:
        raise RuntimeError("The staged ClickHouse table columns changed.")


def _validate_staging_row_count(ch_client, staging_table, expected_count):
    if expected_count is None:
        raise RuntimeError("The staged row count is missing; restage the table.")
    rows = ch_client.query(
        f"SELECT count() FROM {quote_clickhouse_identifier(staging_table)}"
    ).result_rows
    if len(rows) != 1 or len(rows[0]) != 1 or rows[0][0] != expected_count:
        raise RuntimeError("The staged ClickHouse table row count changed.")


def checkpoint_cdc_to_batch_active_table(checkpoint_pk):
    """Record the live table identity during preparation, without cutting over."""
    with transaction.atomic():
        checkpoint = CdcToBatchTableCheckpoint.objects.select_for_update().get(
            pk=checkpoint_pk
        )
        transition = CdcToBatchTransition.objects.get(pk=checkpoint.transition_id)
        profile = SourceProfile.objects.select_for_update().get(pk=transition.source_id)
        transition = CdcToBatchTransition.objects.select_for_update().get(
            pk=transition.pk
        )
        table = SourceTable.objects.select_for_update().get(
            pk=checkpoint.source_table_id
        )
        if (
            transition.status != CdcToBatchTransition.Status.PREPARING
            or checkpoint.status != CdcToBatchTableCheckpoint.Status.STAGED
            or not profile.is_active
            or profile.ingestion_mode != IngestionMode.CDC
            or profile.db_type == DbChoice.MONGODB
            or table.source_id != profile.pk
        ):
            raise RuntimeError("The checkpoint is not eligible for active table observation.")
        validate_cdc_to_batch_table_names(profile, profile.tables.order_by("pk"))
        fields = _stage_fields(profile, table)
        if checkpoint.staging_schema != _field_signature(profile, fields, table):
            raise RuntimeError("Source or table configuration changed after batch staging.")

        active_name = source_table_clickhouse_name(table)
        if active_name == checkpoint.staging_table:
            raise RuntimeError("The active and staging table names must be distinct.")
        ch_client = clickhouse_connect.get_client(
            host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
            port=getattr(settings, "CLICKHOUSE_PORT", 8123),
            username=getattr(settings, "CLICKHOUSE_USER", "default"),
            password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
        )
        try:
            engine_type = ch_client.query(
                "SELECT engine FROM system.databases WHERE name = currentDatabase()"
            ).result_rows
            if not engine_type or engine_type[0][0] != "Atomic":
                raise RuntimeError("Batch transition cutover requires an Atomic ClickHouse database.")
            staged_uuid = _clickhouse_table_uuid(ch_client, checkpoint.staging_table)
            if not checkpoint.staging_uuid or staged_uuid != checkpoint.staging_uuid:
                raise RuntimeError("The staged ClickHouse table is missing or changed.")
            _validate_staging_table(
                ch_client, checkpoint.staging_table, fields, table.primary_key_fields
            )
            _validate_staging_row_count(
                ch_client, checkpoint.staging_table, checkpoint.staging_row_count
            )
            active_uuid = _clickhouse_table_uuid(ch_client, active_name)
            if table.cdc_managed and not active_uuid:
                raise RuntimeError("The CDC-managed active ClickHouse table is missing.")
            if active_uuid and active_uuid == staged_uuid:
                raise RuntimeError("Active and staging table UUIDs must be distinct.")
            if checkpoint.active_table_checked:
                if checkpoint.active_uuid_before != active_uuid:
                    raise RuntimeError("The active ClickHouse table changed after observation.")
                return checkpoint
            checkpoint.active_uuid_before = active_uuid
            checkpoint.active_table_checked = True
            checkpoint.save(update_fields=[
                "active_uuid_before", "active_table_checked", "updated_at",
            ])
            return checkpoint
        finally:
            ch_client.close()


def _mark_cdc_to_batch_cutover_pending(transition_pk):
    """Prepare a pending cutover; the caller must guarantee all writers are tracked.

    The setting alone cannot prove that old workers or external clients are gone.
    This primitive is not an executor and must not be exposed until rollout and
    external-request recovery are coordinated.
    """
    if not getattr(settings, "CDC_EXTERNAL_OPERATION_TRACKING", False):
        raise RuntimeError("CDC external operation tracking is not enabled.")
    with transaction.atomic(durable=True):
        transition = CdcToBatchTransition.objects.get(pk=transition_pk)
        acquire_source_metadata_lock(transition.source_id)
        acquire_report_cutover_lock(transition.source_id)
        checkpoints = list(
            CdcToBatchTableCheckpoint.objects.select_for_update()
            .filter(transition_id=transition_pk).order_by("pk")
        )
        profile = SourceProfile.objects.select_for_update().get(pk=transition.source_id)
        transition = CdcToBatchTransition.objects.select_for_update().get(pk=transition_pk)
        if (
            transition.source_id != profile.pk
            or transition.status != CdcToBatchTransition.Status.PREPARING
            or not profile.is_active
            or profile.ingestion_mode != IngestionMode.CDC
            or profile.db_type == DbChoice.MONGODB
        ):
            raise RuntimeError("The transition is not eligible for cutover preparation.")
        _assert_no_cdc_external_operations(profile)
        if {checkpoint.pk for checkpoint in checkpoints} != set(
            transition.table_checkpoints.values_list("pk", flat=True)
        ):
            raise RuntimeError("The transition checkpoints changed during cutover preparation.")
        tables = list(profile.tables.select_for_update().order_by("pk"))
        if not tables or {table.pk for table in tables} != {
            checkpoint.source_table_id for checkpoint in checkpoints
        }:
            raise RuntimeError("The source table list changed during cutover preparation.")
        validate_cdc_to_batch_table_names(profile, tables)
        if CdcTableRebuild.objects.filter(
            source_table__source=profile,
            status__in=[
                CdcTableRebuild.Status.SNAPSHOTTING,
                CdcTableRebuild.Status.CUTOVER_PENDING,
            ],
        ).exists():
            raise RuntimeError("An active CDC table rebuild prevents cutover preparation.")
        active_names = {
            source_table_clickhouse_name(table)
            for table in tables
        }
        staging_names = [checkpoint.staging_table for checkpoint in checkpoints]
        if len(set(staging_names)) != len(staging_names) or active_names.intersection(staging_names):
            raise RuntimeError("Active and staging ClickHouse table names must be distinct.")
        checkpoints_by_table = {
            checkpoint.source_table_id: checkpoint for checkpoint in checkpoints
        }
        ch_client = clickhouse_connect.get_client(
            host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
            port=getattr(settings, "CLICKHOUSE_PORT", 8123),
            username=getattr(settings, "CLICKHOUSE_USER", "default"),
            password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
        )
        try:
            engine_type = ch_client.query(
                "SELECT engine FROM system.databases WHERE name = currentDatabase()"
            ).result_rows
            if len(engine_type) != 1 or engine_type[0][0] != "Atomic":
                raise RuntimeError("Batch transition cutover requires an Atomic ClickHouse database.")
            for table in tables:
                checkpoint = checkpoints_by_table[table.pk]
                if (
                    checkpoint.status != CdcToBatchTableCheckpoint.Status.STAGED
                    or checkpoint.cutover_command_inflight
                    or not checkpoint.staging_uuid
                ):
                    raise RuntimeError("A transition checkpoint is not staged for cutover.")
                fields = _stage_fields(profile, table)
                if checkpoint.staging_schema != _field_signature(profile, fields, table):
                    raise RuntimeError("Source or table configuration changed after batch staging.")
                staged_uuid = _clickhouse_table_uuid(ch_client, checkpoint.staging_table)
                if staged_uuid != checkpoint.staging_uuid:
                    raise RuntimeError("The staged ClickHouse table is missing or changed.")
                _validate_staging_table(
                    ch_client, checkpoint.staging_table, fields, table.primary_key_fields
                )
                _validate_staging_row_count(
                    ch_client, checkpoint.staging_table, checkpoint.staging_row_count
                )
                active_name = source_table_clickhouse_name(table)
                active_uuid = _clickhouse_table_uuid(ch_client, active_name)
                if table.cdc_managed and not active_uuid:
                    raise RuntimeError("The CDC-managed active ClickHouse table is missing.")
                if active_uuid and active_uuid == staged_uuid:
                    raise RuntimeError("Active and staging table UUIDs must be distinct.")
                if checkpoint.active_table_checked and checkpoint.active_uuid_before != active_uuid:
                    raise RuntimeError("The active ClickHouse table changed after observation.")
                checkpoint.active_table_checked = True
                checkpoint.active_uuid_before = active_uuid
                checkpoint.save(update_fields=[
                    "active_table_checked", "active_uuid_before", "updated_at",
                ])
            transition.status = CdcToBatchTransition.Status.CUTOVER_PENDING
            transition.last_error = ""
            transition.save(update_fields=["status", "last_error", "updated_at"])
            return transition
        finally:
            ch_client.close()


def _lock_cdc_to_batch_cutover_context(checkpoint):
    transition = CdcToBatchTransition.objects.get(pk=checkpoint.transition_id)
    profile = SourceProfile.objects.select_for_update().get(pk=transition.source_id)
    transition = CdcToBatchTransition.objects.select_for_update().get(pk=transition.pk)
    table = SourceTable.objects.select_for_update().get(pk=checkpoint.source_table_id)
    if (
        transition.source_id != profile.pk
        or transition.status != CdcToBatchTransition.Status.CUTOVER_PENDING
        or not profile.is_active
        or profile.ingestion_mode != IngestionMode.CDC
        or profile.db_type == DbChoice.MONGODB
        or table.source_id != profile.pk
    ):
        raise RuntimeError("The checkpoint is not eligible for table cutover.")
    _assert_no_cdc_external_operations(profile)
    if transition.table_checkpoints.exclude(pk=checkpoint.pk).filter(
        cutover_command_inflight=True
    ).exists():
        raise RuntimeError("Another ClickHouse cutover command has an uncertain outcome.")
    tables = list(profile.tables.order_by("pk"))
    if not tables or {item.pk for item in tables} != set(
        transition.table_checkpoints.values_list("source_table_id", flat=True)
    ):
        raise RuntimeError("The source table list changed during cutover.")
    validate_cdc_to_batch_table_names(profile, tables)
    if CdcTableRebuild.objects.filter(
        source_table__source=profile,
        status__in=[
            CdcTableRebuild.Status.SNAPSHOTTING,
            CdcTableRebuild.Status.CUTOVER_PENDING,
        ],
    ).exists():
        raise RuntimeError("An active CDC table rebuild prevents table cutover.")
    fields = _stage_fields(profile, table)
    if checkpoint.staging_schema != _field_signature(profile, fields, table):
        raise RuntimeError("Source or table configuration changed after batch staging.")
    active_name = source_table_clickhouse_name(table)
    if active_name == checkpoint.staging_table:
        raise RuntimeError("The active and staging table names must be distinct.")
    if table.cdc_managed and not checkpoint.active_uuid_before:
        raise RuntimeError("The CDC-managed active ClickHouse table was missing.")
    return table, fields, active_name


def _cutover_cdc_to_batch_table(checkpoint_pk):
    """Exchange one table under a caller-provided fence; uncertain commands block retries."""
    ch_client = None
    try:
        with transaction.atomic(durable=True):
            checkpoint = CdcToBatchTableCheckpoint.objects.select_for_update().get(
                pk=checkpoint_pk
            )
            if checkpoint.cutover_command_inflight:
                raise RuntimeError(
                    "A prior ClickHouse cutover command has an uncertain outcome; "
                    "automatic retry is blocked pending recovery."
                )
            table, fields, active_name = _lock_cdc_to_batch_cutover_context(checkpoint)
            if CdcToBatchTableCheckpoint.objects.filter(
                transition_id=checkpoint.transition_id
            ).exclude(generation_table="").exists():
                raise RuntimeError("Generation publication cannot be mixed with table exchanges.")
            original_cdc_managed = table.cdc_managed
            ch_client = clickhouse_connect.get_client(
                host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
                port=getattr(settings, "CLICKHOUSE_PORT", 8123),
                username=getattr(settings, "CLICKHOUSE_USER", "default"),
                password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
            )
            engine_type = ch_client.query(
                "SELECT engine FROM system.databases WHERE name = currentDatabase()"
            ).result_rows
            if len(engine_type) != 1 or engine_type[0][0] != "Atomic":
                raise RuntimeError("Batch transition cutover requires an Atomic ClickHouse database.")
            action = plan_cdc_to_batch_table_cutover(
                checkpoint,
                _clickhouse_table_uuid(ch_client, active_name),
                _clickhouse_table_uuid(ch_client, checkpoint.staging_table),
            )
            batch_name = (
                active_name if action == CdcToBatchCutoverAction.COMPLETE
                else checkpoint.staging_table
            )
            _validate_staging_table(ch_client, batch_name, fields, table.primary_key_fields)
            _validate_staging_row_count(ch_client, batch_name, checkpoint.staging_row_count)
            if action != CdcToBatchCutoverAction.COMPLETE:
                checkpoint.cutover_command_inflight = True
                checkpoint.save(update_fields=["cutover_command_inflight", "updated_at"])
        if action == CdcToBatchCutoverAction.EXCHANGE:
            ch_client.command(
                f"EXCHANGE TABLES {quote_clickhouse_identifier(active_name)} "
                f"AND {quote_clickhouse_identifier(checkpoint.staging_table)}"
            )
        elif action == CdcToBatchCutoverAction.RENAME:
            ch_client.command(
                f"RENAME TABLE {quote_clickhouse_identifier(checkpoint.staging_table)} "
                f"TO {quote_clickhouse_identifier(active_name)}"
            )
        with transaction.atomic(durable=True):
            current = CdcToBatchTableCheckpoint.objects.select_for_update().get(pk=checkpoint_pk)
            if (
                current.transition_id != checkpoint.transition_id
                or current.source_table_id != checkpoint.source_table_id
                or current.staging_table != checkpoint.staging_table
                or current.staging_uuid != checkpoint.staging_uuid
                or current.active_uuid_before != checkpoint.active_uuid_before
                or current.active_table_checked != checkpoint.active_table_checked
                or current.staging_schema != checkpoint.staging_schema
                or current.staging_row_count != checkpoint.staging_row_count
                or current.status != checkpoint.status
                or current.cutover_command_inflight != checkpoint.cutover_command_inflight
            ):
                raise RuntimeError("The cutover command marker changed unexpectedly.")
            checkpoint = current
            table, fields, current_active_name = _lock_cdc_to_batch_cutover_context(checkpoint)
            if current_active_name != active_name:
                raise RuntimeError("The active ClickHouse table name changed during cutover.")
            if table.cdc_managed != original_cdc_managed:
                raise RuntimeError("The table CDC configuration changed during cutover.")
            if plan_cdc_to_batch_table_cutover(
                checkpoint,
                _clickhouse_table_uuid(ch_client, active_name),
                _clickhouse_table_uuid(ch_client, checkpoint.staging_table),
            ) != CdcToBatchCutoverAction.COMPLETE:
                raise RuntimeError("ClickHouse table cutover did not complete.")
            _validate_staging_table(ch_client, active_name, fields, table.primary_key_fields)
            _validate_staging_row_count(ch_client, active_name, checkpoint.staging_row_count)
            if checkpoint.status != CdcToBatchTableCheckpoint.Status.CUTOVER:
                checkpoint.status = CdcToBatchTableCheckpoint.Status.CUTOVER
                checkpoint.cutover_command_inflight = False
                checkpoint.save(update_fields=["status", "cutover_command_inflight", "updated_at"])
            return checkpoint
    finally:
        if ch_client is not None:
            ch_client.close()


def _copy_cdc_to_batch_generation(checkpoint_pk):
    """Copy into a fresh candidate; an uncertain attempt is never reused."""
    ch_client = None
    try:
        with transaction.atomic(durable=True):
            checkpoint = CdcToBatchTableCheckpoint.objects.select_for_update().get(
                pk=checkpoint_pk
            )
            table, fields, _ = _lock_cdc_to_batch_cutover_context(checkpoint)
            if CdcToBatchTableCheckpoint.objects.filter(
                transition_id=checkpoint.transition_id
            ).exclude(status=CdcToBatchTableCheckpoint.Status.STAGED).exists() or (
                CdcToBatchTableCheckpoint.objects.filter(
                    transition_id=checkpoint.transition_id, cutover_command_inflight=True
                ).exists()
            ):
                raise RuntimeError("Generation publication cannot be mixed with table exchanges.")
            if not checkpoint.staging_uuid:
                raise RuntimeError("The staging UUID is missing.")
            if checkpoint.generation_uuid:
                return checkpoint
            ch_client = clickhouse_connect.get_client(
                host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
                port=getattr(settings, "CLICKHOUSE_PORT", 8123),
                username=getattr(settings, "CLICKHOUSE_USER", "default"),
                password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
                settings={"async_insert": 0},
            )
            if _clickhouse_table_uuid(ch_client, checkpoint.staging_table) != checkpoint.staging_uuid:
                raise RuntimeError("The staged ClickHouse table is missing or changed.")
            _validate_staging_table(
                ch_client, checkpoint.staging_table, fields, table.primary_key_fields
            )
            _validate_staging_row_count(
                ch_client, checkpoint.staging_table, checkpoint.staging_row_count
            )
            generation_table = (
                f"rh_batch_publish_{checkpoint.transition.generation.hex}_"
                f"{table.pk}_{uuid4().hex}"
            )
            checkpoint.generation_table = generation_table
            checkpoint.generation_uuid = ""
            checkpoint.save(update_fields=["generation_table", "generation_uuid", "updated_at"])
            staging_table = checkpoint.staging_table
            staging_uuid = checkpoint.staging_uuid
            staging_schema = checkpoint.staging_schema
            staging_row_count = checkpoint.staging_row_count
        quoted_generation = quote_clickhouse_identifier(generation_table)
        quoted_staging = quote_clickhouse_identifier(staging_table)
        ch_client.command(f"CREATE TABLE {quoted_generation} AS {quoted_staging}")
        ch_client.command(f"INSERT INTO {quoted_generation} SELECT * FROM {quoted_staging}")
        with transaction.atomic(durable=True):
            current = CdcToBatchTableCheckpoint.objects.select_for_update().get(pk=checkpoint_pk)
            table, fields, _ = _lock_cdc_to_batch_cutover_context(current)
            if (
                current.status != CdcToBatchTableCheckpoint.Status.STAGED
                or current.cutover_command_inflight
                or current.generation_table != generation_table
                or current.generation_uuid
                or current.staging_table != staging_table
                or current.staging_uuid != staging_uuid
                or current.staging_schema != staging_schema
                or current.staging_row_count != staging_row_count
            ):
                raise RuntimeError("The generation copy was superseded or its checkpoint changed.")
            if _clickhouse_table_uuid(ch_client, staging_table) != staging_uuid:
                raise RuntimeError("The staged ClickHouse table changed during generation copy.")
            generation_uuid = _clickhouse_table_uuid(ch_client, generation_table)
            if not generation_uuid or generation_uuid == staging_uuid:
                raise RuntimeError("The copied ClickHouse generation has an invalid UUID.")
            _validate_staging_table(
                ch_client, generation_table, fields, table.primary_key_fields
            )
            _validate_staging_row_count(ch_client, generation_table, staging_row_count)
            current.generation_uuid = generation_uuid
            current.save(update_fields=["generation_uuid", "updated_at"])
            return current
    finally:
        if ch_client is not None:
            ch_client.close()


def _finalize_cdc_to_batch_generation(transition_pk):
    """Publish copied tables atomically; callers must coordinate worker rollout."""
    with transaction.atomic(durable=True):
        transition = CdcToBatchTransition.objects.get(pk=transition_pk)
        acquire_source_metadata_lock(transition.source_id)
        acquire_report_cutover_lock(transition.source_id)
        checkpoints = list(
            CdcToBatchTableCheckpoint.objects.select_for_update()
            .filter(transition_id=transition_pk).order_by("pk")
        )
        profile = SourceProfile.objects.select_for_update().get(pk=transition.source_id)
        transition = CdcToBatchTransition.objects.select_for_update().get(pk=transition_pk)
        if transition.status == CdcToBatchTransition.Status.SUCCEEDED:
            if profile.ingestion_mode != IngestionMode.BATCH:
                raise RuntimeError("The completed transition has an unexpected source mode.")
            return transition
        if (
            transition.source_id != profile.pk
            or transition.status != CdcToBatchTransition.Status.CUTOVER_PENDING
            or not profile.is_active
            or profile.ingestion_mode != IngestionMode.CDC
            or profile.db_type == DbChoice.MONGODB
        ):
            raise RuntimeError("The transition is not eligible for generation publication.")
        _assert_no_cdc_external_operations(profile)
        tables = list(profile.tables.select_for_update().order_by("pk"))
        if (
            not tables
            or {checkpoint.pk for checkpoint in checkpoints}
            != set(transition.table_checkpoints.values_list("pk", flat=True))
            or {table.pk for table in tables}
            != {checkpoint.source_table_id for checkpoint in checkpoints}
        ):
            raise RuntimeError("The source tables or transition checkpoints changed.")
        validate_cdc_to_batch_table_names(profile, tables)
        if CdcTableRebuild.objects.filter(
            source_table__source=profile,
            status__in=[CdcTableRebuild.Status.SNAPSHOTTING, CdcTableRebuild.Status.CUTOVER_PENDING],
        ).exists():
            raise RuntimeError("An active CDC table rebuild prevents generation publication.")
        generation_names = [checkpoint.generation_table for checkpoint in checkpoints]
        old_names = {source_table_clickhouse_name(table) for table in tables}
        staging_names = {checkpoint.staging_table for checkpoint in checkpoints}
        if (
            len(set(generation_names)) != len(generation_names)
            or set(generation_names) & (old_names | staging_names)
        ):
            raise RuntimeError("Generation, staging and active table names must be distinct.")
        ch_client = clickhouse_connect.get_client(
            host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
            port=getattr(settings, "CLICKHOUSE_PORT", 8123),
            username=getattr(settings, "CLICKHOUSE_USER", "default"),
            password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
        )
        try:
            checkpoints_by_table = {
                checkpoint.source_table_id: checkpoint for checkpoint in checkpoints
            }
            for table in tables:
                checkpoint = checkpoints_by_table[table.pk]
                if (
                    checkpoint.status != CdcToBatchTableCheckpoint.Status.STAGED
                    or checkpoint.cutover_command_inflight
                    or not checkpoint.generation_table
                    or not checkpoint.generation_uuid
                ):
                    raise RuntimeError("Not every generation copy has completed.")
                if not re.fullmatch(
                    rf"rh_batch_publish_{transition.generation.hex}_{table.pk}_[0-9a-f]{{32}}",
                    checkpoint.generation_table,
                ):
                    raise RuntimeError("The generation table name is invalid for this checkpoint.")
                fields = _stage_fields(profile, table)
                if checkpoint.staging_schema != _field_signature(profile, fields, table):
                    raise RuntimeError("Source or table configuration changed after batch staging.")
                if _clickhouse_table_uuid(ch_client, checkpoint.generation_table) != checkpoint.generation_uuid:
                    raise RuntimeError("A copied ClickHouse generation is missing or changed.")
                _validate_staging_table(
                    ch_client, checkpoint.generation_table, fields, table.primary_key_fields
                )
                _validate_staging_row_count(
                    ch_client, checkpoint.generation_table, checkpoint.staging_row_count
                )
            for table in tables:
                checkpoint = checkpoints_by_table[table.pk]
                table.clickhouse_physical_table = checkpoint.generation_table
                table.cdc_managed = False
                table.clickhouse_uses_replacing_merge_tree = False
                table.incremental_ready = False
                table.incremental_watermark = None
                table.save(update_fields=[
                    "clickhouse_physical_table", "cdc_managed", "clickhouse_uses_replacing_merge_tree",
                    "incremental_ready", "incremental_watermark",
                ])
                checkpoint.status = CdcToBatchTableCheckpoint.Status.CUTOVER
                checkpoint.save(update_fields=["status", "updated_at"])
            profile.ingestion_mode = IngestionMode.BATCH
            profile.save(update_fields=["ingestion_mode"])
            transition.status = CdcToBatchTransition.Status.SUCCEEDED
            transition.last_error = ""
            transition.save(update_fields=["status", "last_error", "updated_at"])
            return transition
        finally:
            ch_client.close()


def _run_cdc_to_batch_generation_step(transition_pk):
    """Advance the private generation executor by one copy or publication."""
    if not getattr(settings, "CDC_EXTERNAL_OPERATION_TRACKING", False):
        raise RuntimeError("CDC external operation tracking is not enabled.")
    if connection.in_atomic_block:
        raise RuntimeError("A generation step cannot run inside a database transaction.")
    transition = CdcToBatchTransition.objects.get(pk=transition_pk)
    if transition.status == CdcToBatchTransition.Status.SUCCEEDED:
        _finalize_cdc_to_batch_generation(transition_pk)
        return CdcToBatchStepResult.SUCCEEDED
    if transition.status != CdcToBatchTransition.Status.CUTOVER_PENDING:
        raise RuntimeError("The transition is not pending cutover.")
    checkpoints = list(transition.table_checkpoints.order_by("pk"))
    if not checkpoints or any(
        checkpoint.status != CdcToBatchTableCheckpoint.Status.STAGED
        or checkpoint.cutover_command_inflight
        for checkpoint in checkpoints
    ):
        raise RuntimeError("Generation publication cannot be mixed with table exchanges.")
    pending = next((checkpoint for checkpoint in checkpoints if not checkpoint.generation_uuid), None)
    if pending:
        _copy_cdc_to_batch_generation(pending.pk)
        return CdcToBatchStepResult.TABLE_CUTOVER
    _finalize_cdc_to_batch_generation(transition_pk)
    return CdcToBatchStepResult.SUCCEEDED


def _finalize_cdc_to_batch_transition(transition_pk):
    """Commit metadata after all exchanges, while the caller maintains a complete fence.

    This internal primitive does not establish that fence or perform exchanges.
    It must not be exposed until the cutover executor provides those guarantees.
    Callers must not already hold the source row lock: checkpoints are locked first.
    """
    with transaction.atomic():
        transition = CdcToBatchTransition.objects.get(pk=transition_pk)
        checkpoints = list(
            CdcToBatchTableCheckpoint.objects.select_for_update()
            .filter(transition_id=transition_pk).order_by("pk")
        )
        profile = SourceProfile.objects.select_for_update().get(pk=transition.source_id)
        transition = CdcToBatchTransition.objects.select_for_update().get(pk=transition_pk)
        if transition.source_id != profile.pk:
            raise RuntimeError("The transition source changed during cutover completion.")
        if transition.status == CdcToBatchTransition.Status.SUCCEEDED:
            if profile.ingestion_mode != IngestionMode.BATCH:
                raise RuntimeError("The completed transition has an unexpected source mode.")
            return transition
        if (
            transition.status != CdcToBatchTransition.Status.CUTOVER_PENDING
            or not profile.is_active
            or profile.ingestion_mode != IngestionMode.CDC
            or profile.db_type == DbChoice.MONGODB
        ):
            raise RuntimeError("The transition is not eligible for cutover completion.")

        _assert_no_cdc_external_operations(profile)
        tables = list(profile.tables.select_for_update().order_by("pk"))
        if {checkpoint.pk for checkpoint in checkpoints} != set(
            transition.table_checkpoints.values_list("pk", flat=True)
        ):
            raise RuntimeError("The transition checkpoints changed during cutover.")
        if not tables or {table.pk for table in tables} != {
            checkpoint.source_table_id for checkpoint in checkpoints
        }:
            raise RuntimeError("The source table list changed during cutover.")
        if any(checkpoint.generation_table for checkpoint in checkpoints):
            raise RuntimeError("Generation publication cannot be mixed with table exchanges.")
        validate_cdc_to_batch_table_names(profile, tables)
        if CdcTableRebuild.objects.filter(
            source_table__source=profile,
            status__in=[
                CdcTableRebuild.Status.SNAPSHOTTING,
                CdcTableRebuild.Status.CUTOVER_PENDING,
            ],
        ).exists():
            raise RuntimeError("An active CDC table rebuild prevents cutover completion.")

        ch_client = clickhouse_connect.get_client(
            host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
            port=getattr(settings, "CLICKHOUSE_PORT", 8123),
            username=getattr(settings, "CLICKHOUSE_USER", "default"),
            password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
        )
        try:
            engine_type = ch_client.query(
                "SELECT engine FROM system.databases WHERE name = currentDatabase()"
            ).result_rows
            if len(engine_type) != 1 or engine_type[0][0] != "Atomic":
                raise RuntimeError("Batch transition cutover requires an Atomic ClickHouse database.")
            checkpoints_by_table = {
                checkpoint.source_table_id: checkpoint for checkpoint in checkpoints
            }
            for table in tables:
                checkpoint = checkpoints_by_table[table.pk]
                if (
                    checkpoint.status != CdcToBatchTableCheckpoint.Status.CUTOVER
                    or checkpoint.cutover_command_inflight
                ):
                    raise RuntimeError("Not every transition table has completed cutover.")
                fields = _stage_fields(profile, table)
                if checkpoint.staging_schema != _field_signature(profile, fields, table):
                    raise RuntimeError("Source or table configuration changed after batch staging.")
                active_name = source_table_clickhouse_name(table)
                if active_name == checkpoint.staging_table:
                    raise RuntimeError("The active and staging table names must be distinct.")
                action = plan_cdc_to_batch_table_cutover(
                    checkpoint,
                    _clickhouse_table_uuid(ch_client, active_name),
                    _clickhouse_table_uuid(ch_client, checkpoint.staging_table),
                )
                if action != CdcToBatchCutoverAction.COMPLETE:
                    raise RuntimeError("A transition table has not completed cutover.")
                _validate_staging_table(
                    ch_client, active_name, fields, table.primary_key_fields
                )
                _validate_staging_row_count(
                    ch_client, active_name, checkpoint.staging_row_count
                )

            for table in tables:
                table.cdc_managed = False
                table.clickhouse_uses_replacing_merge_tree = False
                table.incremental_ready = False
                table.incremental_watermark = None
                table.save(update_fields=[
                    "cdc_managed", "clickhouse_uses_replacing_merge_tree",
                    "incremental_ready", "incremental_watermark",
                ])
            profile.ingestion_mode = IngestionMode.BATCH
            profile.save(update_fields=["ingestion_mode"])
            transition.status = CdcToBatchTransition.Status.SUCCEEDED
            transition.last_error = ""
            transition.save(update_fields=["status", "last_error", "updated_at"])
            return transition
        finally:
            ch_client.close()


def _run_cdc_to_batch_cutover_step(transition_pk):
    """Run one private cutover step under a caller-provided complete fence.

    The tracking setting alone does not establish that fence. The caller must
    coordinate upgraded workers, external requests and report readers first.
    """
    if not getattr(settings, "CDC_EXTERNAL_OPERATION_TRACKING", False):
        raise RuntimeError("CDC external operation tracking is not enabled.")
    if connection.in_atomic_block:
        raise RuntimeError("A cutover step cannot run inside a database transaction.")
    transition = CdcToBatchTransition.objects.get(pk=transition_pk)
    if transition.status == CdcToBatchTransition.Status.SUCCEEDED:
        _finalize_cdc_to_batch_transition(transition_pk)
        return CdcToBatchStepResult.SUCCEEDED
    if transition.status != CdcToBatchTransition.Status.CUTOVER_PENDING:
        raise RuntimeError("The transition is not pending cutover.")
    checkpoints = list(transition.table_checkpoints.order_by("pk"))
    if not checkpoints:
        raise RuntimeError("The transition has no cutover checkpoints.")
    if any(checkpoint.cutover_command_inflight for checkpoint in checkpoints):
        raise RuntimeError(
            "A prior ClickHouse cutover command has an uncertain outcome; "
            "automatic retry is blocked pending recovery."
        )
    if any(
        checkpoint.status not in (
            CdcToBatchTableCheckpoint.Status.STAGED,
            CdcToBatchTableCheckpoint.Status.CUTOVER,
        )
        for checkpoint in checkpoints
    ):
        raise RuntimeError("A transition checkpoint is not staged for cutover.")
    next_checkpoint = next(
        (checkpoint for checkpoint in checkpoints
         if checkpoint.status == CdcToBatchTableCheckpoint.Status.STAGED),
        None,
    )
    if next_checkpoint is not None:
        _cutover_cdc_to_batch_table(next_checkpoint.pk)
        return CdcToBatchStepResult.TABLE_CUTOVER
    _finalize_cdc_to_batch_transition(transition_pk)
    return CdcToBatchStepResult.SUCCEEDED


def cdc_to_batch_checkpoint_needs_staging(checkpoint):
    if checkpoint.status == CdcToBatchTableCheckpoint.Status.PENDING:
        return True
    if checkpoint.status != CdcToBatchTableCheckpoint.Status.STAGED:
        raise RuntimeError("The checkpoint is not eligible for batch staging.")
    if not isinstance(checkpoint.staging_schema, dict):
        raise RuntimeError("The staged checkpoint schema is invalid.")
    return (
        checkpoint.staging_row_count is None
        or "username" not in checkpoint.staging_schema
        or checkpoint.staging_schema.get("synchronous_inserts") is not True
    )


def stage_cdc_to_batch_table(checkpoint_pk, batch_size=None, *, verification_only=False):
    """Stage a whole table without modifying the live table or source mode.

    An interrupted attempt is restarted in a new table; incomplete attempts
    are retained for manual cleanup. This is not a transition executor.
    """
    if batch_size is None:
        batch_size = getattr(settings, "DEFAULT_ETL_BATCH_SIZE", 10000)
    batch_size = int(batch_size)
    if batch_size < 1:
        raise ValueError("Batch size must be positive.")

    with transaction.atomic():
        checkpoint = CdcToBatchTableCheckpoint.objects.select_for_update().get(
            pk=checkpoint_pk
        )
        transition = CdcToBatchTransition.objects.get(pk=checkpoint.transition_id)
        profile = SourceProfile.objects.get(pk=transition.source_id)
        table = checkpoint.source_table
        if (
            transition.status != CdcToBatchTransition.Status.PREPARING
            or not profile.is_active
            or profile.ingestion_mode != IngestionMode.CDC
            or profile.db_type == DbChoice.MONGODB
            or table.source_id != profile.pk
        ):
            raise RuntimeError("The transition is not eligible for batch staging.")
        needs_staging = cdc_to_batch_checkpoint_needs_staging(checkpoint)
        if verification_only and needs_staging:
            raise RuntimeError("The checkpoint requires staging before verification.")
        validate_cdc_to_batch_table_names(profile, profile.tables.order_by("pk"))

        ch_client = clickhouse_connect.get_client(
            host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
            port=getattr(settings, "CLICKHOUSE_PORT", 8123),
            username=getattr(settings, "CLICKHOUSE_USER", "default"),
            password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
            settings={"async_insert": 0},
        )
        try:
            if not needs_staging:
                if (
                    not checkpoint.staging_uuid
                    or _clickhouse_table_uuid(ch_client, checkpoint.staging_table)
                    != checkpoint.staging_uuid
                ):
                    raise RuntimeError("The staged ClickHouse table is missing or changed.")
                fields = _stage_fields(profile, table)
                if checkpoint.staging_schema != _field_signature(profile, fields, table):
                    raise RuntimeError("Source or table configuration changed after batch staging.")
                _validate_staging_table(
                    ch_client, checkpoint.staging_table, fields, table.primary_key_fields
                )
                _validate_staging_row_count(
                    ch_client, checkpoint.staging_table, checkpoint.staging_row_count
                )
                return checkpoint

            engine_type = ch_client.query(
                "SELECT engine FROM system.databases WHERE name = currentDatabase()"
            ).result_rows
            if not engine_type or engine_type[0][0] != "Atomic":
                raise RuntimeError("Batch transition staging requires an Atomic ClickHouse database.")

            fields = _stage_fields(profile, table)
            signature = _field_signature(profile, fields, table)
            field_names = [field.field_name for field in fields]
            columns = ", ".join(
                f"{quote_clickhouse_identifier(field.field_name)} "
                f"{map_to_clickhouse_type(field.data_type, field.field_name not in table.primary_key_fields)}"
                for field in fields
            )
            stage_name = (
                f"rh_batch_{transition.generation.hex}_{table.pk}_{uuid4().hex}"
            )
            ch_client.command(
                f"CREATE TABLE {quote_clickhouse_identifier(stage_name)} "
                f"({columns}) ENGINE = MergeTree() ORDER BY tuple()"
            )
            stage_uuid = _clickhouse_table_uuid(ch_client, stage_name)
            if not stage_uuid:
                raise RuntimeError("Could not verify the created staging table UUID.")

            row_count = 0
            source_engine = create_source_engine(profile)
            try:
                schema = None if profile.db_type == DbChoice.MYSQL else table.schema_name
                source_table = sa.Table(
                    table.table_name, sa.MetaData(), schema=schema,
                    autoload_with=source_engine,
                )
                statement = sa.select(*(source_table.c[name] for name in field_names))
                with source_engine.connect() as connection:
                    connection = connection.execution_options(stream_results=True)
                    for chunk in pd.read_sql(statement, con=connection, chunksize=batch_size):
                        rows = _batch_sanitized_rows(chunk, fields, table.primary_key_fields)
                        if rows:
                            ch_client.insert(stage_name, rows, column_names=field_names)
                            row_count += len(rows)
            finally:
                source_engine.dispose()

            profile = SourceProfile.objects.select_for_update().get(pk=profile.pk)
            transition = CdcToBatchTransition.objects.select_for_update().get(
                pk=transition.pk
            )
            table = type(table).objects.select_for_update().get(pk=table.pk)
            if (
                not profile.is_active
                or profile.ingestion_mode != IngestionMode.CDC
                or transition.status != CdcToBatchTransition.Status.PREPARING
                or table.source_id != profile.pk
                or signature != _field_signature(profile, _stage_fields(profile, table), table)
            ):
                raise RuntimeError("Source or table configuration changed during batch staging.")
            validate_cdc_to_batch_table_names(profile, profile.tables.order_by("pk"))
            if _clickhouse_table_uuid(ch_client, stage_name) != stage_uuid:
                raise RuntimeError("The staging table changed during batch staging.")
            _validate_staging_table(ch_client, stage_name, fields, table.primary_key_fields)
            _validate_staging_row_count(ch_client, stage_name, row_count)
            checkpoint.staging_table = stage_name
            checkpoint.staging_uuid = stage_uuid
            checkpoint.staging_schema = signature
            checkpoint.staging_row_count = row_count
            checkpoint.active_uuid_before = ""
            checkpoint.active_table_checked = False
            checkpoint.status = CdcToBatchTableCheckpoint.Status.STAGED
            checkpoint.save(update_fields=[
                "staging_table", "staging_uuid", "staging_schema", "staging_row_count",
                "active_uuid_before", "active_table_checked", "status", "updated_at",
            ])
            if transition.staging_verified_at is not None:
                transition.staging_verified_at = None
                transition.save(update_fields=["staging_verified_at", "updated_at"])
            return checkpoint
        finally:
            ch_client.close()
