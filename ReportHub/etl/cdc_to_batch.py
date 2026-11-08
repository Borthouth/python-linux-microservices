from enum import Enum
from uuid import uuid4

import clickhouse_connect
import pandas as pd
import sqlalchemy as sa
from django.conf import settings
from django.db import transaction

from control_plane.models import (
    CdcTableRebuild,
    CdcToBatchTableCheckpoint,
    CdcToBatchTransition,
    DbChoice,
    IngestionMode,
    SourceProfile,
    SourceTable,
)
from etl.cdc import _clickhouse_table_uuid
from etl.tasks import (
    _batch_required_fields_by_table,
    _batch_sanitized_rows,
    _batch_selected_fields,
    create_source_engine,
    map_to_clickhouse_type,
)
from reports.query_builder import clickhouse_table_name, quote_clickhouse_identifier


class CdcToBatchCutoverAction(Enum):
    EXCHANGE = "exchange"
    RENAME = "rename"
    COMPLETE = "complete"


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

        transition = source.cdc_to_batch_transitions.filter(
            status__in=[
                CdcToBatchTransition.Status.PREPARING,
                CdcToBatchTransition.Status.CUTOVER_PENDING,
            ],
        ).first()
        if transition:
            return transition

        tables = list(source.tables.order_by("pk"))
        if not tables:
            raise ValueError(
                "A CDC-to-batch transition requires discovered source tables."
            )

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
        "schema_name": table.schema_name,
        "table_name": table.table_name,
        "primary_key_fields": list(table.primary_key_fields),
        "fields": [[field.field_name, field.data_type] for field in fields],
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
        fields = _stage_fields(profile, table)
        if checkpoint.staging_schema != _field_signature(profile, fields, table):
            raise RuntimeError("Source or table configuration changed after batch staging.")

        active_name = clickhouse_table_name(
            profile.pk, table.schema_name, table.table_name
        )
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


def stage_cdc_to_batch_table(checkpoint_pk, batch_size=None):
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
        if checkpoint.status not in (
            CdcToBatchTableCheckpoint.Status.PENDING,
            CdcToBatchTableCheckpoint.Status.STAGED,
        ):
            raise RuntimeError("The checkpoint is not eligible for batch staging.")

        ch_client = clickhouse_connect.get_client(
            host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
            port=getattr(settings, "CLICKHOUSE_PORT", 8123),
            username=getattr(settings, "CLICKHOUSE_USER", "default"),
            password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
        )
        try:
            if checkpoint.status == CdcToBatchTableCheckpoint.Status.STAGED:
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
            if _clickhouse_table_uuid(ch_client, stage_name) != stage_uuid:
                raise RuntimeError("The staging table changed during batch staging.")
            _validate_staging_table(ch_client, stage_name, fields, table.primary_key_fields)
            checkpoint.staging_table = stage_name
            checkpoint.staging_uuid = stage_uuid
            checkpoint.staging_schema = signature
            checkpoint.active_uuid_before = ""
            checkpoint.active_table_checked = False
            checkpoint.status = CdcToBatchTableCheckpoint.Status.STAGED
            checkpoint.save(update_fields=[
                "staging_table", "staging_uuid", "staging_schema",
                "active_uuid_before", "active_table_checked", "status", "updated_at",
            ])
            return checkpoint
        finally:
            ch_client.close()
