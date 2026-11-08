from django.contrib import admin
from etl.tasks import rebuild_cdc_table_task
from .models import (
    CdcTableRebuild,
    DbChoice,
    IngestionMode,
    SourceProfile,
    SourceTable,
    UserSourceAccess,
)

@admin.register(SourceProfile)
class SourceProfileAdmin(admin.ModelAdmin):
    list_display = ('name', 'host', 'database_name', 'is_active', 'created_at')
    search_fields = ('name', 'host', 'database_name')
    list_filter = ('is_active',)


@admin.register(SourceTable)
class SourceTableAdmin(admin.ModelAdmin):
    list_display = (
        "source",
        "schema_name",
        "table_name",
        "cdc_managed",
        "cdc_connector_name",
    )
    list_filter = ("source__ingestion_mode", "cdc_managed")
    search_fields = ("source__name", "schema_name", "table_name")
    actions = ("rebuild_selected_cdc_tables",)

    @admin.action(description="Safely rebuild selected CDC warehouse tables")
    def rebuild_selected_cdc_tables(self, request, queryset):
        queued = 0
        skipped = 0
        for table in queryset.select_related("source"):
            if (
                table.source.ingestion_mode != IngestionMode.CDC
                or not table.cdc_managed
                or not table.primary_key_fields
                or table.source.db_type == DbChoice.MONGODB
            ):
                skipped += 1
                continue
            rebuild_cdc_table_task.delay(table.pk)
            queued += 1
        message = f"Queued {queued} CDC table rebuild(s)."
        if skipped:
            message += f" Skipped {skipped} ineligible table(s)."
        self.message_user(request, message)


@admin.register(CdcTableRebuild)
class CdcTableRebuildAdmin(admin.ModelAdmin):
    list_display = (
        "source_table",
        "status",
        "connector_name",
        "created_at",
        "updated_at",
    )
    list_filter = ("status", "source_table__source__db_type")
    search_fields = (
        "source_table__source__name",
        "source_table__table_name",
        "connector_name",
    )
    readonly_fields = (
        "generation",
        "connector_name",
        "topic_prefix",
        "notification_topic",
        "consumer_group",
        "staging_table",
        "snapshot_end_offsets",
        "staging_uuid",
        "active_uuid_before",
        "active_table_checked",
        "previous_connector_name",
        "source_table",
        "status",
        "last_error",
        "created_at",
        "updated_at",
    )
    fields = readonly_fields

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(UserSourceAccess)
class UserSourceAccessAdmin(admin.ModelAdmin):
    list_display = ('user', 'source', 'role', 'assigned_at')
    list_filter = ('role', 'source')
    search_fields = ('user__username', 'source__name')