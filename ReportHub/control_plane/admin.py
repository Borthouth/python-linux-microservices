from django.contrib import admin
from .admin_metadata import SourceMetadataAdminMixin
from .models import (
    CdcTableRebuild,
    SourceProfile,
    SourceTable,
    UserSourceAccess,
)

@admin.register(SourceProfile)
class SourceProfileAdmin(SourceMetadataAdminMixin, admin.ModelAdmin):
    list_display = ('name', 'host', 'database_name', 'is_active', 'created_at')
    search_fields = ('name', 'host', 'database_name')
    list_filter = ('is_active',)

    def get_readonly_fields(self, request, obj=None):
        fields = super().get_readonly_fields(request, obj)
        return (*fields, "ingestion_mode") if obj else fields


@admin.register(SourceTable)
class SourceTableAdmin(SourceMetadataAdminMixin, admin.ModelAdmin):
    list_display = (
        "source",
        "schema_name",
        "table_name",
        "cdc_managed",
        "cdc_connector_name",
    )
    list_filter = ("source__ingestion_mode", "cdc_managed")
    search_fields = ("source__name", "schema_name", "table_name")


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