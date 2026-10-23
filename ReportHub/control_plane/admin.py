from django.contrib import admin
from .models import SourceProfile, UserSourceAccess

@admin.register(SourceProfile)
class SourceProfileAdmin(admin.ModelAdmin):
    list_display = ('name', 'host', 'database_name', 'is_active', 'created_at')
    search_fields = ('name', 'host', 'database_name')
    list_filter = ('is_active',)

@admin.register(UserSourceAccess)
class UserSourceAccessAdmin(admin.ModelAdmin):
    list_display = ('user', 'source', 'role', 'assigned_at')
    list_filter = ('role', 'source')
    search_fields = ('user__username', 'source__name')