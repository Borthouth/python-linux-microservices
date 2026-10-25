from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, get_object_or_404, redirect
from django.contrib import messages
from control_plane.models import SourceProfile, WarehouseDatasetMapping

def is_admin(user):
    return user.is_staff or user.is_superuser

@login_required
@user_passes_test(is_admin)
def warehouse_list_view(request):
    sources = SourceProfile.objects.all().order_by('name')
    
    sources_data = []
    for source in sources:
        enabled_fields_count = source.warehouse_mappings.filter(is_enabled=True).count()
        
        # Pull the correct attribute for your database name field
        db_identifier = getattr(source, 'db_name', getattr(source, 'database_name', '—'))
        
        sources_data.append({
            'source': source,
            'db_identifier': db_identifier,
            'enabled_fields_count': enabled_fields_count,
        })

    return render(request, 'control_plane/warehouse_list.html', {'sources_data': sources_data})

@login_required
@user_passes_test(is_admin)
def warehouse_field_selection_view(request, source_pk):
    """
    Edit page for selecting specific tables and fields for a data source
    using the relational SourceTable and SourceField models.
    """
    source = get_object_or_404(SourceProfile, pk=source_pk)
    
    # Fetch related tables and prefetch their fields
    tables_qs = source.tables.prefetch_related('fields').all().order_by('table_name')

    if request.method == 'POST':
        selected_fields = set(request.POST.getlist('selected_fields'))  # Format: "table_name.field_name"
        
        # Mark all existing as disabled, then re-enable/create selected ones
        source.warehouse_mappings.all().update(is_enabled=False)
        
        for item in selected_fields:
            if "." in item:
                tbl, fld = item.split(".", 1)
                WarehouseDatasetMapping.objects.update_or_create(
                    source_profile=source,
                    table_name=tbl,
                    field_name=fld,
                    defaults={'is_enabled': True}
                )
                
        messages.success(request, f"Warehouse schema configuration updated successfully for '{source.name}'.")
        return redirect('control_plane:warehouse_list')

    # Query currently enabled fields for pre-checking UI inputs
    enabled_field_keys = set(
        source.warehouse_mappings.filter(is_enabled=True)
        .values_list('table_name', 'field_name')
    )
    enabled_set = {f"{tbl}.{fld}" for tbl, fld in enabled_field_keys}

    context = {
        'source': source,
        'tables_qs': tables_qs,
        'enabled_set': enabled_set,
    }
    return render(request, 'control_plane/warehouse_field_selection.html', context)