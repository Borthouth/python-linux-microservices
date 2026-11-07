from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, get_object_or_404, redirect
from django.contrib import messages
from django.core.paginator import Paginator
from django.db.models import Q
from control_plane.models import SourceProfile, WarehouseDatasetMapping

def is_admin(user):
    return user.is_staff or user.is_superuser

@login_required
@user_passes_test(is_admin)
def warehouse_list_view(request):
    sources = SourceProfile.objects.all().order_by('name')
    search_query = request.GET.get('q', '').strip()
    if search_query:
        sources = sources.filter(
            Q(name__icontains=search_query) |
            Q(db_type__icontains=search_query) |
            Q(database_name__icontains=search_query)
        )
    paginator = Paginator(sources, 15)
    page_obj = paginator.get_page(request.GET.get('page'))

    sources_data = []
    for source in page_obj:
        enabled_fields_count = source.warehouse_mappings.filter(is_enabled=True).count()
        
        # Pull the correct attribute for your database name field
        db_identifier = getattr(source, 'db_name', getattr(source, 'database_name', '—'))
        
        sources_data.append({
            'source': source,
            'db_identifier': db_identifier,
            'enabled_fields_count': enabled_fields_count,
        })

    return render(request, 'control_plane/warehouse_list.html', {
        'sources_data': sources_data,
        'page_obj': page_obj,
        'search_query': search_query,
    })

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
        incremental_fields = {}
        selection_changes = {}
        for table in tables_qs:
            incremental_field_name = request.POST.get(
                f'incremental_field_{table.pk}'
            ) or None
            incremental_field = (
                table.fields.filter(field_name=incremental_field_name).first()
                if incremental_field_name else None
            )
            if incremental_field_name and (
                incremental_field is None
                or not any(
                    kind in incremental_field.data_type.lower()
                    for kind in ('date', 'datetime', 'timestamp')
                )
            ):
                messages.error(
                    request,
                    f"Choose a date/time field for incremental updates on {table.table_name}.",
                )
                return redirect(
                    'control_plane:warehouse_field_selection',
                    source_pk=source.pk,
                )
            incremental_fields[table.pk] = incremental_field_name

            existing_mappings = source.warehouse_mappings.filter(
                table_name=table.table_name
            )
            if existing_mappings.exists():
                previously_selected = set(
                    existing_mappings.filter(is_enabled=True)
                    .values_list('field_name', flat=True)
                )
            else:
                previously_selected = set(
                    table.fields.filter(is_selected=True)
                    .values_list('field_name', flat=True)
                )
            newly_selected = {
                field.field_name for field in table.fields.all()
                if f"{table.table_name}.{field.field_name}" in selected_fields
            }
            selection_changes[table.pk] = previously_selected != newly_selected
        
        # Mark all existing as disabled, then re-enable/create selected ones
        source.warehouse_mappings.all().update(is_enabled=False)
        
        for table in tables_qs:
            for field in table.fields.all():
                field_key = f"{table.table_name}.{field.field_name}"
                WarehouseDatasetMapping.objects.update_or_create(
                    source_profile=source,
                    table_name=table.table_name,
                    field_name=field.field_name,
                    defaults={'is_enabled': field_key in selected_fields}
                )

        for table in tables_qs:
            incremental_field_name = incremental_fields[table.pk]
            if table.incremental_field_name != incremental_field_name:
                table.incremental_field_name = incremental_field_name
                table.incremental_watermark = None
                table.incremental_ready = False
                table.save(update_fields=[
                    'incremental_field_name',
                    'incremental_watermark',
                    'incremental_ready',
                ])
            elif selection_changes[table.pk]:
                table.incremental_ready = False
                table.save(update_fields=['incremental_ready'])
                
        messages.success(request, f"Warehouse schema configuration updated successfully for '{source.name}'.")
        return redirect('control_plane:warehouse_list')

    # Query currently enabled fields for pre-checking UI inputs
    mappings = source.warehouse_mappings.all()
    enabled_field_keys = set(
        mappings.filter(is_enabled=True).values_list('table_name', 'field_name')
    )
    enabled_set = {f"{tbl}.{fld}" for tbl, fld in enabled_field_keys}
    mapped_field_keys = {
        f"{tbl}.{fld}" for tbl, fld in mappings.values_list('table_name', 'field_name')
    }

    context = {
        'source': source,
        'tables_qs': tables_qs,
        'enabled_set': enabled_set,
        'mapped_field_keys': mapped_field_keys,
    }
    return render(request, 'control_plane/warehouse_field_selection.html', context)