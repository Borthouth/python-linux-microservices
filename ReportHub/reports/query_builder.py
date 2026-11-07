from collections import defaultdict

from control_plane.models import SourceField, SourceRelationship, SourceTable


def clickhouse_table_name(source_id, schema_name, table_name):
    return f"source_{source_id}__{schema_name}_{table_name}".lower()


def clickhouse_final_clause(table):
    return " FINAL" if table.clickhouse_uses_replacing_merge_tree else ""


def quote_clickhouse_identifier(identifier):
    return f"`{str(identifier).replace('`', '``')}`"


def relationship_groups(relationships):
    groups = defaultdict(list)
    for relationship in relationships:
        key = (
            relationship.from_schema,
            relationship.from_table,
            relationship.to_schema,
            relationship.to_table,
            relationship.constraint_name or f"relationship-{relationship.pk}",
        )
        groups[key].append(relationship)
    return list(groups.values())


def build_clickhouse_report_query(report, selected_columns):
    base_table = report.base_table
    config = report.config if isinstance(report.config, dict) else {}
    report_type = config.get("reportType", "tabular")
    if report_type not in {"tabular", "grouped_summary"}:
        raise ValueError("The saved report type is not supported.")
    grouped_summary = report_type == "grouped_summary"
    requested_keys = report.join_config or []
    if not isinstance(requested_keys, list):
        raise ValueError("The saved report join configuration is invalid.")
    requested_keys = [str(key) for key in requested_keys]

    relationships = SourceRelationship.objects.filter(source=report.source)
    groups = relationship_groups(relationships)
    groups_by_key = {
        ",".join(str(item.pk) for item in sorted(group, key=lambda item: item.pk)): group
        for group in groups
    }

    if len(set(requested_keys)) != len(requested_keys):
        raise ValueError("The report contains duplicate table relationships.")
    if any(key not in groups_by_key for key in requested_keys):
        raise ValueError("A selected foreign-key relationship no longer exists.")

    base_key = "base"
    joined_tables = {base_key: (base_table, "t0")}
    join_clauses = []
    for alias_index, relationship_key in enumerate(requested_keys, start=1):
        group = groups_by_key[relationship_key]
        first = group[0]
        from_is_base = (
            first.from_schema == base_table.schema_name
            and first.from_table == base_table.table_name
        )
        to_is_base = (
            first.to_schema == base_table.schema_name
            and first.to_table == base_table.table_name
        )
        if not from_is_base and not to_is_base:
            raise ValueError("Selected relationships must connect directly to the base table.")

        related_schema = first.to_schema if from_is_base else first.from_schema
        related_name = first.to_table if from_is_base else first.from_table
        related_table = SourceTable.objects.filter(
            source=report.source,
            schema_name=related_schema,
            table_name=related_name,
        ).first()
        if related_table is None:
            raise ValueError(f"Related table {related_schema}.{related_name} is not available.")

        alias = f"t{alias_index}"
        joined_tables[relationship_key] = (related_table, alias)
        conditions = []
        for relationship in group:
            base_field = relationship.from_field if from_is_base else relationship.to_field
            related_field = relationship.to_field if from_is_base else relationship.from_field
            conditions.append(
                f"t0.{quote_clickhouse_identifier(base_field)} = "
                f"{alias}.{quote_clickhouse_identifier(related_field)}"
            )

        physical_name = clickhouse_table_name(
            report.source_id, related_table.schema_name, related_table.table_name
        )
        join_clauses.append(
            f"LEFT JOIN {quote_clickhouse_identifier(physical_name)}"
            f" AS {alias}{clickhouse_final_clause(related_table)} "
            f"ON {' AND '.join(conditions)}"
        )

    select_parts = []
    group_by_parts = []
    headers = []
    aggregates = []
    for index, column in enumerate(selected_columns):
        if isinstance(column, str):
            table_key = base_key
            field_name = column
            label = column
            aggregate = ""
        elif isinstance(column, dict):
            table_key = str(column.get("table_key") or base_key)
            field_name = column.get("field_name") or column.get("field")
            label = column.get("label") or field_name
            aggregate = str(column.get("aggregate") or "").upper()
        else:
            raise ValueError("A selected report column is invalid.")

        if not isinstance(field_name, str) or not field_name:
            raise ValueError("A selected report column has no field name.")
        if aggregate not in {"", "COUNT", "SUM", "AVG", "MIN", "MAX"}:
            raise ValueError(f"Unsupported aggregate function: {aggregate}")
        if table_key not in joined_tables:
            raise ValueError("A selected report field belongs to a table that is not joined.")

        source_table, alias = joined_tables[table_key]
        if not SourceField.objects.filter(table=source_table, field_name=field_name).exists():
            raise ValueError(f"Field {field_name} does not belong to the selected report table.")
        field_expression = f"{alias}.{quote_clickhouse_identifier(field_name)}"
        if grouped_summary and aggregate:
            expression = f"{aggregate}({field_expression})"
        else:
            expression = field_expression
            if grouped_summary:
                group_by_parts.append(field_expression)
        select_parts.append(
            f"{expression} "
            f"AS {quote_clickhouse_identifier(f'column_{index}')}"
        )
        headers.append(str(label))
        aggregates.append(aggregate)

    if not select_parts:
        raise ValueError("Select at least one report field before running the report.")

    base_physical_name = clickhouse_table_name(
        report.source_id, base_table.schema_name, base_table.table_name
    )
    query = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {quote_clickhouse_identifier(base_physical_name)}"
        f" AS t0{clickhouse_final_clause(base_table)}"
    )
    if join_clauses:
        query += " " + " ".join(join_clauses)
    if group_by_parts:
        query += " GROUP BY " + ", ".join(group_by_parts)
    return query, headers, aggregates
