import json
from .ollama import OllamaUnavailable, generate_text

ALLOWED_AGGREGATES = {"", "COUNT", "SUM", "AVG", "MIN", "MAX"}
ALLOWED_REPORT_TYPES = {"tabular", "grouped_summary"}


def validate_report_design(design, schema):
    if not isinstance(design, dict):
        raise ValueError("The AI response must be a JSON object.")

    related_tables = {table["key"]: table for table in schema["related_tables"]}
    selected_relationships = design.get("selectedRelationships")
    if not isinstance(selected_relationships, list):
        raise ValueError("The AI response contains invalid table relationships.")
    selected_relationships = [str(key) for key in selected_relationships]
    if len(set(selected_relationships)) != len(selected_relationships):
        raise ValueError("The AI response contains duplicate table relationships.")
    if any(key not in related_tables for key in selected_relationships):
        raise ValueError("The AI response selected an unavailable table relationship.")

    fields_by_table = {
        "base": {field["name"] for field in schema["base_table"]["fields"]}
    }
    fields_by_table.update({
        key: {field["name"] for field in table["fields"]}
        for key, table in related_tables.items()
        if key in selected_relationships
    })

    selected_columns = design.get("selectedColumns")
    if not isinstance(selected_columns, list) or not selected_columns:
        raise ValueError("The AI response must select at least one report field.")
    if len(selected_columns) > 50:
        raise ValueError("The AI response selected too many report fields.")

    validated_columns = []
    for column in selected_columns:
        if not isinstance(column, dict):
            raise ValueError("The AI response contains an invalid report field.")
        table_key = str(column.get("table_key") or "")
        field_name = column.get("field_name")
        aggregate = str(column.get("aggregate") or "").strip().upper()
        if aggregate in {"NONE", "NULL", "N/A", "NO AGGREGATE", "NO_AGGREGATE"}:
            aggregate = ""
        if (
            table_key not in fields_by_table
            or not isinstance(field_name, str)
            or field_name not in fields_by_table[table_key]
        ):
            raise ValueError("The AI response selected a field outside the available schema.")
        if aggregate not in ALLOWED_AGGREGATES:
            raise ValueError("The AI response selected an unsupported aggregate.")

        table = schema["base_table"] if table_key == "base" else related_tables[table_key]
        label = column.get("label") or field_name
        if not isinstance(label, str):
            label = str(field_name)
        validated_columns.append({
            "field": f"{table_key}:{field_name}",
            "field_name": field_name,
            "table_key": table_key,
            "table_id": table["table_id"],
            "table_name": table["label"],
            "label": label[:255],
            "aggregate": aggregate,
        })

    report_type = design.get("reportType")
    if not isinstance(report_type, str) or report_type not in ALLOWED_REPORT_TYPES:
        raise ValueError("The AI response contains an unsupported report layout.")
    show_totals = design.get("showTotals")
    if not isinstance(show_totals, bool):
        raise ValueError("The AI response contains an invalid totals setting.")
    summary_position = design.get("summaryPosition")
    if not isinstance(summary_position, str) or summary_position not in {"top", "bottom"}:
        raise ValueError("The AI response contains an invalid summary position.")

    return {
        "reportType": report_type,
        "showTotals": show_totals,
        "summaryPosition": summary_position,
        "selectedRelationships": selected_relationships,
        "selectedColumns": validated_columns,
    }


def generate_report_design(description, schema):
    prompt = (
        "Create a report design from the user's request and the supplied database schema. "
        "Return only a JSON object with keys reportType, showTotals, summaryPosition, "
        "selectedRelationships, and selectedColumns. reportType must be tabular or "
        "grouped_summary; summaryPosition must be top or bottom. selectedRelationships "
        "must contain only relationship keys from the schema. Each selectedColumns item "
        "must contain table_key, field_name, label, and aggregate. table_key must be "
        "'base' or an available relationship key; aggregate must be an empty string when "
        "no aggregation is needed, or one of COUNT, SUM, AVG, MIN, MAX. Choose only "
        "fields in the schema. Do not generate SQL, "
        "invent fields, or include explanations.\n\n"
        f"User request:\n{description}\n\n"
        f"Available schema:\n{json.dumps(schema, separators=(',', ':'))}"
    )
    generated_text = generate_text(prompt, response_format="json")
    try:
        design = json.loads(generated_text)
    except json.JSONDecodeError as exc:
        raise ValueError("The AI returned invalid report configuration. Please try again.") from exc
    return validate_report_design(design, schema)
