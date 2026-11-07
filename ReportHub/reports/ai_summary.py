import json
from decimal import Decimal, InvalidOperation

from .ollama import generate_text


def build_summary_metrics(headers, rows, pdf_truncated=False):
    fields = []
    for column_index, header in enumerate(headers):
        values = [row[column_index] for row in rows if row[column_index] is not None]
        field_metrics = {
            "field": str(header)[:100],
            "non_null_rows": len(values),
            "distinct_values": len({str(value) for value in values}),
        }

        numeric_values = []
        for value in values:
            try:
                number = Decimal(str(value))
            except (InvalidOperation, ValueError, TypeError):
                numeric_values = []
                break
            if not number.is_finite():
                numeric_values = []
                break
            numeric_values.append(number)

        if numeric_values:
            field_metrics["numeric_range"] = {
                "min": str(min(numeric_values)),
                "max": str(max(numeric_values)),
                "average": str(sum(numeric_values) / len(numeric_values)),
            }
        fields.append(field_metrics)

    return {
        "included_row_count": len(rows),
        "pdf_truncated": pdf_truncated,
        "fields": fields,
    }


def generate_report_summary(headers, rows, pdf_truncated=False):
    metrics = build_summary_metrics(headers, rows, pdf_truncated)
    prompt = (
        "Write a concise executive summary for a data report using only the supplied "
        "aggregate statistics. Return 2 to 4 short bullet points as plain text. "
        "Do not infer causes, make recommendations, or claim trends from a single "
        "snapshot. Do not invent facts or numbers. If the statistics do not support "
        "a useful observation, say so. The input contains no source row values. "
        "If pdf_truncated is true, describe only the records included in the PDF, "
        "not the complete underlying result set.\n\n"
        f"Aggregate statistics:\n{json.dumps(metrics, separators=(',', ':'))}"
    )
    summary = generate_text(prompt).strip()
    if not summary:
        raise ValueError("The AI returned an empty report summary. Please try again.")
    return summary[:1800]
