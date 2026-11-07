import io
import json

from openpyxl import Workbook
from openpyxl.chart import BarChart, LineChart, PieChart, Reference
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


def build_xlsx_report(
    headers,
    rows,
    summary_lines,
    show_totals,
    summary_position,
    chart_data=None,
    ai_summary=None,
):
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Report"

    if ai_summary:
        worksheet.append([None])
        worksheet.append([f"AI Summary: {ai_summary}"])
        if len(headers) > 1:
            worksheet.merge_cells(
                start_row=2,
                start_column=1,
                end_row=2,
                end_column=len(headers),
            )
        worksheet["A2"].font = Font(bold=True, color="312E81")
        worksheet["A2"].fill = PatternFill(fill_type="solid", fgColor="EEF2FF")
        worksheet["A2"].alignment = Alignment(vertical="top", wrap_text=True)
        worksheet.row_dimensions[2].height = 36
        worksheet.append([None])

    def add_summaries():
        if show_totals and summary_lines:
            worksheet.append(["Summary Aggregates"])
            for line in summary_lines:
                worksheet.append([line])
            worksheet.append([None])

    if summary_position == "top":
        add_summaries()

    has_preface = any(
        cell.value is not None
        for row in worksheet.iter_rows()
        for cell in row
    )
    header_row = worksheet.max_row + 1 if has_preface else 1
    worksheet.append(headers)
    for row in rows:
        worksheet.append(list(row))

    header_fill = PatternFill(fill_type="solid", fgColor="4F46E5")
    for cell in worksheet[header_row]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = header_fill

    worksheet.freeze_panes = f"A{header_row + 1}"
    worksheet.auto_filter.ref = (
        f"A{header_row}:"
        f"{get_column_letter(max(len(headers), 1))}{header_row + len(rows)}"
    )
    for column_cells in worksheet.columns:
        column_letter = get_column_letter(column_cells[0].column)
        max_width = max(
            (len(str(cell.value)) for cell in column_cells if cell.value is not None),
            default=0,
        )
        worksheet.column_dimensions[column_letter].width = min(max(max_width + 2, 10), 50)

    if summary_position != "top":
        if worksheet.max_row:
            worksheet.append([None])
        add_summaries()

    if chart_data and chart_data["categories"]:
        chart_sheet = workbook.create_sheet("_ChartData")
        chart_sheet.append([chart_data["category_label"], chart_data["value_label"]])
        for category, value in zip(chart_data["categories"], chart_data["values"]):
            chart_sheet.append([category, value])

        chart_classes = {
            "bar": BarChart,
            "line": LineChart,
            "pie": PieChart,
        }
        chart = chart_classes[chart_data["type"]]()
        chart.title = chart_data["title"] or (
            f"{chart_data['aggregation'].title()} of {chart_data['value_label']} by "
            f"{chart_data['category_label']}"
        )
        chart.add_data(
            Reference(chart_sheet, min_col=2, min_row=1, max_row=chart_sheet.max_row),
            titles_from_data=True,
        )
        chart.set_categories(
            Reference(chart_sheet, min_col=1, min_row=2, max_row=chart_sheet.max_row)
        )
        chart.height = 9
        chart.width = 18
        worksheet.add_chart(chart, f"{get_column_letter(len(headers) + 2)}2")
        chart_sheet.sheet_state = "hidden"

    output = io.BytesIO()
    workbook.save(output)
    return output.getvalue()


def build_json_report(headers, rows, summary_lines, show_totals, ai_summary=None):
    payload = {"data": [dict(zip(headers, row)) for row in rows]}
    if show_totals and summary_lines:
        payload["summaries"] = summary_lines
    if ai_summary:
        payload["ai_summary"] = ai_summary
    return json.dumps(payload, default=str, indent=2).encode("utf-8")
