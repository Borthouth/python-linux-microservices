import io
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import (
    KeepInFrame,
    LongTable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    TableStyle,
)

from .charting import build_pdf_chart


def _safe_text(value):
    text = "".join(
        character
        for character in str(value)
        if character in "\t\n\r" or ord(character) >= 32
    )
    return escape(text).replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br/>")


def _column_widths(headers, rows, available_width):
    sample_rows = rows[:50]
    weights = []
    for column_index, header in enumerate(headers):
        content_length = len(str(header))
        for row in sample_rows:
            value = row[column_index]
            if value is not None:
                content_length = max(content_length, min(len(str(value)), 40))
        weights.append(max(8, min(content_length, 40)))

    total_weight = sum(weights) or 1
    return [available_width * weight / total_weight for weight in weights]


def build_pdf_report(
    report_name,
    generated_at,
    headers,
    rows,
    summary_lines,
    show_totals,
    summary_position,
    chart_data=None,
    ai_summary=None,
    pdf_truncated=False,
    max_rows=5000,
):
    page_size = landscape(letter) if len(headers) > 4 else letter
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=page_size,
        rightMargin=36,
        leftMargin=36,
        topMargin=36,
        bottomMargin=36,
        title=report_name,
    )
    styles = getSampleStyleSheet()
    body_style = ParagraphStyle(
        "ReportTableBody",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=7.5,
        leading=9,
        spaceAfter=0,
        wordWrap="CJK",
    )
    header_style = ParagraphStyle(
        "ReportTableHeader",
        parent=body_style,
        fontName="Helvetica-Bold",
        textColor=colors.white,
    )
    story = [
        Paragraph(f"<b>Report:</b> {_safe_text(report_name)}", styles["Heading1"]),
        Paragraph(f"<b>Generated at:</b> {_safe_text(generated_at)}", styles["Normal"]),
        Spacer(1, 10),
    ]

    if ai_summary:
        story.append(Paragraph("AI Summary", styles["Heading2"]))
        story.append(Paragraph(_safe_text(ai_summary), body_style))
        story.append(Spacer(1, 12))

    if show_totals and summary_lines and summary_position == "top":
        story.append(Paragraph(
            "<b>Summary Aggregates:</b><br/>"
            + "<br/>".join(_safe_text(line) for line in summary_lines),
            styles["Normal"],
        ))
        story.append(Spacer(1, 15))

    if chart_data and chart_data["categories"]:
        story.append(KeepInFrame(
            doc.width,
            250,
            [build_pdf_chart(chart_data)],
            mode="shrink",
        ))
        story.append(Spacer(1, 8))

    story.append(Paragraph("Detailed Records", styles["Heading2"]))
    if pdf_truncated:
        story.append(Paragraph(
            f"This PDF is limited to the first {max_rows:,} records. "
            "Use CSV or JSON to export the full result set. Summaries reflect the "
            "records included here.",
            styles["Normal"],
        ))
        story.append(Spacer(1, 8))

    table_data = [[    Paragraph(_safe_text(header), header_style) for header in headers]]
    table_data.extend([
        [
            Paragraph("" if value is None else _safe_text(value), body_style)
            for value in row
        ]
        for row in rows
    ])
    table = LongTable(
        table_data,
        colWidths=_column_widths(headers, rows, doc.width),
        repeatRows=1,
        splitByRow=1,
        hAlign="LEFT",
    )
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#4F46E5")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [
            colors.white,
            colors.HexColor("#F3F4F6"),
        ]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, colors.HexColor("#C7D2FE")),
        ("LINEBELOW", (0, 1), (-1, -1), 0.25, colors.HexColor("#E5E7EB")),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(table)

    if show_totals and summary_lines and summary_position != "top":
        story.extend([
            Spacer(1, 15),
            Paragraph(
                "<b>Summary Aggregates:</b><br/>"
                + "<br/>".join(_safe_text(line) for line in summary_lines),
                styles["Normal"],
            ),
        ])

    doc.build(story)
    artifact = buffer.getvalue()
    buffer.close()
    return artifact
