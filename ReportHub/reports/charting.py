from collections import defaultdict
from decimal import Decimal, InvalidOperation
import math


CHART_TYPES = {"bar", "line", "pie"}
CHART_AGGREGATES = {"COUNT", "SUM", "AVG", "MIN", "MAX"}
MAX_CHART_CATEGORIES = 20


def _aggregate(values, aggregation):
    if aggregation == "COUNT":
        return float(len(values))
    if aggregation == "SUM":
        return sum(values)
    if aggregation == "AVG":
        return sum(values) / len(values)
    if aggregation == "MIN":
        return min(values)
    return max(values)


def build_chart_data(chart_config, headers, rows):
    if not isinstance(chart_config, dict):
        raise ValueError("The chart configuration must be an object.")

    chart_type = chart_config.get("type")
    if not isinstance(chart_type, str) or chart_type not in CHART_TYPES:
        raise ValueError("Choose a supported chart type.")

    category_index = chart_config.get("categoryIndex")
    value_index = chart_config.get("valueIndex")
    if (
        not isinstance(category_index, int)
        or isinstance(category_index, bool)
        or not 0 <= category_index < len(headers)
        or not isinstance(value_index, int)
        or isinstance(value_index, bool)
        or not 0 <= value_index < len(headers)
        or category_index == value_index
    ):
        raise ValueError("Choose different valid category and value report fields for the chart.")

    aggregation = chart_config.get("aggregation")
    if not isinstance(aggregation, str) or aggregation not in CHART_AGGREGATES:
        raise ValueError("Choose a supported chart aggregation.")

    categories = defaultdict(list)
    for row in rows:
        category = row[category_index]
        value = row[value_index]
        if category is None or value is None:
            continue
        if aggregation == "COUNT":
            categories[str(category)].append(1)
            continue
        try:
            numeric_value = float(Decimal(str(value)))
        except (InvalidOperation, ValueError, TypeError):
            continue
        if math.isfinite(numeric_value):
            categories[str(category)].append(numeric_value)

    values_by_category = {}
    for category, values in categories.items():
        values_by_category[category] = _aggregate(values, aggregation)

    if not values_by_category:
        raise ValueError("The chart has no usable data. Choose a numeric value field with data.")
    if chart_type == "pie":
        if any(value < 0 for value in values_by_category.values()):
            raise ValueError("Pie charts require non-negative values.")
        if not any(value > 0 for value in values_by_category.values()):
            raise ValueError("Pie charts require at least one positive value.")

    ranked = list(values_by_category.items())
    if chart_type != "line":
        ranked.sort(key=lambda item: abs(item[1]), reverse=True)
    if len(ranked) > MAX_CHART_CATEGORIES:
        top_categories = ranked[:MAX_CHART_CATEGORIES - 1]
        other_categories = {category for category, _ in ranked[MAX_CHART_CATEGORIES - 1:]}
        other_values = [
            value
            for category in other_categories
            for value in categories[category]
        ]
        ranked = [*top_categories, ("Other", _aggregate(other_values, aggregation))]

    return {
        "type": chart_type,
        "title": str(chart_config.get("title") or "").strip()[:255],
        "category_label": str(headers[category_index]),
        "value_label": str(headers[value_index]),
        "aggregation": aggregation,
        "categories": [category for category, _ in ranked],
        "values": [value for _, value in ranked],
    }


def build_pdf_chart(chart_data):
    from reportlab.graphics.charts.barcharts import VerticalBarChart
    from reportlab.graphics.charts.linecharts import HorizontalLineChart
    from reportlab.graphics.charts.piecharts import Pie
    from reportlab.graphics.shapes import Drawing, String

    drawing = Drawing(500, 250)
    chart_title = chart_data["title"] or (
        f"{chart_data['aggregation'].title()} of {chart_data['value_label']} by "
        f"{chart_data['category_label']}"
    )

    if chart_data["type"] == "pie":
        chart = Pie()
        chart.x = 115
        chart.y = 25
        chart.width = 270
        chart.height = 200
        chart.data = chart_data["values"]
        chart.labels = chart_data["categories"]
        chart.slices.strokeWidth = 0.5
    else:
        chart_class = VerticalBarChart if chart_data["type"] == "bar" else HorizontalLineChart
        chart = chart_class()
        chart.x = 55
        chart.y = 45
        chart.width = 405
        chart.height = 170
        chart.data = [chart_data["values"]]
        chart.categoryAxis.categoryNames = chart_data["categories"]
        chart.categoryAxis.labels.fontSize = 7
        chart.valueAxis.labels.fontSize = 7
        if chart_data["type"] == "bar":
            chart.categoryAxis.labels.angle = 25
            chart.categoryAxis.labels.dx = -5
            chart.categoryAxis.labels.dy = -10

    drawing.add(chart)
    drawing.add(String(250, 230, chart_title, textAnchor="middle", fontSize=11))
    return drawing
