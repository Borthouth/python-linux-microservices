SAVED_FILTER_FIELDS = ('q', 'severity', 'source')


def saved_result_filters(values):
    return {field: (values.get(field) or '').strip() for field in SAVED_FILTER_FIELDS}


def filter_saved_results(rows, filters):
    q = filters.get('q', '').casefold()
    severity = filters.get('severity', '')
    source = filters.get('source', '')
    return [
        row for row in rows
        if (not q or any(q in str(value or '').casefold() for value in row.values()))
        and (not severity or row.get('severity') == severity)
        and (not source or (row.get('source_category') or row.get('source')) == source)
    ]
