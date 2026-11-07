function reportFormSync() {
    return {
        selectedSource: '',
        selectedTable: '',
        availableTables: [],
        tableLoadMessage: '',
        init() {
            const sourceSelect = document.querySelector('#id_source');
            const baseTableSelect = document.querySelector('#id_base_table');
            this.selectedSource = sourceSelect?.value || '';
            this.selectedTable = baseTableSelect?.value || '';

            sourceSelect?.addEventListener('change', () => {
                this.selectedSource = sourceSelect.value;
                this.loadTables(this.selectedSource);
            });
            baseTableSelect?.addEventListener('change', () => {
                this.selectedTable = baseTableSelect.value;
                this.triggerTableChange(this.selectedTable);
            });

            if (this.selectedSource) {
                this.loadTables(this.selectedSource, true);
            }
        },
        loadTables(sourceId, preserveSelected = false) {
            if (!sourceId) {
                this.availableTables = [];
                this.selectedTable = '';
                this.tableLoadMessage = '';
                this.updateTableOptions();
                this.triggerTableChange('');
                return;
            }
            this.tableLoadMessage = 'Loading tables...';
            fetch("/reports/ajax/load-tables/?source_id=" + sourceId)
                .then(res => {
                    if (!res.ok) throw new Error(`Request failed with status ${res.status}`);
                    return res.json();
                })
                .then(data => {
                    if (String(this.selectedSource) !== String(sourceId)) return;
                    if (!Array.isArray(data)) {
                        throw new Error('The tables endpoint returned an invalid response.');
                    }
                    this.availableTables = data;
                    const selectedTableExists = data.some(
                        table => String(table.id) === String(this.selectedTable)
                    );
                    if (!preserveSelected || !selectedTableExists) {
                        this.selectedTable = data.length ? String(data[0].id) : '';
                    }
                    this.tableLoadMessage = data.length
                        ? ''
                        : 'No tables found for this source. Run schema introspection for the data source, then try again.';
                })
                .then(() => {
                    if (String(this.selectedSource) !== String(sourceId)) return;
                    this.updateTableOptions();
                    this.triggerTableChange(this.selectedTable);
                })
                .catch(err => {
                    if (String(this.selectedSource) !== String(sourceId)) return;
                    this.availableTables = [];
                    this.selectedTable = '';
                    this.tableLoadMessage = 'Could not load tables. Check the browser console or server logs.';
                    this.updateTableOptions();
                    this.triggerTableChange('');
                    console.error("Failed to load tables:", err);
                });
        },
        updateTableOptions() {
            const baseTableSelect = document.querySelector('#id_base_table');
            if (!baseTableSelect) return;

            baseTableSelect.replaceChildren(new Option('Select a base table', ''));
            this.availableTables.forEach(table => {
                baseTableSelect.add(new Option(table.table_name, String(table.id)));
            });
            baseTableSelect.value = this.selectedTable;
        },
        triggerTableChange(tableId) {
            window.dispatchEvent(new CustomEvent('table-changed', { detail: tableId }));
        }
    }
}

document.addEventListener('alpine:init', () => {
    Alpine.data('reportBuilder', () => {
        let initialConfig = null;
        const configElement = document.getElementById('report-config-data');
        if (configElement) {
            try { initialConfig = JSON.parse(configElement.textContent); } catch (e) {}
        }

        return {
            reportType: initialConfig?.reportType || 'tabular',
            availableFields: [],
            baseFields: [],
            relatedTables: [],
            selectedRelationships: initialConfig?.selectedRelationships || [],
            selectedColumns: initialConfig?.selectedColumns || [],
            activeTableId: '',
            showTotals: initialConfig?.showTotals ?? true,
            summaryPosition: initialConfig?.summaryPosition || 'bottom',
            exportFormat: document.querySelector('#id_export_format')?.value || 'csv',
            chart: initialConfig?.chart || {
                type: '',
                categoryIndex: null,
                valueIndex: null,
                aggregation: 'SUM',
                title: ''
            },
            aiDescription: '',
            aiLoading: false,
            aiDesignMessage: '',
            aiDesignError: false,
            init() {
                window.addEventListener('table-changed', (e) => {
                    if (e.detail) this.loadColumns(e.detail);
                    else this.availableFields = [];
                });

                const baseTableSelect = document.querySelector('#id_base_table');
                if (baseTableSelect && baseTableSelect.value) {
                    this.activeTableId = String(baseTableSelect.value);
                    this.loadColumns(baseTableSelect.value);
                }

                const exportFormatSelect = document.querySelector('#id_export_format');
                if (exportFormatSelect) {
                    this.exportFormat = exportFormatSelect.value;
                    exportFormatSelect.addEventListener('change', (event) => {
                        this.exportFormat = event.target.value;
                    });
                }
            },
            get supportsChartExport() {
                return ['pdf', 'xlsx'].includes(this.exportFormat);
            },
            get supportsAiSummary() {
                return ['pdf', 'xlsx', 'json'].includes(this.exportFormat);
            },
            loadColumns(tableId) {
                const normalizedTableId = String(tableId);
                if (this.activeTableId && this.activeTableId !== normalizedTableId) {
                    this.selectedColumns = [];
                    this.selectedRelationships = [];
                    this.chart = {
                        type: '',
                        categoryIndex: null,
                        valueIndex: null,
                        aggregation: 'SUM',
                        title: ''
                    };
                }
                this.activeTableId = normalizedTableId;
                this.availableFields = [];
                this.baseFields = [];
                this.relatedTables = [];

                Promise.all([
                    fetch("/reports/ajax/load-columns/?table_id=" + encodeURIComponent(tableId)),
                    fetch("/reports/ajax/load-related-tables/?source_id=" +
                        encodeURIComponent(document.querySelector('#id_source')?.value || '') +
                        "&table_id=" + encodeURIComponent(tableId))
                ])
                    .then(async ([columnsResponse, relationshipsResponse]) => {
                        if (!columnsResponse.ok || !relationshipsResponse.ok) {
                            throw new Error('Failed to load table fields or relationships.');
                        }
                        const [columns, relatedTables] = await Promise.all([
                            columnsResponse.json(),
                            relationshipsResponse.json()
                        ]);
                        const baseTableSelect = document.querySelector('#id_base_table');
                        if (baseTableSelect && String(baseTableSelect.value) !== normalizedTableId) return;

                        const selectedOption = baseTableSelect?.selectedOptions[0];
                        const baseTableName = selectedOption?.textContent || 'Base table';
                        this.baseFields = columns.map(fieldName => ({
                            field: `base:${fieldName}`,
                            table_key: 'base',
                            table_id: Number(normalizedTableId),
                            table_name: baseTableName,
                            field_name: fieldName
                        }));
                        this.relatedTables = relatedTables.map(table => ({
                            ...table,
                            key: String(table.key),
                            fields: table.fields.map(fieldName => ({
                                field: `${table.key}:${fieldName}`,
                                table_key: String(table.key),
                                table_id: Number(table.table_id),
                                table_name: table.label,
                                field_name: fieldName
                            }))
                        }));

                        this.selectedColumns = this.selectedColumns.map(column => {
                            if (typeof column === 'string') {
                                const field = this.baseFields.find(item => item.field_name === column);
                                return field
                                    ? { ...field, label: column, aggregate: '' }
                                    : { field: column, field_name: column, table_name: baseTableName, label: column, aggregate: '' };
                            }
                            if (!column.table_key) {
                                const field = this.baseFields.find(item => item.field_name === column.field);
                                return field
                                    ? { ...field, label: column.label || column.field, aggregate: column.aggregate || '' }
                                    : column;
                            }
                            return column;
                        });
                        const validKeys = new Set(this.relatedTables.map(table => table.key));
                        this.selectedRelationships = this.selectedRelationships
                            .map(String)
                            .filter(key => validKeys.has(key));
                        this.refreshAvailableFields();
                    })
                    .catch(err => console.error("Failed to load table columns:", err));
            },
            refreshAvailableFields() {
                const selectedKeys = new Set(this.selectedRelationships.map(String));
                this.availableFields = [
                    ...this.baseFields,
                    ...this.relatedTables
                        .filter(table => selectedKeys.has(table.key))
                        .flatMap(table => table.fields)
                ];
            },
            toggleRelationship(table) {
                const key = String(table.key);
                if (this.selectedRelationships.includes(key)) {
                    const categoryColumn = this.selectedColumns[this.chart.categoryIndex];
                    const valueColumn = this.selectedColumns[this.chart.valueIndex];
                    this.selectedRelationships = this.selectedRelationships.filter(item => item !== key);
                    this.selectedColumns = this.selectedColumns.filter(column => column.table_key !== key);
                    this.chart.categoryIndex = this.selectedColumns.indexOf(categoryColumn);
                    this.chart.valueIndex = this.selectedColumns.indexOf(valueColumn);
                    if (this.chart.categoryIndex < 0) this.chart.categoryIndex = null;
                    if (this.chart.valueIndex < 0) this.chart.valueIndex = null;
                } else {
                    this.selectedRelationships.push(key);
                }
                this.refreshAvailableFields();
            },
            isFieldValid(fieldName) {
                return this.availableFields.some(field => field.field === fieldName);
            },
            get invalidColumns() {
                if (this.availableFields.length === 0) return this.selectedColumns;
                const fieldKeys = new Set(this.availableFields.map(field => field.field));
                return this.selectedColumns.filter(col => !fieldKeys.has(col.field));
            },
            get duplicateColumns() {
                const seen = new Set();
                const duplicates = new Set();
                this.selectedColumns.forEach(column => {
                    const aggregate = String(column.aggregate || '').toUpperCase();
                    const key = `${column.field}\u0000${aggregate}`;
                    if (seen.has(key)) duplicates.add(`${column.table_name}.${column.field_name}`);
                    seen.add(key);
                });
                return [...duplicates];
            },
            get hasInvalidColumns() { return this.invalidColumns.length > 0; },
            get invalidColumnNames() { return this.invalidColumns.map(col => col.field_name || col.field).join(', '); },
            get hasDuplicateColumns() { return this.duplicateColumns.length > 0; },
            get duplicateColumnNames() { return this.duplicateColumns.join(', '); },
            setReportType(type) { this.reportType = type; },
            addField(field) {
                this.selectedColumns.push({
                    ...field,
                    label: field.field_name,
                    aggregate: ''
                });
            },
            removeField(index) {
                if (this.chart.categoryIndex === index) this.chart.categoryIndex = null;
                else if (this.chart.categoryIndex > index) this.chart.categoryIndex -= 1;
                if (this.chart.valueIndex === index) this.chart.valueIndex = null;
                else if (this.chart.valueIndex > index) this.chart.valueIndex -= 1;
                this.selectedColumns.splice(index, 1);
            },
            moveUp(index) {
                if (index > 0) {
                    if (this.chart.categoryIndex === index) this.chart.categoryIndex = index - 1;
                    else if (this.chart.categoryIndex === index - 1) this.chart.categoryIndex = index;
                    if (this.chart.valueIndex === index) this.chart.valueIndex = index - 1;
                    else if (this.chart.valueIndex === index - 1) this.chart.valueIndex = index;
                    let temp = this.selectedColumns[index];
                    this.selectedColumns[index] = this.selectedColumns[index - 1];
                    this.selectedColumns[index - 1] = temp;
                }
            },
            moveDown(index) {
                if (index < this.selectedColumns.length - 1) {
                    if (this.chart.categoryIndex === index) this.chart.categoryIndex = index + 1;
                    else if (this.chart.categoryIndex === index + 1) this.chart.categoryIndex = index;
                    if (this.chart.valueIndex === index) this.chart.valueIndex = index + 1;
                    else if (this.chart.valueIndex === index + 1) this.chart.valueIndex = index;
                    let temp = this.selectedColumns[index];
                    this.selectedColumns[index] = this.selectedColumns[index + 1];
                    this.selectedColumns[index + 1] = temp;
                }
            },
            async generateReportDesign() {
                const sourceId = document.querySelector('#id_source')?.value;
                const tableId = document.querySelector('#id_base_table')?.value;
                if (!sourceId || !tableId) {
                    this.aiDesignError = true;
                    this.aiDesignMessage = 'Select a data source and base table first.';
                    return;
                }
                if (!this.aiDescription.trim()) {
                    this.aiDesignError = true;
                    this.aiDesignMessage = 'Describe the report you want to create.';
                    return;
                }

                this.aiLoading = true;
                this.aiDesignMessage = 'Asking the local AI to design your report...';
                this.aiDesignError = false;
                try {
                    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]')?.value;
                    const response = await fetch('/reports/ajax/ai-design/', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'X-CSRFToken': csrfToken
                        },
                        body: JSON.stringify({
                            description: this.aiDescription,
                            source_id: sourceId,
                            table_id: tableId
                        })
                    });
                    const result = await response.json();
                    if (!response.ok) {
                        throw new Error(result.error || 'The report design request failed.');
                    }

                    const config = result.config;
                    if (!config
                        || !Array.isArray(config.selectedRelationships)
                        || !Array.isArray(config.selectedColumns)
                        || !config.selectedColumns.length) {
                        throw new Error('The AI returned an incomplete report design. Please try again.');
                    }
                    const availableRelationshipKeys = new Set(
                        this.relatedTables.map(table => String(table.key))
                    );
                    if (config.selectedRelationships.some(
                        key => !availableRelationshipKeys.has(String(key))
                    )) {
                        throw new Error('The AI selected a relationship that is no longer available. Reload the table fields and try again.');
                    }

                    const previousChart = this.chart;
                    const previousChartColumns = this.selectedColumns;
                    this.selectedRelationships = config.selectedRelationships.map(String);
                    this.refreshAvailableFields();
                    this.selectedColumns = config.selectedColumns.map(column => {
                        const field = this.availableFields.find(
                            available => available.table_key === String(column.table_key)
                                && available.field_name === column.field_name
                        );
                        if (!field) {
                            throw new Error(`The AI selected an unavailable field: ${column.field_name}`);
                        }
                        return {
                            ...field,
                            label: column.label,
                            aggregate: column.aggregate
                        };
                    });
                    const remapChartIndex = index => {
                        if (!Number.isInteger(index)) return null;
                        const previousColumn = previousChartColumns[index];
                        if (!previousColumn) return null;
                        const newIndex = this.selectedColumns.findIndex(column =>
                            String(column.table_key) === String(previousColumn.table_key)
                            && (column.field_name || column.field) ===
                                (previousColumn.field_name || previousColumn.field)
                        );
                        return newIndex >= 0 ? newIndex : null;
                    };
                    this.chart = {
                        ...previousChart,
                        categoryIndex: remapChartIndex(previousChart.categoryIndex),
                        valueIndex: remapChartIndex(previousChart.valueIndex)
                    };
                    this.reportType = config.reportType;
                    this.showTotals = config.showTotals;
                    this.summaryPosition = config.summaryPosition;
                    this.aiDesignMessage = 'Draft applied. Review the selected fields and layout before saving.';
                } catch (error) {
                    this.aiDesignError = true;
                    this.aiDesignMessage = error.message || 'Could not generate the report design.';
                } finally {
                    this.aiLoading = false;
                }
            }
        };
    });
});