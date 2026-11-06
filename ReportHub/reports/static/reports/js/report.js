function reportFormSync() {
    return {
        selectedSource: '',
        selectedTable: '',
        availableTables: [],
        init() {
            if (this.selectedSource) {
                this.loadTables(this.selectedSource, true);
            }
        },
        loadTables(sourceId, preserveSelected = false) {
            if (!sourceId) {
                this.availableTables = [];
                this.selectedTable = '';
                return;
            }
            fetch("/reports/ajax/load-tables/?source_id=" + sourceId)
                .then(res => res.json())
                .then(data => {
                    this.availableTables = data;
                    if (!preserveSelected) {
                        this.selectedTable = '';
                        window.dispatchEvent(new CustomEvent('table-changed', { detail: '' }));
                    }
                })
                .catch(err => console.error("Failed to load tables:", err));
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
            selectedColumns: initialConfig?.selectedColumns || [],
            showTotals: initialConfig?.showTotals ?? true,
            summaryPosition: initialConfig?.summaryPosition || 'bottom',
            init() {
                window.addEventListener('table-changed', (e) => {
                    if (e.detail) this.loadColumns(e.detail);
                    else this.availableFields = [];
                });

                const baseTableSelect = document.querySelector('#id_base_table');
                if (baseTableSelect && baseTableSelect.value) {
                    this.loadColumns(baseTableSelect.value);
                }
            },
            loadColumns(tableId) {
                fetch("/reports/ajax/load-columns/?table_id=" + tableId)
                    .then(res => res.json())
                    .then(data => {
                        this.availableFields = data ? data.map(item => typeof item === 'string' ? item : (item.field_name || item.name)) : [];
                    });
            },
            isFieldValid(fieldName) {
                if (this.availableFields.length === 0) return true;
                return this.availableFields.includes(fieldName);
            },
            get invalidColumns() {
                if (this.availableFields.length === 0) return [];
                return this.selectedColumns.filter(col => !this.availableFields.includes(col.field));
            },
            get hasInvalidColumns() { return this.invalidColumns.length > 0; },
            get invalidColumnNames() { return this.invalidColumns.map(col => col.field).join(', '); },
            setReportType(type) { this.reportType = type; },
            addField(field) {
                this.selectedColumns.push({ field: field, label: field, aggregate: '' });
            },
            removeField(index) { this.selectedColumns.splice(index, 1); },
            moveUp(index) {
                if (index > 0) {
                    let temp = this.selectedColumns[index];
                    this.selectedColumns[index] = this.selectedColumns[index - 1];
                    this.selectedColumns[index - 1] = temp;
                }
            },
            moveDown(index) {
                if (index < this.selectedColumns.length - 1) {
                    let temp = this.selectedColumns[index];
                    this.selectedColumns[index] = this.selectedColumns[index + 1];
                    this.selectedColumns[index + 1] = temp;
                }
            }
        };
    });
});