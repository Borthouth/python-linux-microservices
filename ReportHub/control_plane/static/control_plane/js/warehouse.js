// Warehouse Field Selection & Navigation Handlers
function switchTable(tableName) {
    document.querySelectorAll('.table-panel').forEach(panel => panel.classList.add('hidden'));
    const targetPanel = document.getElementById('table-panel-' + tableName);
    if (targetPanel) targetPanel.classList.remove('hidden');

    document.querySelectorAll('.table-nav-item').forEach(btn => {
        btn.classList.remove('bg-indigo-600', 'text-white', 'shadow-lg', 'shadow-indigo-600/20');
        btn.classList.add('text-slate-400', 'hover:bg-slate-800', 'hover:text-white');
    });
    
    const activeBtn = document.getElementById('nav-btn-' + tableName);
    if (activeBtn) {
        activeBtn.classList.add('bg-indigo-600', 'text-white', 'shadow-lg', 'shadow-indigo-600/20');
        activeBtn.classList.remove('text-slate-400', 'hover:bg-slate-800', 'hover:text-white');
    }
}

function toggleTableFields(tableName, state) {
    document.querySelectorAll('.table-field-' + tableName).forEach(cb => {
        const parentCard = cb.closest('.column-item');
        if (parentCard && parentCard.style.display !== 'none') {
            cb.checked = state;
        }
    });
}

function filterTableList() {
    let query = document.getElementById('tableSearch').value.toLowerCase();
    document.querySelectorAll('.table-nav-item').forEach(btn => {
        let name = btn.getAttribute('data-tablename');
        if (name.includes(query)) {
            btn.style.display = "";
        } else {
            btn.style.display = "none";
        }
    });
}

function filterColumns(input, tableName) {
    let query = input.value.toLowerCase();
    let panel = document.getElementById('table-panel-' + tableName);
    if (panel) {
        panel.querySelectorAll('.column-item').forEach(item => {
            let colName = item.getAttribute('data-colname');
            if (colName.includes(query)) {
                item.style.display = "";
            } else {
                item.style.display = "none";
            }
        });
    }
}