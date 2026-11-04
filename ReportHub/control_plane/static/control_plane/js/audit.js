function openAuditModal(button) {
    const changesJson = button.getAttribute('data-changes');
    const recordName = button.getAttribute('data-record-name');
    const timestamp = button.getAttribute('data-timestamp');
    
    let changesObj = {};
    try {
        changesObj = JSON.parse(changesJson);
    } catch (e) {
        changesObj = {};
    }

    document.getElementById('modalTitle').innerText = `Changes for: ${recordName}`;
    document.getElementById('modalTimestamp').innerText = `Timestamp: ${timestamp}`;
    
    const container = document.getElementById('modalChangesBody');
    container.innerHTML = '';
    
    if (Object.keys(changesObj).length === 0) {
        container.innerHTML = '<p class="text-gray-400 text-sm text-center py-4">No specific field diffs recorded.</p>';
    } else {
        for (const [field, data] of Object.entries(changesObj)) {
            const fieldCard = document.createElement('div');
            fieldCard.className = 'bg-gray-950 p-3 rounded-xl border border-gray-800/80';
            
            const fieldTitle = document.createElement('div');
            fieldTitle.className = 'text-xs font-mono text-indigo-400 uppercase mb-1.5 font-semibold';
            fieldTitle.textContent = field;
            
            const flexDiv = document.createElement('div');
            flexDiv.className = 'flex items-center justify-start text-sm gap-3';
            
            const fromSpan = document.createElement('span');
            fromSpan.className = 'px-2.5 py-1 bg-red-950/60 text-red-300 border border-red-900/40 rounded-lg line-through truncate';
            fromSpan.textContent = data.from !== null ? data.from : 'None';
            
            const arrowSpan = document.createElement('span');
            arrowSpan.className = 'text-gray-500 font-bold';
            arrowSpan.textContent = '→';
            
            const toSpan = document.createElement('span');
            toSpan.className = 'px-2.5 py-1 bg-green-950/60 text-green-300 border border-green-900/40 rounded-lg truncate';
            toSpan.textContent = data.to !== null ? data.to : 'None';
            
            flexDiv.appendChild(fromSpan);
            flexDiv.appendChild(arrowSpan);
            flexDiv.appendChild(toSpan);
            
            fieldCard.appendChild(fieldTitle);
            fieldCard.appendChild(flexDiv);
            container.appendChild(fieldCard);
        }
    }
    
    const modal = document.getElementById('auditModal');
    if (modal) {
        modal.classList.remove('hidden');
        modal.classList.add('flex');
    }
}

function closeAuditModal() {
    const modal = document.getElementById('auditModal');
    if (modal) {
        modal.classList.remove('flex');
        modal.classList.add('hidden');
    }
}

// Close modal on background click
window.onclick = function(event) {
    const modal = document.getElementById('auditModal');
    if (event.target === modal) {
        closeAuditModal();
    }
}