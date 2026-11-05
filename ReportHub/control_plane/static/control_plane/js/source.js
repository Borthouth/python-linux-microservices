// Data Source Deletion Modal Handlers
function openDeleteModal(button) {
    const deleteUrl = button.getAttribute('data-delete-url');
    const sourceName = button.getAttribute('data-source-name');
    
    const modal = document.getElementById('deleteModal');
    const form = document.getElementById('deleteForm');
    const nameSpan = document.getElementById('modalSourceName');
    
    if (form && modal && nameSpan) {
        form.action = deleteUrl;
        nameSpan.textContent = sourceName;
        modal.classList.remove('hidden');
    }
}

function closeDeleteModal() {
    const modal = document.getElementById('deleteModal');
    if (modal) {
        modal.classList.add('hidden');
    }
}