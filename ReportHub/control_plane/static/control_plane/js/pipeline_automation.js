// Pipeline Automation Management Modal Handlers
function openDeleteModal(button) {
    const deleteUrl = button.getAttribute('data-delete-url');
    const taskName = button.getAttribute('data-task-name');
    
    const modal = document.getElementById('deleteModal');
    const form = document.getElementById('deleteForm');
    const nameSpan = document.getElementById('modalTaskName');
    
    if (form && modal && nameSpan) {
        form.action = deleteUrl;
        nameSpan.textContent = taskName;
        modal.classList.remove('hidden');
    }
}

function closeDeleteModal() {
    const modal = document.getElementById('deleteModal');
    if (modal) {
        modal.classList.add('hidden');
    }
}