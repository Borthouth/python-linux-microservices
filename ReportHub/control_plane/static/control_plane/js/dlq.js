window.openDlqModal = function(btn) {
    const taskName = btn.getAttribute('data-task-name');
    const traceback = btn.getAttribute('data-traceback');

    const titleEl = document.getElementById('dlq-modal-title');
    const bodyEl = document.getElementById('dlq-modal-body');
    const modalEl = document.getElementById('dlq-modal');

    if (titleEl) {
        titleEl.textContent = `Failure Traceback: ${taskName}`;
    }
    if (bodyEl) {
        bodyEl.textContent = traceback || 'No traceback recorded for this task.';
    }
    
    if (modalEl) {
        modalEl.classList.remove('hidden');
    }
};

window.closeDlqModal = function() {
    const modalEl = document.getElementById('dlq-modal');
    if (modalEl) {
        modalEl.classList.add('hidden');
    }
};

// Close when clicking outside the modal box
document.addEventListener('click', function(e) {
    const modalEl = document.getElementById('dlq-modal');
    if (modalEl && e.target === modalEl) {
        closeDlqModal();
    }
});