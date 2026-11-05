window.openDlqModal = function(btn) {
    const taskName = btn.getAttribute('data-task-name');
    const traceback = btn.getAttribute('data-traceback') || 'No traceback recorded for this task.';

    const titleEl = document.getElementById('dlq-modal-title');
    const bodyEl = document.getElementById('dlq-modal-body');
    const modalEl = document.getElementById('dlq-modal');

    if (titleEl) {
        titleEl.textContent = `Failure Traceback: ${taskName}`;
    }
    if (bodyEl) {
        bodyEl.textContent = traceback;
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

document.addEventListener('click', function(e) {
    const modalEl = document.getElementById('dlq-modal');
    if (modalEl && e.target === modalEl) {
        closeDlqModal();
    }
});