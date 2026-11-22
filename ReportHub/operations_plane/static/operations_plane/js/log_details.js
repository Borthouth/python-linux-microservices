(() => {
    const dialog = document.getElementById('log-event-dialog');
    if (!dialog) return;
    const content = dialog.querySelector('[data-log-full-message]');
    const metadata = dialog.querySelector('[data-log-metadata]');
    const error = dialog.querySelector('[data-log-detail-error]');
    const fragment = dialog.querySelector('[data-log-fragment]');
    const copy = dialog.querySelector('[data-copy-log]');
    let controller;
    let opener;
    document.addEventListener('click', async (event) => {
        const button = event.target.closest('[data-log-detail]');
        if (!button) return;
        opener = button;
        if (controller) controller.abort();
        controller = new AbortController();
        const current = controller;
        content.textContent = 'Loading full event...';
        metadata.textContent = '';
        error.classList.add('hidden');
        fragment.classList.add('hidden');
        copy.disabled = true;
        document.dispatchEvent(new Event('operations-log-open'));
        dialog.showModal();
        try {
            const response = await fetch(button.dataset.logDetail, {
                signal: current.signal, headers: {'Accept': 'application/json'}, cache: 'no-store',
            });
            if (response.redirected) throw new Error('Your session has ended. Reload the page to sign in.');
            const data = await response.json();
            if (!response.ok) throw new Error(data.error || 'The log event could not be loaded.');
            if (!dialog.open || current !== controller) return;
            content.textContent = data.message;
            metadata.textContent = [
                data.event_time, data.severity, data.source_label, data.service,
                data.logger, data.task_id ? `Task ${data.task_id}` : '',
            ].filter(Boolean).join(' | ');
            fragment.classList.toggle('hidden', !data.partial_id);
            copy.disabled = false;
        } catch (failure) {
            if (failure.name === 'AbortError') return;
            if (!dialog.open || current !== controller) return;
            content.textContent = '';
            error.textContent = failure.message || 'The log event could not be loaded.';
            error.classList.remove('hidden');
        }
    });
    dialog.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
            event.preventDefault();
            dialog.close();
        }
    });
    dialog.addEventListener('close', () => {
        if (controller) controller.abort();
        if (opener && opener.isConnected) opener.focus({preventScroll: true});
    });
    copy.addEventListener('click', async () => {
        try {
            if (!navigator.clipboard || !navigator.clipboard.writeText) {
                throw new Error('Clipboard access is unavailable here. Select the message and copy it manually.');
            }
            await navigator.clipboard.writeText(content.textContent);
            window.OperationsMessages.show('Log message copied.', 'success');
        } catch (failure) {
            error.textContent = failure.message || 'Could not copy the log message.';
            error.classList.remove('hidden');
        }
    });
})();
