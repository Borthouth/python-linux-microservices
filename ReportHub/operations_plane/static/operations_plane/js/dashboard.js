document.addEventListener('change', async function (event) {
    const checkbox = event.target.closest('[data-widget-toggle]');
    if (!checkbox) return;

    const widget = checkbox.dataset.widgetToggle;
    const wrapper = document.getElementById(`widget-${widget}-wrapper`);
    const feedback = document.getElementById('widget-preference-feedback');
    const visible = checkbox.checked;
    checkbox.disabled = true;
    if (wrapper) wrapper.classList.toggle('hidden', !visible);

    try {
        const preferenceUrl = document.querySelector('meta[name="update-widget-preferences-url"]').content;
        const response = await fetch(preferenceUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': document.querySelector('meta[name="csrf-token"]').content,
            },
            body: JSON.stringify({widget: widget, visible: visible}),
        });
        const result = await response.json();
        if (!response.ok || response.redirected || result.status !== 'success') {
            throw new Error(result.message || 'Widget preference could not be saved.');
        }
        feedback.textContent = 'Widget preferences saved.';
        feedback.classList.remove('hidden');
    } catch (error) {
        checkbox.checked = !visible;
        if (wrapper) wrapper.classList.toggle('hidden', visible);
        feedback.textContent = error.message || 'Widget preference could not be saved.';
        feedback.classList.remove('hidden');
        feedback.classList.add('text-rose-300');
    } finally {
        checkbox.disabled = false;
    }
});
