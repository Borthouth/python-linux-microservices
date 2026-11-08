function toggleWidget(widgetName, isVisible, csrfToken) {
    let targetId = `widget-${widgetName}`;
    if (widgetName === 'pipelines' || widgetName === 'reports' || widgetName === 'dlq') {
        targetId = `widget-${widgetName}-wrapper`;
    }
    
    const el = document.getElementById(targetId);
    if (el) {
        el.classList.toggle('hidden', !isVisible);
    }

    return fetch(window.updateWidgetPreferencesUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrfToken
        },
        body: JSON.stringify({ widget: widgetName, visible: isVisible })
    });
}

async function toggleEmailAuditWidget(checkbox, csrfToken) {
    const visible = checkbox.checked;
    const wrapper = document.getElementById('widget-email_audit-wrapper');
    checkbox.disabled = true;
    wrapper.classList.toggle('hidden', !visible);
    try {
        const response = await toggleWidget('email_audit', visible, csrfToken);
        if (!response.ok || response.redirected) {
            throw new Error('Widget preference request failed');
        }
        const result = await response.json();
        if (result.status !== 'success') {
            throw new Error('Widget preference was not saved');
        }
    } catch (error) {
        console.error('Unable to save email audit widget preference:', error);
        checkbox.checked = !visible;
        wrapper.classList.toggle('hidden', visible);
        showToast('Unable to save the email audit widget preference. Please try again.', 'error');
    } finally {
        checkbox.disabled = false;
    }
}