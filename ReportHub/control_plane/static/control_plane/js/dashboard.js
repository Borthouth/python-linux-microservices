document.addEventListener('keydown', function (event) {
    if (event.key !== 'Escape') return;
    const navigation = event.target.closest('details[data-widget-navigation][open]');
    if (!navigation) return;
    navigation.open = false;
    navigation.querySelector('summary').focus();
});

document.addEventListener('click', function (event) {
    document.querySelectorAll('details[data-widget-navigation][open]').forEach(function (navigation) {
        if (!navigation.contains(event.target)) navigation.open = false;
    });
});

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
    return toggleAuditWidget('email_audit', 'email audit', checkbox, csrfToken);
}

async function toggleFileDeliveriesWidget(checkbox, csrfToken) {
    return toggleAuditWidget('file_deliveries', 'file delivery', checkbox, csrfToken);
}

async function toggleAuditWidget(widgetName, label, checkbox, csrfToken) {
    const visible = checkbox.checked;
    const wrapper = document.getElementById(`widget-${widgetName}-wrapper`);
    checkbox.disabled = true;
    wrapper.classList.toggle('hidden', !visible);
    try {
        const response = await toggleWidget(widgetName, visible, csrfToken);
        if (!response.ok || response.redirected) {
            throw new Error('Widget preference request failed');
        }
        const result = await response.json();
        if (result.status !== 'success') {
            throw new Error('Widget preference was not saved');
        }
    } catch (error) {
        console.error(`Unable to save ${label} widget preference:`, error);
        checkbox.checked = !visible;
        wrapper.classList.toggle('hidden', visible);
        showToast(`Unable to save the ${label} widget preference. Please try again.`, 'error');
    } finally {
        checkbox.disabled = false;
    }
}