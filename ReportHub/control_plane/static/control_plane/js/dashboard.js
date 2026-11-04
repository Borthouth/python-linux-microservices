function toggleWidget(widgetName, isVisible, csrfToken) {
    let targetId = `widget-${widgetName}`;
    if (widgetName === 'pipelines' || widgetName === 'reports' || widgetName === 'dlq') {
        targetId = `widget-${widgetName}-wrapper`;
    }
    
    const el = document.getElementById(targetId);
    if (el) {
        el.classList.toggle('hidden', !isVisible);
    }

    fetch(window.updateWidgetPreferencesUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrfToken
        },
        body: JSON.stringify({ widget: widgetName, visible: isVisible })
    });
}