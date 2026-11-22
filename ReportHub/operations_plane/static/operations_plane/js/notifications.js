document.addEventListener('click', function (event) {
    const button = event.target.closest('[data-dismiss-message]');
    if (!button) return;

    const message = button.closest('[data-message]');
    if (message) message.remove();
    const container = document.querySelector('[data-messages]');
    if (container && !container.children.length) container.classList.add('hidden');
});

window.OperationsMessages = {
    show(text, level = 'info') {
        const container = document.querySelector('[data-messages]');
        if (!container) return;
        const kind = ['success', 'error'].includes(level) ? level : 'info';
        const template = document.querySelector(`[data-message-template="${kind}"]`);
        const message = template.content.firstElementChild.cloneNode(true);
        message.querySelector('[data-message-text]').textContent = text;
        container.replaceChildren(message);
        container.classList.remove('hidden');
    },
};
