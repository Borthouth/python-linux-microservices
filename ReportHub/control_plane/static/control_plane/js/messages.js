document.addEventListener('click', function (event) {
    const button = event.target.closest('[data-dismiss-message]');
    if (!button) return;

    const message = button.closest('[data-message]');
    if (message) message.remove();
});
