document.addEventListener('submit', function (event) {
    if (!event.target.matches('form[data-confirm-resend]')) return;
    if (!window.confirm('Resend this failed file delivery?\n\nDelivery is at-least-once. The recipient may already have consumed or moved the file despite a failed acknowledgement. Resending can cause downstream duplicates. Check with the recipient first.')) {
        event.preventDefault();
    }
});
