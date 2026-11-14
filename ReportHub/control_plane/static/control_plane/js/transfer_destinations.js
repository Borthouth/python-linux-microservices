function updateTransferAuthenticationFields() {
    const authentication = document.getElementById('id_authentication');
    if (!authentication) return;
    document.querySelectorAll('[data-transfer-authentication]').forEach(function (field) {
        const visible = field.dataset.transferAuthentication === authentication.value;
        field.closest('[data-transfer-field]').hidden = !visible;
        field.disabled = !visible;
    });
}

document.addEventListener('change', function (event) {
    if (event.target.id === 'id_authentication') updateTransferAuthenticationFields();
});
window.addEventListener('pageshow', updateTransferAuthenticationFields);
updateTransferAuthenticationFields();
