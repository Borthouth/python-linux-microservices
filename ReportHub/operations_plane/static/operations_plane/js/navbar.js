document.addEventListener('keydown', function (event) {
    if (event.key !== 'Escape') return;
    const navigation = event.target.closest('details[data-operations-navigation][open]');
    if (!navigation) return;
    navigation.open = false;
    navigation.querySelector('summary').focus();
});

document.addEventListener('click', function (event) {
    document.querySelectorAll('details[data-operations-navigation][open]').forEach(function (navigation) {
        if (!navigation.contains(event.target)) navigation.open = false;
    });
});
