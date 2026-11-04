function showToast(message, type = 'success', toastId = null) {
    const container = document.getElementById('toast-container');
    if (!container) return;

    // If a toastId is provided, remove any existing toast with that same ID (e.g. replacing "Started" with "Completed")
    if (toastId) {
        const existingToast = document.getElementById(toastId);
        if (existingToast) {
            existingToast.remove();
        }
    }

    const toast = document.createElement('div');
    if (toastId) {
        toast.id = toastId;
    }
    
    let bgColor = 'bg-emerald-950/90 border-emerald-500/30 text-emerald-300';
    let icon = '✓';
    
    if (type === 'error' || type === 'danger') {
        bgColor = 'bg-red-950/90 border-red-500/30 text-red-300';
        icon = '⚠️';
    } else if (type === 'info' || type === 'start') {
        bgColor = 'bg-blue-950/90 border-blue-500/30 text-blue-300';
        icon = '⏳';
    }
    
    toast.className = `pointer-events-auto flex items-center space-x-3 px-4 py-3 rounded-xl border shadow-2xl backdrop-blur-md transition-all duration-300 transform translate-y-2 opacity-0 ${bgColor}`;
    toast.innerHTML = `<span class="text-lg">${icon}</span><span class="text-sm font-medium">${message}</span>`;
    
    container.appendChild(toast);
    
    // Animate in
    setTimeout(() => {
        toast.classList.remove('translate-y-2', 'opacity-0');
    }, 10);
    
    // Animate out & remove after 4 seconds
    setTimeout(() => {
        if (toast.parentElement) {
            toast.classList.add('translate-y-2', 'opacity-0');
            setTimeout(() => toast.remove(), 300);
        }
    }, 4000);
}