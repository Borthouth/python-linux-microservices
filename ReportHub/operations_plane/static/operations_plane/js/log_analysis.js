(() => {
    const panel = document.querySelector('[data-ai-analysis]');
    if (!panel) return;
    const form = panel.querySelector('form');
    const button = form && form.querySelector('button[type="submit"]');
    const result = panel.querySelector('[data-ai-result]');
    const feedback = panel.querySelector('[data-ai-feedback]');
    let statusUrl = panel.dataset.statusUrl;
    let timer;
    let stopped = false;
    let delay = 2000;
    const active = (status) => status === 'QUEUED' || status === 'RUNNING';
    const schedule = () => {
        if (!stopped) timer = window.setTimeout(poll, delay);
    };
    const update = (data) => {
        statusUrl = data.status_url;
        result.innerHTML = data.html;
        const savedNotice = panel.querySelector('[data-ai-saved-notice]');
        if (savedNotice) savedNotice.hidden = true;
        const runs = panel.querySelector('[data-ai-runs]');
        if (runs) {
            const previous = runs.querySelector('details');
            const expanded = previous ? previous.open : true;
            runs.innerHTML = data.runs_html;
            const current = runs.querySelector('details');
            if (current) current.open = expanded;
        }
        feedback.textContent = '';
        delay = 2000;
        const running = active(data.status);
        panel.setAttribute('aria-busy', String(running));
        if (button) {
            button.disabled = running;
            button.textContent = data.status === 'COMPLETED' ? 'Run a new analysis' : 'Analyze saved results with AI';
        }
        if (running) schedule();
        else feedback.textContent = data.status === 'COMPLETED' ? 'Analysis completed and saved.' : 'Analysis failed. You can try again.';
    };
    const readResponse = async (response) => {
        if (response.redirected) throw new Error('Your session may have expired. Reload the page to sign in.');
        if (!(response.headers.get('content-type') || '').includes('application/json')) {
            throw new Error('Unexpected server response. Reload the page before trying again.');
        }
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || `Request failed (${response.status}).`);
        return data;
    };
    async function poll() {
        try {
            const response = await fetch(statusUrl, {
                headers: {Accept: 'application/json'}, cache: 'no-store',
                signal: AbortSignal.timeout(15000),
            });
            const data = await readResponse(response);
            if (!stopped) update(data);
        } catch (error) {
            if (stopped) return;
            feedback.textContent = `Status update unavailable: ${error.message} Retrying; the analysis continues in the background. You can also recover it from Search history.`;
            delay = Math.min(delay * 2, 30000);
            schedule();
        }
    }
    if (form) form.addEventListener('submit', async (event) => {
        event.preventDefault();
        window.clearTimeout(timer);
        button.disabled = true;
        feedback.textContent = 'Submitting analysis...';
        try {
            const response = await fetch(form.action, {
                method: 'POST', body: new FormData(form),
                headers: {Accept: 'application/json'},
                signal: AbortSignal.timeout(15000),
            });
            const data = await readResponse(response);
            if (!stopped) update(data);
        } catch (error) {
            if (stopped) return;
            feedback.textContent = `Could not confirm submission: ${error.message} Check Search history before retrying.`;
            button.disabled = false;
        }
    });
    window.addEventListener('pagehide', () => {
        stopped = true;
        window.clearTimeout(timer);
    });
    window.addEventListener('pageshow', (event) => {
        if (event.persisted) {
            stopped = false;
            if (statusUrl) poll();
        }
    });
    if (active(panel.dataset.status)) {
        panel.setAttribute('aria-busy', 'true');
        schedule();
    }
})();
