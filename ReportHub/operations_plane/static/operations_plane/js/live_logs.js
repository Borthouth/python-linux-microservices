(() => {
    const feed = document.querySelector('[data-live-feed]');
    if (!feed) return;
    const tbody = feed.querySelector('[data-live-rows]');
    const viewport = feed.querySelector('[data-feed-scroll]');
    const status = feed.querySelector('[data-feed-status]');
    const note = feed.querySelector('[data-feed-note]');
    const checked = feed.querySelector('[data-feed-checked]');
    const error = feed.querySelector('[data-feed-error]');
    const empty = feed.querySelector('[data-feed-empty]');
    const toggle = feed.querySelector('[data-pause-live]');
    const storeState = document.querySelector('[data-log-store-state]');
    const storeIndicator = document.querySelector('[data-log-store-indicator]');
    const storeLatest = document.querySelector('[data-log-store-latest]');
    const selectedSource = document.querySelector('[data-selected-source-state]');
    const parameters = new URLSearchParams(location.search);
    const limit = Number(parameters.get('limit') || 25);
    let cursor = null;
    let paused = false;
    let timer;
    let controller;
    let generation = 0;
    let retryDelay = 5000;

    function setStoreState(label, tone) {
        const colors = {
            connected: ['text-emerald-300', 'bg-emerald-400'],
            paused: ['text-amber-300', 'bg-amber-400'],
            unavailable: ['text-rose-300', 'bg-rose-400'],
        };
        storeState.textContent = label;
        storeState.className = colors[tone][0];
        storeIndicator.className = `h-2 w-2 shrink-0 rounded-full ${colors[tone][1]}`;
    }

    function pause() {
        if (paused) return;
        paused = true;
        generation += 1;
        clearTimeout(timer);
        if (controller) controller.abort();
        status.textContent = 'Paused';
        toggle.textContent = 'Resume';
        toggle.setAttribute('aria-pressed', 'true');
        setStoreState('Paused - status not refreshing', 'paused');
        window.OperationsMessages.show('Live view paused. Log collection continues.');
    }

    async function refresh() {
        if (paused || document.hidden) return;
        const currentGeneration = generation;
        controller = new AbortController();
        const query = new URLSearchParams(parameters);
        if (cursor) query.set('since', cursor);
        try {
            const response = await fetch(`${feed.dataset.endpoint}?${query}`, {
                signal: controller.signal, cache: 'no-store', headers: {'Accept': 'application/json'},
            });
            if (response.redirected) throw new Error('Your session has ended. Reload the page to sign in.');
            const result = await response.json();
            if (!response.ok) throw new Error(result.error || 'Live logs could not be loaded.');
            if (paused || document.hidden || currentGeneration !== generation) return;
            const template = document.createElement('template');
            template.innerHTML = result.html;
            const existing = new Map(Array.from(tbody.querySelectorAll('[data-log-event]')).map(
                (row) => [row.dataset.logEvent, row],
            ));
            let changed = false;
            for (const row of template.content.querySelectorAll('[data-log-event]')) {
                if (!existing.has(row.dataset.logEvent)) {
                    existing.set(row.dataset.logEvent, row);
                    changed = true;
                }
            }
            if (changed) {
                const rows = Array.from(existing.values()).sort((left, right) => {
                    const time = Date.parse(left.dataset.ingestedAt) - Date.parse(right.dataset.ingestedAt);
                    return time || left.dataset.logEvent.localeCompare(right.dataset.logEvent);
                }).slice(-limit);
                tbody.replaceChildren(...rows);
                viewport.scrollTop = viewport.scrollHeight;
            }
            if (result.cursor && (!cursor || Date.parse(result.cursor) > Date.parse(cursor))) cursor = result.cursor;
            const sourcePanel = document.querySelector('[data-live-source-status]');
            const wasOpen = sourcePanel.querySelector('details').open;
            sourcePanel.innerHTML = result.source_status;
            sourcePanel.querySelector('details').open = wasOpen;
            const count = tbody.querySelectorAll('[data-log-event]').length;
            setStoreState('Connected', 'connected');
            storeLatest.textContent = cursor
                ? `Latest matching receipt: ${new Date(cursor).toLocaleString()}`
                : 'No matching receipts in the last 24 hours.';
            selectedSource.textContent = result.selected_source_status;
            empty.classList.toggle('hidden', count !== 0);
            status.textContent = `Live (${count}/${limit})`;
            note.textContent = result.has_more
                ? `Only the latest ${limit} entries are retained here. More matches exist; use Log search for history or busy intervals.`
                : `At most ${limit} entries are retained here. This near-live view is not a gap-free audit stream.`;
            checked.textContent = `Last checked ${new Date(result.checked_at).toLocaleTimeString()}. Ordered by receipt time; displayed times are event times.`;
            error.classList.add('hidden');
            retryDelay = 5000;
        } catch (failure) {
            if (failure.name === 'AbortError') return;
            if (paused || currentGeneration !== generation) return;
            status.textContent = 'Retrying';
            setStoreState('Unavailable - live query failed', 'unavailable');
            error.textContent = failure.message || 'Live logs could not be loaded. Retrying shortly.';
            error.classList.remove('hidden');
            retryDelay = Math.min(retryDelay * 2, 60000);
        } finally {
            if (!paused && !document.hidden && currentGeneration === generation) {
                timer = setTimeout(refresh, retryDelay);
            }
        }
    }

    toggle.setAttribute('aria-pressed', 'false');
    toggle.addEventListener('click', () => {
        if (!paused) {
            pause();
        } else {
            viewport.scrollTop = viewport.scrollHeight;
            paused = false;
            generation += 1;
            toggle.textContent = 'Pause live view';
            toggle.setAttribute('aria-pressed', 'false');
            status.textContent = 'Resuming';
            window.OperationsMessages.show('Live view resumed; following the latest entries. Log collection was not interrupted.', 'success');
            refresh();
        }
    });
    viewport.addEventListener('scroll', () => {
        if (!paused && viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight > 48) pause();
    });
    document.addEventListener('operations-log-open', pause);
    document.addEventListener('visibilitychange', () => {
        generation += 1;
        clearTimeout(timer);
        if (controller) controller.abort();
        if (!paused) {
            status.textContent = document.hidden ? 'Suspended (tab hidden)' : 'Resuming';
            if (document.hidden) {
                setStoreState('Tab hidden - status not refreshing', 'paused');
            }
            if (!document.hidden) refresh();
        }
    });
    window.addEventListener('pagehide', () => {
        clearTimeout(timer);
        if (controller) controller.abort();
    });
    refresh();
})();
