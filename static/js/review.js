/**
 * Review Tab — Experiment data viewer
 * Loads completed experiment CSVs, plots with Chart.js, supports multi-device
 * overlay, channel selection, and CSV download.
 */
window.reviewUI = (() => {

  const CHANNELS = [
    { key: 'mass_flow',      label: 'Mass Flow',    unit: 'SLPM' },
    { key: 'vol_flow',       label: 'Vol Flow',     unit: 'LPM'  },
    { key: 'pressure',       label: 'Pressure',     unit: 'psia' },
    { key: 'temperature',    label: 'Temperature',  unit: '°C'   },
    { key: 'setpoint',       label: 'Setpoint',     unit: 'SLPM' },
    { key: 'accumulated_sl', label: 'Accum. Flow',  unit: 'SL'   },
  ];

  // Palette for multi-device lines
  const PALETTE = [
    '#58a6ff', '#f0883e', '#3fb950', '#f78166',
    '#d2a8ff', '#ffa657', '#79c0ff', '#56d364',
  ];

  let _chart       = null;
  let _currentData = null;   // {meta, device_names, data}
  let _currentFolder = null;
  let _initialized = false;

  // ── Public: called when Review tab becomes visible ─────────────────────────

  function onTabShow() {
    if (!_initialized) {
      _initialized = true;
      _loadExperimentList();
    }
  }

  // ── Experiment list ────────────────────────────────────────────────────────

  function _loadExperimentList() {
    const list = document.getElementById('reviewExpList');
    if (!list) return;
    list.innerHTML = '<div class="text-muted small px-2 py-2">Loading…</div>';

    fetch('/api/review/experiments')
      .then(r => r.json())
      .then(exps => {
        if (!exps.length) {
          list.innerHTML = '<div class="text-muted small px-2 py-3 text-center">No completed experiments found in Data/Experiments/</div>';
          return;
        }
        list.innerHTML = exps.map(e => `
          <div class="review-exp-item" onclick="reviewUI.selectExperiment('${_esc(e.folder)}')" id="rei-${_hashFolder(e.folder)}">
            <div style="flex:1;min-width:0">
              <div class="review-exp-name">${_esc(e.name)}</div>
              <div class="review-exp-meta">${_esc(e.operator || '—')} · ${_fmtDate(e.started_at)}</div>
            </div>
            <button class="btn btn-xs btn-outline-danger ms-1 flex-shrink-0"
                    title="Delete run"
                    onclick="event.stopPropagation();reviewUI.deleteRun('${_esc(e.folder)}','${_esc(e.name)}')">
              <i class="fa fa-trash"></i>
            </button>
          </div>`).join('');
      })
      .catch(() => {
        list.innerHTML = '<div class="text-danger small px-2 py-2">Failed to load experiment list.</div>';
      });
  }

  function refreshList() {
    _initialized = true;
    _loadExperimentList();
  }

  // ── Select & load experiment ───────────────────────────────────────────────

  function selectExperiment(folder) {
    _currentFolder = folder;

    // Highlight selected item
    document.querySelectorAll('.review-exp-item').forEach(el => el.classList.remove('selected'));
    const sel = document.getElementById(`rei-${_hashFolder(folder)}`);
    if (sel) sel.classList.add('selected');

    // Show loading state
    _setReviewStatus('Loading…');
    _destroyChart();

    fetch(`/api/review/experiments/${encodeURIComponent(folder)}/data`)
      .then(r => r.json())
      .then(payload => {
        if (payload.error) { _setReviewStatus(payload.error); return; }
        _currentData = payload;
        _renderControls(payload);
        _drawChart();
      })
      .catch(e => _setReviewStatus('Error loading data: ' + e));
  }

  // ── Controls ──────────────────────────────────────────────────────────────

  function _renderControls(payload) {
    const meta = payload.meta || {};
    const devices = payload.device_names || [];

    // Meta header
    const header = document.getElementById('reviewMetaHeader');
    if (header) {
      header.innerHTML = `
        <strong>${_esc(meta.name || _currentFolder)}</strong>
        <span class="text-muted ms-2 small">${_esc(meta.operator || '')}</span>
        <span class="text-muted ms-2 small">${_fmtDate(meta.started_at || meta.start_time || '')}</span>`;
    }

    // Channel selector
    const chanSel = document.getElementById('reviewChannelSel');
    if (chanSel) {
      chanSel.innerHTML = CHANNELS.map(c =>
        `<option value="${c.key}">${c.label} (${c.unit})</option>`
      ).join('');
    }

    // Device checkboxes
    const devBox = document.getElementById('reviewDeviceChecks');
    if (devBox) {
      devBox.innerHTML = devices.map((name, i) => `
        <label class="review-dev-check">
          <input type="checkbox" value="${_esc(name)}" checked
                 onchange="reviewUI.redraw()"
                 style="accent-color:${PALETTE[i % PALETTE.length]}">
          <span style="color:${PALETTE[i % PALETTE.length]}">${_esc(name)}</span>
        </label>`).join('');
    }

    document.getElementById('reviewPlaceholder')?.classList.add('d-none');
    document.getElementById('reviewChartWrap')?.classList.remove('d-none');
    document.getElementById('reviewDownloadBtn')?.classList.remove('d-none');
  }

  // ── Chart ─────────────────────────────────────────────────────────────────

  function redraw() { _drawChart(); }

  function _drawChart() {
    if (!_currentData) return;

    const chanKey = document.getElementById('reviewChannelSel')?.value || 'mass_flow';
    const chan = CHANNELS.find(c => c.key === chanKey) || CHANNELS[0];

    const checkedDevices = [...document.querySelectorAll('#reviewDeviceChecks input:checked')]
      .map(el => el.value);

    const datasets = [];
    checkedDevices.forEach((devName, i) => {
      const rows = (_currentData.data[devName] || []).filter(r => r[chanKey] !== null);
      const colorIdx = (_currentData.device_names || []).indexOf(devName);
      datasets.push({
        label: devName,
        // Convert ISO string → ms number: required for parsing:false on a time axis
        data: rows.map(r => ({ x: new Date(r.timestamp).getTime(), y: r[chanKey] })),
        borderColor: PALETTE[colorIdx % PALETTE.length],
        backgroundColor: 'transparent',
        borderWidth: 1.5,
        pointRadius: rows.length > 500 ? 0 : 2,
        tension: 0.1,
      });
    });

    _setReviewStatus('');

    const canvas = document.getElementById('reviewChart');
    if (!canvas) return;

    if (_chart) {
      _chart.data.datasets = datasets;
      _chart.options.scales.y.title.text = `${chan.label} (${chan.unit})`;
      _chart.update('none');
      return;
    }

    _chart = new Chart(canvas, {
      type: 'line',
      data: { datasets },
      options: {
        animation: false,
        responsive: true,
        maintainAspectRatio: false,
        parsing: false,
        scales: {
          x: {
            type: 'time',
            time: { tooltipFormat: 'HH:mm:ss', displayFormats: { second: 'HH:mm:ss', minute: 'HH:mm', hour: 'HH:mm' } },
            ticks: { color: '#8b949e', maxTicksLimit: 8 },
            grid: { color: 'rgba(255,255,255,0.06)' },
          },
          y: {
            ticks: { color: '#8b949e' },
            grid: { color: 'rgba(255,255,255,0.06)' },
            title: { display: true, text: `${chan.label} (${chan.unit})`, color: '#8b949e', font: { size: 11 } },
          },
        },
        plugins: {
          legend: { labels: { color: '#c9d1d9', boxWidth: 12, padding: 12 } },
          tooltip: { mode: 'index', intersect: false },
        },
        interaction: { mode: 'nearest', axis: 'x', intersect: false },
      },
    });
  }

  function _destroyChart() {
    if (_chart) { _chart.destroy(); _chart = null; }
    document.getElementById('reviewPlaceholder')?.classList.remove('d-none');
    document.getElementById('reviewChartWrap')?.classList.add('d-none');
    document.getElementById('reviewDownloadBtn')?.classList.add('d-none');
    const header = document.getElementById('reviewMetaHeader');
    if (header) header.innerHTML = '';
    const devBox = document.getElementById('reviewDeviceChecks');
    if (devBox) devBox.innerHTML = '';
  }

  // ── Delete run ────────────────────────────────────────────────────────────

  function deleteRun(folder, name) {
    if (!confirm(`Delete completed run "${name}"?\n\nThis permanently removes the recorded data and cannot be undone.`)) return;
    fetch(`/api/review/experiments/${encodeURIComponent(folder)}`, { method: 'DELETE' })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          if (_currentFolder === folder) {
            _currentFolder = null;
            _currentData = null;
            _destroyChart();
          }
          _loadExperimentList();
        } else {
          alert('Delete failed: ' + (data.error || 'Unknown error'));
        }
      })
      .catch(e => alert('Delete failed: ' + e));
  }

  // ── CSV download ──────────────────────────────────────────────────────────

  function downloadCsv() {
    if (!_currentFolder) return;
    window.location.href = `/api/review/experiments/${encodeURIComponent(_currentFolder)}/csv`;
  }

  // ── Status helper ─────────────────────────────────────────────────────────

  function _setReviewStatus(msg) {
    const el = document.getElementById('reviewStatus');
    if (!el) return;
    el.textContent = msg;
    el.classList.toggle('d-none', !msg);
  }

  // ── Utilities ─────────────────────────────────────────────────────────────

  function _esc(s) {
    return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function _fmtDate(iso) {
    if (!iso) return '—';
    try {
      const d = new Date(iso);
      return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    } catch (_) { return iso; }
  }

  function _hashFolder(f) {
    // Simple stable key for element IDs
    let h = 0;
    for (let i = 0; i < f.length; i++) h = (h * 31 + f.charCodeAt(i)) | 0;
    return Math.abs(h).toString(36);
  }

  return { onTabShow, refreshList, selectExperiment, redraw, downloadCsv, deleteRun };

})();
