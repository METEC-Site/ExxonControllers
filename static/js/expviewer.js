/**
 * Experiment Schedule Viewer
 *
 * Renders a step-chart of setpoints vs. time for any configured experiment.
 * When an experiment is running, a live "now" marker advances each second.
 *
 * Public API (window.expViewerUI):
 *   onTabShow()              — called when the Experiment tab becomes visible
 *   handleFullState(state)   — called with every full_state event
 *   selectExperiment(id)     — load & render a specific experiment by ID
 */
window.expViewerUI = (() => {

  const PALETTE = [
    '#58a6ff', '#f0883e', '#3fb950', '#f78166',
    '#d2a8ff', '#ffa657', '#79c0ff', '#56d364',
  ];

  let _chart         = null;
  let _experimentList = [];   // from full_state.experiments (summary objects)
  let _selectedId    = null;
  let _scheduleData  = null;  // full experiment JSON from /api/experiments/<id>
  let _running       = false;
  let _startedAt     = null;  // Date object (UTC)
  let _elapsedSeconds = 0;

  // Zoom/select state
  let _isTimeMode = false;   // true when chart uses 'time' axis (running w/ _startedAt)
  let _xMin       = null;    // full data extent for clamping
  let _xMax       = null;
  let _isDragging = false;
  let _dragStartX = 0;       // kept for mouseleave cleanup compatibility

  // ── Tab show ───────────────────────────────────────────────────────────────

  function onTabShow() {
    _populateSelector();
  }

  // ── Full state ─────────────────────────────────────────────────────────────

  function handleFullState(state) {
    _experimentList = state.experiments || [];
    _populateSelector();

    const ce = state.current_experiment;
    if (ce) {
      _running        = true;
      _startedAt      = ce.started_at ? new Date(ce.started_at) : null;
      _elapsedSeconds = ce.elapsed_seconds || 0;

      // Auto-select the running experiment if we haven't already shown it.
      if (_selectedId !== ce.experiment_id) {
        selectExperiment(ce.experiment_id);
      }
      _updateRunBadge(true);
    } else {
      _running        = false;
      _startedAt      = null;
      _elapsedSeconds = 0;
      _updateRunBadge(false);
      _updateNowLine();
    }
  }

  // ── Experiment selector ────────────────────────────────────────────────────

  function _populateSelector() {
    const sel = document.getElementById('expViewerSel');
    if (!sel) return;
    sel.innerHTML =
      '<option value="">— select experiment to preview —</option>' +
      _experimentList.map(e => {
        const badge = e.status === 'running'   ? ' ● Running'
                    : e.status === 'completed' ? ' ✓' : '';
        return `<option value="${_esc(e.experiment_id)}">${_esc(e.name)}${badge}</option>`;
      }).join('');

    // Restore selection
    if (_selectedId && _experimentList.find(e => e.experiment_id === _selectedId)) {
      sel.value = _selectedId;
    }
  }

  function selectExperiment(experimentId) {
    if (!experimentId) {
      _selectedId   = null;
      _scheduleData = null;
      _destroyChart();
      document.getElementById('expViewerPlaceholder')?.classList.remove('d-none');
      document.getElementById('expViewerChartWrap')?.classList.add('d-none');
      document.getElementById('expViewerNowInfo')?.classList.add('d-none');
      document.getElementById('expViewerResetZoom')?.classList.add('d-none');
      return;
    }

    _selectedId = experimentId;
    // Keep selector in sync (e.g. when auto-selected by running experiment).
    const sel = document.getElementById('expViewerSel');
    if (sel && sel.value !== experimentId) sel.value = experimentId;

    // Show loading indicator
    const placeholder = document.getElementById('expViewerPlaceholder');
    if (placeholder) {
      placeholder.classList.remove('d-none');
      placeholder.innerHTML =
        '<i class="fa fa-spinner fa-spin text-muted mb-1"></i>' +
        '<span class="text-muted small">Loading schedule…</span>';
    }

    fetch(`/api/experiments/${encodeURIComponent(experimentId)}`)
      .then(r => r.json())
      .then(data => {
        if (!data || !data.device_schedules) {
          if (placeholder) {
            placeholder.innerHTML =
              '<span class="text-danger small">Could not load schedule data.</span>';
          }
          return;
        }
        _scheduleData = data;
        _drawChart();
      })
      .catch(() => {
        if (placeholder) {
          placeholder.classList.remove('d-none');
          placeholder.innerHTML =
            '<span class="text-danger small">Error loading experiment.</span>';
        }
      });
  }

  function _updateRunBadge(running) {
    const badge = document.getElementById('expViewerRunBadge');
    if (badge) badge.classList.toggle('d-none', !running);
  }

  // ── Chart (setpoints vs time, step interpolation) ─────────────────────────

  // In-line Chart.js plugin to draw the "now" vertical line without needing
  // chartjs-plugin-annotation.
  const _nowLinePlugin = {
    id: 'evNowLine',
    afterDatasetsDraw(chart) {
      if (!_running || _elapsedSeconds <= 0) return;
      const xScale = chart.scales.x;
      const yScale = chart.scales.y;
      if (!xScale || !yScale) return;
      const nowVal = _isTimeMode && _startedAt
        ? _startedAt.getTime() + _elapsedSeconds * 1000
        : _elapsedSeconds;
      const x = xScale.getPixelForValue(nowVal);
      if (x < xScale.left || x > xScale.right) return;

      const ctx = chart.ctx;
      ctx.save();
      ctx.strokeStyle = 'rgba(255,255,255,0.70)';
      ctx.lineWidth   = 1.5;
      ctx.setLineDash([5, 4]);
      ctx.beginPath();
      ctx.moveTo(x, yScale.top);
      ctx.lineTo(x, yScale.bottom);
      ctx.stroke();
      // Label
      ctx.font       = '10px sans-serif';
      ctx.fillStyle  = 'rgba(255,255,255,0.70)';
      ctx.textAlign  = 'left';
      ctx.fillText('now', x + 3, yScale.top + 12);
      ctx.restore();
    },
  };

  function _drawChart() {
    if (!_scheduleData) return;

    const globalStartIso = _scheduleData.global_start_iso || null;
    const newIsTimeMode = (_running && !!_startedAt) || !!globalStartIso;
    // Destroy and recreate if switching between time/linear modes
    if (_chart && newIsTimeMode !== _isTimeMode) {
      _destroyChart();
    }
    _isTimeMode = newIsTimeMode;

    // When running use actual started_at as epoch; otherwise use global_start_iso.
    const refMs = _isTimeMode
      ? (_running && _startedAt ? _startedAt.getTime() : new Date(globalStartIso).getTime())
      : 0;
    const schedules = _scheduleData.device_schedules || {};
    const deviceNames = Object.keys(schedules);

    // Compute the global max schedule time across all devices for uniform tail padding.
    const globalMaxS = deviceNames.reduce((mx, name) => {
      const steps = schedules[name].schedule || [];
      return steps.length ? Math.max(mx, steps[steps.length - 1].time) : mx;
    }, 0);
    const tailPadS = Math.max(5, globalMaxS * 0.05);   // 5% of schedule or 5 s minimum

    const allX = [];
    const datasets = deviceNames.map((name, i) => {
      const steps = (schedules[name].schedule || []).slice();
      const pts = steps.map(s => ({
        x: _isTimeMode ? (refMs + s.time * 1000) : s.time,
        y: s.setpoint,
      }));
      if (pts.length > 0) {
        const last = pts[pts.length - 1];
        const tail = _isTimeMode
          ? refMs + (globalMaxS + tailPadS) * 1000
          : globalMaxS + tailPadS;
        pts.push({ x: tail, y: last.y });
      }
      pts.forEach(p => allX.push(p.x));
      return {
        label:           name,
        data:            pts,
        borderColor:     PALETTE[i % PALETTE.length],
        backgroundColor: 'transparent',
        borderWidth:     2,
        stepped:         'before',
        pointRadius:     pts.length > 200 ? 0 : 3,
        tension:         0,
      };
    });

    _xMin = allX.length ? Math.min(...allX) : 0;
    _xMax = allX.length ? Math.max(...allX) : (_isTimeMode ? refMs + 3600000 : 3600);

    document.getElementById('expViewerPlaceholder')?.classList.add('d-none');
    const wrap = document.getElementById('expViewerChartWrap');
    if (wrap) wrap.classList.remove('d-none');
    document.getElementById('expViewerResetZoom')?.classList.remove('d-none');

    const canvas = document.getElementById('expViewerChart');
    if (!canvas) return;

    if (_chart) {
      _chart.data.datasets = datasets;
      _chart.update('none');
      return;
    }

    const xAxis = _isTimeMode ? {
      type:  'time',
      time:  {
        tooltipFormat: 'HH:mm:ss',
        displayFormats: { second: 'HH:mm:ss', minute: 'HH:mm', hour: 'HH:mm', day: 'MMM d HH:mm' },
      },
      title: { display: true, text: 'Time', color: '#8b949e', font: { size: 11 } },
      ticks: { color: '#8b949e', maxTicksLimit: 8 },
      grid:  { color: 'rgba(255,255,255,0.06)' },
    } : {
      type:  'linear',
      title: { display: true, text: 'Elapsed', color: '#8b949e', font: { size: 11 } },
      ticks: { color: '#8b949e', maxTicksLimit: 10, callback: v => _fmtSeconds(v) },
      grid:  { color: 'rgba(255,255,255,0.06)' },
    };

    _chart = new Chart(canvas, {
      type:    'line',
      plugins: [_nowLinePlugin],
      data:    { datasets },
      options: {
        animation:           false,
        responsive:          true,
        maintainAspectRatio: false,
        parsing:             false,
        scales: {
          x: xAxis,
          y: {
            beginAtZero: true,
            title: { display: true, text: 'Setpoint (SLPM)', color: '#8b949e', font: { size: 11 } },
            ticks: { color: '#8b949e' },
            grid:  { color: 'rgba(255,255,255,0.06)' },
          },
        },
        plugins: {
          legend: {
            display: deviceNames.length > 1,
            labels:  { color: '#c9d1d9', font: { size: 11 } },
          },
          tooltip: {
            callbacks: {
              title: items => {
                const v = items[0].parsed.x;
                if (_isTimeMode) {
                  return new Date(v).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
                }
                return 't = ' + _fmtSeconds(v);
              },
              label: item => `${item.dataset.label}: ${item.parsed.y} SLPM`,
            },
          },
        },
      },
    });

    canvas.style.cursor = 'crosshair';
    _setupZoomPan(canvas);
  }

  function _destroyChart() {
    if (_chart) { _chart.destroy(); _chart = null; }
  }

  function resetZoom() {
    if (!_chart || _xMin == null) return;
    _chart.options.scales.x.min = undefined;
    _chart.options.scales.x.max = undefined;
    _chart.update('none');
  }

  function _setupZoomPan(canvas) {
    const selBox = document.getElementById('expViewerSelBox');

    // --- Wheel to zoom (centered on cursor) ---
    canvas.addEventListener('wheel', (e) => {
      e.preventDefault();
      if (!_chart) return;
      const xScale   = _chart.scales.x;
      const curMin   = xScale.min;
      const curMax   = xScale.max;
      const fullRange = _xMax - _xMin;
      const range    = curMax - curMin;
      const factor   = e.deltaY < 0 ? 0.75 : 1 / 0.75;   // zoom in / out
      const newRange = range * factor;

      // If zooming out past full extent, just reset
      if (newRange >= fullRange) {
        _chart.options.scales.x.min = _xMin;
        _chart.options.scales.x.max = _xMax;
        _chart.update('none');
        return;
      }

      const rect   = canvas.getBoundingClientRect();
      const px     = e.clientX - rect.left;
      const cursor = xScale.getValueForPixel(px);
      let newMin   = cursor - (cursor - curMin) * factor;
      let newMax   = newMin + newRange;
      // Clamp — only one edge at a time to avoid double-shift
      if (newMin < _xMin) { newMin = _xMin; newMax = _xMin + newRange; }
      else if (newMax > _xMax) { newMax = _xMax; newMin = _xMax - newRange; }
      _chart.options.scales.x.min = newMin;
      _chart.options.scales.x.max = newMax;
      _chart.update('none');
    }, { passive: false });

    // --- Drag to select a region and zoom into it ---
    let _selStartPx = null;   // canvas-relative px where drag started

    canvas.addEventListener('mousedown', (e) => {
      if (!_chart || e.button !== 0) return;
      const xScale = _chart.scales.x;
      const rect   = canvas.getBoundingClientRect();
      const px     = e.clientX - rect.left;
      // Only start selection inside the plot area
      if (px < xScale.left || px > xScale.right) return;
      _isDragging   = true;
      _selStartPx   = px;
      _dragStartX   = e.clientX;
      if (selBox) {
        selBox.style.left    = `${px}px`;
        selBox.style.width   = '0px';
        selBox.style.display = 'block';
      }
      canvas.style.cursor = 'crosshair';
    });

    canvas.addEventListener('mousemove', (e) => {
      if (!_isDragging || !_chart || _selStartPx == null) return;
      const rect   = canvas.getBoundingClientRect();
      const xScale = _chart.scales.x;
      const curPx  = Math.max(xScale.left, Math.min(xScale.right, e.clientX - rect.left));
      const left   = Math.min(_selStartPx, curPx);
      const width  = Math.abs(curPx - _selStartPx);
      if (selBox) {
        selBox.style.left  = `${left}px`;
        selBox.style.width = `${width}px`;
      }
    });

    const _endDrag = (e) => {
      if (!_isDragging) return;
      _isDragging = false;
      if (selBox) selBox.style.display = 'none';
      canvas.style.cursor = 'crosshair';

      if (!_chart || _selStartPx == null) { _selStartPx = null; return; }
      const rect   = canvas.getBoundingClientRect();
      const xScale = _chart.scales.x;
      const endPx  = Math.max(xScale.left, Math.min(xScale.right, e.clientX - rect.left));
      const spanPx = Math.abs(endPx - _selStartPx);

      if (spanPx > 8) {
        // Zoom to the selected range
        const v1 = xScale.getValueForPixel(Math.min(_selStartPx, endPx));
        const v2 = xScale.getValueForPixel(Math.max(_selStartPx, endPx));
        _chart.options.scales.x.min = Math.max(_xMin, v1);
        _chart.options.scales.x.max = Math.min(_xMax, v2);
        _chart.update('none');
      }
      _selStartPx = null;
    };
    canvas.addEventListener('mouseup',    _endDrag);
    canvas.addEventListener('mouseleave', () => {
      if (_isDragging) {
        _isDragging = false;
        if (selBox) selBox.style.display = 'none';
        canvas.style.cursor = 'crosshair';
        _selStartPx = null;
      }
    });
  }

  function _updateNowLine() {
    if (_running && _startedAt) {
      _elapsedSeconds = (Date.now() - _startedAt.getTime()) / 1000;
    }
    if (_chart) _chart.update('none');

    const info = document.getElementById('expViewerNowInfo');
    if (info) {
      if (_running) {
        info.classList.remove('d-none');
        info.textContent = `Running · ${_fmtSeconds(_elapsedSeconds)} elapsed`;
      } else {
        info.classList.add('d-none');
      }
    }
  }

  // Advance the "now" line every second
  setInterval(_updateNowLine, 1000);

  // ── Utilities ──────────────────────────────────────────────────────────────

  function _fmtSeconds(s) {
    if (s == null || isNaN(s) || s < 0) return '0:00';
    const h   = Math.floor(s / 3600);
    const m   = Math.floor((s % 3600) / 60);
    const sec = Math.floor(s % 60);
    if (h > 0) {
      return `${h}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`;
    }
    return `${m}:${String(sec).padStart(2,'0')}`;
  }

  function _esc(s) {
    return String(s ?? '')
      .replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function refreshIfSelected(experimentId) {
    if (_selectedId === experimentId) selectExperiment(experimentId);
  }

  return { onTabShow, handleFullState, selectExperiment, refreshIfSelected, resetZoom };
})();
