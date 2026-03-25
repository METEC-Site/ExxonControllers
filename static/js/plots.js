/**
 * plots.js — Real-time Chart.js plot management
 * Manages two independent plot panels, each supporting multiple channels.
 * All device readings are pushed in here; subscribed channels are rendered.
 */

'use strict';

const plots = (() => {

  // ── Chart.js global defaults ──────────────────────────────────────────────
  Chart.defaults.color = '#8b949e';
  Chart.defaults.borderColor = '#30363d';
  Chart.defaults.font.family = "'Segoe UI', system-ui, sans-serif";
  Chart.defaults.font.size = 11;

  // ── Color palette (distinct colors for channels) ──────────────────────────
  const PALETTE = [
    '#58a6ff', '#3fb950', '#ff7b72', '#d2a8ff', '#ffa657',
    '#79c0ff', '#56d364', '#ff9492', '#e2c5ff', '#ffc680',
    '#1f6feb', '#238636', '#da3633', '#8957e5', '#b08800',
    '#388bfd', '#2ea043', '#f85149', '#bc8cff', '#d29922',
  ];

  let _colorIndex = 0;
  const _channelColors = {};  // channelKey -> color
  const _channelUnits = {};   // channelKey -> unit string

  // Known engineering units for Alicat fields
  const FIELD_UNITS = {
    mass_flow: 'SLPM', setpoint: 'SLPM',
    vol_flow: 'LPM',
    pressure: 'psia',
    temperature: '°C',
  };

  function _getColor(channelKey) {
    if (!_channelColors[channelKey]) {
      _channelColors[channelKey] = PALETTE[_colorIndex % PALETTE.length];
      _colorIndex++;
    }
    return _channelColors[channelKey];
  }

  function _registerChannelUnit(channelKey, unit) {
    if (unit) _channelUnits[channelKey] = unit;
  }

  function _unitAxisId(unit) {
    return 'y_' + unit.replace(/[^a-zA-Z0-9]/g, '_');
  }

  // Build Chart.js scales config for the given set of channel keys.
  // First unique unit gets the left axis; all others get right axes.
  function _buildScales(channelKeys) {
    const seenUnits = [];
    for (const ck of channelKeys) {
      const u = _channelUnits[ck] || 'value';
      if (!seenUnits.includes(u)) seenUnits.push(u);
    }
    if (seenUnits.length === 0) seenUnits.push('value');

    const scales = {
      x: {
        type: 'time',
        time: { tooltipFormat: 'HH:mm:ss', displayFormats: { second: 'HH:mm:ss', minute: 'HH:mm', hour: 'HH:mm' } },
        ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8, color: '#8b949e' },
        grid: { color: '#21262d' },
      },
    };
    seenUnits.forEach((unit, i) => {
      scales[_unitAxisId(unit)] = {
        position: i === 0 ? 'left' : 'right',
        ticks: { color: '#8b949e', maxTicksLimit: 6 },
        grid: i === 0 ? { color: '#21262d' } : { drawOnChartArea: false },
        title: { display: true, text: unit, color: '#8b949e', font: { size: 10 } },
      };
    });
    return scales;
  }

  // ── Channel key format: "deviceId:fieldName" ──────────────────────────────
  // e.g. "abc12345:mass_flow", "abc12345:setpoint", "periph123:ch0"

  function _makeChannelKey(deviceId, field) {
    return `${deviceId}:${field}`;
  }

  // ── Data store: ring buffer per channel ───────────────────────────────────
  const MAX_POINTS = 3600;  // 1 hour at 1 Hz

  // Flat map: channelKey -> {labels: [], data: []}
  const _dataStore = {};

  function _ensureChannel(channelKey) {
    if (!_dataStore[channelKey]) {
      _dataStore[channelKey] = { labels: [], data: [] };
    }
  }

  function _pushPoint(channelKey, timestamp, value) {
    _ensureChannel(channelKey);
    const store = _dataStore[channelKey];
    store.labels.push(new Date(timestamp));
    store.data.push(value);
    if (store.labels.length > MAX_POINTS) {
      store.labels.shift();
      store.data.shift();
    }
  }

  // ── Plot state ────────────────────────────────────────────────────────────
  const _plots = {
    1: {
      chart: null,
      channels: [],      // list of channelKey strings
      timeRangeSec: 300, // 0 = show all
    },
    2: {
      chart: null,
      channels: [],
      timeRangeSec: 300,
    },
  };

  // ── Chart creation ────────────────────────────────────────────────────────
  function _createChart(canvasId) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    return new Chart(ctx, {
      type: 'line',
      data: { datasets: [] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: {
          mode: 'index',
          intersect: false,
        },
        plugins: {
          legend: {
            display: true,
            position: 'top',
            labels: {
              boxWidth: 12,
              padding: 10,
              color: '#8b949e',
              font: { size: 11 },
            },
          },
          tooltip: {
            backgroundColor: '#1c2128',
            borderColor: '#30363d',
            borderWidth: 1,
            titleColor: '#c9d1d9',
            bodyColor: '#8b949e',
            callbacks: {
              label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(3)}`,
              title: (items) => {
                if (!items.length) return '';
                const d = items[0].parsed.x;
                if (!d) return '';
                return new Date(d).toLocaleTimeString();
              },
            },
          },
        },
        scales: _buildScales([]),
      },
    });
  }

  function _initCharts() {
    _plots[1].chart = _createChart('plot1Canvas');
    _plots[2].chart = _createChart('plot2Canvas');
  }

  // ── Build dataset from stored data ────────────────────────────────────────
  function _buildDataset(channelKey, label) {
    const color = _getColor(channelKey);
    const store = _dataStore[channelKey] || { labels: [], data: [] };
    return {
      label,
      data: store.labels.map((t, i) => ({ x: t, y: store.data[i] })),
      borderColor: color,
      backgroundColor: color + '22',
      borderWidth: 1.5,
      pointRadius: 0,
      pointHoverRadius: 4,
      tension: 0.1,
      fill: false,
    };
  }

  // ── Refresh a plot's chart with current subscriptions and data ────────────
  function _refreshPlot(plotIdx) {
    const p = _plots[plotIdx];
    if (!p.chart) return;

    const now = new Date();
    const cutoff = p.timeRangeSec > 0 ? new Date(now - p.timeRangeSec * 1000) : null;

    const datasets = p.channels.map(ck => {
      const label = _channelLabels[ck] || ck;
      const color = _getColor(ck);
      const unit = _channelUnits[ck] || 'value';
      const store = _dataStore[ck] || { labels: [], data: [] };

      let points = store.labels.map((t, i) => ({ x: t, y: store.data[i] }));
      if (cutoff) {
        points = points.filter(pt => pt.x >= cutoff);
      }
      return {
        label,
        data: points,
        borderColor: color,
        backgroundColor: color + '22',
        borderWidth: 1.5,
        pointRadius: 0,
        pointHoverRadius: 4,
        tension: 0.1,
        fill: false,
        yAxisID: _unitAxisId(unit),
      };
    });

    // Rebuild Y axes to match currently displayed channels
    p.chart.options.scales = _buildScales(p.channels);
    p.chart.data.datasets = datasets;
    p.chart.update('none');  // 'none' = no animation for performance

    // Show/hide empty state
    const emptyEl = document.getElementById(`plot${plotIdx}Empty`);
    if (emptyEl) emptyEl.style.display = p.channels.length === 0 ? 'flex' : 'none';
  }

  // ── Channel human-readable labels ─────────────────────────────────────────
  const _channelLabels = {};

  function _registerChannelLabel(channelKey, label) {
    _channelLabels[channelKey] = label;
  }

  // ── Public: ingest a readings_update event ────────────────────────────────
  function ingestReadings(readingsEvent) {
    const ts = readingsEvent.timestamp;

    // Alicat devices
    const alicat = readingsEvent.alicat || {};
    for (const [deviceId, reading] of Object.entries(alicat)) {
      for (const field of ['pressure', 'temperature', 'vol_flow', 'mass_flow', 'setpoint']) {
        if (reading[field] !== undefined && reading[field] !== null) {
          const key = _makeChannelKey(deviceId, field);
          _pushPoint(key, ts, reading[field]);
        }
      }
    }

    // Peripherals
    const peripherals = readingsEvent.peripherals || {};
    for (const [peripheralId, periph] of Object.entries(peripherals)) {
      const values = periph.values || [];
      const labels = periph.channel_labels || [];
      const unit = periph.units || 'value';
      values.forEach((val, i) => {
        if (val !== null && val !== undefined) {
          const key = _makeChannelKey(peripheralId, `ch${i}`);
          _pushPoint(key, ts, typeof val === 'boolean' ? (val ? 1 : 0) : val);
          if (labels[i]) {
            _registerChannelLabel(key, `${periph.name || peripheralId} ${labels[i]}`);
          }
          _registerChannelUnit(key, unit);
        }
      });
    }

    // Update active plots
    for (const idx of [1, 2]) {
      if (_plots[idx].channels.length > 0) {
        _refreshPlot(idx);
      }
    }
  }

  // ── Plot selection persistence ────────────────────────────────────────────
  const STORAGE_KEY = 'ec_plot_state';

  function _savePlotState() {
    try {
      const state = {};
      [1, 2].forEach(idx => {
        state[idx] = {
          channels: _plots[idx].channels.slice(),
          timeRangeSec: _plots[idx].timeRangeSec,
        };
      });
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch (e) { /* storage unavailable */ }
  }

  let _plotStateRestored = false;

  function _loadPlotState() {
    if (_plotStateRestored) return;
    _plotStateRestored = true;
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return;
      const state = JSON.parse(raw);
      [1, 2].forEach(idx => {
        const saved = state[idx];
        if (!saved) return;
        // Restore time range select
        const tr = saved.timeRangeSec;
        if (tr !== undefined) {
          _plots[idx].timeRangeSec = tr;
          const sel = document.getElementById(`plot${idx}TimeRange`);
          if (sel) sel.value = String(tr);
        }
        // Restore channels (labels register later via updateSources)
        (saved.channels || []).forEach(ck => {
          if (!_plots[idx].channels.includes(ck)) {
            _plots[idx].channels.push(ck);
            _ensureChannel(ck);
          }
        });
        _updateChannelTags(idx);
        _refreshPlot(idx);
      });
    } catch (e) { /* corrupt storage */ }
  }

  // ── Public: add a channel to a plot ──────────────────────────────────────
  function addChannel(plotIdx, channelKey, label) {
    const p = _plots[plotIdx];
    if (p.channels.includes(channelKey)) return;
    p.channels.push(channelKey);
    if (label) _registerChannelLabel(channelKey, label);
    _ensureChannel(channelKey);
    _updateChannelTags(plotIdx);
    _refreshPlot(plotIdx);
    _savePlotState();
  }

  // ── Public: remove a channel from a plot ─────────────────────────────────
  function removeChannel(plotIdx, channelKey) {
    const p = _plots[plotIdx];
    p.channels = p.channels.filter(k => k !== channelKey);
    _updateChannelTags(plotIdx);
    _refreshPlot(plotIdx);
    _savePlotState();
  }

  // ── Public: set time range ────────────────────────────────────────────────
  function setTimeRange(plotIdx, seconds) {
    _plots[plotIdx].timeRangeSec = parseInt(seconds, 10);
    _refreshPlot(plotIdx);
    _savePlotState();
  }

  // ── Public: clear plot ────────────────────────────────────────────────────
  function clearPlot(plotIdx) {
    _plots[plotIdx].channels = [];
    _updateChannelTags(plotIdx);
    _refreshPlot(plotIdx);
    _savePlotState();
  }

  // ── Channel tags UI ───────────────────────────────────────────────────────
  function _updateChannelTags(plotIdx) {
    const container = document.getElementById(`plot${plotIdx}Channels`);
    if (!container) return;
    container.innerHTML = '';
    const p = _plots[plotIdx];
    p.channels.forEach(ck => {
      const color = _getColor(ck);
      const label = _channelLabels[ck] || ck;
      const tag = document.createElement('span');
      tag.className = 'channel-tag';
      tag.style.borderColor = color;
      tag.style.color = color;
      tag.style.background = color + '20';
      tag.innerHTML = `<span>${label}</span><span class="tag-remove" onclick="plots.removeChannel(${plotIdx},'${ck}')">✕</span>`;
      container.appendChild(tag);
    });
  }

  // ── Channel selector modal ────────────────────────────────────────────────
  let _selectorTargetPlot = 1;

  // Available sources: updated when full_state / device_update arrives
  const _availableSources = {
    alicat: {},      // deviceId -> {device_name, max_flow, fields:[]}
    peripherals: {}, // peripheralId -> {name, type, channel_labels:[]}
  };

  function updateSources(allDeviceStates, allPeripheralStates) {
    _availableSources.alicat = {};
    (allDeviceStates || []).forEach(d => {
      if (d && d.device_id) {
        _availableSources.alicat[d.device_id] = {
          device_name: d.device_name || d.device_id,
          max_flow: d.max_flow,
          fields: ['mass_flow', 'vol_flow', 'setpoint', 'pressure', 'temperature'],
        };
        // Register labels
        d.fields && d.fields.forEach(f => {
          _registerChannelLabel(_makeChannelKey(d.device_id, f), `${d.device_name} ${_fieldLabel(f)}`);
        });
        ['mass_flow','vol_flow','setpoint','pressure','temperature'].forEach(f => {
          const key = _makeChannelKey(d.device_id, f);
          _registerChannelLabel(key, `${d.device_name || d.device_id} ${_fieldLabel(f)}`);
          if (FIELD_UNITS[f]) _registerChannelUnit(key, FIELD_UNITS[f]);
        });
      }
    });
    _availableSources.peripherals = {};
    (allPeripheralStates || []).forEach(p => {
      if (p && p.peripheral_id) {
        _availableSources.peripherals[p.peripheral_id] = {
          name: p.name || p.peripheral_id,
          type: p.type,
          channel_labels: p.channel_labels || [],
          units: p.units || '',
        };
        (p.channel_labels || []).forEach((lbl, i) => {
          const key = _makeChannelKey(p.peripheral_id, `ch${i}`);
          _registerChannelLabel(key, `${p.name} ${lbl}`);
          _registerChannelUnit(key, p.units || 'value');
        });
      }
    });
    // Restore saved plot state once labels are available
    _loadPlotState();
    // Re-render tags so labels are shown correctly even if channels were already restored
    [1, 2].forEach(idx => { _updateChannelTags(idx); });
  }

  function _fieldLabel(f) {
    return { mass_flow: 'Mass Flow', vol_flow: 'Vol Flow', setpoint: 'Setpoint',
             pressure: 'Pressure', temperature: 'Temp' }[f] || f;
  }

  function showChannelSelector(plotIdx) {
    _selectorTargetPlot = plotIdx;
    const title = document.getElementById('channelSelectorTitle');
    if (title) title.textContent = `Add Channel to Plot ${plotIdx}`;

    const list = document.getElementById('channelSelectorList');
    const empty = document.getElementById('channelSelectorEmpty');
    if (!list) return;

    list.innerHTML = '';
    let count = 0;

    // Alicat channels
    for (const [deviceId, info] of Object.entries(_availableSources.alicat)) {
      for (const field of info.fields) {
        const key = _makeChannelKey(deviceId, field);
        const color = _getColor(key);
        const label = `${info.device_name} — ${_fieldLabel(field)}`;
        _registerChannelLabel(key, label);

        const item = document.createElement('div');
        item.className = 'channel-selector-item';
        const alreadyIn = _plots[plotIdx].channels.includes(key);
        item.innerHTML = `
          <div class="ch-swatch" style="background:${color}"></div>
          <div style="flex:1">
            <div style="font-weight:600;color:#c9d1d9">${info.device_name}</div>
            <div style="color:#8b949e;font-size:0.75rem">${_fieldLabel(field)}</div>
          </div>
          ${alreadyIn ? '<span style="color:#3fb950;font-size:0.75rem">Added</span>' : ''}
        `;
        if (!alreadyIn) {
          item.style.cursor = 'pointer';
          item.onclick = () => {
            addChannel(plotIdx, key, label);
            bootstrap.Modal.getInstance(document.getElementById('channelSelectorModal'))?.hide();
          };
        } else {
          item.style.opacity = '0.5';
        }
        list.appendChild(item);
        count++;
      }
    }

    // Peripheral channels
    for (const [peripheralId, info] of Object.entries(_availableSources.peripherals)) {
      if (info.type === 'relay') continue; // Don't plot relay boolean states typically
      info.channel_labels.forEach((lbl, i) => {
        const key = _makeChannelKey(peripheralId, `ch${i}`);
        const color = _getColor(key);
        const label = `${info.name} ${lbl}${info.units ? ' (' + info.units + ')' : ''}`;
        _registerChannelLabel(key, label);

        const item = document.createElement('div');
        item.className = 'channel-selector-item';
        const alreadyIn = _plots[plotIdx].channels.includes(key);
        item.innerHTML = `
          <div class="ch-swatch" style="background:${color}"></div>
          <div style="flex:1">
            <div style="font-weight:600;color:#c9d1d9">${info.name}</div>
            <div style="color:#8b949e;font-size:0.75rem">${lbl}${info.units ? ' (' + info.units + ')' : ''}</div>
          </div>
          ${alreadyIn ? '<span style="color:#3fb950;font-size:0.75rem">Added</span>' : ''}
        `;
        if (!alreadyIn) {
          item.style.cursor = 'pointer';
          item.onclick = () => {
            addChannel(plotIdx, key, label);
            bootstrap.Modal.getInstance(document.getElementById('channelSelectorModal'))?.hide();
          };
        } else {
          item.style.opacity = '0.5';
        }
        list.appendChild(item);
        count++;
      });
    }

    if (empty) empty.style.display = count === 0 ? 'block' : 'none';

    const modal = new bootstrap.Modal(document.getElementById('channelSelectorModal'));
    modal.show();
  }

  // ── Load history from server (called on connect) ──────────────────────────
  async function loadHistory(deviceId) {
    try {
      const resp = await fetch(`/api/history/${deviceId}?limit=300`);
      if (!resp.ok) return;
      const history = await resp.json();
      history.forEach(reading => {
        if (!reading.timestamp) return;
        for (const field of ['pressure', 'temperature', 'vol_flow', 'mass_flow', 'setpoint']) {
          if (reading[field] !== undefined && reading[field] !== null) {
            _pushPoint(_makeChannelKey(deviceId, field), reading.timestamp, reading[field]);
          }
        }
      });
    } catch (e) {
      // Non-fatal
    }
  }

  // ── Init ──────────────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', _initCharts);

  // ── Public API ────────────────────────────────────────────────────────────
  return {
    ingestReadings,
    addChannel,
    removeChannel,
    setTimeRange,
    clearPlot,
    updateSources,
    showChannelSelector,
    loadHistory,
    getColor: _getColor,
    makeChannelKey: _makeChannelKey,
  };

})();
