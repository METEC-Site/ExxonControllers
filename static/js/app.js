/**
 * app.js — Main application logic
 * Manages SocketIO connection, device/peripheral rendering,
 * modal interactions, crash recovery, and UI synchronization.
 */

'use strict';

// Shared between the app IIFE (server_log handler) and the module-level
// _applyLogFilter / setLogLevel functions below.
let _logLevelFilter = 'all';

const app = (() => {

  // ── SocketIO Connection ───────────────────────────────────────────────────
  const socket = io({ transports: ['websocket', 'polling'] });
  // Expose socket globally so expUI (loaded after this file) can attach handlers
  window._appSocket = socket;

  let _devices = {};         // deviceId -> device state dict
  window._getAppDevices = () => _devices;  // expose for expUI (getter always returns current ref)
  let _peripherals = {};     // peripheralId -> peripheral state dict
  let _parsedSchedule = {};  // deviceId -> [{time, rate}] (pending load)

  // ── Sessions & Chat State ─────────────────────────────────────────────────────
  let _mySessionToken = null;   // token returned by server identifying this browser
  let _allSessions = [];         // latest sessions list
  let _chatUnreadCount = 0;     // messages received while modal was closed
  let _chatModalShowing = false;
  let _groundElevAdd  = null;   // terrain elevation (m ASL) fetched for Add Device form
  let _groundElevEdit = null;   // terrain elevation (m ASL) fetched for Edit Device form

  // ── Collapsed device cards (persisted to localStorage) ────────────────────
  const _COLLAPSE_KEY = 'ec_collapsed_devices';
  let _collapsedDevices = new Set(
    JSON.parse(localStorage.getItem(_COLLAPSE_KEY) || '[]')
  );

  // ── Connection Status ─────────────────────────────────────────────────────
  let _wsConnected = false;
  let _lastDataTime = 0;         // epoch ms of last readings_update or full_state
  let _disconnectTimer = null;   // debounce: don't show banner for brief drops

  socket.on('connect', () => {
    _wsConnected = true;
    // Cancel any pending "show disconnect banner" timer
    if (_disconnectTimer) { clearTimeout(_disconnectTimer); _disconnectTimer = null; }
    _updateSystemStatus();
    _showToast('Connected to server', 'success');
    const banner = document.getElementById('disconnectBanner');
    if (banner) banner.classList.add('d-none');
    // Reset session token — server will re-emit it after this event
    _mySessionToken = null;

    // Deterministic experiment fetch: as soon as Socket.IO confirms the
    // connection, fetch experiments via REST.  This is independent of
    // full_state delivery and guarantees experiments are populated even if
    // full_state was delayed, had an error, or arrived with empty experiments
    // due to a transient backend subsystem failure.
    if (window.expUI) window.expUI.refreshExperimentList();
  });

  socket.on('disconnect', () => {
    _wsConnected = false;
    // Only show the status text and banner after 4 s without a reconnect.
    // Brief socket.io heartbeat gaps / gevent scheduling jitter should not
    // trigger the "lost connection" banner — the reconnect fires within ~1 s.
    _disconnectTimer = setTimeout(() => {
      if (!_wsConnected) {
        _updateSystemStatus();
        _showToast('Disconnected from server — attempting reconnect...', 'danger');
        const banner = document.getElementById('disconnectBanner');
        if (banner) banner.classList.remove('d-none');
      }
    }, 4000);
  });

  // Refresh status bar every 5 s so "server disconnected" appears promptly
  // once data stops flowing, even if no socket event triggers it.
  setInterval(_updateSystemStatus, 5000);

  function _updateSystemStatus() {
    const el = document.getElementById('wsStatusText');
    if (!el) return;

    // Drive the Socket.IO connection dot independently of server/data state
    const dot = document.getElementById('wsSocketDot');
    if (dot) {
      dot.className = 'status-dot me-1 ' + (_wsConnected ? 'connected' : 'disconnected');
      dot.title = 'Socket.IO: ' + (_wsConnected ? 'connected' : 'disconnected');
    }

    // Treat as connected if the socket says so, OR if we've received data
    // within the last 10 s (covers brief Socket.IO transport drops while the
    // server's polling loop is still running and data is flowing).
    const dataFresh = (Date.now() - _lastDataTime) < 10000;
    if (!_wsConnected && !dataFresh) {
      el.textContent = 'server disconnected';
      el.className = 'status-text disconnected';
      return;
    }

    const allDevices  = Object.values(_devices).filter(d => !d.disabled);
    const allPeriphs  = Object.values(_peripherals).filter(p => !p.disabled);
    const total       = allDevices.length + allPeriphs.length;
    const downDevices = allDevices.filter(d => !d.connected).map(d => d.device_name);
    const downPeriphs = allPeriphs.filter(p => !p.connected).map(p => p.name || p.peripheral_id);
    const downNames   = [...downDevices, ...downPeriphs];
    const upCount     = total - downNames.length;

    if (downNames.length > 0) {
      el.textContent = `server active · ${upCount}/${total} online · ${downNames.length} offline`;
      el.className = 'status-text partial';
      el.title = 'Offline: ' + downNames.join(', ');
    } else {
      el.textContent = total === 0 ? 'server active' : `server active · ${total}/${total} online`;
      el.className = 'status-text connected';
      el.title = '';
    }
  }

  // ── Full State Sync ───────────────────────────────────────────────────────
  socket.on('full_state', (state) => {
    _lastDataTime = Date.now();
    const devices = state.devices || [];
    const peripherals = state.peripherals || [];

    _devices = {};
    devices.forEach(d => { _devices[d.device_id] = d; });
    _peripherals = {};
    peripherals.forEach(p => { _peripherals[p.peripheral_id] = p; });

    // If this full_state was sent on our own connect, the server embeds our
    // session token so that _mySessionToken is definite before the first render,
    // eliminating any race with a concurrent sessions_update broadcast.
    if (state.your_session_token) {
      _mySessionToken = state.your_session_token;
    }

    _renderAllDevices();
    _renderAllPeripherals();
    _updateSectionLeds();
    _renderSessions(state.sessions || []);
    _loadChatHistory(state.chat_messages || []);

    plots.updateSources(devices, peripherals);
    _updateSystemStatus();

    // Update map markers
    if (window.ecMap) window.ecMap.updateDevices(devices);

    if (state.crash_info && state.crash_info.detected) {
      _showCrashRecovery(state.crash_info);
    }

    // Delegate experiment state to expUI.
    // Use a microtask so that if full_state fires before window.expUI is assigned
    // (edge case on very fast reconnects), we still pick it up.
    Promise.resolve().then(() => {
      if (window.expUI) {
        window.expUI.handleFullState(state);
        socket.off('pre_run_check_result');
        socket.on('pre_run_check_result', window.expUI.handlePreRunCheckResult);
      }
      if (window.expViewerUI) window.expViewerUI.handleFullState(state);
    }).catch(err => {
      console.warn('[expUI] handleFullState error:', err);
    });

    // Update MQTT / NAS status badges
    if (state.mqtt) _updateMqttUI(state.mqtt);
    if (state.nas) _updateNasUI(state.nas);
    _updateFileRotationUI(state.raw_file_rotation_minutes, state.exp_file_rotation_minutes);
  });

  // ── Live Readings ─────────────────────────────────────────────────────────
  socket.on('readings_update', (data) => {
    _lastDataTime = Date.now();
    plots.ingestReadings(data);

    // Update reading values on device cards
    const alicat = data.alicat || {};
    for (const [deviceId, reading] of Object.entries(alicat)) {
      _updateDeviceReadings(deviceId, reading);
      _flashSectionLed('device', deviceId);
    }

    // Update peripheral channel values
    const peripherals = data.peripherals || {};
    for (const [peripheralId, periph] of Object.entries(peripherals)) {
      _updatePeripheralReadings(peripheralId, periph);
      _flashSectionLed('periph', peripheralId);
    }
  });

  // ── Per-Device Updates ────────────────────────────────────────────────────
  socket.on('device_update', (deviceState) => {
    if (!deviceState || !deviceState.device_id) return;
    _devices[deviceState.device_id] = deviceState;
    _renderDevice(deviceState);
    _updateSectionLeds();
    plots.updateSources(Object.values(_devices), Object.values(_peripherals));
    if (window.ecMap) window.ecMap.updateDevice(deviceState);
    _updateSystemStatus();
  });

  // ── Peripheral Updates ────────────────────────────────────────────────────
  socket.on('peripheral_update', (periphState) => {
    if (!periphState || !periphState.peripheral_id) return;
    _peripherals[periphState.peripheral_id] = periphState;
    _renderPeripheral(periphState);
    _updateSectionLeds();
    _updateSystemStatus();
  });

  // ── Sessions ──────────────────────────────────────────────────────────────
  socket.on('sessions_update', (sessions) => {
    _renderSessions(sessions);
  });

  // ── Chat & Session Events ───────────────────────────────────────────────────
  socket.on('chat_message', (msg) => {
    _appendChatMessage(msg);
    if (!_chatModalShowing) {
      _chatUnreadCount++;
      _updateChatBadge();
    }
  });

  socket.on('chat_message_deleted', ({ timestamp }) => {
    const container = document.getElementById('chatMessages');
    if (!container) return;
    const el = container.querySelector(`[data-ts="${timestamp}"]`);
    if (el) el.remove();
  });

  socket.on('chat_cleared', () => {
    const container = document.getElementById('chatMessages');
    if (container) container.innerHTML = '';
  });

  socket.on('your_session_token', (data) => {
    _mySessionToken = data.token;
    if (_allSessions.length) _renderSessions(_allSessions);
  });

  socket.on('kicked', () => {
    const overlay = document.getElementById('kickedOverlay');
    if (overlay) overlay.style.display = 'flex';
    // If user doesn't click the button, redirect automatically after 15 s
    setTimeout(() => { window.location.href = '/logout'; }, 15000);
  });

  // Set up sessions modal listeners for unread-count management
  (() => {
    const modalEl = document.getElementById('sessionsModal');
    if (!modalEl) return;
    modalEl.addEventListener('show.bs.modal', () => {
      _chatModalShowing = true;
      _chatUnreadCount = 0;
      _updateChatBadge();
    });
    modalEl.addEventListener('hidden.bs.modal', () => {
      _chatModalShowing = false;
    });
  })();

  // Wire Alt ↔ AGL sync inputs for device location forms
  (() => {
    ['add', 'edit'].forEach(ctx => {
      const altEl = document.getElementById(`${ctx}DeviceAlt`);
      const aglEl = document.getElementById(`${ctx}DeviceAgl`);
      if (altEl) altEl.addEventListener('input', () => _syncAltAgl(ctx, 'alt'));
      if (aglEl) aglEl.addEventListener('input', () => _syncAltAgl(ctx, 'agl'));
    });
  })();

  // ── Schedule Progress ─────────────────────────────────────────────────────
  socket.on('schedule_progress', (updates) => {
    updates.forEach(u => _updateScheduleProgress(u));
  });

  // ── Crash Recovery ────────────────────────────────────────────────────────
  socket.on('crash_dismissed', () => {
    const modal = bootstrap.Modal.getInstance(document.getElementById('crashRecoveryModal'));
    modal?.hide();
  });

  // ── Toast Messages ────────────────────────────────────────────────────────
  socket.on('toast', (data) => {
    _showToast(data.message, data.type || 'info');
  });

  // ── Action Results ────────────────────────────────────────────────────────
  socket.on('action_result', (result) => {
    if (!result.success && result.error) {
      _showToast(`Error: ${result.error}`, 'danger');
    } else if (result.success && result.message) {
      _showToast(result.message, 'success');
    }
  });

  // ── UI Action Broadcast (optional UI sync) ────────────────────────────────
  socket.on('ui_action_broadcast', (data) => {
    // Example: show a brief indicator of what another user did
    if (data.action === 'setpoint_changed') {
      _showToast(`${data.from} changed setpoint on ${data.payload?.device_name}`, 'info');
    }
  });

  // ── Device Rendering ──────────────────────────────────────────────────────

  function _renderAllDevices() {
    const list = document.getElementById('deviceList');
    const noMsg = document.getElementById('noDevicesMsg');
    if (!list) return;

    // Remove cards not in _devices
    const existing = Array.from(list.querySelectorAll('.device-card'));
    existing.forEach(card => {
      if (!_devices[card.dataset.deviceId]) card.remove();
    });

    const ids = Object.keys(_devices);
    if (noMsg) noMsg.style.display = ids.length === 0 ? 'block' : 'none';

    ids.forEach(id => _renderDevice(_devices[id]));
  }

  function _renderDevice(d) {
    const list = document.getElementById('deviceList');
    if (!list) return;

    let card = document.getElementById(`device-card-${d.device_id}`);
    const isNew = !card;
    if (isNew) {
      card = document.createElement('div');
      card.className = 'device-card';
      card.id = `device-card-${d.device_id}`;
      card.dataset.deviceId = d.device_id;
      list.appendChild(card);
    }

    // Update classes
    card.className = 'device-card ' + (d.logging ? 'logging' : (d.connected ? 'connected' : 'disconnected'));

    const reading = d.last_reading || {};
    const maxFlow = d.max_flow || null;  // null means not configured by user
    const massFlow = reading.mass_flow ?? 0;
    const setpoint = reading.setpoint ?? 0;
    const flowPct = maxFlow ? Math.min(100, (massFlow / maxFlow) * 100).toFixed(1) : '0';
    const _flowBarLabel = (() => {
      if (!maxFlow)
        return `<span style="color:var(--danger)">Max flow not set</span>`;
      if (d.max_flow_user != null && d.max_flow_reported != null && d.max_flow_user < d.max_flow_reported)
        return `${flowPct}% of ${maxFlow} SLPM <span title="Capped from device max ${d.max_flow_reported} SLPM" style="color:#8b949e;font-size:0.65rem;cursor:help">(cap)</span>`;
      return `${flowPct}% of ${maxFlow} SLPM`;
    })();
    const gasName = _gasName(d.gas_number);
    const collapsed = _collapsedDevices.has(d.device_id);

    const scheduleHtml = _scheduleHtml(d);

    // Alert banner for fields that couldn't be read from device and need user input
    const _alertHtml = (() => {
      if (!d.connected) return '';
      const errors = [];
      const warnings = [];
      if (!d.max_flow) errors.push('Max flow not configured');
      if (!d.serial_number && !d.expected_serial) warnings.push('Serial number unknown');
      if (!errors.length && !warnings.length) return '';
      const items = [
        ...errors.map(m => `<span class="device-alert-item device-alert-error"><i class="fa fa-circle-exclamation"></i> ${m}</span>`),
        ...warnings.map(m => `<span class="device-alert-item device-alert-warn"><i class="fa fa-triangle-exclamation"></i> ${m}</span>`),
      ].join('');
      return `<div class="device-alert">${items} <a href="#" class="device-alert-link" onclick="app.openEditDeviceModal('${d.device_id}');return false">→ Edit Device</a></div>`;
    })();

    card.innerHTML = `
      <div class="device-card-header">
        <span class="drag-handle" title="Drag to reorder"><i class="fa fa-grip-vertical"></i></span>
        <button class="btn-device-collapse" title="${collapsed ? 'Expand' : 'Collapse'}"
                onclick="app.toggleDeviceCollapse('${d.device_id}')">
          <i class="fa fa-chevron-${collapsed ? 'right' : 'down'}" id="device-chevron-${d.device_id}"></i>
        </button>
        <div style="min-width:0;flex:1">
          <div class="device-name">${_esc(d.device_name)}
            <span id="device-flow-inline-${d.device_id}"
                  class="device-flow-inline${collapsed ? '' : ' d-none'}">
              &nbsp;·&nbsp;${_fmt(reading.mass_flow)} SLPM
            </span>
          </div>
          <div class="device-meta">${_esc(d.host)}:${d.port} · ${d.device_type} · Unit ${d.unit_id}</div>
          <div class="device-meta" style="font-family:monospace;font-size:0.7rem">
            ${(() => {
              const reported = d.serial_number;
              const expected = d.expected_serial;
              if (!expected) return `SN: ${reported || '—'}`;
              if (!reported) return `SN: ${_esc(expected)} <span title="Manually entered; device did not report a serial number" style="color:#8b949e;font-size:0.75em">(manual)</span>`;
              if (String(reported).trim() !== String(expected).trim())
                return `SN: ${_esc(reported)} <span title="Expected ${_esc(expected)}" style="color:#f85149">✗ expected ${_esc(expected)}</span>`;
              return `SN: ${_esc(reported)} <span title="Serial matches" style="color:#3fb950">✓</span>`;
            })()}
          </div>
        </div>
        <div class="d-flex flex-column align-items-end gap-1 ms-2">
          ${d.disabled
            ? '<span class="device-badge" style="background:rgba(139,148,158,0.15);color:#8b949e">⊘ Disabled</span>'
            : `<span class="device-badge ${d.connected ? 'badge-connected' : 'badge-disconnected'}">${d.connected ? '● Connected' : '○ Disconnected'}</span>`
          }
          ${d.logging ? '<span class="device-badge badge-logging">● Logging</span>' : ''}
          <div class="d-flex gap-1 mt-1">
            <button class="btn btn-xs btn-outline-secondary" title="Edit" onclick="app.openEditDeviceModal('${d.device_id}')">
              <i class="fa fa-pen"></i>
            </button>
            <button class="btn btn-xs ${d.disabled ? 'btn-outline-success' : 'btn-outline-warning'}"
              title="${d.disabled ? 'Re-enable' : 'Disable'}"
              onclick="app.toggleDisableDevice('${d.device_id}', ${!!d.disabled})">
              <i class="fa ${d.disabled ? 'fa-play' : 'fa-pause'}"></i>
            </button>
            <button class="btn btn-xs btn-outline-danger" title="Remove" onclick="app.removeDevice('${d.device_id}')">
              <i class="fa fa-trash"></i>
            </button>
          </div>
        </div>
      </div>

      ${_alertHtml}

      <div id="device-body-${d.device_id}" class="${collapsed ? 'd-none' : ''}">
        <div class="readings-grid">
          <div class="reading-item">
            <div class="reading-label">Mass Flow</div>
            <div class="reading-value highlight" id="rv-mf-${d.device_id}">${_fmt(reading.mass_flow)} SLPM</div>
          </div>
          <div class="reading-item">
            <div class="reading-label">Setpoint</div>
            <div class="reading-value" id="rv-sp-${d.device_id}">${_fmt(reading.setpoint, 2)} SLPM</div>
          </div>
          <div class="reading-item">
            <div class="reading-label">Vol Flow</div>
            <div class="reading-value" id="rv-vf-${d.device_id}">${_fmt(reading.vol_flow)} LPM</div>
          </div>
          <div class="reading-item">
            <div class="reading-label">Pressure</div>
            <div class="reading-value" id="rv-pr-${d.device_id}">${_fmt(reading.pressure)} psia</div>
          </div>
          <div class="reading-item">
            <div class="reading-label">Temperature</div>
            <div class="reading-value" id="rv-tm-${d.device_id}">${_fmt(reading.temperature)} °C</div>
          </div>
          <div class="reading-item">
            <div class="reading-label">Gas</div>
            <div class="reading-value" id="rv-gs-${d.device_id}" style="font-size:0.78rem">${_esc(gasName)}</div>
          </div>
          <div class="reading-item" id="rv-accum-row-${d.device_id}" style="${d.accumulated_sl > 0 ? '' : 'display:none'}">
            <div class="reading-label">Accum. Flow</div>
            <div class="reading-value" id="rv-accum-${d.device_id}" style="color:var(--ec-accent)">${(d.accumulated_sl || 0).toFixed(3)} SL</div>
          </div>
        </div>

        <div class="flow-bar-container">
          <div class="flow-bar-fill" id="flowbar-${d.device_id}" style="width:${flowPct}%"></div>
        </div>
        <div class="d-flex justify-content-between text-muted" style="font-size:0.7rem;margin-top:-0.2rem;margin-bottom:0.4rem">
          <span>0</span>
          <span id="fb-pct-${d.device_id}">${_flowBarLabel}</span>
          <span>${maxFlow ?? '—'}</span>
        </div>

        <div class="device-controls">
          <button class="btn btn-sm ${d.logging ? 'btn-danger' : 'btn-success'}"
            onclick="app.toggleLogging('${d.device_id}', ${d.logging})" ${d.connected && !d.disabled ? '' : 'disabled'}>
            <i class="fa ${d.logging ? 'fa-stop' : 'fa-play'} me-1"></i>${d.logging ? 'Stop' : 'Start Log'}
          </button>
          <button class="btn btn-sm btn-info text-dark"
            onclick="app.openSetpointModal('${d.device_id}')" ${d.connected && !d.disabled ? '' : 'disabled'}>
            <i class="fa fa-sliders me-1"></i>Setpoint
          </button>
          <button class="btn btn-sm btn-outline-secondary"
            onclick="app.openGasModal('${d.device_id}', ${d.gas_number ?? 'null'})" ${d.connected && !d.disabled ? '' : 'disabled'}>
            <i class="fa fa-flask me-1"></i>Gas
          </button>
          <button class="btn btn-sm btn-outline-secondary"
            onclick="app.openScheduleModal('${d.device_id}')" ${!d.disabled ? '' : 'disabled'}>
            <i class="fa fa-calendar-days me-1"></i>Schedule
          </button>
        </div>

        ${scheduleHtml}
      </div>
    `;

    const noMsg = document.getElementById('noDevicesMsg');
    if (noMsg) noMsg.style.display = 'none';
  }

  function _scheduleHtml(d) {
    const s = d.schedule;
    if (!s || !s.loaded) return '';

    const running = s.running;
    const pct = 0; // will be updated via schedule_progress events
    return `
      <div class="mt-2" id="schedule-section-${d.device_id}">
        <div class="d-flex align-items-center justify-content-between mb-1">
          <span class="text-muted" style="font-size:0.72rem">
            <i class="fa fa-calendar-days me-1 text-purple"></i>
            Schedule: ${s.steps} steps
            ${running ? `— SP: ${_fmt(s.current_setpoint)} SLPM` : ''}
          </span>
          <div class="d-flex gap-1">
            ${running
              ? `<button class="btn btn-xs btn-outline-danger" onclick="app.stopSchedule('${d.device_id}')">Stop</button>`
              : `<button class="btn btn-xs btn-success" onclick="app.startSchedule('${d.device_id}')">Run</button>`
            }
          </div>
        </div>
        <div class="schedule-bar-outer">
          <div class="schedule-bar-fill" id="sched-bar-${d.device_id}" style="width:${pct}%"></div>
        </div>
        <div class="schedule-info">
          <span id="sched-elapsed-${d.device_id}">—</span>
          <span id="sched-pct-${d.device_id}">—</span>
          <span id="sched-remain-${d.device_id}">—</span>
        </div>
      </div>
    `;
  }

  function _updateDeviceReadings(deviceId, reading) {
    _setEl(`rv-mf-${deviceId}`, `${_fmt(reading.mass_flow)} SLPM`);
    _setEl(`rv-sp-${deviceId}`, `${_fmt(reading.setpoint, 2)} SLPM`);
    _setEl(`rv-vf-${deviceId}`, `${_fmt(reading.vol_flow)} LPM`);
    _setEl(`rv-pr-${deviceId}`, `${_fmt(reading.pressure)} psia`);
    _setEl(`rv-tm-${deviceId}`, `${_fmt(reading.temperature)} °C`);

    // Update collapsed inline flow display
    const inlineEl = document.getElementById(`device-flow-inline-${deviceId}`);
    if (inlineEl) inlineEl.innerHTML = `&nbsp;·&nbsp;${_fmt(reading.mass_flow)} SLPM`;

    const d = _devices[deviceId];
    const maxFlow = d && d.max_flow || null;
    const massFlow = reading.mass_flow ?? 0;
    const pct = maxFlow ? Math.min(100, (massFlow / maxFlow) * 100).toFixed(1) : '0';

    const bar = document.getElementById(`flowbar-${deviceId}`);
    if (bar) bar.style.width = pct + '%';
    const pctEl = document.getElementById(`fb-pct-${deviceId}`);
    if (pctEl) {
      let label;
      if (!maxFlow)
        label = `<span style="color:var(--danger)">Max flow not set</span>`;
      else if (d && d.max_flow_user != null && d.max_flow_reported != null && d.max_flow_user < d.max_flow_reported)
        label = `${pct}% of ${maxFlow} SLPM <span title="Capped from device max ${d.max_flow_reported} SLPM" style="color:#8b949e;font-size:0.65rem;cursor:help">(cap)</span>`;
      else
        label = `${pct}% of ${maxFlow} SLPM`;
      pctEl.innerHTML = label;
    }

    // Accumulated flow (only non-zero during experiments)
    const accumSL = reading.accumulated_sl ?? 0;
    const accumRow = document.getElementById(`rv-accum-row-${deviceId}`);
    if (accumRow) accumRow.style.display = accumSL > 0 ? '' : 'none';
    _setEl(`rv-accum-${deviceId}`, `${accumSL.toFixed(3)} SL`);
  }

  function _updateScheduleProgress(update) {
    const bar = document.getElementById(`sched-bar-${update.device_id}`);
    const elapsed = document.getElementById(`sched-elapsed-${update.device_id}`);
    const pctEl = document.getElementById(`sched-pct-${update.device_id}`);
    const remain = document.getElementById(`sched-remain-${update.device_id}`);
    if (bar) bar.style.width = update.pct.toFixed(1) + '%';
    if (elapsed) elapsed.textContent = `${_fmtDuration(update.elapsed)}`;
    if (pctEl) pctEl.textContent = `${update.pct.toFixed(1)}%`;
    if (remain) remain.textContent = `${_fmtDuration(update.total - update.elapsed)} rem`;
  }

  // ── Peripheral Rendering ──────────────────────────────────────────────────

  function _renderAllPeripherals() {
    const list = document.getElementById('peripheralList');
    const noMsg = document.getElementById('noPeripheralsMsg');
    if (!list) return;

    const existing = Array.from(list.querySelectorAll('.peripheral-card'));
    existing.forEach(card => {
      if (!_peripherals[card.dataset.peripheralId]) card.remove();
    });

    const ids = Object.keys(_peripherals);
    if (noMsg) noMsg.style.display = ids.length === 0 ? 'block' : 'none';
    ids.forEach(id => _renderPeripheral(_peripherals[id]));
  }

  function _renderPeripheral(p) {
    const list = document.getElementById('peripheralList');
    if (!list) return;

    let card = document.getElementById(`periph-card-${p.peripheral_id}`);
    const isNew = !card;
    if (isNew) {
      card = document.createElement('div');
      card.className = 'peripheral-card';
      card.id = `periph-card-${p.peripheral_id}`;
      card.dataset.peripheralId = p.peripheral_id;
      list.appendChild(card);
    }

    // connected = actually attached via Phidget callback; opened = ch.open() was called
    const isConnected = p.connected === true;
    const isDisabled  = p.disabled === true;
    card.className = 'peripheral-card' + (!isConnected && !isDisabled ? ' not-opened' : '') + (isDisabled ? ' device-disabled' : '');

    const typeLabel = {
      thermocouple: 'Thermocouple',
      relay: 'Solid State Relay',
      relay_mechanical: 'Mechanical Relay',
      pressure_vint: 'Raw Voltage Input',
    }[p.type] || p.type;

    let statusBadge = '';
    if (isDisabled) {
      statusBadge = `<span class="badge bg-secondary ms-2" style="font-size:0.68rem">Disabled</span>`;
    } else if (!isConnected) {
      statusBadge = `<span class="badge bg-warning text-dark ms-2" style="font-size:0.68rem">Disconnected</span>`;
    }

    const channelsHtml = isDisabled ? '' : _peripheralChannelsHtml(p);

    card.innerHTML = `
      <div class="d-flex justify-content-between align-items-center mb-1">
        <div class="d-flex align-items-center gap-2">
          <span class="drag-handle" title="Drag to reorder"><i class="fa fa-grip-vertical"></i></span>
          <div>
            <span class="device-name">${_esc(p.name)}</span>
            <span class="text-muted ms-2" style="font-size:0.72rem">${typeLabel}</span>
            ${p.hub_port != null ? `<span class="text-muted ms-1" style="font-size:0.7rem">· Port ${p.hub_port}</span>` : ''}
            ${statusBadge}
            ${p.error && !isDisabled ? `<span class="text-danger ms-2" style="font-size:0.72rem"><i class="fa fa-exclamation-triangle me-1"></i>${_esc(p.error)}</span>` : ''}
          </div>
        </div>
        <div class="d-flex gap-1">
          <button class="btn btn-xs ${isDisabled ? 'btn-outline-success' : 'btn-outline-warning'}"
            title="${isDisabled ? 'Re-enable peripheral' : 'Disable peripheral'}"
            onclick="app.toggleDisablePeripheral('${p.peripheral_id}', ${isDisabled})">
            <i class="fa ${isDisabled ? 'fa-play' : 'fa-pause'}"></i>
          </button>
          <button class="btn btn-xs btn-outline-secondary" title="Edit" onclick="app.openEditPeripheralModal('${p.peripheral_id}')">
            <i class="fa fa-pen"></i>
          </button>
          <button class="btn btn-xs btn-outline-danger" onclick="app.removePeripheral('${p.peripheral_id}')">
            <i class="fa fa-trash"></i>
          </button>
        </div>
      </div>
      ${channelsHtml}
    `;

    const noMsg = document.getElementById('noPeripheralsMsg');
    if (noMsg) noMsg.style.display = 'none';
  }

  function _peripheralChannelsHtml(p) {
    const labels = p.channel_labels || [];
    const values = p.values || [];
    const units = p.units || '';

    if (p.type === 'relay' || p.type === 'relay_mechanical') {
      return `<div class="channel-grid">${labels.map((lbl, i) => {
        const on = values[i] === true;
        return `<div class="channel-cell">
          <div class="ch-label">${_esc(lbl)}</div>
          <button class="relay-btn ${on ? 'on' : ''}" id="relay-btn-${p.peripheral_id}-${i}"
            onclick="app.toggleRelay('${p.peripheral_id}', ${i}, ${!on})">
            ${on ? 'ON' : 'OFF'}
          </button>
        </div>`;
      }).join('')}</div>`;
    }

    return `<div class="channel-grid" id="periph-channels-${p.peripheral_id}">${labels.map((lbl, i) => {
      const val = values[i];
      const display = val !== null && val !== undefined ? _fmt(val) + (units ? ' ' + units : '') : '—';
      return `<div class="channel-cell">
        <div class="ch-label">${_esc(lbl)}</div>
        <div class="ch-value" id="pch-${p.peripheral_id}-${i}">${display}</div>
      </div>`;
    }).join('')}</div>`;
  }

  function _updatePeripheralReadings(peripheralId, periph) {
    const values = periph.values || [];
    const units = periph.units || '';

    if (periph.type === 'relay' || periph.type === 'relay_mechanical') {
      values.forEach((val, i) => {
        const btn = document.getElementById(`relay-btn-${peripheralId}-${i}`);
        if (btn) {
          const on = val === true;
          btn.className = `relay-btn ${on ? 'on' : ''}`;
          btn.textContent = on ? 'ON' : 'OFF';
          btn.onclick = () => app.toggleRelay(peripheralId, i, !on);
        }
      });
    } else {
      values.forEach((val, i) => {
        const el = document.getElementById(`pch-${peripheralId}-${i}`);
        if (el) el.textContent = val !== null && val !== undefined ? _fmt(val) + (units ? ' ' + units : '') : '—';
      });
    }
  }

  // ── Session List ──────────────────────────────────────────────────────────

  function _renderSessions(sessions) {
    _allSessions = sessions;
    const countEl   = document.getElementById('sessionCountNum');
    const countWrap = document.getElementById('sessionCount');

    if (countEl) countEl.textContent = sessions.length;

    // Flash indicator based on session count
    if (countWrap) {
      countWrap.classList.remove('session-warn', 'session-danger');
      if (sessions.length >= 3) countWrap.classList.add('session-danger');
      else if (sessions.length >= 2) countWrap.classList.add('session-warn');
    }

    // Update modal title
    const titleEl = document.getElementById('sessionsModalTitle');
    if (titleEl) {
      titleEl.textContent = `${sessions.length} Connected Session${sessions.length !== 1 ? 's' : ''}`;
    }

    // Render modal session list
    const modalList = document.getElementById('sessionModalList');
    if (!modalList) return;

    modalList.innerHTML = sessions.map(s => {
      const dt = new Date(s.connected_at);
      const timeStr = isNaN(dt.getTime()) ? '—' : _timeAgo(dt);
      const isYou = s.session_token && s.session_token === _mySessionToken;

      const rightSide = isYou
        ? `<span class="session-you-badge">YOU</span><span class="text-muted ms-2" style="font-size:0.72rem">${timeStr}</span>`
        : `<span class="text-muted" style="font-size:0.72rem">${timeStr}</span>
           <button class="btn btn-xs btn-outline-danger ms-2" title="Kick this session"
             onclick="app.kickSession('${_esc(s.session_token)}')">
             <i class="fa fa-right-from-bracket"></i>
           </button>`;

      return `<div class="session-modal-item">
        <span class="session-dot"></span>
        <div style="flex:1;min-width:0">
          <strong>${_esc(s.username)}</strong>
          <span class="text-muted ms-1" style="font-size:0.72rem">${_esc(s.ip)}</span>
        </div>
        ${rightSide}
      </div>`;
    }).join('');
  }

  // ── Chat Helpers ──────────────────────────────────────────────────────────────────

  function _loadChatHistory(messages) {
    const container = document.getElementById('chatMessages');
    if (!container) return;
    container.innerHTML = '';
    messages.forEach(msg => _appendChatMessage(msg, true));
    container.scrollTop = container.scrollHeight;
  }

  function _appendChatMessage(msg, suppressScroll) {
    const container = document.getElementById('chatMessages');
    if (!container) return;
    const dt = new Date(msg.timestamp);
    const timeStr = isNaN(dt.getTime()) ? '' : dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    const div = document.createElement('div');
    div.className = 'chat-msg';
    div.dataset.ts = msg.timestamp || '';
    const safeTs = (msg.timestamp || '').replace(/'/g, '');
    div.innerHTML =
      `<div class="chat-meta d-flex justify-content-between align-items-center">` +
        `<span><strong>${_esc(msg.username)}</strong> · ${timeStr}</span>` +
        `<button class="btn-chat-delete" title="Delete message" onclick="app.deleteChatMessage('${safeTs}')">` +
          `<i class="fa fa-trash-can"></i>` +
        `</button>` +
      `</div>` +
      `<div>${_esc(msg.text)}</div>`;
    container.appendChild(div);
    if (!suppressScroll) container.scrollTop = container.scrollHeight;
  }

  function deleteChatMessage(timestamp) {
    socket.emit('delete_chat_message', { timestamp });
  }

  function clearChat() {
    if (!confirm('Clear the entire chat log?')) return;
    socket.emit('clear_chat');
  }

  function _updateChatBadge() {
    const badge = document.getElementById('chatUnreadBadge');
    if (!badge) return;
    if (_chatUnreadCount > 0) {
      badge.textContent = _chatUnreadCount > 99 ? '99+' : String(_chatUnreadCount);
      badge.classList.remove('d-none');
    } else {
      badge.classList.add('d-none');
    }
  }

  function sendChat() {
    const input = document.getElementById('chatInput');
    if (!input) return;
    const text = input.value.trim();
    if (!text) return;
    socket.emit('send_chat', { text });
    input.value = '';
  }

  function kickSession(token) {
    if (!token) return;
    socket.emit('kick_session', { token });
  }

  // ── Add Device ────────────────────────────────────────────────────────────

  function addDevice() {
    const host      = document.getElementById('addDeviceHost').value.trim();
    const port      = parseInt(document.getElementById('addDevicePort').value) || 502;
    const name      = document.getElementById('addDeviceName').value.trim() || host;
    const type      = document.getElementById('addDeviceType').value;
    const unitId    = parseInt(document.getElementById('addDeviceUnitId').value) || 1;
    const maxFlow   = document.getElementById('addDeviceMaxFlow').value;
    const errEl     = document.getElementById('addDeviceError');

    if (!host) { _showModalError(errEl, 'Host/IP is required'); return; }

    const lat = document.getElementById('addDeviceLat').value;
    const lon = document.getElementById('addDeviceLon').value;
    const alt = document.getElementById('addDeviceAlt').value;
    const expectedSerial = document.getElementById('addDeviceExpSerial')?.value.trim() || '';

    if (lat === '' || lon === '') { _showModalError(errEl, 'Location (lat/lon) is required'); return; }
    if (!expectedSerial) { _showModalError(errEl, 'Expected Serial # is required'); return; }

    if (errEl) errEl.classList.add('d-none');
    const addModalEl = document.getElementById('addDeviceModal');
    socket.once('action_result', (result) => {
      if (result.success) {
        bootstrap.Modal.getInstance(addModalEl)?.hide();
        _clearAddDeviceForm();
      } else if (result.error) {
        _showModalError(errEl, result.error);
      }
    });
    socket.emit('add_device', {
      host, port, device_name: name, device_type: type,
      unit_id: unitId, max_flow: maxFlow ? parseFloat(maxFlow) : null,
      expected_serial: expectedSerial || null,
      lat: lat !== '' ? parseFloat(lat) : null,
      lon: lon !== '' ? parseFloat(lon) : null,
      alt: alt !== '' ? parseFloat(alt) : null,
    });
  }

  function _clearAddDeviceForm() {
    ['addDeviceHost','addDeviceName','addDeviceMaxFlow','addDeviceLat','addDeviceLon','addDeviceAlt','addDeviceAgl','addDeviceExpSerial'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.value = '';
    });
    const hintEl = document.getElementById('addDeviceAglHint');
    if (hintEl) hintEl.textContent = '';
    _groundElevAdd = null;
  }

  // ── Collapse / Expand Device Card ────────────────────────────────────────

  function toggleDeviceCollapse(deviceId) {
    const nowCollapsed = !_collapsedDevices.has(deviceId);
    if (nowCollapsed) _collapsedDevices.add(deviceId);
    else              _collapsedDevices.delete(deviceId);
    localStorage.setItem(_COLLAPSE_KEY, JSON.stringify([..._collapsedDevices]));

    const body    = document.getElementById(`device-body-${deviceId}`);
    const chevron = document.getElementById(`device-chevron-${deviceId}`);
    const inline  = document.getElementById(`device-flow-inline-${deviceId}`);
    if (body)    body.classList.toggle('d-none', nowCollapsed);
    if (chevron) { chevron.className = `fa fa-chevron-${nowCollapsed ? 'right' : 'down'}`; }
    if (inline)  inline.classList.toggle('d-none', !nowCollapsed);
  }

  // ── Remove Device ─────────────────────────────────────────────────────────

  function removeDevice(deviceId) {
    if (!confirm(`Remove device "${_devices[deviceId]?.device_name || deviceId}"?`)) return;
    socket.emit('remove_device', { device_id: deviceId });
  }

  // ── Collapsible panel sections (Flow Controllers / Peripherals) ────────────

  function toggleSectionCollapse(sectionId) {
    const listEl   = document.getElementById(sectionId === 'devices' ? 'deviceList' : 'peripheralList');
    const chevron  = document.getElementById('sectionChevron-' + sectionId);
    const ledStrip = document.getElementById(sectionId === 'devices' ? 'deviceLedStrip' : 'peripheralLedStrip');
    if (!listEl) return;
    const collapsed = listEl.classList.toggle('d-none');
    if (chevron)   chevron.className = `fa fa-chevron-${collapsed ? 'right' : 'down'} me-1`;
    if (ledStrip)  ledStrip.classList.toggle('d-none', !collapsed);
    try { localStorage.setItem('section-collapsed-' + sectionId, collapsed ? '1' : '0'); } catch (_e) {}
  }

  document.addEventListener('DOMContentLoaded', () => {
    ['devices', 'peripherals'].forEach(sid => {
      try {
        const val      = localStorage.getItem('section-collapsed-' + sid);
        const listEl   = document.getElementById(sid === 'devices' ? 'deviceList' : 'peripheralList');
        const chevron  = document.getElementById('sectionChevron-' + sid);
        const ledStrip = document.getElementById(sid === 'devices' ? 'deviceLedStrip' : 'peripheralLedStrip');
        if (val === '1' && listEl) {
          listEl.classList.add('d-none');
          if (chevron)   chevron.className = 'fa fa-chevron-right me-1';
          if (ledStrip)  ledStrip.classList.remove('d-none');
        }
      } catch (_e) {}
    });
  });

  // ── Collapse All / Expand All ─────────────────────────────────────────────

  function collapseAllDevices() {
    Object.keys(_devices).forEach(id => {
      if (!_collapsedDevices.has(id)) toggleDeviceCollapse(id);
    });
  }

  function expandAllDevices() {
    Object.keys(_devices).forEach(id => {
      if (_collapsedDevices.has(id)) toggleDeviceCollapse(id);
    });
  }

  // ── Section LED Strip ─────────────────────────────────────────────────────

  function _updateSectionLeds() {
    // Devices
    const deviceStrip = document.getElementById('deviceLedStrip');
    if (deviceStrip) {
      Object.entries(_devices).forEach(([id, d]) => {
        let led = deviceStrip.querySelector(`[data-led-id="${id}"]`);
        if (!led) {
          led = document.createElement('span');
          led.className = 'section-led';
          led.dataset.ledId = id;
          deviceStrip.appendChild(led);
        }
        led.title = d.device_name || id;
        led.className = `section-led ${d.connected ? 'led-connected' : 'led-disconnected'}`;
      });
      // Remove stale LEDs
      deviceStrip.querySelectorAll('.section-led').forEach(el => {
        if (!_devices[el.dataset.ledId]) el.remove();
      });
    }
    // Peripherals
    const periphStrip = document.getElementById('peripheralLedStrip');
    if (periphStrip) {
      Object.entries(_peripherals).forEach(([id, p]) => {
        let led = periphStrip.querySelector(`[data-led-id="${id}"]`);
        if (!led) {
          led = document.createElement('span');
          led.className = 'section-led';
          led.dataset.ledId = id;
          periphStrip.appendChild(led);
        }
        led.title = p.name || id;
        led.className = `section-led ${p.connected ? 'led-connected' : 'led-disconnected'}`;
      });
      periphStrip.querySelectorAll('.section-led').forEach(el => {
        if (!_peripherals[el.dataset.ledId]) el.remove();
      });
    }
  }

  function _flashSectionLed(type, id) {
    const stripId = type === 'device' ? 'deviceLedStrip' : 'peripheralLedStrip';
    const strip   = document.getElementById(stripId);
    if (!strip || strip.classList.contains('d-none')) return;
    const led = strip.querySelector(`[data-led-id="${id}"]`);
    if (!led) return;
    led.classList.remove('led-pulse');
    // Reflow to restart animation
    void led.offsetWidth;
    led.classList.add('led-pulse');
    setTimeout(() => led.classList.remove('led-pulse'), 400);
  }

  // ── Geotag map picker ────────────────────────────────────────────────────

  async function _fetchGroundElev(lat, lon) {
    try {
      const ctrl = new AbortController();
      const tid  = setTimeout(() => ctrl.abort(), 5000);
      const resp = await fetch('https://api.open-elevation.com/api/v1/lookup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ locations: [{ latitude: lat, longitude: lon }] }),
        signal: ctrl.signal,
      });
      clearTimeout(tid);
      if (!resp.ok) return null;
      const data = await resp.json();
      return data.results?.[0]?.elevation ?? null;
    } catch {
      return null;
    }
  }

  function _syncAltAgl(ctx, changed) {
    const groundElev = ctx === 'edit' ? _groundElevEdit : _groundElevAdd;
    const altEl  = document.getElementById(`${ctx}DeviceAlt`);
    const aglEl  = document.getElementById(`${ctx}DeviceAgl`);
    const hintEl = document.getElementById(`${ctx}DeviceAglHint`);
    if (!altEl || !aglEl) return;
    if (groundElev == null) {
      if (hintEl && changed === 'agl' && aglEl.value !== '')
        hintEl.textContent = 'Use “Pick on Map” to enable AGL→Alt sync';
      return;
    }
    if (hintEl) hintEl.textContent = `Ground: ${groundElev.toFixed(1)} m ASL`;
    if (changed === 'alt') {
      const alt = parseFloat(altEl.value);
      aglEl.value = isNaN(alt) ? '' : (alt - groundElev).toFixed(1);
    } else {
      const agl = parseFloat(aglEl.value);
      altEl.value = isNaN(agl) ? '' : (groundElev + agl).toFixed(1);
    }
  }

  function pickGeoLocation(context) {
    const modalId = context === 'edit' ? 'editDeviceModal' : 'addDeviceModal';
    const latId   = context === 'edit' ? 'editDeviceLat'   : 'addDeviceLat';
    const lonId   = context === 'edit' ? 'editDeviceLon'   : 'addDeviceLon';
    const altId   = context === 'edit' ? 'editDeviceAlt'   : 'addDeviceAlt';
    const aglId   = context === 'edit' ? 'editDeviceAgl'   : 'addDeviceAgl';
    const hintId  = context === 'edit' ? 'editDeviceAglHint' : 'addDeviceAglHint';

    // Hide the current modal so the map is visible
    const modalEl = document.getElementById(modalId);
    const bsModal = bootstrap.Modal.getInstance(modalEl) || new bootstrap.Modal(modalEl);
    bsModal.hide();

    // 'map' is the correct tab identifier (not 'mapView')
    window.switchMainTab('map');

    if (!window.ecMap) {
      console.warn('[pickGeoLocation] ecMap not ready');
      return;
    }

    window.ecMap.startGeoPick(async (lat, lon) => {
      document.getElementById(latId).value = lat;
      document.getElementById(lonId).value = lon;

      // Fetch terrain elevation and populate Alt + AGL fields
      const hintEl = document.getElementById(hintId);
      const altEl  = document.getElementById(altId);
      const aglEl  = document.getElementById(aglId);
      if (hintEl) hintEl.textContent = 'Fetching elevation…';
      const elev = await _fetchGroundElev(lat, lon);
      if (context === 'edit') _groundElevEdit = elev;
      else                    _groundElevAdd  = elev;
      if (elev != null) {
        if (altEl) altEl.value = elev.toFixed(1);
        if (aglEl) aglEl.value = '0';
        if (hintEl) hintEl.textContent = `Ground: ${elev.toFixed(1)} m ASL`;
      } else {
        if (hintEl) hintEl.textContent = '(elevation unavailable)';
      }

      bsModal.show();
    });
  }

  // ── Edit Device ───────────────────────────────────────────────────────────

  function openEditDeviceModal(deviceId) {
    const d = _devices[deviceId];
    if (!d) return;
    document.getElementById('editDeviceId').value = deviceId;
    document.getElementById('editDeviceName').value = d.device_name || '';
    document.getElementById('editDeviceHost').value = d.host || '';
    document.getElementById('editDevicePort').value = d.port || 502;
    document.getElementById('editDeviceUnit').value = d.unit_id || 1;
    document.getElementById('editDeviceType').value = d.device_type || 'MCP';
    document.getElementById('editDeviceMaxFlow').value = d.max_flow_user != null ? d.max_flow_user : '';
    const reportedHint = document.getElementById('editDeviceMaxFlowHint');
    if (reportedHint) {
      if (d.max_flow_reported != null)
        reportedHint.textContent = `Device reports ${d.max_flow_reported} SLPM`;
      else
        reportedHint.textContent = 'Device max not yet read';
    }
    document.getElementById('editDeviceLat').value = d.lat != null ? d.lat : '';
    document.getElementById('editDeviceAlt').value = d.alt != null ? d.alt : '';
    document.getElementById('editDeviceLon').value = d.lon != null ? d.lon : '';
    // Reset ground elevation — user can re-pick from map to get updated ground level
    _groundElevEdit = null;
    const _eAglEl   = document.getElementById('editDeviceAgl');
    const _eAglHint = document.getElementById('editDeviceAglHint');
    if (_eAglEl)   _eAglEl.value = '';
    if (_eAglHint) _eAglHint.textContent = '';
    document.getElementById('editDeviceExpSerial').value = d.expected_serial || '';
    document.getElementById('editDeviceError').classList.add('d-none');
    new bootstrap.Modal(document.getElementById('editDeviceModal')).show();
  }

  function saveEditDevice() {
    const deviceId = document.getElementById('editDeviceId').value;
    const host = document.getElementById('editDeviceHost').value.trim();
    const editLat = document.getElementById('editDeviceLat').value;
    const editLon = document.getElementById('editDeviceLon').value;
    const editAlt = document.getElementById('editDeviceAlt').value;
    const editExpSerial = document.getElementById('editDeviceExpSerial')?.value.trim() || '';
    const errEl = document.getElementById('editDeviceError');

    if (!host) { _showModalError(errEl, 'Host/IP is required'); return; }
    if (editLat === '' || editLon === '') { _showModalError(errEl, 'Location (lat/lon) is required'); return; }
    if (!editExpSerial) { _showModalError(errEl, 'Expected Serial # is required'); return; }

    errEl.classList.add('d-none');
    const modalEl = document.getElementById('editDeviceModal');
    socket.once('action_result', (result) => {
      if (result.success) {
        bootstrap.Modal.getInstance(modalEl)?.hide();
      } else if (result.error) {
        _showModalError(errEl, result.error);
      }
    });
    socket.emit('edit_device', {
      device_id: deviceId,
      device_name: document.getElementById('editDeviceName').value.trim(),
      host,
      port: parseInt(document.getElementById('editDevicePort').value) || 502,
      unit_id: parseInt(document.getElementById('editDeviceUnit').value) || 1,
      device_type: document.getElementById('editDeviceType').value,
      max_flow: document.getElementById('editDeviceMaxFlow').value || null,
      lat: editLat !== '' ? parseFloat(editLat) : '',
      lon: editLon !== '' ? parseFloat(editLon) : '',
      alt: editAlt !== '' ? parseFloat(editAlt) : '',
      expected_serial: editExpSerial,
    });
  }

  // ── Edit Peripheral ───────────────────────────────────────────────────────

  function openEditPeripheralModal(peripheralId) {
    const p = _peripherals[peripheralId];
    if (!p) return;

    document.getElementById('editPeriphId').value = peripheralId;
    document.getElementById('editPeriphType').value = p.type || '';
    document.getElementById('editPeriphName').value = p.name || '';
    document.getElementById('editPeriphSerial').value = p.hub_serial != null ? p.hub_serial : '';
    document.getElementById('editPeriphServerHost').value = p.server_hostname || '';
    document.getElementById('editPeriphServerPort').value = p.server_port || 5661;
    document.getElementById('editPeriphServerPass').value = p.server_password || '';

    // Common hub port (all types)
    const hubPortCommonEl = document.getElementById('editPeriphHubPortCommon');
    if (hubPortCommonEl) hubPortCommonEl.value = p.hub_port != null ? p.hub_port : '';

    const isPressure = p.type === 'pressure_vint';
    document.getElementById('editPeriphPressureFields').classList.toggle('d-none', !isPressure);

    if (isPressure) {
      document.getElementById('editPeriphUnits').value = p.units || 'psia';
      const cal = p.calibration || [1.0, 0.0];
      document.getElementById('editPeriphCalScale').value = cal[0] ?? 1.0;
      document.getElementById('editPeriphCalOffset').value = cal[1] ?? 0.0;
    }

    // Build channel label inputs dynamically
    const grid = document.getElementById('editPeriphLabelsGrid');
    grid.innerHTML = '';
    const labels = p.channel_labels || [];
    const count = isPressure ? 1 : 4;
    const defaults = isPressure ? ['Pressure'] : ['CH0', 'CH1', 'CH2', 'CH3'];
    for (let i = 0; i < count; i++) {
      const colClass = isPressure ? 'col-12' : 'col-6';
      grid.innerHTML += `<div class="${colClass}">
        <label class="form-label small text-muted mb-1">Channel ${i}</label>
        <input type="text" class="form-control ec-input" id="editPeriphLabel${i}"
               value="${_esc(labels[i] || '')}" placeholder="${defaults[i]}">
      </div>`;
    }

    document.getElementById('editPeriphError').classList.add('d-none');
    new bootstrap.Modal(document.getElementById('editPeripheralModal')).show();
  }

  function saveEditPeripheral() {
    const peripheralId = document.getElementById('editPeriphId').value;
    const type = document.getElementById('editPeriphType').value;
    const name = document.getElementById('editPeriphName').value.trim();
    if (!name) {
      document.getElementById('editPeriphError').textContent = 'Name is required.';
      document.getElementById('editPeriphError').classList.remove('d-none');
      return;
    }

    const isPressure = type === 'pressure_vint';
    const count = isPressure ? 1 : 4;
    const channel_labels = Array.from({ length: count }, (_, i) => {
      const el = document.getElementById(`editPeriphLabel${i}`);
      return el ? el.value.trim() : '';
    });

    const serialRaw = document.getElementById('editPeriphSerial').value.trim();
    const hubPortRaw = document.getElementById('editPeriphHubPortCommon')?.value.trim();
    const payload = {
      peripheral_id: peripheralId,
      name,
      hub_serial: serialRaw ? parseInt(serialRaw) : null,
      hub_port: hubPortRaw !== '' ? parseInt(hubPortRaw) : null,
      server_hostname: document.getElementById('editPeriphServerHost').value.trim() || null,
      server_port: parseInt(document.getElementById('editPeriphServerPort').value) || 5661,
      server_password: document.getElementById('editPeriphServerPass').value,
      channel_labels,
    };

    if (isPressure) {
      payload.units = document.getElementById('editPeriphUnits').value || 'psia';
      payload.calibration = [
        parseFloat(document.getElementById('editPeriphCalScale').value ?? 1),
        parseFloat(document.getElementById('editPeriphCalOffset').value ?? 0),
      ];
    }

    socket.emit('edit_peripheral', payload);
    bootstrap.Modal.getInstance(document.getElementById('editPeripheralModal'))?.hide();
  }

  // ── Disable / Re-enable Device ────────────────────────────────────────────

  function toggleDisableDevice(deviceId, currentlyDisabled) {
    socket.emit('disable_device', { device_id: deviceId, disabled: !currentlyDisabled });
  }

  function toggleDisablePeripheral(peripheralId, currentlyDisabled) {
    socket.emit('disable_peripheral', { peripheral_id: peripheralId, disabled: !currentlyDisabled });
  }

  // ── Start/Stop Logging ────────────────────────────────────────────────────

  function toggleLogging(deviceId, currentlyLogging) {
    if (currentlyLogging) {
      socket.emit('stop_device', { device_id: deviceId });
    } else {
      socket.emit('start_device', { device_id: deviceId });
    }
  }

  // ── Setpoint Modal ────────────────────────────────────────────────────────

  function openSetpointModal(deviceId) {
    const d = _devices[deviceId];
    if (!d) return;
    document.getElementById('setpointDeviceId').value = deviceId;
    document.getElementById('setpointDeviceName').textContent = d.device_name || deviceId;
    const maxFlow = d.max_flow || 100;
    document.getElementById('setpointSlider').max = maxFlow;
    document.getElementById('setpointMax').textContent = maxFlow + ' SLPM';
    const currentSP = d.last_reading?.setpoint ?? 0;
    document.getElementById('setpointValue').value = currentSP.toFixed(2);
    document.getElementById('setpointSlider').value = currentSP;
    document.getElementById('setpointError').classList.add('d-none');
    new bootstrap.Modal(document.getElementById('setpointModal')).show();
  }

  function applySetpoint() {
    const deviceId = document.getElementById('setpointDeviceId').value;
    const val = parseFloat(document.getElementById('setpointValue').value);
    const errEl = document.getElementById('setpointError');

    if (isNaN(val) || val < 0) { _showModalError(errEl, 'Enter a valid non-negative flow rate'); return; }
    const d = _devices[deviceId];
    if (d?.max_flow && val > d.max_flow) {
      _showModalError(errEl, `Exceeds max flow of ${d.max_flow} SLPM`);
      return;
    }
    socket.emit('set_setpoint', { device_id: deviceId, setpoint: val });
    socket.emit('ui_action', { action: 'setpoint_changed', payload: { device_name: d?.device_name, setpoint: val }});
    document.activeElement?.blur();
    bootstrap.Modal.getInstance(document.getElementById('setpointModal'))?.hide();
  }

  // ── Gas Modal ─────────────────────────────────────────────────────────────

  function openGasModal(deviceId, currentGas) {
    document.getElementById('gasDeviceId').value = deviceId;
    const sel = document.getElementById('gasSelect');
    if (sel && currentGas !== null && currentGas !== undefined) {
      sel.value = String(currentGas);
    }
    document.getElementById('gasCustom').value = '';
    new bootstrap.Modal(document.getElementById('gasModal')).show();
  }

  function applyGas() {
    const deviceId = document.getElementById('gasDeviceId').value;
    const customVal = document.getElementById('gasCustom').value.trim();
    const gasNumber = customVal !== '' ? parseInt(customVal) : parseInt(document.getElementById('gasSelect').value);
    if (isNaN(gasNumber)) { _showToast('Invalid gas number', 'danger'); return; }
    socket.emit('set_gas', { device_id: deviceId, gas_number: gasNumber });
    bootstrap.Modal.getInstance(document.getElementById('gasModal'))?.hide();
  }

  // ── Schedule Modal ────────────────────────────────────────────────────────

  function openScheduleModal(deviceId) {
    document.getElementById('scheduleDeviceId').value = deviceId;
    document.getElementById('scheduleFile').value = '';
    document.getElementById('schedulePreview').classList.add('d-none');
    document.getElementById('scheduleError').classList.add('d-none');
    document.getElementById('loadScheduleBtn').disabled = true;
    delete _parsedSchedule[deviceId];
    new bootstrap.Modal(document.getElementById('scheduleModal')).show();
  }

  // File input handler — parse schedule file
  document.addEventListener('DOMContentLoaded', () => {
    const fileInput = document.getElementById('scheduleFile');
    if (fileInput) {
      fileInput.addEventListener('change', async () => {
        const deviceId = document.getElementById('scheduleDeviceId').value;
        const file = fileInput.files[0];
        if (!file) return;

        const formData = new FormData();
        formData.append('file', file);
        formData.append('device_id', deviceId);

        try {
          const resp = await fetch('/api/upload_schedule', { method: 'POST', body: formData });
          const data = await resp.json();
          const previewEl = document.getElementById('schedulePreview');
          const errEl = document.getElementById('scheduleError');
          const loadBtn = document.getElementById('loadScheduleBtn');

          if (data.error) {
            _showModalError(errEl, data.error);
            previewEl.classList.add('d-none');
            loadBtn.disabled = true;
            return;
          }

          errEl.classList.add('d-none');
          _parsedSchedule[deviceId] = data.schedule;
          document.getElementById('scheduleStepCount').textContent = `${data.count} steps`;

          // Build preview table
          const table = document.getElementById('schedulePreviewTable');
          const preview = data.schedule.slice(0, 20);
          table.innerHTML = preview.map(s => `t=${_fmtDuration(s.time)}: ${s.rate} SLPM`).join('\n')
            + (data.schedule.length > 20 ? `\n... and ${data.schedule.length - 20} more` : '');

          previewEl.classList.remove('d-none');
          loadBtn.disabled = false;
        } catch (e) {
          _showModalError(document.getElementById('scheduleError'), 'Upload failed: ' + e.message);
        }
      });
    }
  });

  function loadSchedule() {
    const deviceId = document.getElementById('scheduleDeviceId').value;
    const schedule = _parsedSchedule[deviceId];
    if (!schedule) { _showToast('No schedule parsed', 'danger'); return; }
    socket.emit('load_schedule', { device_id: deviceId, schedule });
    bootstrap.Modal.getInstance(document.getElementById('scheduleModal'))?.hide();
  }

  function startSchedule(deviceId) {
    socket.emit('start_schedule', { device_id: deviceId });
  }

  function stopSchedule(deviceId) {
    socket.emit('stop_schedule', { device_id: deviceId });
  }

  // ── Peripheral ────────────────────────────────────────────────────────────

  function updatePeriphFields() {
    const type = document.getElementById('addPeriphType').value;
    const pressureFields = document.getElementById('pressureCalFields');
    if (pressureFields) {
      pressureFields.classList.toggle('d-none', type !== 'pressure_vint');
    }
  }

  function addPeripheral() {
    const name = document.getElementById('addPeriphName').value.trim();
    const type = document.getElementById('addPeriphType').value;
    const serialRaw = document.getElementById('addPeriphSerial').value.trim();
    const errEl = document.getElementById('addPeriphError');

    if (!name) { _showModalError(errEl, 'Name is required'); return; }

    const serverHost = document.getElementById('addPeriphServerHost').value.trim();
    const serverPort = parseInt(document.getElementById('addPeriphServerPort').value) || 5661;
    const serverPass = document.getElementById('addPeriphServerPass').value;

    const config = {
      name, type,
      hub_serial: serialRaw ? parseInt(serialRaw) : null,
      server_hostname: serverHost || null,
      server_port: serverPort,
      server_password: serverPass,
    };

    // Hub port — used by all types (optional; restricts to a specific VINT port on the hub)
    const hubPortRaw = document.getElementById('addPeriphHubPortCommon')?.value.trim();
    if (hubPortRaw !== '') config.hub_port = parseInt(hubPortRaw);

    if (type === 'pressure_vint') {
      // For pressure, hub_port is mandatory (already set above or override)
      config.units = document.getElementById('addPeriphUnits')?.value || 'psia';
      const scale = parseFloat(document.getElementById('calScale0')?.value ?? 1);
      const offset = parseFloat(document.getElementById('calOffset0')?.value ?? 0);
      config.calibration = [scale, offset];
    }

    socket.emit('add_peripheral', config);
    bootstrap.Modal.getInstance(document.getElementById('addPeripheralModal'))?.hide();
    errEl?.classList.add('d-none');
  }

  function removePeripheral(peripheralId) {
    const p = _peripherals[peripheralId];
    if (!confirm(`Remove peripheral "${p?.name || peripheralId}"?`)) return;
    socket.emit('remove_peripheral', { peripheral_id: peripheralId });
  }

  function toggleRelay(peripheralId, channel, newState) {
    socket.emit('set_relay', { peripheral_id: peripheralId, channel, state: newState });
  }

  // ── Crash Recovery ────────────────────────────────────────────────────────

  function _showCrashRecovery(crashInfo) {
    const modal = document.getElementById('crashRecoveryModal');
    if (!modal) return;

    _setEl('crashLastHeartbeat', crashInfo.crash_time || '—');
    _setEl('crashDowntime', crashInfo.downtime_human || '—');

    const deviceListEl = document.getElementById('crashDeviceList');
    if (deviceListEl) {
      const runningState = crashInfo.running_state || {};
      const entries = Object.values(runningState).filter(Boolean);
      if (entries.length === 0) {
        deviceListEl.innerHTML = '<span class="text-muted small">No active experiments were running.</span>';
      } else {
        deviceListEl.innerHTML = entries.map(e =>
          `<div class="session-item mb-1">
            <span class="session-dot"></span>
            <strong>${_esc(e.device_name || '?')}</strong>
            ${e.schedule_running ? '— Schedule was running' : ''}
            ${e.last_setpoint ? `— Setpoint: ${_fmt(e.last_setpoint)} SLPM` : ''}
          </div>`
        ).join('');
      }
    }

    new bootstrap.Modal(modal, { backdrop: 'static', keyboard: false }).show();
  }

  function crashRecoveryResponse(resume) {
    socket.emit('crash_recovery_response', { resume });
  }

  // ── Password Settings ─────────────────────────────────────────────────────

  function updatePassword() {
    const np = document.getElementById('newPassword').value;
    const cp = document.getElementById('confirmPassword').value;
    const msgEl = document.getElementById('settingsMsg');

    if (np !== cp) {
      _showModalMsg(msgEl, 'Passwords do not match', 'danger');
      return;
    }
    if (np.length < 4) {
      _showModalMsg(msgEl, 'Password must be at least 4 characters', 'danger');
      return;
    }
    socket.emit('update_password', { new_password: np });
    _showModalMsg(msgEl, 'Password updated. Please log in again.', 'success');
    document.getElementById('newPassword').value = '';
    document.getElementById('confirmPassword').value = '';
  }

  // ── MQTT Settings ────────────────────────────────────────────────────────

  function _updateMqttUI(status) {
    const badge = document.getElementById('mqttStatusBadge');
    if (!badge) return;
    if (status.connected) {
      badge.textContent = `Connected · ${status.host}:${status.port}`;
      badge.className = 'mqtt-badge connected ms-2';
    } else {
      badge.textContent = 'Disconnected';
      badge.className = 'mqtt-badge disconnected ms-2';
    }
    // Pre-fill fields if we have a saved config
    if (status.host) {
      const h = document.getElementById('mqttHost');
      const p = document.getElementById('mqttPort');
      const x = document.getElementById('mqttPrefix');
      if (h && !h.value) h.value = status.host;
      if (p && !p.value) p.value = status.port;
      if (x && !x.value) x.value = status.prefix || 'ec';
    }
  }

  function mqttConnect() {
    const host   = document.getElementById('mqttHost').value.trim();
    const port   = parseInt(document.getElementById('mqttPort').value) || 1883;
    const prefix = document.getElementById('mqttPrefix').value.trim() || 'ec';
    const msgEl  = document.getElementById('mqttMsg');
    if (!host) { _showModalMsg(msgEl, 'Broker host is required', 'danger'); return; }
    _showModalMsg(msgEl, 'Connecting…', 'info');
    socket.emit('mqtt_connect', { host, port, prefix });
  }

  function mqttDisconnect() {
    socket.emit('mqtt_disconnect', {});
  }

  socket.on('mqtt_status', (status) => {
    _updateMqttUI(status);
    const msgEl = document.getElementById('mqttMsg');
    if (msgEl) {
      if (status.connected) {
        _showModalMsg(msgEl, `Connected to ${status.host}:${status.port}`, 'success');
      } else if (msgEl.textContent === 'Connecting…') {
        _showModalMsg(msgEl, 'Connection failed — check broker address and port', 'danger');
      }
    }
  });

  // ── NAS Settings ──────────────────────────────────────────────────────────

  function _updateNasUI(status) {
    const badge = document.getElementById('nasStatusBadge');
    if (!badge) return;
    if (status.enabled && status.accessible === true) {
      badge.textContent = 'Enabled — Online';
      badge.className = 'mqtt-badge connected ms-2';
    } else if (status.enabled && status.accessible === false) {
      badge.textContent = 'Enabled — Offline';
      badge.className = 'mqtt-badge disconnected ms-2';
    } else if (status.enabled) {
      badge.textContent = 'Enabled — Checking…';
      badge.className = 'mqtt-badge ms-2';
    } else {
      badge.textContent = 'Disabled';
      badge.className = 'mqtt-badge disconnected ms-2';
    }
    if (status.path) {
      const p = document.getElementById('nasPath');
      if (p && !p.value) p.value = status.path;
    }
  }

  function nasEnable() {
    const path = document.getElementById('nasPath').value.trim();
    const msgEl = document.getElementById('nasMsg');
    if (!path) { _showModalMsg(msgEl, 'Output path is required', 'danger'); return; }
    _showModalMsg(msgEl, 'Enabling…', 'info');
    socket.emit('nas_configure', { path });
  }

  function nasDisable() {
    socket.emit('nas_disable', {});
  }

  // ── File Rotation Settings ────────────────────────────────────────────────

  function _updateFileRotationUI(rawMin, expMin) {
    const rawEl = document.getElementById('rawFileRotation');
    const expEl = document.getElementById('expFileRotation');
    if (rawEl) rawEl.value = String(rawMin ?? 1440);
    if (expEl) expEl.value = String(expMin ?? 0);
  }

  function saveFileRotationSettings() {
    const rawEl = document.getElementById('rawFileRotation');
    const expEl = document.getElementById('expFileRotation');
    const msgEl = document.getElementById('fileRotationMsg');
    const rawMin = parseInt(rawEl?.value ?? 1440);
    const expMin = parseInt(expEl?.value ?? 0);
    socket.emit('save_file_rotation_settings', {
      raw_file_rotation_minutes: rawMin,
      exp_file_rotation_minutes: expMin,
    });
    if (msgEl) {
      msgEl.textContent = 'Saved.';
      msgEl.className = 'text-success mt-2 small';
      setTimeout(() => msgEl.classList.add('d-none'), 2000);
    }
  }

  // ── Shutdown ───────────────────────────────────────────────────────────────

  async function shutdownServer() {
    const btn = document.getElementById('shutdownBtn');
    const msgEl = document.getElementById('shutdownMsg');
    if (!btn) return;

    // Two-click confirmation: first click arms the button.
    if (btn.dataset.armed !== 'true') {
      btn.dataset.armed = 'true';
      btn.textContent = 'Click again to confirm shutdown';
      btn.classList.replace('btn-outline-danger', 'btn-danger');
      setTimeout(() => {
        btn.dataset.armed = 'false';
        btn.innerHTML = '<i class="fa fa-power-off me-1"></i>Shut Down Server';
        btn.classList.replace('btn-danger', 'btn-outline-danger');
      }, 4000);
      return;
    }

    btn.disabled = true;
    btn.textContent = 'Shutting down…';
    if (msgEl) { msgEl.className = 'mt-2 small text-warning'; msgEl.textContent = 'Server is shutting down. You may close this tab.'; }
    try {
      await fetch('/api/shutdown', { method: 'POST' });
    } catch (_) { /* server going down — expected */ }
  }

  socket.on('nas_status', (status) => {
    _updateNasUI(status);
    const msgEl = document.getElementById('nasMsg');
    if (msgEl) {
      if (status.enabled) {
        _showModalMsg(msgEl, `Writing to: ${status.path}`, 'success');
      } else if (msgEl.textContent === 'Enabling…') {
        _showModalMsg(msgEl, 'Failed to enable — check path is accessible', 'danger');
      }
    }
  });

  // ── Toast Notifications ───────────────────────────────────────────────────

  function _showToast(message, type = 'info') {
    const container = document.getElementById('toastContainer');
    if (!container) return;

    const icons = { success: 'fa-check-circle', danger: 'fa-exclamation-circle',
                    warning: 'fa-triangle-exclamation', info: 'fa-info-circle' };
    const colors = { success: '#3fb950', danger: '#f85149', warning: '#d29922', info: '#58a6ff' };
    const icon = icons[type] || icons.info;
    const color = colors[type] || colors.info;

    const el = document.createElement('div');
    el.className = 'toast ec-toast align-items-center show';
    el.setAttribute('role', 'alert');
    el.innerHTML = `
      <div class="d-flex align-items-center gap-2 p-2">
        <i class="fa ${icon}" style="color:${color}"></i>
        <div class="toast-body p-0 flex-grow-1">${_esc(message)}</div>
        <button type="button" class="btn-close btn-close-white ms-2" onclick="this.closest('.toast').remove()"></button>
      </div>`;
    container.appendChild(el);
    setTimeout(() => el.remove(), 5000);
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  function _esc(str) {
    if (str === null || str === undefined) return '';
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
      .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
  }

  function _fmt(val, decimals = 3) {
    if (val === null || val === undefined || isNaN(val)) return '—';
    return Number(val).toFixed(decimals);
  }

  function _setEl(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  }

  function _fmtDuration(sec) {
    if (!sec && sec !== 0) return '—';
    sec = Math.max(0, sec);
    const m = Math.floor(sec / 60);
    const s = Math.floor(sec % 60);
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
  }

  function _timeAgo(dt) {
    const sec = Math.floor((Date.now() - dt.getTime()) / 1000);
    if (sec < 10) return 'just now';
    if (sec < 60) return `${sec}s ago`;
    const m = Math.floor(sec / 60);
    if (m < 60) return `${m}m ago`;
    return `${Math.floor(m / 60)}h ago`;
  }

  function _gasName(num) {
    const table = {0:'Air',1:'Ar',2:'CH4',3:'CO',4:'CO2',5:'C2H6',6:'H2',7:'He',
                   8:'N2',9:'N2O',10:'Ne',11:'O2',12:'C3H8',13:'nC4H10',14:'C2H2',
                   15:'C2H4',16:'iC4H10',17:'Kr',18:'Xe',19:'SF6'};
    if (num === null || num === undefined) return '—';
    return `${num}${table[num] ? ' — ' + table[num] : ''}`;
  }

  function _showModalError(el, msg) {
    if (!el) return;
    el.textContent = msg;
    el.classList.remove('d-none');
  }

  function _showModalMsg(el, msg, type) {
    if (!el) return;
    el.textContent = msg;
    el.className = `alert alert-${type} py-2 small`;
    el.classList.remove('d-none');
  }

  // ── Global Experiment Checklist ───────────────────────────────────────────

  let _globalChecklist = [];

  async function _loadGlobalChecklist() {
    try {
      const resp = await fetch('/api/solenoid_checklist');
      if (resp.ok) {
        _globalChecklist = await resp.json();
        _renderGlobalChecklist();
      }
    } catch (e) { /* non-fatal */ }
  }

  async function _saveGlobalChecklist() {
    try {
      await fetch('/api/solenoid_checklist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(_globalChecklist),
      });
    } catch (e) { /* non-fatal */ }
  }

  function _escHtmlGlobal(str) {
    if (!str && str !== 0) return '';
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
      .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
  }

  function _renderGlobalChecklist() {
    const container = document.getElementById('globalChecklistRows');
    const noMsg = document.getElementById('noGlobalChecklist');
    if (!container) return;
    container.innerHTML = '';
    if (_globalChecklist.length === 0) {
      if (noMsg) noMsg.style.display = 'block';
      return;
    }
    if (noMsg) noMsg.style.display = 'none';
    const stateLabel = v => v === true ? '<span class="badge" style="background:#238636">OPEN</span>'
                          : v === false ? '<span class="badge bg-secondary">CLOSED</span>'
                          : '<span class="badge" style="background:#30363d;color:#8b949e">Any</span>';
    _globalChecklist.forEach((item, i) => {
      const preState = item.pre_state !== undefined ? item.pre_state : item.expected_state;
      const div = document.createElement('div');
      div.className = 'd-flex justify-content-between align-items-center py-1 px-1 mb-1';
      div.style.cssText = 'background:#0d1117;border:1px solid #21262d;border-radius:5px';
      div.innerHTML = `
        <div style="flex:1;min-width:0">
          <div class="fw-semibold text-truncate" style="font-size:0.82rem">${_escHtmlGlobal(item.label || 'Unnamed')}</div>
          <div style="font-size:0.7rem;color:#8b949e">${_escHtmlGlobal(item.peripheral_name || '—')} · CH${item.channel ?? 0}</div>
          <div class="d-flex gap-1 mt-1">
            <span style="font-size:0.68rem;color:#8b949e">Before:</span>${stateLabel(preState)}
            <span style="font-size:0.68rem;color:#8b949e;margin-left:4px">After:</span>${stateLabel(item.post_state)}
          </div>
        </div>
        <div class="d-flex gap-1 ms-2">
          <button class="btn btn-xs btn-outline-secondary" onclick="app.openChecklistItemModal(${i})" title="Edit">
            <i class="fa fa-pen"></i>
          </button>
          <button class="btn btn-xs btn-outline-danger" onclick="app.removeGlobalChecklistRow(${i})" title="Delete">
            <i class="fa fa-trash"></i>
          </button>
        </div>`;
      container.appendChild(div);
    });
  }

  window._globalChecklistUpdate = (i, field, val) => {
    if (_globalChecklist[i]) {
      _globalChecklist[i][field] = val;
      _saveGlobalChecklist();
    }
  };

  function addGlobalChecklistRow() {
    openChecklistItemModal(-1);
  }

  function removeGlobalChecklistRow(i) {
    _globalChecklist.splice(i, 1);
    _renderGlobalChecklist();
    _saveGlobalChecklist();
  }

  let _checklistModal = null;

  function openChecklistItemModal(index) {
    const item = index >= 0 ? _globalChecklist[index] : { peripheral_name: '', channel: 0, label: '', pre_state: true, post_state: null };
    const preState = item.pre_state !== undefined ? item.pre_state : item.expected_state;
    document.getElementById('checklistItemIndex').value = index;
    document.getElementById('checklistItemModalTitle').innerHTML =
      `<i class="fa fa-toggle-on me-2 text-warning"></i>${index >= 0 ? 'Edit' : 'Add'} Checklist Item`;
    document.getElementById('clItemPeriphName').value = item.peripheral_name || '';
    document.getElementById('clItemChannel').value = item.channel ?? 0;
    document.getElementById('clItemLabel').value = item.label || '';
    document.getElementById('clItemPreState').value = preState === true ? 'true' : preState === false ? 'false' : '';
    document.getElementById('clItemPostState').value = item.post_state === true ? 'true' : item.post_state === false ? 'false' : '';
    if (!_checklistModal) _checklistModal = new bootstrap.Modal(document.getElementById('checklistItemModal'));
    _checklistModal.show();
  }

  function saveChecklistItem() {
    const index = parseInt(document.getElementById('checklistItemIndex').value);
    const preRaw = document.getElementById('clItemPreState').value;
    const postRaw = document.getElementById('clItemPostState').value;
    const item = {
      peripheral_name: document.getElementById('clItemPeriphName').value.trim(),
      channel: parseInt(document.getElementById('clItemChannel').value) || 0,
      label: document.getElementById('clItemLabel').value.trim(),
      pre_state: preRaw === '' ? null : preRaw === 'true',
      post_state: postRaw === '' ? null : postRaw === 'true',
    };
    if (index >= 0) {
      _globalChecklist[index] = item;
    } else {
      _globalChecklist.push(item);
    }
    if (_checklistModal) _checklistModal.hide();
    _renderGlobalChecklist();
    _saveGlobalChecklist();
  }

  // Load on startup
  _loadGlobalChecklist();

  // ── Drag-to-reorder devices & peripherals ────────────────────────────────
  const DEVICE_ORDER_KEY = 'ec_device_order';
  const PERIPH_ORDER_KEY = 'ec_periph_order';

  function _loadOrder(key) {
    try { return JSON.parse(localStorage.getItem(key) || 'null') || []; } catch { return []; }
  }
  function _saveOrder(key, ids) {
    localStorage.setItem(key, JSON.stringify(ids));
  }
  function _applyStoredOrder(listEl, dataAttr, key) {
    const order = _loadOrder(key);
    if (!order.length) return;
    const cards = Array.from(listEl.children);
    const map = Object.fromEntries(cards.map(c => [c.dataset[dataAttr], c]));
    order.forEach(id => { if (map[id]) listEl.appendChild(map[id]); });
  }

  (function initSortable() {
    const devList = document.getElementById('deviceList');
    const periphList = document.getElementById('peripheralList');
    if (!devList || !periphList || typeof Sortable === 'undefined') return;

    Sortable.create(devList, {
      animation: 150,
      handle: '.drag-handle',
      onEnd() {
        const ids = Array.from(devList.querySelectorAll('.device-card')).map(c => c.dataset.deviceId);
        _saveOrder(DEVICE_ORDER_KEY, ids);
        socket.emit('reorder_devices', { order: ids });
      }
    });

    Sortable.create(periphList, {
      animation: 150,
      handle: '.drag-handle',
      onEnd() {
        const ids = Array.from(periphList.querySelectorAll('.peripheral-card')).map(c => c.dataset.peripheralId);
        _saveOrder(PERIPH_ORDER_KEY, ids);
        socket.emit('reorder_peripherals', { order: ids });
      }
    });
  })();

  // Re-apply stored order after each full state refresh
  socket.on('state', () => {
    setTimeout(() => {
      const dl = document.getElementById('deviceList');
      if (dl) _applyStoredOrder(dl, 'deviceId', DEVICE_ORDER_KEY);
      const pl = document.getElementById('peripheralList');
      if (pl) _applyStoredOrder(pl, 'peripheralId', PERIPH_ORDER_KEY);
    }, 0);
  });

  // ── Experiment: receive updates from server ───────────────────────────────

  socket.on('experiments_update', (experiments) => {
    expUI.renderExperimentList(experiments);
  });

  socket.on('experiment_updated', (exp) => {
    // If the editor is open for this experiment, refresh it
    const openId = document.getElementById('expEditorId')?.value;
    if (openId === exp?.experiment_id) {
      expUI.populateEditor(exp);
    }
    // If the viewer is showing this experiment, reload the chart
    if (exp?.experiment_id) {
      window.expViewerUI?.refreshIfSelected(exp.experiment_id);
    }
    expUI.refreshExperimentList();
  });

  socket.on('current_experiment_update', (current) => {
    expUI.updateRunningBanner(current);
  });

  socket.on('post_run_check_result', (result) => {
    if (result && result.warnings && result.warnings.length > 0) {
      result.warnings.forEach(w => _showAppToast(w, 'warning'));
    }
  });

  socket.on('server_log', (entry) => {
    const pane = document.getElementById('serverLogPane');
    if (!pane) return;
    const colors = { error: '#f85149', warning: '#d29922', info: '#58a6ff', debug: '#8b949e' };
    const levelColor = colors[entry.level] || '#c9d1d9';
    const levelBadges = {
      error:   `<span style="color:#f85149;font-weight:600">[ERR] </span>`,
      warning: `<span style="color:#d29922;font-weight:600">[WRN] </span>`,
      info:    `<span style="color:#58a6ff;font-weight:600">[INF] </span>`,
      debug:   `<span style="color:#8b949e">[DBG] </span>`,
    };
    const badge = levelBadges[entry.level] || '';
    const line = document.createElement('div');
    line.dataset.level = entry.level || 'info';
    line.dataset.msg = (entry.msg || '').toLowerCase();
    line.innerHTML = `<span style="color:#484f58">${_esc(entry.ts || '')}</span> ${badge}<span style="color:${levelColor}">${_esc(entry.msg || '')}</span>`;
    pane.appendChild(line);
    // Keep last 500 lines
    while (pane.children.length > 500) pane.removeChild(pane.firstChild);
    // Apply current filter to new line
    _applyLogFilter(line);
    // Auto-scroll to bottom if already near bottom
    if (pane.scrollHeight - pane.scrollTop - pane.clientHeight < 60) {
      pane.scrollTop = pane.scrollHeight;
    }
  });

  // Public API ────────────────────────────────────────────────────────────
  return {
    addDevice, removeDevice, openEditDeviceModal, saveEditDevice, toggleDisableDevice, toggleDeviceCollapse, toggleSectionCollapse,
    collapseAllDevices, expandAllDevices,
    deleteChatMessage, clearChat,
    toggleDisablePeripheral,
    pickGeoLocation,
    addGlobalChecklistRow, removeGlobalChecklistRow, openChecklistItemModal, saveChecklistItem,
    toggleLogging,
    openSetpointModal, applySetpoint,
    openGasModal, applyGas,
    openScheduleModal, loadSchedule,
    startSchedule, stopSchedule,
    updatePeriphFields, addPeripheral, removePeripheral, toggleRelay,
    openEditPeripheralModal, saveEditPeripheral,
    crashRecoveryResponse,
    updatePassword,
    mqttConnect, mqttDisconnect,
    nasEnable, nasDisable,
    saveFileRotationSettings,
    shutdownServer,
    sendChat, kickSession,
  };

})();


/* ═══════════════════════════════════════════════════════════════════════════
   expUI — Experiment Manager UI
   Separate IIFE so it doesn't clutter the main app namespace.
═══════════════════════════════════════════════════════════════════════════ */
const expUI = (() => {

  'use strict';

  let _experiments = [];          // list from server
  let _currentExperiment = null;  // running experiment info
  let _editorSchedules = {};      // device_name -> [{time,setpoint}]
  let _editorGlobalStartIso = null; // global_start_iso for current editor session
  let _preRunExpId = '';

  // ── Receive full_state ────────────────────────────────────────────────────

  function handleFullState(state) {
    _experiments = state.experiments || [];
    _currentExperiment = state.current_experiment || null;
    renderExperimentList(_experiments);
    updateRunningBanner(_currentExperiment);
  }

  // ── Experiment List ───────────────────────────────────────────────────────

  function renderExperimentList(experiments) {
    _experiments = experiments || [];
    const list = document.getElementById('experimentList');
    const noMsg = document.getElementById('noExperimentsMsg');
    if (!list) return;

    if (_experiments.length === 0) {
      list.innerHTML = '';
      if (noMsg) noMsg.style.display = 'block';
      return;
    }
    if (noMsg) noMsg.style.display = 'none';

    const running = _currentExperiment?.experiment_id;
    // Sort: running experiment first, then by most recent started_at/created_at descending
    const sorted = [..._experiments].sort((a, b) => {
      if (a.experiment_id === running) return -1;
      if (b.experiment_id === running) return 1;
      const aDate = a.started_at || a.created_at || '';
      const bDate = b.started_at || b.created_at || '';
      return bDate.localeCompare(aDate);
    });
    list.innerHTML = sorted.map(e => {
      const isRunning = e.experiment_id === running;
      const statusClass = isRunning ? 'running' : (e.status === 'completed' ? 'completed' : (e.status === 'crashed' ? 'crashed' : ''));
      const statusBadge = isRunning
        ? `<span style="color:#3fb950;font-size:0.7rem">● Running</span>`
        : (e.status === 'completed' ? `<span style="color:#8b949e;font-size:0.7rem">Completed</span>` :
           e.status === 'crashed' ? `<span style="color:#f85149;font-size:0.7rem">Crashed</span>` : '');

      const deviceSummary = Object.entries(e.step_counts || {})
        .map(([n, c]) => `${_escHtml(n)}: ${c} steps`)
        .join(' · ') || '<span class="text-muted">No schedules</span>';

      return `<div class="experiment-card ${statusClass}">
        <div class="d-flex justify-content-between align-items-start">
          <div style="flex:1;min-width:0">
            <div class="fw-semibold text-truncate" title="${_escHtml(e.name)}">${_escHtml(e.name)}</div>
            <div style="font-size:0.72rem;color:#8b949e">
              ${_escHtml(e.operator || '—')} ·
              ${e.started_at
                ? `<span title="Last run">Ran: ${_escHtml(e.started_at.slice(0,16).replace('T',' '))} UTC</span>${e.completed_at ? ` · Ended: ${_escHtml(e.completed_at.slice(0,16).replace('T',' '))} UTC` : ''}`
                : `<span title="Created">Created: ${_escHtml(e.created_at?.slice(0,10) || '—')}</span>`}
            </div>
            <div style="font-size:0.7rem;color:#484f58;margin-top:0.15rem">${deviceSummary}</div>
          </div>
          <div class="d-flex flex-column align-items-end gap-1 ms-2">
            ${statusBadge}
            <div class="d-flex gap-1">
              <button class="btn btn-xs btn-outline-secondary" onclick="expUI.openEditor('${e.experiment_id}')" title="Edit">
                <i class="fa fa-pen"></i>
              </button>
              ${!isRunning
                ? `<button class="btn btn-xs btn-success" onclick="expUI.runExperiment('${e.experiment_id}')" title="Run">
                     <i class="fa fa-play"></i>
                   </button>
                   <button class="btn btn-xs btn-outline-danger" onclick="expUI.deleteExperiment('${e.experiment_id}')" title="Delete">
                     <i class="fa fa-trash"></i>
                   </button>`
                : `<button class="btn btn-xs btn-outline-danger" onclick="expUI.stopExperiment()" title="Stop">
                     <i class="fa fa-stop"></i>
                   </button>`
              }
              <div class="dropdown">
                <button class="btn btn-xs btn-outline-secondary dropdown-toggle" data-bs-toggle="dropdown"></button>
                <ul class="dropdown-menu dropdown-menu-dark dropdown-menu-end">
                  <li><a class="dropdown-item small" href="/api/experiments/${e.experiment_id}/export.json" target="_blank">
                    <i class="fa fa-download me-1"></i>Export JSON</a></li>
                  ${e.has_global_start ? `<li><a class="dropdown-item small" href="/api/experiments/${e.experiment_id}/export_multi_device_csv" target="_blank">
                    <i class="fa fa-file-csv me-1 text-success"></i>Export Multi-Device CSV</a></li>` : ''}
                  ${Object.keys(e.step_counts || {}).map(dn =>
                    `<li><a class="dropdown-item small" href="/api/experiments/${e.experiment_id}/export_csv/${encodeURIComponent(dn)}" target="_blank">
                      <i class="fa fa-file-csv me-1"></i>Export CSV: ${_escHtml(dn)}</a></li>`
                  ).join('')}
                  <li><a class="dropdown-item small" href="#" onclick="expUI.shiftExistingExperiment('${e.experiment_id}');return false">
                    <i class="fa fa-clock-rotate-left me-1"></i>Shift Timestamps</a></li>
                  <li><hr class="dropdown-divider"></li>
                  <li><a class="dropdown-item small text-danger" href="#" onclick="expUI.deleteExperiment('${e.experiment_id}');return false">
                    <i class="fa fa-trash me-1"></i>Delete</a></li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </div>`;
    }).join('');
  }

  async function refreshExperimentList() {
    try {
      const resp = await fetch('/api/experiments');
      if (!resp.ok) {
        console.warn('[expUI] /api/experiments returned', resp.status, '— session may have expired');
        return;
      }
      const data = await resp.json();
      if (!Array.isArray(data)) {
        console.warn('[expUI] /api/experiments returned non-array:', data);
        return;
      }
      _experiments = data;
      renderExperimentList(_experiments);
      // Keep expViewerUI's selector in sync
      if (window.expViewerUI) {
        window.expViewerUI.handleFullState({ experiments: _experiments, current_experiment: _currentExperiment });
      }
    } catch (e) {
      console.warn('[expUI] refreshExperimentList fetch error:', e);
    }
  }

  // ── Running Experiment Banner ─────────────────────────────────────────────

  function updateRunningBanner(current) {
    _currentExperiment = current;
    const banner = document.getElementById('runningExperimentBanner');
    if (!banner) return;
    if (!current) {
      banner.classList.add('d-none');
      return;
    }
    banner.classList.remove('d-none');
    const nameEl = document.getElementById('runningExpName');
    const elapsedEl = document.getElementById('runningExpElapsed');
    const dirEl = document.getElementById('runningExpDir');
    if (nameEl) nameEl.textContent = current.name || '—';
    if (elapsedEl) elapsedEl.textContent = current.elapsed_human || '—';
    if (dirEl) dirEl.textContent = current.data_dir ? `Data/${current.data_dir}` : '';
    // Re-render list to update running badge
    renderExperimentList(_experiments);
  }

  // Update elapsed time every second
  setInterval(() => {
    if (!_currentExperiment) return;
    _currentExperiment.elapsed_seconds = (_currentExperiment.elapsed_seconds || 0) + 1;
    const elapsedEl = document.getElementById('runningExpElapsed');
    if (elapsedEl) elapsedEl.textContent = _fmtDurationExp(_currentExperiment.elapsed_seconds);
  }, 1000);

  // ── New Experiment ────────────────────────────────────────────────────────

  function openNewExperiment() {
    _clearEditor();
    document.getElementById('expEditorTitle').innerHTML =
      '<i class="fa fa-flask-vial me-2 text-purple"></i>New Experiment';
    document.getElementById('expEditorId').value = '';
    const exportBtn = document.getElementById('expExportBtn');
    if (exportBtn) exportBtn.style.display = 'none';
    new bootstrap.Modal(document.getElementById('experimentEditorModal')).show();
  }

  // ── Open Editor for Existing ──────────────────────────────────────────────

  async function openEditor(experimentId) {
    try {
      const resp = await fetch(`/api/experiments/${experimentId}`);
      if (!resp.ok) { _showAppToast('Failed to load experiment', 'danger'); return; }
      const exp = await resp.json();
      _clearEditor();
      populateEditor(exp);
      document.getElementById('expEditorTitle').innerHTML =
        '<i class="fa fa-flask-vial me-2 text-purple"></i>Edit Experiment';
      const exportBtn = document.getElementById('expExportBtn');
      if (exportBtn) { exportBtn.style.removeProperty('display'); exportBtn.disabled = false; }
      new bootstrap.Modal(document.getElementById('experimentEditorModal')).show();
    } catch (e) {
      _showAppToast('Error loading experiment: ' + e.message, 'danger');
    }
  }

  function populateEditor(exp) {
    document.getElementById('expEditorId').value = exp.experiment_id || '';
    document.getElementById('expName').value = exp.name || '';
    document.getElementById('expOperator').value = exp.operator || '';
    document.getElementById('expNotes').value = exp.notes || '';

    _editorSchedules = {};
    _editorGlobalStartIso = exp.global_start_iso || null;

    // Render device schedule rows
    const container = document.getElementById('expDeviceSchedules');
    container.innerHTML = '';
    for (const [deviceName, schedInfo] of Object.entries(exp.device_schedules || {})) {
      _editorSchedules[deviceName] = schedInfo.schedule || [];
      _addDeviceScheduleRowElement(deviceName, schedInfo.schedule || []);
    }
    document.getElementById('noExpDevices').style.display =
      Object.keys(_editorSchedules).length === 0 ? 'block' : 'none';
  }

  function _clearEditor() {
    ['expName','expOperator','expNotes'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.value = '';
    });
    _editorSchedules = {};
    _editorGlobalStartIso = null;
    const container = document.getElementById('expDeviceSchedules');
    if (container) container.innerHTML = '';
    document.getElementById('noExpDevices').style.display = 'block';
    document.getElementById('expEditorError')?.classList.add('d-none');
    // Hide the add-device picker if it was left open
    cancelAddDevice();
  }

  // ── Device Schedule Rows ──────────────────────────────────────────────────

  function addDeviceScheduleRow() {
    // Populate datalist with current device names
    const datalist = document.getElementById('deviceNameDatalist');
    if (datalist) {
      datalist.innerHTML = '';
      const devices = window._getAppDevices ? window._getAppDevices() : {};
      Object.values(devices).forEach(d => {
        const opt = document.createElement('option');
        opt.value = d.device_name || d.device_id;
        datalist.appendChild(opt);
      });
    }
    // Show inline picker
    const picker = document.getElementById('addDevicePicker');
    const input = document.getElementById('addDeviceInput');
    if (picker) picker.classList.remove('d-none');
    if (input) { input.value = ''; input.focus(); }
  }

  function confirmAddDevice() {
    const input = document.getElementById('addDeviceInput');
    const name = (input?.value || '').trim();
    if (!name) { input?.focus(); return; }
    if (_editorSchedules[name] !== undefined) {
      _showAppToast(`Device "${name}" already has a schedule row`, 'warning');
      return;
    }
    _editorSchedules[name] = [];
    _addDeviceScheduleRowElement(name, []);
    document.getElementById('noExpDevices').style.display = 'none';
    cancelAddDevice();
  }

  function cancelAddDevice() {
    const picker = document.getElementById('addDevicePicker');
    if (picker) picker.classList.add('d-none');
  }

  function _addDeviceScheduleRowElement(deviceName, schedule) {
    const container = document.getElementById('expDeviceSchedules');
    const row = document.createElement('div');
    row.className = 'exp-device-row';
    row.id = `expDevRow-${_safeId(deviceName)}`;

    const editHtml = _buildScheduleEditHtml(deviceName, schedule);
    row.innerHTML = `
      <div class="d-flex align-items-center justify-content-between mb-2">
        <span class="fw-semibold text-info">${_escHtml(deviceName)}</span>
        <div class="d-flex gap-1 align-items-center">
          <span class="text-muted small" id="expDevSteps-${_safeId(deviceName)}">${schedule.length} steps</span>
          <button class="btn btn-xs btn-outline-secondary" onclick="expUI.importDeviceCSV('${_escHtml(deviceName)}')" title="Import CSV">
            <i class="fa fa-file-csv me-1"></i>Import CSV
          </button>
          <button class="btn btn-xs btn-outline-danger" onclick="expUI.removeDeviceRow('${_escHtml(deviceName)}')">
            <i class="fa fa-trash"></i>
          </button>
        </div>
      </div>
      <div class="schedule-step-table" id="expDevTable-${_safeId(deviceName)}">${editHtml}</div>
      <input type="file" class="d-none" id="expDevFile-${_safeId(deviceName)}" accept=".csv"
             onchange="expUI.handleDeviceCSVFile('${_escHtml(deviceName)}', this)">
    `;
    container.appendChild(row);
  }

  function _buildSchedulePreviewHtml(schedule) {
    if (!schedule || schedule.length === 0) {
      return '<div class="text-muted small text-center py-2">No schedule data. Import a CSV file.</div>';
    }
    const baseMs = _editorGlobalStartIso ? new Date(_editorGlobalStartIso).getTime() : null;
    const rows = schedule.slice(0, 50).map(s => {
      const utcCell = baseMs != null
        ? `<td>${new Date(baseMs + s.time * 1000).toISOString().slice(11, 19)} UTC</td>`
        : `<td style="color:#8b949e">—</td>`;
      return `<tr><td>${_fmtDurationExp(s.time)}</td>${utcCell}<td style="text-align:right">${Number(s.setpoint).toFixed(3)} SLPM</td></tr>`;
    }).join('');
    const more = schedule.length > 50 ? `<tr><td colspan="3" style="color:#8b949e;text-align:center">… ${schedule.length - 50} more steps</td></tr>` : '';
    return `<table><thead><tr><th>Elapsed</th><th>Time (UTC)</th><th>Setpoint</th></tr></thead><tbody>${rows}${more}</tbody></table>`;
  }

  // ── Inline schedule editing ────────────────────────────────────────────────

  function _buildScheduleEditHtml(deviceName, schedule) {
    const esc = _escHtml(deviceName);
    const safeId = _safeId(deviceName);
    if (!schedule || schedule.length === 0) {
      return `<div class="text-muted small text-center py-2">No steps yet.
        <button class="btn btn-xs btn-outline-info ms-2" onclick="expUI.addScheduleRow('${esc}')"><i class="fa fa-plus me-1"></i>Add Row</button>
      </div>`;
    }
    const baseMs = _editorGlobalStartIso ? new Date(_editorGlobalStartIso).getTime() : null;
    const rows = schedule.map((s, i) => {
      const utcCell = baseMs != null
        ? `<td class="text-muted small" id="sched-utc-${safeId}-${i}">${new Date(baseMs + s.time * 1000).toISOString().slice(11, 19)}</td>`
        : `<td class="text-muted small" id="sched-utc-${safeId}-${i}">—</td>`;
      return `<tr>
        <td><input type="text" class="sched-time-input" value="${_fmtDurationExp(s.time)}"
             onchange="expUI._onScheduleChange('${esc}',${i},'time',this.value)" title="HH:MM:SS or seconds"></td>
        ${utcCell}
        <td><input type="number" class="sched-sp-input" value="${Number(s.setpoint).toFixed(3)}" step="0.001" min="0"
             onchange="expUI._onScheduleChange('${esc}',${i},'setpoint',this.value)"></td>
        <td><button class="btn btn-xs btn-outline-danger" onclick="expUI._deleteScheduleRow('${esc}',${i})" title="Delete row"><i class="fa fa-trash"></i></button></td>
      </tr>`;
    }).join('');
    return `<table class="sched-edit-table w-100">
      <thead><tr><th>Elapsed</th><th>UTC Time</th><th>Setpoint (SLPM)</th><th></th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <button class="btn btn-xs btn-outline-info mt-1" onclick="expUI.addScheduleRow('${esc}')"><i class="fa fa-plus me-1"></i>Add Row</button>`;
  }

  /** Parse "HH:MM:SS", "MM:SS", or plain seconds string → number of seconds. */
  function _parseTimeInput(val) {
    val = (val || '').trim();
    if (/^\d+(\.\d+)?$/.test(val)) return parseFloat(val);
    const parts = val.split(':').map(Number);
    if (parts.some(isNaN)) return null;
    if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
    if (parts.length === 2) return parts[0] * 60 + parts[1];
    return null;
  }

  function _onScheduleChange(deviceName, idx, field, value) {
    const schedule = _editorSchedules[deviceName];
    if (!schedule || idx < 0 || idx >= schedule.length) return;
    if (field === 'time') {
      const parsed = _parseTimeInput(value);
      if (parsed === null || parsed < 0) return; // ignore invalid input
      schedule[idx].time = parsed;
    } else if (field === 'setpoint') {
      const parsed = parseFloat(value);
      if (isNaN(parsed) || parsed < 0) return;
      schedule[idx].setpoint = parsed;
    }
    // Refresh UTC column only (avoid full re-render which loses focus)
    const baseMs = _editorGlobalStartIso ? new Date(_editorGlobalStartIso).getTime() : null;
    const utcEl = document.getElementById(`sched-utc-${_safeId(deviceName)}-${idx}`);
    if (utcEl && baseMs != null) {
      utcEl.textContent = new Date(baseMs + schedule[idx].time * 1000).toISOString().slice(11, 19);
    }
    const stepsEl = document.getElementById(`expDevSteps-${_safeId(deviceName)}`);
    if (stepsEl) stepsEl.textContent = `${schedule.length} steps`;
  }

  function _deleteScheduleRow(deviceName, idx) {
    const schedule = _editorSchedules[deviceName];
    if (!schedule) return;
    schedule.splice(idx, 1);
    _refreshDeviceTable(deviceName);
  }

  function addScheduleRow(deviceName) {
    if (!_editorSchedules[deviceName]) _editorSchedules[deviceName] = [];
    const schedule = _editorSchedules[deviceName];
    const lastTime = schedule.length > 0 ? schedule[schedule.length - 1].time : 0;
    schedule.push({ time: lastTime + 3600, setpoint: 0 });
    _refreshDeviceTable(deviceName);
  }

  function _refreshDeviceTable(deviceName) {
    const schedule = _editorSchedules[deviceName] || [];
    const tableEl = document.getElementById(`expDevTable-${_safeId(deviceName)}`);
    if (tableEl) tableEl.innerHTML = _buildScheduleEditHtml(deviceName, schedule);
    const stepsEl = document.getElementById(`expDevSteps-${_safeId(deviceName)}`);
    if (stepsEl) stepsEl.textContent = `${schedule.length} steps`;
    document.getElementById('noExpDevices').style.display =
      Object.keys(_editorSchedules).length > 0 ? 'none' : 'block';
  }

  function importDeviceCSV(deviceName) {
    const fileInput = document.getElementById(`expDevFile-${_safeId(deviceName)}`);
    if (fileInput) fileInput.click();
  }

  function showCsvTemplate() {
    const modal = new bootstrap.Modal(document.getElementById('csvTemplateModal'));
    modal.show();
  }

  function downloadCsvTemplate() {
    const rows = [
      'Emission ID,Time (UTC),Flow (SLPM)',
      'MR-01,2026-03-11T00:00:00Z,0.0',
      'MR-01,2026-03-11T01:00:00Z,100.0',
      'MR-01,2026-03-11T02:00:00Z,250.0',
      'MR-01,2026-03-11T03:00:00Z,0.0',
      'MR-04,2026-03-11T00:00:00Z,0.0',
      'MR-04,2026-03-11T01:30:00Z,50.0',
      'MR-04,2026-03-11T03:00:00Z,0.0',
    ];
    const blob = new Blob([rows.join('\r\n') + '\r\n'], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'experiment_template.csv';
    a.click();
    URL.revokeObjectURL(url);
  }

  function importMultiDeviceCSV() {
    const fileInput = document.getElementById('multiDeviceCsvFile');
    if (fileInput) fileInput.click();
  }

  // Pending CSV state for the shift modal
  let _pendingCsvData = null;   // { schedules, global_start_iso, expId }
  let _csvShiftModal = null;

  async function handleMultiDeviceCSVFile(input) {
    const file = input.files[0];
    if (!file) return;
    input.value = '';

    const expId = document.getElementById('expEditorId').value;
    const formData = new FormData();
    formData.append('file', file);
    const errEl = document.getElementById('expEditorError');

    try {
      if (expId) {
        // Existing experiment: parse via server import endpoint
        const resp = await fetch(`/api/experiments/${expId}/import_multi_device_schedule`, {
          method: 'POST', body: formData
        });
        const data = await resp.json();
        if (!resp.ok || data.error) { _showCsvImportErrors(errEl, data); return; }

        const globalStart = data.global_start_iso ? new Date(data.global_start_iso) : null;
        // Always offer the shift option, whether timestamps are past or future
        const expResp = await fetch(`/api/experiments/${expId}`);
        const exp = await expResp.json();
        if (globalStart) {
          _pendingCsvData = {
            schedules: Object.fromEntries(
              Object.entries(exp.device_schedules || {}).map(([k, v]) => [k, v.schedule])
            ),
            global_start_iso: data.global_start_iso,
            expId,
          };
          _showCsvShiftModal(globalStart);
        } else {
          populateEditor(exp);
          const names = data.assigned.map(a => a.device_name).join(', ');
          _showAppToast(`Assigned ${data.device_count} device(s): ${names}`, 'success');
          errEl.classList.add('d-none');
        }
      } else {
        // New experiment: parse only
        const resp = await fetch('/api/parse_multi_device_csv', { method: 'POST', body: formData });
        const data = await resp.json();
        if (!resp.ok || data.error) { _showCsvImportErrors(errEl, data); return; }

        const globalStart = data.global_start_iso ? new Date(data.global_start_iso) : null;
        // Always offer the shift option, whether timestamps are past or future
        if (globalStart) {
          _pendingCsvData = { schedules: data.schedules, global_start_iso: data.global_start_iso, expId: null };
          _showCsvShiftModal(globalStart);
        } else {
          _applyCsvSchedules(data.schedules);
          errEl.classList.add('d-none');
        }
      }
    } catch (e) {
      errEl.textContent = 'Upload failed: ' + e.message;
      errEl.classList.remove('d-none');
    }
  }

  function _showCsvShiftModal(globalStart) {
    const infoEl = document.getElementById('csvShiftOriginalInfo');
    const isPast = globalStart < new Date();
    infoEl.textContent = `Original start: ${globalStart.toLocaleString()}${isPast ? ' (in the past)' : ''}`;

    // Default new start = now + 5 minutes, seconds zeroed
    const defaultStart = new Date(Date.now() + 5 * 60000);
    defaultStart.setSeconds(0, 0);
    const pad = n => String(n).padStart(2, '0');
    // Include seconds (:00) so the step="1" input shows the full value
    const localIso = `${defaultStart.getFullYear()}-${pad(defaultStart.getMonth()+1)}-${pad(defaultStart.getDate())}T${pad(defaultStart.getHours())}:${pad(defaultStart.getMinutes())}:00`;
    const input = document.getElementById('csvShiftNewStart');
    input.value = localIso;
    input.removeAttribute('min');  // don't constrain — user may pick any time

    updateCsvShiftPreview();

    if (!_csvShiftModal) _csvShiftModal = new bootstrap.Modal(document.getElementById('csvShiftModal'));
    _csvShiftModal.show();
  }

  function updateCsvShiftPreview() {
    const input = document.getElementById('csvShiftNewStart');
    const previewEl = document.getElementById('csvShiftPreview');
    if (!input.value || !_pendingCsvData) { previewEl.textContent = ''; return; }
    const newStart = new Date(input.value);
    const schedules = _pendingCsvData.schedules;
    const deviceNames = Object.keys(schedules).join(', ');
    const allTimes = Object.values(schedules).flat().map(e => e.time);
    const tMin = allTimes.length ? Math.min(...allTimes) : 0;
    const tMax = allTimes.length ? Math.max(...allTimes) : 0;
    const runDuration = tMax - tMin;   // actual schedule span, unchanged by shift
    // Delay from now (what the schedule engine will actually wait before first event)
    const desiredDelayS = Math.max(0, Math.round((newStart.getTime() - Date.now()) / 1000));
    const endTime = new Date(newStart.getTime() + runDuration * 1000);
    previewEl.textContent = `Devices: ${deviceNames} · Start: ${newStart.toLocaleTimeString()} · End: ${endTime.toLocaleTimeString()} · Delay: +${_formatDuration(desiredDelayS)} · Run: ${_formatDuration(runDuration)}`;
  }

  function _formatDuration(seconds) {
    if (seconds < 0) return `−${_formatDuration(-seconds)}`;
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = Math.floor(seconds % 60);
    if (h > 0) return `${h}h ${m}m`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
  }

  function applyCsvNoShift() {
    if (!_pendingCsvData) return;
    _finishCsvApply(_pendingCsvData.schedules);
  }

  function applyCsvWithShift() {
    if (!_pendingCsvData) return;
    const input = document.getElementById('csvShiftNewStart');
    if (!input.value) { alert('Please enter a new start time.'); return; }
    const newStart = new Date(input.value);
    // desiredDelay = seconds from NOW until the first event should fire.
    // We compute the delta from Date.now(), not from global_start_iso, because
    // the schedule engine uses elapsed = time.time() - start_time (not an absolute clock).
    // Using global_start_iso (which can be hours in the past) would produce a delay of hours.
    const allTimes = Object.values(_pendingCsvData.schedules).flat().map(e => e.time);
    const tMin = allTimes.length ? Math.min(...allTimes) : 0;
    // Round to whole seconds so floating-point drift doesn't produce e.g. "00:10:00.550"
    const desiredDelayS = Math.max(0, Math.round((newStart.getTime() - Date.now()) / 1000));
    const shift = desiredDelayS - tMin;  // normalise tMin to become desiredDelayS
    const shifted = {};
    for (const [dev, schedule] of Object.entries(_pendingCsvData.schedules)) {
      shifted[dev] = schedule.map(entry => ({ ...entry, time: entry.time + shift }));
    }
    // global_start_iso = now so that (global_start_iso + shifted_time) = absolute event time
    _finishCsvApply(shifted, new Date().toISOString());
  }

  async function shiftExistingExperiment(expId) {
    // Allow the user to shift the timestamps of an already-saved experiment.
    // If global_start_iso is stored, the virtual global start is derived from it
    // (= reference + tMin), giving the actual current earliest absolute time.
    // Otherwise, fall back to "now + tMin" for experiments without a reference.
    try {
      const resp = await fetch(`/api/experiments/${expId}`);
      if (!resp.ok) { alert('Could not load experiment.'); return; }
      const exp = await resp.json();
      const schedules = Object.fromEntries(
        Object.entries(exp.device_schedules || {}).map(([k, v]) => [k, v.schedule])
      );
      if (Object.keys(schedules).length === 0) {
        alert('This experiment has no device schedules to shift.'); return;
      }
      const allTimes = Object.values(schedules).flat().map(e => e.time);
      const tMin = Math.min(...allTimes);
      let virtualGlobalStart;
      if (exp.global_start_iso) {
        // Actual earliest absolute event = stored reference + tMin offset
        virtualGlobalStart = new Date(new Date(exp.global_start_iso).getTime() + tMin * 1000);
      } else {
        // No reference stored — show "starts now" as a relative placeholder
        virtualGlobalStart = new Date(Date.now() + tMin * 1000);
      }
      _pendingCsvData = {
        schedules,
        global_start_iso: exp.global_start_iso || virtualGlobalStart.toISOString(),
        expId,
      };
      _showCsvShiftModal(virtualGlobalStart);
    } catch (e) {
      alert('Error loading experiment: ' + e.message);
    }
  }

  async function _finishCsvApply(schedules, newGlobalStartIso) {
    if (_csvShiftModal) _csvShiftModal.hide();
    const errEl = document.getElementById('expEditorError');
    const expId = _pendingCsvData && _pendingCsvData.expId;
    // Use the caller-supplied override (from Shift & Apply), otherwise keep original
    const globalStartIso = newGlobalStartIso || (_pendingCsvData && _pendingCsvData.global_start_iso);
    _pendingCsvData = null;

    if (expId) {
      // Persist the (possibly shifted) schedules to the existing experiment.
      // Also persist global_start_iso so the experiment can be exported back to
      // multi-device CSV with correct absolute timestamps.
      try {
        const deviceSchedules = {};
        for (const [name, sched] of Object.entries(schedules)) {
          deviceSchedules[name] = { device_name: name, schedule: sched };
        }
        const body = { device_schedules: deviceSchedules };
        if (globalStartIso) body.global_start_iso = globalStartIso;
        const resp = await fetch(`/api/experiments/${expId}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const data = await resp.json();
        if (!resp.ok || data.error) { errEl.textContent = data.error || 'Save failed'; errEl.classList.remove('d-none'); return; }
        const expResp = await fetch(`/api/experiments/${expId}`);
        const exp = await expResp.json();
        populateEditor(exp);
        // Reload the viewer chart if this experiment is currently displayed there.
        window.expViewerUI?.refreshIfSelected(expId);
        _showAppToast(`Imported ${Object.keys(schedules).length} device schedule(s)`, 'success');
        errEl.classList.add('d-none');
      } catch (e) {
        errEl.textContent = 'Save failed: ' + e.message;
        errEl.classList.remove('d-none');
      }
    } else {
      // New experiment: store global_start_iso so saveExperiment can persist it
      _editorGlobalStartIso = globalStartIso || null;
      _applyCsvSchedules(schedules);
      errEl.classList.add('d-none');
    }
  }

  function _applyCsvSchedules(schedules) {
    let added = 0;
    for (const [deviceName, schedule] of Object.entries(schedules)) {
      if (_editorSchedules[deviceName] === undefined) {
        _addDeviceScheduleRowElement(deviceName, schedule);
      } else {
        const tableEl = document.getElementById(`expDevTable-${_safeId(deviceName)}`);
        if (tableEl) tableEl.innerHTML = _buildScheduleEditHtml(deviceName, schedule);
        const stepsEl = document.getElementById(`expDevSteps-${_safeId(deviceName)}`);
        if (stepsEl) stepsEl.textContent = `${schedule.length} steps`;
      }
      _editorSchedules[deviceName] = schedule;
      added++;
    }
    document.getElementById('noExpDevices').style.display = added > 0 ? 'none' : 'block';
    _showAppToast(`Loaded schedules for ${added} device(s) from CSV`, 'success');
  }

  function _showCsvImportErrors(errEl, data) {
    const rowErrors = data.row_errors || [];
    let msg = data.error || 'Upload failed';
    if (rowErrors.length) {
      msg += '\n' + rowErrors.map(e => `  Row ${e.row}: ${e.message}`).join('\n');
    }
    errEl.style.whiteSpace = 'pre-wrap';
    errEl.textContent = msg;
    errEl.classList.remove('d-none');
  }

  async function handleDeviceCSVFile(deviceName, input) {
    const file = input.files[0];
    if (!file) return;
    const expId = document.getElementById('expEditorId').value;

    // If editing an existing experiment, use the server-side parse endpoint
    // Otherwise parse client-side (for new experiments)
    const formData = new FormData();
    formData.append('file', file);
    formData.append('device_name', deviceName);

    try {
      let schedule;
      if (expId) {
        const resp = await fetch(`/api/experiments/${expId}/import_device_schedule`, {
          method: 'POST', body: formData
        });
        const data = await resp.json();
        if (!resp.ok || data.error) {
          _showAppToast(`CSV parse error: ${data.error}`, 'danger');
          return;
        }
        // Re-fetch full experiment to update editor
        const expResp = await fetch(`/api/experiments/${expId}`);
        const exp = await expResp.json();
        schedule = exp.device_schedules[deviceName]?.schedule || [];
        _showAppToast(`Loaded ${data.count} steps (${data.duration_human}) for ${deviceName}`, 'success');
      } else {
        // New experiment — parse locally via generic schedule endpoint
        const schedForm = new FormData();
        schedForm.append('file', file);
        schedForm.append('device_id', '__new__');
        const resp = await fetch('/api/upload_schedule', { method: 'POST', body: schedForm });
        const data = await resp.json();
        if (data.error) { _showAppToast(`CSV parse error: ${data.error}`, 'danger'); return; }
        schedule = data.schedule;
        _showAppToast(`Parsed ${data.count} steps for ${deviceName}`, 'success');
      }

      _editorSchedules[deviceName] = schedule;
      // Update editable table
      const tableEl = document.getElementById(`expDevTable-${_safeId(deviceName)}`);
      if (tableEl) tableEl.innerHTML = _buildScheduleEditHtml(deviceName, schedule);
      const stepsEl = document.getElementById(`expDevSteps-${_safeId(deviceName)}`);
      if (stepsEl) stepsEl.textContent = `${schedule.length} steps`;
    } catch (e) {
      _showAppToast('Upload failed: ' + e.message, 'danger');
    }

    input.value = '';
  }

  function removeDeviceRow(deviceName) {
    delete _editorSchedules[deviceName];
    const row = document.getElementById(`expDevRow-${_safeId(deviceName)}`);
    if (row) row.remove();
    const expId = document.getElementById('expEditorId').value;
    if (expId) {
      // Also remove from server if editing existing experiment
      fetch(`/api/experiments/${expId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ device_schedules: Object.fromEntries(
          Object.entries(_editorSchedules).map(([n, s]) => [n, {device_name:n, schedule:s}])
        )}),
      });
    }
    const container = document.getElementById('expDeviceSchedules');
    if (container && container.children.length === 0) {
      document.getElementById('noExpDevices').style.display = 'block';
    }
  }

  // ── Save Experiment ───────────────────────────────────────────────────────

  async function saveExperiment() {
    const name = document.getElementById('expName').value.trim();
    const errEl = document.getElementById('expEditorError');
    if (!name) { _showEditorError(errEl, 'Experiment name is required'); return; }
    errEl?.classList.add('d-none');

    const expId = document.getElementById('expEditorId').value;
    const metadata = {
      name,
      operator: document.getElementById('expOperator').value.trim(),
      notes: document.getElementById('expNotes').value.trim(),
    };

    try {
      if (expId) {
        // Update existing
        const deviceSchedules = {};
        for (const [n, s] of Object.entries(_editorSchedules)) {
          deviceSchedules[n] = { device_name: n, schedule: s };
        }
        const putBody = { ...metadata, device_schedules: deviceSchedules };
        if (_editorGlobalStartIso) putBody.global_start_iso = _editorGlobalStartIso;
        await fetch(`/api/experiments/${expId}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(putBody),
        });
        _showAppToast(`Experiment "${name}" saved`, 'success');
      } else {
        // Create new, then assign ALL schedules in a single PUT
        const resp = await fetch('/api/experiments', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(metadata),
        });
        const data = await resp.json();
        const newId = data.experiment_id;
        const allSchedules = {};
        for (const [deviceName, schedule] of Object.entries(_editorSchedules)) {
          if (schedule.length > 0) {
            allSchedules[deviceName] = { device_name: deviceName, schedule };
          }
        }
        if (Object.keys(allSchedules).length > 0) {
          const putBody = { device_schedules: allSchedules };
          if (_editorGlobalStartIso) putBody.global_start_iso = _editorGlobalStartIso;
          await fetch(`/api/experiments/${newId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(putBody),
          });
        }
        _showAppToast(`Experiment "${name}" created`, 'success');
      }
      await refreshExperimentList();
      bootstrap.Modal.getInstance(document.getElementById('experimentEditorModal'))?.hide();
    } catch (e) {
      _showEditorError(errEl, 'Save failed: ' + e.message);
    }
  }

  function exportExperimentJson() {
    const expId = document.getElementById('expEditorId').value;
    if (expId) window.open(`/api/experiments/${expId}/export.json`, '_blank');
  }

  async function deleteExperiment(experimentId) {
    const exp = _experiments.find(e => e.experiment_id === experimentId);
    if (!confirm(`Delete experiment "${exp?.name || experimentId}"? This cannot be undone.`)) return;
    const resp = await fetch(`/api/experiments/${experimentId}`, { method: 'DELETE' });
    const data = await resp.json();
    if (data.success) {
      await refreshExperimentList();
      _showAppToast('Experiment deleted', 'info');
    } else {
      _showAppToast('Delete failed: ' + data.error, 'danger');
    }
  }

  // ── Run Experiment (pre-run check) ────────────────────────────────────────

  function runExperiment(experimentId) {
    _preRunExpId = experimentId;
    // Request pre-run check from server
    const sock = _getSocket();
    sock.emit('pre_run_check', { experiment_id: experimentId });

    // Show modal while waiting
    new bootstrap.Modal(document.getElementById('preRunModal')).show();
    document.getElementById('preRunExpId').value = experimentId;
    document.getElementById('preRunDeviceChecks').innerHTML =
      '<div class="text-muted small"><i class="fa fa-spinner fa-spin me-1"></i>Checking…</div>';
    document.getElementById('preRunRelayStates').innerHTML = '';
    document.getElementById('preRunWarnings').classList.add('d-none');
    document.getElementById('preRunConfirmRelays').checked = false;
    document.getElementById('preRunConfirmReady').checked = false;
    document.getElementById('preRunStartBtn').disabled = true;
  }

  // Called from app.js socket event
  function handlePreRunCheckResult(result) {
    if (!result.success) { _showAppToast(result.error, 'danger'); return; }

    // Device checks
    const dcEl = document.getElementById('preRunDeviceChecks');
    if (dcEl) {
      dcEl.innerHTML = result.device_checks.map(d => {
        const icon = d.connected ? 'fa-check-circle check-icon-ok'
          : d.found ? 'fa-exclamation-circle check-icon-warn'
          : 'fa-times-circle check-icon-fail';
        const statusText = d.connected ? 'Connected' : d.found ? 'Not connected' : 'Not found in system';
        return `<div class="check-item">
          <i class="fa ${icon}"></i>
          <span class="fw-semibold">${_escHtml(d.device_name)}</span>
          <span class="text-muted">${statusText}</span>
          <span class="text-muted ms-auto">${d.steps} steps</span>
        </div>`;
      }).join('') || '<div class="text-muted small">No devices in experiment.</div>';
    }

    // Relay states
    const rsEl = document.getElementById('preRunRelayStates');
    if (rsEl) {
      if (result.relay_states.length === 0) {
        rsEl.innerHTML = '<div class="text-muted small">No relay peripherals connected.</div>';
      } else {
        rsEl.innerHTML = result.relay_states.map(rs => {
          const channels = (rs.states || []).map((state, i) => {
            const lbl = (rs.channel_labels || [])[i] || `CH${i}`;
            const on = state === true;
            return `<span class="relay-btn ${on ? 'on' : ''}" style="cursor:default;margin-right:0.3rem">
              ${_escHtml(lbl)}: ${on ? 'OPEN' : 'CLOSED'}
            </span>`;
          }).join('');
          return `<div class="check-item flex-column align-items-start">
            <div class="fw-semibold mb-1">${_escHtml(rs.name)}</div>
            <div>${channels}</div>
          </div>`;
        }).join('');
      }
    }

    // Warnings
    const warnEl = document.getElementById('preRunWarnings');
    const warnListEl = document.getElementById('preRunWarningList');
    if (result.warnings && result.warnings.length > 0) {
      warnEl?.classList.remove('d-none');
      if (warnListEl) {
        warnListEl.innerHTML = result.warnings.map(w =>
          `<div class="check-item"><i class="fa fa-triangle-exclamation check-icon-warn me-1"></i>${_escHtml(w)}</div>`
        ).join('');
      }
    } else {
      warnEl?.classList.add('d-none');
    }

    // Enable confirmation checkboxes → they unlock start button
    document.getElementById('preRunConfirmRelays').addEventListener('change', _checkPreRunReady);
    document.getElementById('preRunConfirmReady').addEventListener('change', _checkPreRunReady);
  }

  function _checkPreRunReady() {
    const r1 = document.getElementById('preRunConfirmRelays')?.checked;
    const r2 = document.getElementById('preRunConfirmReady')?.checked;
    const startBtn = document.getElementById('preRunStartBtn');
    if (startBtn) startBtn.disabled = !(r1 && r2);
  }

  function confirmStart() {
    const expId = document.getElementById('preRunExpId').value;
    const operator = document.getElementById('preRunOperator').value.trim();
    bootstrap.Modal.getInstance(document.getElementById('preRunModal'))?.hide();
    _getSocket().emit('start_experiment', { experiment_id: expId, operator });
  }

  function stopExperiment() {
    if (!confirm('Stop the running experiment? Logging will be finalized.')) return;
    _getSocket().emit('stop_experiment', {});
  }

  // ── Import Experiment ─────────────────────────────────────────────────────

  function openImportExperiment() {
    document.getElementById('importExpFile').value = '';
    document.getElementById('importExpError')?.classList.add('d-none');
    new bootstrap.Modal(document.getElementById('importExperimentModal')).show();
  }

  async function doImportExperiment() {
    const file = document.getElementById('importExpFile')?.files[0];
    const errEl = document.getElementById('importExpError');
    if (!file) { _showEditorError(errEl, 'Please select a JSON file'); return; }

    const formData = new FormData();
    formData.append('file', file);
    try {
      const resp = await fetch('/api/import_experiment', { method: 'POST', body: formData });
      const data = await resp.json();
      if (!resp.ok || !data.success) {
        _showEditorError(errEl, data.error || 'Import failed');
        return;
      }
      bootstrap.Modal.getInstance(document.getElementById('importExperimentModal'))?.hide();
      await refreshExperimentList();
      _showAppToast(`Imported "${data.name}"`, 'success');
    } catch (e) {
      _showEditorError(errEl, 'Import failed: ' + e.message);
    }
  }

  // ── Data Browser ──────────────────────────────────────────────────────────

  async function openDataBrowser() {
    new bootstrap.Modal(document.getElementById('dataBrowserModal')).show();
    await refreshDataBrowser();
  }

  async function refreshDataBrowser() {
    const content = document.getElementById('dataBrowserContent');
    if (!content) return;
    content.innerHTML = '<div class="text-muted text-center py-4"><i class="fa fa-spinner fa-spin me-2"></i>Loading…</div>';

    try {
      const resp = await fetch('/api/data/list');
      const files = await resp.json();
      if (files.length === 0) {
        content.innerHTML = '<div class="text-muted text-center py-4">No data files found.</div>';
        return;
      }
      content.innerHTML = files.map(entry => {
        if (entry.type === 'directory') {
          const fileRows = (entry.files || []).map(f =>
            `<div class="data-file-row">
              <i class="fa fa-file-csv text-muted"></i>
              <span class="data-file-name">${_escHtml(f.filename)}</span>
              <span class="data-file-size">${f.size_human}</span>
              <a class="btn btn-xs btn-outline-secondary" href="/api/data/download?path=${encodeURIComponent(f.path)}" download>
                <i class="fa fa-download"></i>
              </a>
            </div>`
          ).join('');
          return `<div class="data-dir-card">
            <div class="data-dir-header">
              <div>
                <span class="fw-semibold">${_escHtml(entry.experiment_name)}</span>
                <span class="text-muted small ms-2">${_escHtml(entry.operator || '')}</span>
                <span class="text-muted small ms-2">${_escHtml(entry.started_at?.slice(0,19).replace('T',' ') || '')}</span>
              </div>
              <div class="d-flex align-items-center gap-2">
                <span class="text-muted small">${entry.file_count} files · ${entry.total_size_human}</span>
                <a class="btn btn-sm btn-outline-info" href="/api/data/zip/${encodeURIComponent(entry.dir_name)}" download>
                  <i class="fa fa-file-zipper me-1"></i>Download All
                </a>
              </div>
            </div>
            ${fileRows}
          </div>`;
        } else {
          return `<div class="data-file-row">
            <i class="fa fa-file-csv text-muted"></i>
            <span class="data-file-name">${_escHtml(entry.name)}</span>
            <span class="data-file-size">${entry.size_human}</span>
            <a class="btn btn-xs btn-outline-secondary" href="/api/data/download?path=${encodeURIComponent(entry.path)}" download>
              <i class="fa fa-download"></i>
            </a>
          </div>`;
        }
      }).join('');
    } catch (e) {
      content.innerHTML = `<div class="text-danger text-center py-4">Error loading file list: ${e.message}</div>`;
    }
  }

  // ── Socket wiring ─────────────────────────────────────────────────────────
  // Hook into the app's socket — the socket is created in app.js but we need it here.
  // We do this after DOMContentLoaded so app.js has already run.
  document.addEventListener('DOMContentLoaded', () => {
    // The `socket` variable is inside the app IIFE; we expose it via a tiny global:
    if (window._appSocket) {
      window._appSocket.on('pre_run_check_result', handlePreRunCheckResult);
    }
  });

  function _getSocket() {
    return window._appSocket;
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  function _escHtml(str) {
    if (!str && str !== 0) return '';
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
      .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
  }

  function _safeId(str) {
    return String(str).replace(/[^a-zA-Z0-9_-]/g, '_');
  }

  function _fmtDurationExp(sec) {
    if (!sec && sec !== 0) return '—';
    sec = Math.max(0, sec);
    const d = Math.floor(sec / 86400);
    const h = Math.floor((sec % 86400) / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = Math.floor(sec % 60);
    if (d > 0) return `${d}d ${h}h ${m}m`;
    if (h > 0) return `${h}h ${m}m ${s}s`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
  }

  function _showEditorError(el, msg) {
    if (!el) return;
    el.textContent = msg;
    el.classList.remove('d-none');
  }

  function _showAppToast(msg, type) {
    // Delegate to app.js internal toast if available, otherwise use Bootstrap
    const container = document.getElementById('toastContainer');
    if (!container) return;
    const icons = { success:'fa-check-circle', danger:'fa-exclamation-circle', warning:'fa-triangle-exclamation', info:'fa-info-circle' };
    const colors = { success:'#3fb950', danger:'#f85149', warning:'#d29922', info:'#58a6ff' };
    const el = document.createElement('div');
    el.className = 'toast ec-toast align-items-center show';
    el.innerHTML = `<div class="d-flex align-items-center gap-2 p-2">
      <i class="fa ${icons[type]||icons.info}" style="color:${colors[type]||colors.info}"></i>
      <div class="toast-body p-0 flex-grow-1">${_escHtml(msg)}</div>
      <button type="button" class="btn-close btn-close-white ms-2" onclick="this.closest('.toast').remove()"></button>
    </div>`;
    container.appendChild(el);
    setTimeout(() => el.remove(), 5000);
  }

  // ── Public ────────────────────────────────────────────────────────────────
  return {
    handleFullState,
    renderExperimentList,
    updateRunningBanner,
    handlePreRunCheckResult,
    openNewExperiment,
    openEditor,
    populateEditor,
    addDeviceScheduleRow,
    confirmAddDevice,
    cancelAddDevice,
    importDeviceCSV,
    handleDeviceCSVFile,
    showCsvTemplate,
    downloadCsvTemplate,
    importMultiDeviceCSV,
    handleMultiDeviceCSVFile,
    updateCsvShiftPreview,
    applyCsvNoShift,
    applyCsvWithShift,
    removeDeviceRow,
    addScheduleRow,
    _onScheduleChange,
    _deleteScheduleRow,
    saveExperiment,
    exportExperimentJson,
    deleteExperiment,
    runExperiment,
    confirmStart,
    stopExperiment,
    openImportExperiment,
    doImportExperiment,
    openDataBrowser,
    refreshDataBrowser,
    refreshExperimentList,
    shiftExistingExperiment,
  };

})();

// Make expUI accessible as window.expUI so the app IIFE's full_state handler
// (which uses window.expUI) can find it. (const at script top-level is a global
// lexical binding but NOT a property of window.)
window.expUI = expUI;

function clearServerLogs() {
  const pane = document.getElementById('serverLogPane');
  if (pane) pane.innerHTML = '';
}

function _applyLogFilter(line) {
  const textFilter = (document.getElementById('logFilterInput')?.value || '').toLowerCase();
  const levelMatch = (_logLevelFilter === 'all') || (line.dataset.level === _logLevelFilter);
  const textMatch  = !textFilter || line.dataset.msg.includes(textFilter);
  line.style.display = (levelMatch && textMatch) ? '' : 'none';
}

function filterServerLogs() {
  document.querySelectorAll('#serverLogPane > div').forEach(_applyLogFilter);
}

function setLogLevel(btn) {
  _logLevelFilter = btn.dataset.level;
  document.querySelectorAll('.log-filter-btn').forEach(b => b.classList.toggle('active', b === btn));
  filterServerLogs();
}
