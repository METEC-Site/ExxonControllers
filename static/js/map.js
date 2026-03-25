/**
 * map.js — Leaflet map panel for ExxonController
 * Displays Alicat flow controller geotags on a satellite map.
 * Supports custom image overlays aligned by corner GPS coordinates.
 */

'use strict';

window.ecMap = (() => {

  // ── State ─────────────────────────────────────────────────────────────────
  let _map = null;
  let _markers = {};        // deviceId -> L.marker
  let _devices = {};        // deviceId -> device state dict (from app._devices)
  let _overlayLayers = {};  // overlayId -> L.imageOverlay
  let _currentConfig = { overlays: [] };  // mirrors server's map_config.json
  let _initialized = false;
  let _saveViewTimer = null;
  let _editingOverlayId = null;

  // Drag-on-map editing state
  let _dragHandles       = {};   // { nw, ne, sw, se, center } L.Marker
  let _dragOverlayId     = null;
  let _dragOrigBounds    = null; // saved bounds for cancel
  let _mapEditControl    = null; // floating Save/Cancel Leaflet control

  // ── Tile layer URLs — routed through local Flask proxy for offline caching ──
  // Proxy serves from disk cache; on miss, fetches from Esri and saves to disk.
  // Imagery Hybrid = World Imagery (JPEG) + World Boundaries & Places (PNG labels).
  const IMAGERY_TILE = '/tiles/imagery/{z}/{y}/{x}';
  const LABELS_TILE  = '/tiles/labels/{z}/{y}/{x}';
  const TILE_ATTR = 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community';

  const STREETS_TILE = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
  const STREETS_ATTR = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>';

  // ── Tile cache state ───────────────────────────────────────────────────────
  let _cacheStatus = { tile_count: 0, size_mb: 0, active: false };

  // ── Fetch config from server ──────────────────────────────────────────────
  async function _fetchConfig() {
    try {
      const resp = await fetch('/api/map_config');
      if (resp.ok) {
        _currentConfig = await resp.json();
        if (!_currentConfig.overlays) _currentConfig.overlays = [];
      }
    } catch (e) { /* non-fatal — use defaults */ }
    return _currentConfig;
  }

  async function _saveConfig() {
    try {
      await fetch('/api/map_config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(_currentConfig),
      });
    } catch (e) { /* non-fatal */ }
  }

  // ── Auto-save view (center + zoom) on moveend, debounced 1 s ─────────────
  function _onMoveEnd() {
    clearTimeout(_saveViewTimer);
    _saveViewTimer = setTimeout(() => {
      if (!_map) return;
      const c = _map.getCenter();
      _currentConfig.center_lat = c.lat;
      _currentConfig.center_lon = c.lng;
      _currentConfig.zoom = _map.getZoom();
      _saveConfig();
    }, 1000);
  }

  // ── Initialize the Leaflet map ────────────────────────────────────────────
  function _init() {
    if (_initialized) return;
    const container = document.getElementById('mapContainer');
    if (!container || typeof L === 'undefined') return;

    const centerLat = _currentConfig.center_lat ?? 39.8283;
    const centerLon = _currentConfig.center_lon ?? -98.5795;
    const zoom      = _currentConfig.zoom       ?? 4;

    _map = L.map('mapContainer', {
      center: [centerLat, centerLon],
      zoom,
      zoomControl: true,
    });

    // Imagery Hybrid = satellite photos + label/boundary overlay
    const imageryBase   = L.tileLayer(IMAGERY_TILE, { attribution: TILE_ATTR, maxZoom: 20 });
    const imageryLabels = L.tileLayer(LABELS_TILE,  { maxZoom: 20, opacity: 1 });
    const imageryHybrid = L.layerGroup([imageryBase, imageryLabels]);

    const streets = L.tileLayer(STREETS_TILE, {
      attribution: STREETS_ATTR,
      maxZoom: 19,
    });

    imageryHybrid.addTo(_map);

    L.control.layers(
      { 'Imagery Hybrid': imageryHybrid, 'Street Map': streets },
      {},
      { position: 'topright' }
    ).addTo(_map);

    // Auto-save center/zoom whenever the user finishes panning or zooming
    _map.on('moveend', _onMoveEnd);

    _initialized = true;

    // Apply any saved overlays
    (_currentConfig.overlays || []).forEach(ov => _applyOverlay(ov));

    // Force Leaflet to recalculate its size in case the container was reflowed
    _map.invalidateSize();
  }

  // ── Device marker management ──────────────────────────────────────────────

  function _makeIcon(color) {
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="24" height="32" viewBox="0 0 24 32">
      <path d="M12 0C5.373 0 0 5.373 0 12c0 9 12 20 12 20S24 21 24 12C24 5.373 18.627 0 12 0z"
            fill="${color}" stroke="#fff" stroke-width="1.5"/>
      <circle cx="12" cy="12" r="5" fill="#fff" opacity="0.9"/>
    </svg>`;
    return L.divIcon({
      html: svg,
      className: '',
      iconSize: [24, 32],
      iconAnchor: [12, 32],
      popupAnchor: [0, -32],
    });
  }

  let _iconConnected, _iconDisconnected, _iconDisabled;

  function _getIcon(d) {
    if (!_iconConnected) {
      _iconConnected    = _makeIcon('#3fb950');
      _iconDisconnected = _makeIcon('#ff7b72');
      _iconDisabled     = _makeIcon('#8b949e');
    }
    if (d.disabled) return _iconDisabled;
    return d.connected ? _iconConnected : _iconDisconnected;
  }

  function _buildPopupHtml(d) {
    const gas = d.gas_number != null ? `Gas #${d.gas_number}` : '';
    const reading = d.last_reading;
    const mf  = reading?.mass_flow  != null ? `${Number(reading.mass_flow).toFixed(2)} SLPM` : '—';
    const sp  = reading?.setpoint   != null ? `${Number(reading.setpoint).toFixed(2)} SLPM`  : '—';
    const tmp = reading?.temperature != null ? `${Number(reading.temperature).toFixed(1)} °C` : '—';
    const prs = reading?.pressure    != null ? `${Number(reading.pressure).toFixed(2)} psia`  : '—';
    const statusBadge = d.disabled
      ? `<span style="color:#8b949e">⊘ Disabled</span>`
      : d.connected
        ? `<span style="color:#3fb950">● Connected</span>`
        : `<span style="color:#ff7b72">○ Disconnected</span>`;
    return `
      <div style="min-width:180px;font-family:monospace;font-size:0.82rem">
        <div style="font-weight:700;font-size:0.95rem;margin-bottom:4px">${d.device_name}</div>
        <div style="margin-bottom:4px">${statusBadge} &nbsp; ${gas}</div>
        <table style="width:100%;border-collapse:collapse">
          <tr><td style="color:#8b949e;padding-right:6px">Mass Flow</td><td>${mf}</td></tr>
          <tr><td style="color:#8b949e;padding-right:6px">Setpoint</td><td>${sp}</td></tr>
          <tr><td style="color:#8b949e;padding-right:6px">Temp</td><td>${tmp}</td></tr>
          <tr><td style="color:#8b949e;padding-right:6px">Pressure</td><td>${prs}</td></tr>
          ${d.serial_number ? `<tr><td style="color:#8b949e;padding-right:6px">S/N</td><td>${d.serial_number}</td></tr>` : ''}
          <tr><td style="color:#8b949e;padding-right:6px">Lat/Lon</td>
              <td>${d.lat != null ? d.lat.toFixed(5) : '—'}, ${d.lon != null ? d.lon.toFixed(5) : '—'}</td></tr>
        </table>
        <div style="margin-top:6px;text-align:center">
          <button onclick="ecMap.highlightDevice('${d.device_id}')"
                  style="font-size:0.75rem;padding:2px 8px;cursor:pointer;
                         background:#1f6feb;border:none;color:#fff;border-radius:4px">
            Highlight in panel
          </button>
        </div>
      </div>`;
  }

  function _placeOrUpdateMarker(d) {
    if (!_map) return;
    if (d.lat == null || d.lon == null) {
      if (_markers[d.device_id]) {
        _markers[d.device_id].remove();
        delete _markers[d.device_id];
      }
      return;
    }

    const icon = _getIcon(d);
    if (_markers[d.device_id]) {
      _markers[d.device_id]
        .setLatLng([d.lat, d.lon])
        .setIcon(icon)
        .getPopup()?.setContent(_buildPopupHtml(d));
    } else {
      const marker = L.marker([d.lat, d.lon], { icon })
        .bindPopup(_buildPopupHtml(d), { maxWidth: 240 })
        .addTo(_map);

      marker.on('click', () => {
        highlightDevice(d.device_id);
        marker.openPopup();
      });

      _markers[d.device_id] = marker;
    }

    // Pulse the icon slowly when this device has an active setpoint (> 0 SLPM)
    const el = _markers[d.device_id].getElement();
    if (el) {
      const isFlowing = !d.disabled && d.connected && (d.last_reading?.setpoint ?? 0) > 0;
      el.classList.toggle('marker-active-flow', isFlowing);
    }
  }

  function _removeMarker(deviceId) {
    if (_markers[deviceId]) {
      _markers[deviceId].remove();
      delete _markers[deviceId];
    }
  }

  // ── Public: update map from a full device list ────────────────────────────
  function updateDevices(deviceList) {
    _devices = {};
    (deviceList || []).forEach(d => { _devices[d.device_id] = d; });
    if (_initialized) _redrawAllMarkers();
  }

  function _redrawAllMarkers() {
    Object.keys(_markers).forEach(id => {
      if (!_devices[id]) _removeMarker(id);
    });
    Object.values(_devices).forEach(d => _placeOrUpdateMarker(d));
  }

  // ── Public: update a single device (from device_update event) ────────────
  function updateDevice(d) {
    if (!_initialized) return;
    _devices[d.device_id] = d;
    if (d.lat == null || d.lon == null) {
      _removeMarker(d.device_id);
    } else {
      _placeOrUpdateMarker(d);
      const m = _markers[d.device_id];
      if (m && m.isPopupOpen()) m.setPopupContent(_buildPopupHtml(d));
    }
  }

  // ── Public: highlight a device card in the sidebar ───────────────────────
  function highlightDevice(deviceId) {
    document.querySelectorAll('.device-card.map-highlight').forEach(el => {
      el.classList.remove('map-highlight');
    });
    const card = document.getElementById(`device-card-${deviceId}`);
    if (card) {
      card.classList.add('map-highlight');
      card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      setTimeout(() => card.classList.remove('map-highlight'), 3000);
    }
  }

  // ── Public: fit map to current markers ───────────────────────────────────
  function fitToMarkers() {
    if (!_map) return;
    const coords = Object.values(_markers).map(m => m.getLatLng());
    if (coords.length === 0) return;
    if (coords.length === 1) {
      _map.setView(coords[0], 16);
    } else {
      _map.fitBounds(L.latLngBounds(coords), { padding: [40, 40] });
    }
  }

  // ── Image Overlays ────────────────────────────────────────────────────────

  function _applyOverlay(ov) {
    if (!_map || !ov.url) return;
    const bounds = [[ov.nlat, ov.wlon], [ov.slat, ov.elon]];
    const layer = L.imageOverlay(ov.url, bounds, { opacity: ov.opacity ?? 0.7 })
      .addTo(_map);
    _overlayLayers[ov.id] = layer;
  }

  function _renderOverlayList() {
    const container = document.getElementById('overlayList');
    if (!container) return;
    const overlays = _currentConfig.overlays || [];
    if (overlays.length === 0) {
      container.innerHTML = '<div class="text-muted small">No overlays added yet.</div>';
      return;
    }
    container.innerHTML = overlays.map(ov => `
      <div class="d-flex align-items-center justify-content-between mb-2 p-2"
           style="background:#0d1117;border:1px solid #30363d;border-radius:6px">
        <div>
          <div class="fw-semibold small">${ov.label || ov.filename || ov.id}</div>
          <div class="text-muted" style="font-size:0.72rem">
            TL: ${Number(ov.nlat).toFixed(5)}, ${Number(ov.wlon).toFixed(5)} |
            BR: ${Number(ov.slat).toFixed(5)}, ${Number(ov.elon).toFixed(5)} |
            Opacity: ${ov.opacity}
          </div>
        </div>
        <div class="d-flex gap-1">
          <button class="btn btn-xs btn-outline-info"
                  onclick="ecMap.enableOverlayDrag('${ov.id}')" title="Drag to align on map">
            <i class="fa fa-up-down-left-right"></i>
          </button>
          <button class="btn btn-xs btn-outline-warning"
                  onclick="ecMap.startEditOverlay('${ov.id}')" title="Edit coordinates">
            <i class="fa fa-pen"></i>
          </button>
          <button class="btn btn-xs btn-outline-secondary"
                  onclick="ecMap.toggleOverlay('${ov.id}')" title="Toggle visibility">
            <i class="fa fa-eye"></i>
          </button>
          <button class="btn btn-xs btn-outline-danger"
                  onclick="ecMap.deleteOverlay('${ov.id}')" title="Remove overlay">
            <i class="fa fa-trash"></i>
          </button>
        </div>
      </div>`).join('');
  }

  function openOverlayManager() {
    cancelEditOverlay();  // collapse any open edit form
    _renderOverlayList();
    ['overlayLabel','overlayOpacity','overlayNLat','overlayWLon','overlaySLat','overlayELon'].forEach(id => {
      const el = document.getElementById(id);
      if (el && id === 'overlayOpacity') el.value = '0.7';
      else if (el) el.value = '';
    });
    const fi = document.getElementById('overlayFileInput');
    if (fi) fi.value = '';
    document.getElementById('overlayAddError')?.classList.add('d-none');
    new bootstrap.Modal(document.getElementById('overlayManagerModal')).show();
  }

  async function addOverlay() {
    const errEl = document.getElementById('overlayAddError');
    const fileInput = document.getElementById('overlayFileInput');
    const label   = document.getElementById('overlayLabel').value.trim();
    const opacity = parseFloat(document.getElementById('overlayOpacity').value) || 0.7;
    const nlat = parseFloat(document.getElementById('overlayNLat').value);
    const wlon = parseFloat(document.getElementById('overlayWLon').value);
    const slat = parseFloat(document.getElementById('overlaySLat').value);
    const elon = parseFloat(document.getElementById('overlayELon').value);

    if (!fileInput?.files[0]) {
      _showOverlayError(errEl, 'Please select an image file.'); return;
    }
    if ([nlat, wlon, slat, elon].some(v => isNaN(v))) {
      _showOverlayError(errEl, 'All four corner coordinates are required.'); return;
    }
    if (nlat <= slat) {
      _showOverlayError(errEl, 'Top-Left Lat must be greater than Bottom-Right Lat.'); return;
    }

    errEl?.classList.add('d-none');

    const fd = new FormData();
    fd.append('file', fileInput.files[0]);
    let uploadResp;
    try {
      const r = await fetch('/api/map_config/upload_image', { method: 'POST', body: fd });
      uploadResp = await r.json();
      if (!uploadResp.success) throw new Error(uploadResp.error || 'Upload failed');
    } catch (e) {
      _showOverlayError(errEl, `Upload error: ${e.message}`); return;
    }

    const ov = {
      id: `ov_${Date.now()}`,
      url: uploadResp.url,
      filename: uploadResp.filename,
      label: label || uploadResp.filename,
      nlat, wlon, slat, elon, opacity,
    };
    _currentConfig.overlays.push(ov);
    _applyOverlay(ov);
    await _saveConfig();

    if (_map) {
      _map.fitBounds([[ov.nlat, ov.wlon], [ov.slat, ov.elon]], { padding: [20, 20] });
    }

    bootstrap.Modal.getInstance(document.getElementById('overlayManagerModal'))?.hide();
  }

  function toggleOverlay(overlayId) {
    const layer = _overlayLayers[overlayId];
    if (!layer || !_map) return;
    if (_map.hasLayer(layer)) {
      _map.removeLayer(layer);
    } else {
      _map.addLayer(layer);
    }
  }

  async function deleteOverlay(overlayId) {
    const ov = (_currentConfig.overlays || []).find(o => o.id === overlayId);
    if (!ov) return;
    if (!confirm(`Remove overlay "${ov.label || overlayId}"?`)) return;

    if (_overlayLayers[overlayId]) {
      _overlayLayers[overlayId].remove();
      delete _overlayLayers[overlayId];
    }

    if (ov.filename) {
      try {
        await fetch('/api/map_config/delete_image', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ filename: ov.filename }),
        });
      } catch (e) { /* non-fatal */ }
    }

    _currentConfig.overlays = _currentConfig.overlays.filter(o => o.id !== overlayId);
    await _saveConfig();
    _renderOverlayList();
  }

  // ── Drag-on-map overlay editing ───────────────────────────────────────────

  function _makeCornerIcon() {
    return L.divIcon({
      html: '<div style="width:14px;height:14px;background:#f0883e;border:2px solid #fff;border-radius:3px;box-shadow:0 1px 4px rgba(0,0,0,0.6);cursor:crosshair"></div>',
      className: '',
      iconSize: [14, 14],
      iconAnchor: [7, 7],
    });
  }

  function _makeCenterIcon() {
    return L.divIcon({
      html: '<div style="width:22px;height:22px;background:rgba(240,136,62,0.85);border:2px solid #fff;border-radius:50%;display:flex;align-items:center;justify-content:center;box-shadow:0 1px 4px rgba(0,0,0,0.6);cursor:move;font-size:13px;line-height:1">✥</div>',
      className: '',
      iconSize: [22, 22],
      iconAnchor: [11, 11],
    });
  }

  function enableOverlayDrag(overlayId) {
    // Close the overlay manager modal so the map is fully accessible
    bootstrap.Modal.getInstance(document.getElementById('overlayManagerModal'))?.hide();

    _disableOverlayDrag(); // clear any existing handles

    const ov = (_currentConfig.overlays || []).find(o => o.id === overlayId);
    const layer = _overlayLayers[overlayId];
    if (!ov || !layer || !_map) return;

    _dragOverlayId  = overlayId;
    _dragOrigBounds = { nlat: ov.nlat, wlon: ov.wlon, slat: ov.slat, elon: ov.elon };

    const ci = _makeCornerIcon();
    const mi = _makeCenterIcon();

    const nw = L.marker([ov.nlat, ov.wlon], { draggable: true, icon: ci, zIndexOffset: 1000 }).addTo(_map);
    const ne = L.marker([ov.nlat, ov.elon], { draggable: true, icon: ci, zIndexOffset: 1000 }).addTo(_map);
    const sw = L.marker([ov.slat, ov.wlon], { draggable: true, icon: ci, zIndexOffset: 1000 }).addTo(_map);
    const se = L.marker([ov.slat, ov.elon], { draggable: true, icon: ci, zIndexOffset: 1000 }).addTo(_map);
    const mv = L.marker(
      [(ov.nlat + ov.slat) / 2, (ov.wlon + ov.elon) / 2],
      { draggable: true, icon: mi, zIndexOffset: 999 }
    ).addTo(_map);

    function _syncLayer() {
      layer.setBounds([[ov.nlat, ov.wlon], [ov.slat, ov.elon]]);
      mv.setLatLng([(ov.nlat + ov.slat) / 2, (ov.wlon + ov.elon) / 2]);
    }

    nw.on('drag', e => { ov.nlat = e.latlng.lat; ov.wlon = e.latlng.lng; ne.setLatLng([ov.nlat, ov.elon]); sw.setLatLng([ov.slat, ov.wlon]); _syncLayer(); });
    ne.on('drag', e => { ov.nlat = e.latlng.lat; ov.elon = e.latlng.lng; nw.setLatLng([ov.nlat, ov.wlon]); se.setLatLng([ov.slat, ov.elon]); _syncLayer(); });
    sw.on('drag', e => { ov.slat = e.latlng.lat; ov.wlon = e.latlng.lng; nw.setLatLng([ov.nlat, ov.wlon]); se.setLatLng([ov.slat, ov.elon]); _syncLayer(); });
    se.on('drag', e => { ov.slat = e.latlng.lat; ov.elon = e.latlng.lng; ne.setLatLng([ov.nlat, ov.elon]); sw.setLatLng([ov.slat, ov.wlon]); _syncLayer(); });

    let _prevCenter = null;
    mv.on('dragstart', e => { _prevCenter = e.target.getLatLng(); });
    mv.on('drag', e => {
      if (!_prevCenter) return;
      const dlat = e.latlng.lat - _prevCenter.lat;
      const dlon = e.latlng.lng - _prevCenter.lng;
      _prevCenter = e.latlng;
      ov.nlat += dlat; ov.slat += dlat;
      ov.wlon += dlon; ov.elon += dlon;
      nw.setLatLng([ov.nlat, ov.wlon]);
      ne.setLatLng([ov.nlat, ov.elon]);
      sw.setLatLng([ov.slat, ov.wlon]);
      se.setLatLng([ov.slat, ov.elon]);
      layer.setBounds([[ov.nlat, ov.wlon], [ov.slat, ov.elon]]);
    });

    _dragHandles = { nw, ne, sw, se, mv };

    // Floating Save / Cancel control on the map
    _mapEditControl = L.control({ position: 'bottomleft' });
    _mapEditControl.onAdd = () => {
      const div = L.DomUtil.create('div');
      div.innerHTML = `
        <div style="background:#161b22;border:1px solid #f0883e;border-radius:8px;
                    padding:8px 14px;display:flex;gap:10px;align-items:center;
                    box-shadow:0 2px 8px rgba(0,0,0,0.6)">
          <span style="color:#f0883e;font-size:0.82rem;font-weight:600">
            ✥ Drag corners or center to align overlay
          </span>
          <button id="_mapDragSave"
                  style="background:#f0883e;border:none;color:#000;border-radius:4px;
                         padding:4px 12px;font-size:0.8rem;cursor:pointer;font-weight:700">
            Save
          </button>
          <button id="_mapDragCancel"
                  style="background:#30363d;border:none;color:#c9d1d9;border-radius:4px;
                         padding:4px 10px;font-size:0.8rem;cursor:pointer">
            Cancel
          </button>
        </div>`;
      L.DomEvent.disableClickPropagation(div);
      return div;
    };
    _mapEditControl.addTo(_map);

    // Wire buttons after the control is in the DOM
    setTimeout(() => {
      document.getElementById('_mapDragSave')?.addEventListener('click', _saveOverlayDrag);
      document.getElementById('_mapDragCancel')?.addEventListener('click', _cancelOverlayDrag);
    }, 0);
  }

  function _disableOverlayDrag() {
    Object.values(_dragHandles).forEach(m => m.remove());
    _dragHandles    = {};
    _dragOverlayId  = null;
    _dragOrigBounds = null;
    if (_mapEditControl) { _mapEditControl.remove(); _mapEditControl = null; }
  }

  async function _saveOverlayDrag() {
    await _saveConfig();
    _disableOverlayDrag();
  }

  function _cancelOverlayDrag() {
    if (_dragOrigBounds && _dragOverlayId) {
      const ov = (_currentConfig.overlays || []).find(o => o.id === _dragOverlayId);
      if (ov) {
        Object.assign(ov, _dragOrigBounds);
        const layer = _overlayLayers[_dragOverlayId];
        if (layer) layer.setBounds([[ov.nlat, ov.wlon], [ov.slat, ov.elon]]);
      }
    }
    _disableOverlayDrag();
  }

  // ── Edit existing overlay ─────────────────────────────────────────────────

  function startEditOverlay(overlayId) {
    const ov = (_currentConfig.overlays || []).find(o => o.id === overlayId);
    if (!ov) return;
    _editingOverlayId = overlayId;

    document.getElementById('editOverlayLabel').value   = ov.label || '';
    document.getElementById('editOverlayOpacity').value = ov.opacity ?? 0.7;
    document.getElementById('editOverlayNLat').value    = ov.nlat;
    document.getElementById('editOverlayWLon').value    = ov.wlon;
    document.getElementById('editOverlaySLat').value    = ov.slat;
    document.getElementById('editOverlayELon').value    = ov.elon;
    document.getElementById('overlayEditError')?.classList.add('d-none');
    document.getElementById('overlayEditSection').classList.remove('d-none');
    document.getElementById('overlayEditSection').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }

  async function saveEditOverlay() {
    const errEl = document.getElementById('overlayEditError');
    const label   = document.getElementById('editOverlayLabel').value.trim();
    const opacity = parseFloat(document.getElementById('editOverlayOpacity').value) || 0.7;
    const nlat = parseFloat(document.getElementById('editOverlayNLat').value);
    const wlon = parseFloat(document.getElementById('editOverlayWLon').value);
    const slat = parseFloat(document.getElementById('editOverlaySLat').value);
    const elon = parseFloat(document.getElementById('editOverlayELon').value);

    if ([nlat, wlon, slat, elon].some(v => isNaN(v))) {
      _showOverlayError(errEl, 'All four corner coordinates are required.'); return;
    }
    if (nlat <= slat) {
      _showOverlayError(errEl, 'Top-Left Lat must be greater than Bottom-Right Lat.'); return;
    }
    errEl?.classList.add('d-none');

    const idx = (_currentConfig.overlays || []).findIndex(o => o.id === _editingOverlayId);
    if (idx === -1) return;

    const ov = _currentConfig.overlays[idx];
    ov.label   = label || ov.filename || ov.id;
    ov.opacity = opacity;
    ov.nlat    = nlat;
    ov.wlon    = wlon;
    ov.slat    = slat;
    ov.elon    = elon;

    // Remove old layer and re-apply with new bounds/opacity
    if (_overlayLayers[ov.id]) {
      _overlayLayers[ov.id].remove();
      delete _overlayLayers[ov.id];
    }
    _applyOverlay(ov);

    await _saveConfig();
    cancelEditOverlay();
    _renderOverlayList();
  }

  function cancelEditOverlay() {
    _editingOverlayId = null;
    document.getElementById('overlayEditSection').classList.add('d-none');
  }

  function _showOverlayError(el, msg) {
    if (!el) return;
    el.textContent = msg;
    el.classList.remove('d-none');
  }

  // ── Offline tile cache ────────────────────────────────────────────────────

  async function openTileCache() {
    // Refresh cache status
    try {
      const r = await fetch('/api/tiles/status');
      _cacheStatus = await r.json();
    } catch (_) {}
    _renderCacheStatus();

    // Pre-fill zoom inputs from current map zoom
    const curZoom = _map ? _map.getZoom() : 15;
    const zMin = Math.max(1, curZoom - 4);
    const zMax = Math.min(20, curZoom + 1);
    document.getElementById('tileZoomMin').value = zMin;
    document.getElementById('tileZoomMax').value = zMax;

    _clearCacheEstimate();
    new bootstrap.Modal(document.getElementById('tileCacheModal')).show();
  }

  function _renderCacheStatus() {
    const el = document.getElementById('tileCacheStatus');
    if (!el) return;
    if (_cacheStatus.tile_count === 0) {
      el.textContent = 'No tiles cached';
      el.className = 'text-muted small';
    } else {
      el.textContent = `${_cacheStatus.tile_count.toLocaleString()} tiles cached — ${_cacheStatus.size_mb} MB on disk`;
      el.className = 'text-success small';
    }
  }

  function _clearCacheEstimate() {
    const el = document.getElementById('tileCacheEstimate');
    if (el) el.textContent = '';
    const btn = document.getElementById('tileCacheDownloadBtn');
    if (btn) btn.disabled = true;
  }

  async function estimateTiles() {
    if (!_map) return;
    const bounds = _map.getBounds();
    const zMin = parseInt(document.getElementById('tileZoomMin').value) || 10;
    const zMax = parseInt(document.getElementById('tileZoomMax').value) || 19;
    const el = document.getElementById('tileCacheEstimate');
    if (el) el.textContent = 'Calculating…';

    try {
      const r = await fetch('/api/tiles/estimate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          lat_min: bounds.getSouth(), lat_max: bounds.getNorth(),
          lon_min: bounds.getWest(),  lon_max: bounds.getEast(),
          z_min: zMin, z_max: zMax,
        }),
      });
      const data = await r.json();
      if (el) {
        el.textContent = `~${data.total_tiles.toLocaleString()} tiles (${data.tile_pairs.toLocaleString()} imagery+labels pairs) — ~${data.est_mb} MB`;
      }
      const btn = document.getElementById('tileCacheDownloadBtn');
      if (btn) btn.disabled = false;
    } catch (e) {
      if (el) el.textContent = 'Estimate failed — check connection';
    }
  }

  async function startTileDownload() {
    if (!_map) return;
    const bounds = _map.getBounds();
    const zMin = parseInt(document.getElementById('tileZoomMin').value) || 10;
    const zMax = parseInt(document.getElementById('tileZoomMax').value) || 19;

    // Show progress UI
    document.getElementById('tileCacheProgressWrap').classList.remove('d-none');
    document.getElementById('tileCacheDownloadBtn').disabled = true;
    document.getElementById('tileCacheEstimateBtn').disabled = true;
    _setTileProgress(0, 0, '');

    try {
      await fetch('/api/tiles/download', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          lat_min: bounds.getSouth(), lat_max: bounds.getNorth(),
          lon_min: bounds.getWest(),  lon_max: bounds.getEast(),
          z_min: zMin, z_max: zMax,
        }),
      });
    } catch (e) {
      document.getElementById('tileCacheProgressLabel').textContent = 'Failed to start download';
    }
  }

  function _setTileProgress(done, total, label) {
    const bar = document.getElementById('tileCacheProgressBar');
    const lbl = document.getElementById('tileCacheProgressLabel');
    const pct = total > 0 ? Math.round(done / total * 100) : 0;
    if (bar) { bar.style.width = pct + '%'; bar.textContent = pct + '%'; }
    if (lbl) lbl.textContent = label;
  }

  async function clearTileCache() {
    if (!confirm('Delete all cached tiles? This cannot be undone.')) return;
    try {
      await fetch('/api/tiles/clear', { method: 'POST' });
      _cacheStatus = { tile_count: 0, size_mb: 0, active: false };
      _renderCacheStatus();
    } catch (_) {}
  }

  // Called from socket listener (wired in DOMContentLoaded below)
  function _onTileDownloadProgress(data) {
    const { done, total, errors, skipped, active, error } = data;
    if (error) {
      _setTileProgress(0, 0, `Error: ${error}`);
      return;
    }
    const label = active
      ? `Downloading… ${done.toLocaleString()} / ${total.toLocaleString()} (${skipped || 0} already cached, ${errors || 0} errors)`
      : `Done — ${done.toLocaleString()} processed, ${skipped || 0} already cached, ${errors || 0} errors`;
    _setTileProgress(done, total, label);

    if (!active) {
      document.getElementById('tileCacheEstimateBtn').disabled = false;
      document.getElementById('tileCacheDownloadBtn').disabled = false;
      // Refresh cache status display
      fetch('/api/tiles/status')
        .then(r => r.json())
        .then(s => { _cacheStatus = s; _renderCacheStatus(); })
        .catch(() => {});
    }
  }

  // ── Geotag picker ─────────────────────────────────────────────────────────

  let _geoPickActive   = false;
  let _geoPickControl  = null;
  let _geoPickCallback = null;

  function startGeoPick(callback) {
    if (!_map) {
      // Map not yet initialized — switch to map tab which will init it, then retry
      window.switchMainTab('map');
      setTimeout(() => startGeoPick(callback), 600);
      return;
    }

    _cancelGeoPick(); // cancel any existing pick session

    _geoPickActive   = true;
    _geoPickCallback = callback;

    // Floating instruction control
    _geoPickControl = L.control({ position: 'topright' });
    _geoPickControl.onAdd = () => {
      const div = L.DomUtil.create('div', '');
      div.innerHTML = `
        <div style="background:#1a2332;border:2px solid #f0883e;border-radius:6px;padding:8px 12px;color:#f0883e;font-size:0.82rem;font-weight:600;box-shadow:0 2px 8px rgba(0,0,0,0.6);display:flex;align-items:center;gap:8px">
          <i class="fa fa-crosshairs"></i>
          <span>Click map to set device location</span>
          <button id="_geoPickCancel" style="background:transparent;border:1px solid #555;color:#aaa;border-radius:4px;padding:1px 7px;font-size:0.75rem;cursor:pointer;margin-left:4px">Cancel</button>
        </div>`;
      L.DomEvent.disableClickPropagation(div);
      setTimeout(() => {
        const btn = document.getElementById('_geoPickCancel');
        if (btn) btn.addEventListener('click', _cancelGeoPick);
      }, 0);
      return div;
    };
    _geoPickControl.addTo(_map);

    // Crosshair cursor on map container
    _map.getContainer().style.cursor = 'crosshair';

    _map.once('click', _onGeoPickClick);
  }

  function _onGeoPickClick(e) {
    _geoPickActive = false;
    const lat = e.latlng.lat.toFixed(6);
    const lon = e.latlng.lng.toFixed(6);
    _cleanupGeoPick();
    if (_geoPickCallback) {
      _geoPickCallback(parseFloat(lat), parseFloat(lon));
      _geoPickCallback = null;
    }
  }

  function _cancelGeoPick() {
    _geoPickActive = false;
    _geoPickCallback = null;
    _map?.off('click', _onGeoPickClick);
    _cleanupGeoPick();
  }

  function _cleanupGeoPick() {
    if (_geoPickControl) {
      try { _map?.removeControl(_geoPickControl); } catch (_) {}
      _geoPickControl = null;
    }
    if (_map) _map.getContainer().style.cursor = '';
  }

  // ── Public: called when the Map tab becomes visible ───────────────────────
  async function onTabShow() {
    if (!_initialized) {
      // Fetch saved center/zoom before creating the map so we restore the view
      await _fetchConfig();
      _init();
      _redrawAllMarkers();
    }
    // Always invalidate: the container may have been zero-size while hidden
    if (_map) setTimeout(() => _map.invalidateSize(), 50);
  }

  // ── Wire socket listeners after scripts load ──────────────────────────────
  document.addEventListener('DOMContentLoaded', () => {
    if (window._appSocket) {
      window._appSocket.on('device_update', d => updateDevice(d));
      window._appSocket.on('tile_download_progress', _onTileDownloadProgress);
    }
  });

  // ── Public API ────────────────────────────────────────────────────────────
  return {
    onTabShow,
    updateDevices,
    updateDevice,
    highlightDevice,
    fitToMarkers,
    openOverlayManager,
    addOverlay,
    enableOverlayDrag,
    startEditOverlay,
    saveEditOverlay,
    cancelEditOverlay,
    toggleOverlay,
    deleteOverlay,
    startGeoPick,
    cancelGeoPick: _cancelGeoPick,
    openTileCache,
    estimateTiles,
    startTileDownload,
    clearTileCache,
  };

})();
