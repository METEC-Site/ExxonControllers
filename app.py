#!/usr/bin/env python3
"""
ExxonController Web Application
Flask + Flask-SocketIO server for real-time control of Alicat flow controllers
and Phidget peripherals, with synchronized multi-client UI.

Run:
    pip install -r requirements.txt
    python app.py
Then open http://<host>:52424 in a browser.
Default credentials: password = "admin"  (change via Settings in the UI)
"""

# gevent MUST be monkey-patched before any other imports so that all socket
# and threading primitives are replaced with cooperative (non-blocking) versions.
from gevent import monkey
monkey.patch_all()

import faulthandler
import gevent
import hashlib
import math
import os
import requests as _requests
import shutil
import signal
import socket as _socket
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from functools import wraps

from flask import Flask, redirect, render_template, request, session, url_for, jsonify, send_from_directory, send_file, Response
from flask_socketio import SocketIO, disconnect, emit

from core.device_manager import DeviceManager
from core.emission_point_manager import EmissionPointManager, TEST_EP_ID
from core.experiment_manager import ExperimentManager
from core.mqtt_relay import MqttRelay
from core.nas_relay import NasRelay
from core.state_manager import StateManager

_ver_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
APP_VERSION = open(_ver_file).read().strip() if os.path.exists(_ver_file) else "unknown"

# ── Paths ─────────────────────────────────────────────────────────────────────
# When compiled with Nuitka --onefile, the bundle is extracted to a temp dir.
# User-writable data (config, logs) must live next to the .exe so they persist.
try:
    __compiled__                           # Nuitka named-tuple
    # __compiled__.containing_dir is the directory of the original .exe,
    # NOT the extraction cache.  Works for both standalone and onefile.
    BASE_DIR = __compiled__.containing_dir
    # In onefile mode the extraction dir has bundled read-only defaults.
    _BUNDLE_DIR = os.path.dirname(os.path.abspath(__file__)) if __compiled__.onefile else None
except NameError:
    # Running from source.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    _BUNDLE_DIR = None

CONFIG_DIR = os.path.join(BASE_DIR, 'config')
DATA_DIR = os.path.join(BASE_DIR, 'Data')
MAP_UPLOADS_DIR = os.path.join(BASE_DIR, 'static', 'map_uploads')
EP_PHOTOS_DIR = os.path.join(BASE_DIR, 'static', 'ep_photos')
TILE_CACHE_DIR = os.path.join(BASE_DIR, 'tile_cache')

# First-run seed: if no config/ exists beside the exe, copy bundled defaults.
# This lets the exe ship with pre-configured devices/experiments out of the box.
if _BUNDLE_DIR and not os.path.exists(CONFIG_DIR):
    _default_src = os.path.join(_BUNDLE_DIR, '_default_config')
    if os.path.exists(_default_src):
        shutil.copytree(_default_src, CONFIG_DIR)
        print(f"[Startup] Created config/ from bundled defaults.", flush=True)

os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MAP_UPLOADS_DIR, exist_ok=True)
os.makedirs(EP_PHOTOS_DIR, exist_ok=True)
os.makedirs(TILE_CACHE_DIR, exist_ok=True)

# ── Flask App ─────────────────────────────────────────────────────────────────
state = StateManager(CONFIG_DIR)
app = Flask(__name__)
app.secret_key = state.get_secret_key()
# Vendor files are immutable — cache aggressively; app files change frequently — no cache.
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0


@app.after_request
def _set_cache_headers(response):
    """Long-cache vendor files; no-cache everything else."""
    path = request.path
    if path.startswith('/static/vendor/'):
        response.cache_control.max_age = 86400
        response.cache_control.public = True
    else:
        response.cache_control.no_cache = True
        response.cache_control.no_store = True
    return response

socketio = SocketIO(app, cors_allowed_origins='*', async_mode='gevent',
                    logger=False, engineio_logger=False)


def _emit_log(msg: str, level: str = 'info'):
    """Print to terminal and broadcast to connected clients for the UI log tab."""
    print(msg, flush=True)
    try:
        socketio.emit('server_log', {
            'msg': msg,
            'level': level,
            'ts': datetime.now(timezone.utc).strftime('%H:%M:%S'),
        })
    except Exception:
        pass


# ── Thread & Greenlet Error Forwarding ───────────────────────────────────────
# gevent.monkey.patch_all() replaces threading.Thread with greenlet-backed
# wrappers whose unhandled exceptions route through the gevent hub rather than
# threading.excepthook.  We install handlers on both paths so that no exception
# from any concurrent context can disappear silently.

def _thread_excepthook(args):
    """Forward any unhandled threading.Thread exception to the CLI and UI log."""
    if args.exc_type is SystemExit:
        return
    tb_str = ''.join(traceback.format_exception(
        args.exc_type, args.exc_value, args.exc_tb))
    label = getattr(args.thread, 'name', repr(args.thread))
    _emit_log(f"[Thread:{label}] Unhandled exception:\n{tb_str.rstrip()}", 'error')

threading.excepthook = _thread_excepthook

# Gevent hub error handler is patched at startup (after the hub exists) inside
# the __main__ block; see _install_gevent_error_handler() below.

def _install_gevent_error_handler():
    """
    Patch the gevent hub instance so unhandled greenlet exceptions are logged.
    Must be called after monkey-patching and hub creation (i.e. inside __main__).
    GreenletExit and SystemExit are intentional lifecycle events and are left
    to gevent's default handler unchanged.
    """
    _hub = gevent.get_hub()
    _orig = _hub.handle_error

    def _hub_error_handler(context, exc_type, exc_value, exc_tb):
        if not issubclass(exc_type, (gevent.GreenletExit, SystemExit)):
            tb_str = ''.join(traceback.format_exception(exc_type, exc_value, exc_tb))
            _emit_log(
                f"[Greenlet] Unhandled exception in {context!r}:\n{tb_str.rstrip()}",
                'error',
            )
        return _orig(context, exc_type, exc_value, exc_tb)

    _hub.handle_error = _hub_error_handler


device_mgr = DeviceManager(state, socketio)
ep_mgr = EmissionPointManager()
ep_mgr.load_from_config(state.get_emission_points())
device_mgr.set_ep_mgr(ep_mgr)
experiment_mgr = ExperimentManager(CONFIG_DIR, DATA_DIR)
mqtt_relay = MqttRelay()
nas_relay = NasRelay()

# ── Session Tracking ──────────────────────────────────────────────────────────
connected_sessions: dict = {}   # sid -> {username, ip, connected_at, session_token}
_sessions_lock = threading.Lock()

# Incremented each polling cycle; read by the hub watchdog to detect real stalls.
_poll_heartbeat = [0]

# ── Chat ─────────────────────────────────────────────────────────────────────
_chat_messages: list = []   # [{username, text, timestamp}]
_chat_lock = threading.Lock()
_CHAT_MAX = 500


# ── Auth Helpers ──────────────────────────────────────────────────────────────

def _check_password(password: str) -> bool:
    settings = state.get_settings()
    stored_hash = settings.get('password_hash', '')
    salt = settings.get('password_salt', '')
    test_hash = hashlib.sha256((salt + password).encode()).hexdigest()
    return test_hash == stored_hash


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login_page'))
        return f(*args, **kwargs)
    return decorated


def ws_login_required(f):
    """Decorator for SocketIO event handlers — disconnects unauthenticated clients."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            disconnect()
            return
        return f(*args, **kwargs)
    return decorated


# ── HTTP Routes ───────────────────────────────────────────────────────────────

@app.route('/login', methods=['GET', 'POST'])
def login_page():
    error = None
    if request.method == 'POST':
        username = request.form.get('username', 'User').strip() or 'User'
        password = request.form.get('password', '')
        if _check_password(password):
            session['logged_in'] = True
            session['username'] = username
            return redirect(url_for('index'))
        error = 'Incorrect password. Please try again.'
    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login_page'))


@app.route('/')
@login_required
def index():
    return render_template('index.html',
                           username=session.get('username', 'User'),
                           phidget_available=device_mgr.phidget_available(),
                           version=APP_VERSION)


@app.route('/api/history/<device_id>')
@login_required
def get_history(device_id):
    """Return recent history for initial chart population when a client connects."""
    limit = int(request.args.get('limit', 300))
    history = device_mgr.get_history(device_id, limit=limit)
    return jsonify(history)


@app.route('/api/upload_schedule', methods=['POST'])
@login_required
def upload_schedule():
    """Parse an uploaded CSV schedule and return the parsed steps as JSON."""
    device_id = request.form.get('device_id', '')
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    f = request.files['file']
    try:
        content = f.read().decode('utf-8-sig')
    except Exception:
        return jsonify({'error': 'Could not decode file (must be UTF-8 CSV)'}), 400

    schedule = device_mgr.parse_schedule(content)
    if schedule is None:
        return jsonify({'error': 'Invalid schedule format. Expected CSV with "time" and rate columns.'}), 400

    return jsonify({'schedule': schedule, 'count': len(schedule), 'device_id': device_id})


# ── SocketIO Events ───────────────────────────────────────────────────────────

@socketio.on('connect')
def on_connect():
    if not session.get('logged_in'):
        return False  # Reject unauthenticated WS connections

    sid = request.sid
    username = session.get('username', 'Unknown')
    ip = request.remote_addr
    session_token = str(uuid.uuid4())

    with _sessions_lock:
        connected_sessions[sid] = {
            'username': username,
            'ip': ip,
            'connected_at': datetime.now(timezone.utc).isoformat(),
            'session_token': session_token,
        }

    # Tell THIS client their own session token (so they can identify themselves)
    emit('your_session_token', {'token': session_token})
    # Send full application state to the newly connected client.
    # _build_full_state() is now resilient per-subsystem, so individual
    # failures (e.g. device_mgr timeout) won't blank unrelated fields.
    personal_state = _build_full_state()
    personal_state['your_session_token'] = session_token
    exp_count = len(personal_state.get('experiments', []))
    emit('full_state', personal_state)
    # Notify ALL clients about updated session list
    socketio.emit('sessions_update', _get_sessions_list())
    _emit_log(f"[WS] {username}@{ip} connected (sid={sid[:8]}) — sending {exp_count} experiment(s)")


@socketio.on('disconnect')
def on_disconnect():
    sid = request.sid
    with _sessions_lock:
        info = connected_sessions.pop(sid, {})
    socketio.emit('sessions_update', _get_sessions_list())
    _emit_log(f"[WS] {info.get('username', '?')}@{info.get('ip', '?')} disconnected")


@socketio.on('kick_session')
@ws_login_required
def on_kick_session(data):
    target_token = str(data.get('token', '')).strip()
    if not target_token:
        return
    target_sid = None
    with _sessions_lock:
        for sid, info in connected_sessions.items():
            if info.get('session_token') == target_token:
                target_sid = sid
                break
    if target_sid:
        socketio.emit('kicked', {}, to=target_sid)
        socketio.server.disconnect(target_sid)


@socketio.on('send_chat')
@ws_login_required
def on_send_chat(data):
    text = str(data.get('text', '')).strip()
    if not text or len(text) > 500:
        return
    username = session.get('username', 'Unknown')
    msg = {
        'username': username,
        'text': text,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    }
    with _chat_lock:
        _chat_messages.append(msg)
        if len(_chat_messages) > _CHAT_MAX:
            _chat_messages.pop(0)
        snapshot = list(_chat_messages)
    state.save_chat_log(snapshot)
    socketio.emit('chat_message', msg)


@socketio.on('delete_chat_message')
@ws_login_required
def on_delete_chat_message(data):
    ts = data.get('timestamp', '')
    if not ts:
        return
    with _chat_lock:
        idx = next((i for i, m in enumerate(_chat_messages) if m.get('timestamp') == ts), None)
        if idx is not None:
            _chat_messages.pop(idx)
        snapshot = list(_chat_messages)
    state.save_chat_log(snapshot)
    socketio.emit('chat_message_deleted', {'timestamp': ts})


@socketio.on('clear_chat')
@ws_login_required
def on_clear_chat():
    with _chat_lock:
        _chat_messages.clear()
    state.save_chat_log([])
    socketio.emit('chat_cleared')


# ── Device Events ─────────────────────────────────────────────────────────────

@socketio.on('add_device')
@ws_login_required
def on_add_device(data):
    # Resolve emission point and inject lat/lon/alt from EP
    ep_id = data.get('emission_point_id') or TEST_EP_ID
    ep = ep_mgr.get_ep(ep_id)
    if not ep:
        emit('action_result', {'success': False, 'error': 'Selected emission point not found'})
        return
    data['emission_point_id'] = ep_id
    data['lat'] = ep['lat']
    data['lon'] = ep['lon']
    data['alt'] = ep.get('alt')
    result = device_mgr.add_device(data)
    if result['success']:
        state.save_devices(device_mgr.get_device_configs())
        socketio.emit('full_state', _build_full_state())
    emit('action_result', result)


@socketio.on('remove_device')
@ws_login_required
def on_remove_device(data):
    device_id = data.get('device_id', '')
    result = device_mgr.remove_device(device_id)
    if result['success']:
        state.save_devices(device_mgr.get_device_configs())
        socketio.emit('full_state', _build_full_state())
    emit('action_result', result)


@socketio.on('edit_device')
@ws_login_required
def on_edit_device(data):
    device_id = data.get('device_id', '')
    # If EP assignment is changing, inject updated lat/lon/alt from new EP
    if 'emission_point_id' in data:
        ep_id = data['emission_point_id'] or TEST_EP_ID
        ep = ep_mgr.get_ep(ep_id)
        if not ep:
            emit('action_result', {'success': False, 'error': 'Selected emission point not found'})
            return
        data['emission_point_id'] = ep_id
        data['lat'] = ep['lat']
        data['lon'] = ep['lon']
        data['alt'] = ep.get('alt')
    result = device_mgr.edit_device(device_id, data)
    if result['success']:
        state.save_devices(device_mgr.get_device_configs())
        # device_update is emitted by do_connect() once reconnect finishes
    emit('action_result', result)


@socketio.on('disable_device')
@ws_login_required
def on_disable_device(data):
    device_id = data.get('device_id', '')
    disabled = bool(data.get('disabled', True))
    result = device_mgr.disable_device(device_id, disabled)
    if result['success']:
        state.save_devices(device_mgr.get_device_configs())
        socketio.emit('device_update', device_mgr.get_device_state(device_id))
    emit('action_result', result)


@socketio.on('disable_peripheral')
@ws_login_required
def on_disable_peripheral(data):
    peripheral_id = data.get('peripheral_id', '')
    disabled = bool(data.get('disabled', True))
    result = device_mgr.disable_peripheral(peripheral_id, disabled)
    if result['success']:
        state.save_devices(device_mgr.get_device_configs())
        socketio.emit('peripheral_update', device_mgr.get_peripheral_state(peripheral_id))
    emit('action_result', result)


def _dev_name(device_id):
    d = device_mgr._alicat.get(device_id)
    return d.device_name if d else device_id


@socketio.on('start_device')
@ws_login_required
def on_start_device(data):
    device_id = data.get('device_id', '')
    result = device_mgr.start_device(device_id)
    socketio.emit('device_update', device_mgr.get_device_state(device_id))
    if result.get('success'):
        _emit_log(f"[Logging] {session.get('username','?')} started logging — {_dev_name(device_id)}")
    emit('action_result', result)


@socketio.on('stop_device')
@ws_login_required
def on_stop_device(data):
    device_id = data.get('device_id', '')
    result = device_mgr.stop_device(device_id)
    socketio.emit('device_update', device_mgr.get_device_state(device_id))
    if result.get('success'):
        _emit_log(f"[Logging] {session.get('username','?')} stopped logging — {_dev_name(device_id)}")
    emit('action_result', result)


@socketio.on('set_setpoint')
@ws_login_required
def on_set_setpoint(data):
    device_id = data.get('device_id', '')
    setpoint = data.get('setpoint')
    result = device_mgr.set_setpoint(device_id, setpoint)
    # Broadcast device update so all clients reflect the new setpoint immediately
    socketio.emit('device_update', device_mgr.get_device_state(device_id))
    if result.get('success'):
        msg = result.get('message', '')
        if 'local mode' in msg or 'acknowledged but' in msg:
            # Write was accepted at Modbus level but device didn't apply it
            _emit_log(f"[Setpoint] {session.get('username','?')} → {_dev_name(device_id)}: {setpoint} SLPM — WARNING: {msg}", 'warning')
        else:
            _emit_log(f"[Setpoint] {session.get('username','?')} → {_dev_name(device_id)}: {setpoint} SLPM")
    emit('action_result', result)


@socketio.on('set_gas')
@ws_login_required
def on_set_gas(data):
    device_id = data.get('device_id', '')
    gas_number = data.get('gas_number')
    result = device_mgr.set_gas(device_id, gas_number)
    state.save_devices(device_mgr.get_device_configs())
    socketio.emit('device_update', device_mgr.get_device_state(device_id))
    emit('action_result', result)


# ── Schedule Events ───────────────────────────────────────────────────────────

@socketio.on('load_schedule')
@ws_login_required
def on_load_schedule(data):
    device_id = data.get('device_id', '')
    schedule = data.get('schedule', [])
    result = device_mgr.load_schedule(device_id, schedule)
    if result['success']:
        socketio.emit('device_update', device_mgr.get_device_state(device_id))
    emit('action_result', result)


@socketio.on('start_schedule')
@ws_login_required
def on_start_schedule(data):
    device_id = data.get('device_id', '')
    result = device_mgr.start_schedule(device_id)
    socketio.emit('device_update', device_mgr.get_device_state(device_id))
    if result.get('success'):
        _emit_log(f"[Schedule] {session.get('username','?')} started schedule — {_dev_name(device_id)}")
    emit('action_result', result)


@socketio.on('stop_schedule')
@ws_login_required
def on_stop_schedule(data):
    device_id = data.get('device_id', '')
    result = device_mgr.stop_schedule(device_id)
    socketio.emit('device_update', device_mgr.get_device_state(device_id))
    _emit_log(f"[Schedule] {session.get('username','?')} stopped schedule — {_dev_name(device_id)}")


# ── Peripheral Events ─────────────────────────────────────────────────────────

@socketio.on('add_peripheral')
@ws_login_required
def on_add_peripheral(data):
    result = device_mgr.add_peripheral(data)
    if result['success']:
        state.save_devices(device_mgr.get_device_configs())
        socketio.emit('full_state', _build_full_state())
    emit('action_result', result)


@socketio.on('remove_peripheral')
@ws_login_required
def on_remove_peripheral(data):
    peripheral_id = data.get('peripheral_id', '')
    result = device_mgr.remove_peripheral(peripheral_id)
    if result['success']:
        state.save_devices(device_mgr.get_device_configs())
        socketio.emit('full_state', _build_full_state())
    emit('action_result', result)


@socketio.on('edit_peripheral')
@ws_login_required
def on_edit_peripheral(data):
    peripheral_id = data.get('peripheral_id', '')
    result = device_mgr.edit_peripheral(peripheral_id, data)
    if result['success']:
        state.save_devices(device_mgr.get_device_configs())
        socketio.emit('peripheral_update', device_mgr.get_peripheral_state(peripheral_id))
    emit('action_result', result)


@socketio.on('reorder_devices')
@ws_login_required
def on_reorder_devices(data):
    result = device_mgr.reorder_devices(data.get('order', []))
    if result['success']:
        state.save_devices(device_mgr.get_device_configs())


@socketio.on('reorder_peripherals')
@ws_login_required
def on_reorder_peripherals(data):
    result = device_mgr.reorder_peripherals(data.get('order', []))
    if result['success']:
        state.save_devices(device_mgr.get_device_configs())


@socketio.on('set_relay')
@ws_login_required
def on_set_relay(data):
    peripheral_id = data.get('peripheral_id', '')
    channel = data.get('channel', 0)
    relay_state = data.get('state', False)
    result = device_mgr.set_relay(peripheral_id, channel, relay_state)
    socketio.emit('peripheral_update', device_mgr.get_peripheral_state(peripheral_id))
    periph = device_mgr._peripherals.get(peripheral_id)
    pname = periph.name if periph else peripheral_id
    labels = periph.channel_labels if periph and hasattr(periph, 'channel_labels') else []
    ch_label = labels[channel] if channel < len(labels) else f'CH{channel}'
    if result.get('success'):
        _emit_log(f"[Relay] {session.get('username','?')} → {pname} / {ch_label}: {'ON' if relay_state else 'OFF'}")
    else:
        _emit_log(f"[Relay] {session.get('username','?')} → {pname} / {ch_label}: FAILED — {result.get('error','')}", 'error')
    emit('action_result', result)


# ── Emission Point Events ─────────────────────────────────────────────────────

@socketio.on('add_emission_point')
@ws_login_required
def on_add_emission_point(data):
    result = ep_mgr.add_ep(data)
    if result['success']:
        state.save_emission_points(ep_mgr.get_configs())
        socketio.emit('full_state', _build_full_state())
    emit('action_result', result)


@socketio.on('edit_emission_point')
@ws_login_required
def on_edit_emission_point(data):
    ep_id = data.get('ep_id', '')
    result = ep_mgr.edit_ep(ep_id, data)
    if result['success']:
        updated_ep = result['ep']
        # Cascade updated lat/lon/alt to all devices assigned to this EP
        for device in device_mgr._alicat.values():
            if getattr(device, 'emission_point_id', None) == ep_id:
                device.lat = updated_ep['lat']
                device.lon = updated_ep['lon']
                device.alt = updated_ep.get('alt')
        state.save_devices(device_mgr.get_device_configs())
        state.save_emission_points(ep_mgr.get_configs())
        socketio.emit('full_state', _build_full_state())
    emit('action_result', result)


@socketio.on('delete_emission_point')
@ws_login_required
def on_delete_emission_point(data):
    ep_id = data.get('ep_id', '')
    result = ep_mgr.delete_ep(ep_id)
    if result['success']:
        # Cascade: reassign all devices using this EP back to TEST
        test_ep = ep_mgr.get_ep(TEST_EP_ID)
        affected = []
        for device in device_mgr._alicat.values():
            if getattr(device, 'emission_point_id', None) == ep_id:
                device.emission_point_id = TEST_EP_ID
                device.lat = test_ep['lat']
                device.lon = test_ep['lon']
                device.alt = test_ep.get('alt')
                affected.append(device.device_name)
        state.save_devices(device_mgr.get_device_configs())
        state.save_emission_points(ep_mgr.get_configs())
        socketio.emit('full_state', _build_full_state())
        if affected:
            result['warning'] = (
                f"{len(affected)} device(s) reassigned to DEFAULT: {', '.join(affected)}. "
                "Update their emission point assignment before use."
            )
    emit('action_result', result)


@socketio.on('reorder_emission_points')
@ws_login_required
def on_reorder_emission_points(data):
    result = ep_mgr.reorder_eps(data.get('order', []))
    if result['success']:
        state.save_emission_points(ep_mgr.get_configs())
    emit('action_result', result)


@socketio.on('query_rtk_location')
@ws_login_required
def on_query_rtk_location(data):
    """
    Query a SparkFun RTK Postcard (or compatible device) for its current location.
    The device must be actively broadcasting UDP NMEA packets on port 13521.
    Listens for up to 2 seconds for a GGA sentence from the specified IP.
    Returns lat/lon/altitude and RTK fix quality.
    """
    ip = (data.get('ip') or '').strip()
    if not ip:
        emit('rtk_location_result', {'success': False, 'error': 'IP address is required'})
        return

    RTK_PORT = 13521
    TIMEOUT_S = 2.0

    def _parse_nmea_coord(value: str, hemisphere: str) -> float | None:
        """Convert NMEA DDMM.MMMM format to decimal degrees."""
        try:
            value = value.strip()
            if not value:
                return None
            # Find decimal point position to split degrees from minutes
            dot_idx = value.index('.')
            # Degrees are all digits before the last 2 digits left of the decimal
            deg_digits = dot_idx - 2
            degrees = float(value[:deg_digits])
            minutes = float(value[deg_digits:])
            dec = degrees + minutes / 60.0
            if hemisphere.strip().upper() in ('S', 'W'):
                dec = -dec
            return dec
        except (ValueError, IndexError):
            return None

    _FIX_LABELS = {
        0: 'No fix',
        1: 'GPS fix (SPS)',
        2: 'DGPS fix',
        3: 'PPS fix',
        4: 'RTK Fixed',
        5: 'RTK Float',
        6: 'Estimated',
        7: 'Manual input',
        8: 'Simulation',
    }

    try:
        sock = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('0.0.0.0', RTK_PORT))
        except OSError as bind_err:
            emit('rtk_location_result', {
                'success': False,
                'error': f'Cannot bind to UDP port {RTK_PORT}: {bind_err}. '
                         'Ensure no other application is using this port.',
            })
            sock.close()
            return

        sock.settimeout(TIMEOUT_S)
        deadline = time.monotonic() + TIMEOUT_S

        result = None
        while time.monotonic() < deadline:
            try:
                raw, addr = sock.recvfrom(4096)
            except _socket.timeout:
                break
            except OSError:
                break

            if addr[0] != ip:
                continue  # packet from a different host

            # The payload may contain multiple NMEA sentences (newline-separated)
            try:
                text = raw.decode('ascii', errors='ignore')
            except Exception:
                continue

            for line in text.splitlines():
                line = line.strip()
                # Accept any GGA talker: $GNGGA, $GPGGA, $GLGGA, $GBGGA ...
                if not line or '$' not in line:
                    continue
                try:
                    start = line.index('$')
                    sentence = line[start:]
                    # Strip checksum if present
                    if '*' in sentence:
                        sentence = sentence[:sentence.index('*')]
                    fields = sentence.split(',')
                    if len(fields) < 10:
                        continue
                    talker_msg = fields[0][1:]  # e.g. GNGGA, GPGGA
                    if not talker_msg.endswith('GGA'):
                        continue

                    lat = _parse_nmea_coord(fields[2], fields[3])
                    lon = _parse_nmea_coord(fields[4], fields[5])
                    fix_quality = int(fields[6]) if fields[6].strip() else 0
                    alt_str = fields[9].strip()
                    alt = float(alt_str) if alt_str else None

                    if lat is None or lon is None:
                        continue

                    result = {
                        'success': True,
                        'lat': round(lat, 8),
                        'lon': round(lon, 8),
                        'alt': round(alt, 2) if alt is not None else None,
                        'fix_quality': fix_quality,
                        'fix_label': _FIX_LABELS.get(fix_quality, f'Quality {fix_quality}'),
                        'rtk_fixed': fix_quality == 4,
                        'rtk_float': fix_quality == 5,
                    }
                    break  # got a valid GGA — done
                except (ValueError, IndexError):
                    continue

            if result:
                break
    except Exception as exc:
        emit('rtk_location_result', {'success': False, 'error': str(exc)})
        return
    finally:
        try:
            sock.close()
        except Exception:
            pass

    if result:
        emit('rtk_location_result', result)
    else:
        emit('rtk_location_result', {
            'success': False,
            'error': f'No GGA response from {ip} within {TIMEOUT_S:.0f}s. '
                     'Verify the IP address and that the RTK device is powered and broadcasting.',
        })


@app.route('/api/emission_points/upload_photo', methods=['POST'])
@login_required
def upload_ep_photo():
    import uuid as _uuid
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    f = request.files['file']
    if not f.filename:
        return jsonify({'error': 'Empty filename'}), 400
    allowed = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.tif', '.tiff'}
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in allowed:
        return jsonify({'error': f'File type {ext} not allowed'}), 400
    safe_name = f"{_uuid.uuid4().hex[:8]}{ext}"
    save_path = os.path.join(EP_PHOTOS_DIR, safe_name)
    f.save(save_path)
    return jsonify({'success': True, 'filename': safe_name})


@app.route('/api/emission_points/delete_photo', methods=['POST'])
@login_required
def delete_ep_photo():
    data = request.get_json() or {}
    filename = os.path.basename(data.get('filename', ''))
    if not filename:
        return jsonify({'error': 'No filename'}), 400
    path = os.path.join(EP_PHOTOS_DIR, filename)
    if os.path.exists(path):
        os.remove(path)
    return jsonify({'success': True})


# ── Crash Recovery Events ─────────────────────────────────────────────────────

@socketio.on('crash_recovery_response')
@ws_login_required
def on_crash_recovery(data):
    resume = data.get('resume', False)
    experiment_state = state.get_crash_experiment_state()
    state.clear_crash_state()

    if resume and experiment_state:
        device_mgr.resume_experiment(experiment_state)
        socketio.emit('full_state', _build_full_state())
        socketio.emit('toast', {'type': 'success', 'message': 'Experiment resumed from last checkpoint.'})
    else:
        socketio.emit('toast', {'type': 'info', 'message': 'Crash recovery dismissed. Starting fresh.'})

    socketio.emit('crash_dismissed', {})


# ── Settings Events ───────────────────────────────────────────────────────────

@socketio.on('update_password')
@ws_login_required
def on_update_password(data):
    import hashlib, secrets as _secrets
    new_password = data.get('new_password', '').strip()
    if len(new_password) < 4:
        emit('action_result', {'success': False, 'error': 'Password must be at least 4 characters'})
        return
    salt = _secrets.token_hex(16)
    pw_hash = hashlib.sha256((salt + new_password).encode()).hexdigest()
    state.update_settings({'password_hash': pw_hash, 'password_salt': salt})
    emit('action_result', {'success': True, 'message': 'Password updated successfully'})


@socketio.on('save_file_rotation_settings')
@ws_login_required
def on_save_file_rotation_settings(data):
    raw_min = data.get('raw_file_rotation_minutes')
    exp_min = data.get('exp_file_rotation_minutes')
    updates = {}
    if raw_min is not None:
        updates['raw_file_rotation_minutes'] = int(raw_min)
    if exp_min is not None:
        updates['exp_file_rotation_minutes'] = int(exp_min)
    state.update_settings(updates)
    emit('action_result', {'success': True, 'message': 'File rotation settings saved'})


@app.route('/api/shutdown', methods=['POST'])
@login_required
def shutdown():
    """Gracefully shut down the server process."""
    def _do_shutdown():
        time.sleep(0.5)   # allow the HTTP response to be sent first
        os._exit(0)
    gevent.spawn(_do_shutdown)
    return jsonify({'success': True, 'message': 'Server shutting down…'})


# ── Sync Events (UI state mirroring across all clients) ──────────────────────

@socketio.on('ui_action')
@ws_login_required
def on_ui_action(data):
    """
    Broadcast a UI state change to all OTHER clients.
    Used so that modal opens, tab switches, etc. can optionally be mirrored.
    data: {action: str, payload: any}
    """
    socketio.emit('ui_action_broadcast', {
        'from': session.get('username', 'Unknown'),
        'action': data.get('action'),
        'payload': data.get('payload'),
    })


# ── Solenoid Checklist Routes ─────────────────────────────────────────────────

@app.route('/api/solenoid_checklist')
@login_required
def get_solenoid_checklist():
    return jsonify(state.get_solenoid_checklist())


@app.route('/api/solenoid_checklist', methods=['POST'])
@login_required
def save_solenoid_checklist():
    data = request.get_json() or []
    if not isinstance(data, list):
        return jsonify({'error': 'Expected a JSON array'}), 400
    state.save_solenoid_checklist(data)
    return jsonify({'success': True})


# ── Map Routes ────────────────────────────────────────────────────────────────

@app.route('/api/map_config')
@login_required
def get_map_config():
    return jsonify(state.get_map_config())


@app.route('/api/map_config', methods=['POST'])
@login_required
def save_map_config():
    data = request.get_json() or {}
    state.save_map_config(data)
    return jsonify({'success': True})


@app.route('/api/map_config/upload_image', methods=['POST'])
@login_required
def upload_map_image():
    import uuid as _uuid
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    f = request.files['file']
    if not f.filename:
        return jsonify({'error': 'Empty filename'}), 400
    # Only allow image types
    allowed = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.tif', '.tiff'}
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in allowed:
        return jsonify({'error': f'File type {ext} not allowed'}), 400
    safe_name = f"{_uuid.uuid4().hex[:8]}_{os.path.basename(f.filename)}"
    save_path = os.path.join(MAP_UPLOADS_DIR, safe_name)
    f.save(save_path)
    url = f'/static/map_uploads/{safe_name}'
    return jsonify({'success': True, 'url': url, 'filename': safe_name})


@app.route('/api/map_config/delete_image', methods=['POST'])
@login_required
def delete_map_image():
    data = request.get_json() or {}
    filename = os.path.basename(data.get('filename', ''))
    if not filename:
        return jsonify({'error': 'No filename'}), 400
    path = os.path.join(MAP_UPLOADS_DIR, filename)
    if os.path.exists(path):
        os.remove(path)
    return jsonify({'success': True})


# ── Tile Proxy & Offline Cache ────────────────────────────────────────────────

_TILE_LAYERS = {
    'imagery': {
        'url': 'https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        'ext': 'jpg',
        'mime': 'image/jpeg',
    },
    'labels': {
        'url': 'https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}',
        'ext': 'png',
        'mime': 'image/png',
    },
}

_tile_download_active = False


@app.route('/tiles/<layer>/<int:z>/<int:y>/<int:x>')
def tile_proxy(layer, z, y, x):
    """Serve map tiles from local disk cache; fetch from Esri on cache miss."""
    cfg = _TILE_LAYERS.get(layer)
    if not cfg:
        return '', 404
    cache_path = os.path.join(TILE_CACHE_DIR, layer, str(z), str(y), f'{x}.{cfg["ext"]}')
    if os.path.exists(cache_path):
        return send_file(cache_path, mimetype=cfg['mime'])
    try:
        url = cfg['url'].format(z=z, y=y, x=x)
        resp = _requests.get(url, timeout=10,
                             headers={'User-Agent': 'ExxonController/1.0 (CSU Research)'})
        if resp.status_code == 200:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, 'wb') as f:
                f.write(resp.content)
            return send_file(cache_path, mimetype=cfg['mime'])
    except Exception:
        pass
    return '', 404


def _tiles_in_bbox(lat_min, lat_max, lon_min, lon_max, z):
    """Yield all (z, x, y) tile coords covering the given lat/lon bbox at zoom z."""
    n = 2 ** z

    def lon2x(lon):
        return int((lon + 180) / 360 * n)

    def lat2y(lat):
        r = math.radians(lat)
        return int((1 - math.log(math.tan(r) + 1 / math.cos(r)) / math.pi) / 2 * n)

    x0 = max(0, lon2x(lon_min));  x1 = min(n - 1, lon2x(lon_max))
    y0 = max(0, lat2y(lat_max));  y1 = min(n - 1, lat2y(lat_min))  # lat_max → smaller y
    for x in range(x0, x1 + 1):
        for y in range(y0, y1 + 1):
            yield z, x, y


@app.route('/api/tiles/estimate', methods=['POST'])
@login_required
def api_tiles_estimate():
    """Return tile count + estimated MB for the given bbox and zoom range."""
    data = request.get_json() or {}
    try:
        lat_min = float(data['lat_min']); lat_max = float(data['lat_max'])
        lon_min = float(data['lon_min']); lon_max = float(data['lon_max'])
        z_min = int(data.get('z_min', 10)); z_max = int(data.get('z_max', 19))
    except (KeyError, ValueError):
        return jsonify({'error': 'Missing or invalid bbox parameters'}), 400

    total_pairs = sum(
        sum(1 for _ in _tiles_in_bbox(lat_min, lat_max, lon_min, lon_max, z))
        for z in range(z_min, z_max + 1)
    )
    # Imagery ~22 KB avg (JPEG), labels ~9 KB avg (mostly transparent PNG)
    est_bytes = total_pairs * (22_000 + 9_000)
    return jsonify({
        'tile_pairs': total_pairs,
        'total_tiles': total_pairs * 2,
        'est_mb': round(est_bytes / 1_048_576, 1),
    })


@app.route('/api/tiles/download', methods=['POST'])
@login_required
def api_tiles_download():
    """Start a background tile download for the given bbox and zoom range."""
    global _tile_download_active
    if _tile_download_active:
        return jsonify({'error': 'Download already in progress'}), 409

    data = request.get_json() or {}
    try:
        lat_min = float(data['lat_min']); lat_max = float(data['lat_max'])
        lon_min = float(data['lon_min']); lon_max = float(data['lon_max'])
        z_min = int(data.get('z_min', 10)); z_max = int(data.get('z_max', 19))
    except (KeyError, ValueError):
        return jsonify({'error': 'Missing or invalid bbox parameters'}), 400

    def _do_download():
        global _tile_download_active
        _tile_download_active = True
        try:
            # Build the full (layer, z, x, y) work list
            jobs = []
            for z in range(z_min, z_max + 1):
                for _, x, y in _tiles_in_bbox(lat_min, lat_max, lon_min, lon_max, z):
                    for layer_name, cfg in _TILE_LAYERS.items():
                        jobs.append((layer_name, cfg, z, x, y))

            total = len(jobs)
            done = errors = skipped = 0
            socketio.emit('tile_download_progress',
                          {'done': 0, 'total': total, 'errors': 0, 'skipped': 0, 'active': True})

            for layer_name, cfg, z, x, y in jobs:
                cache_path = os.path.join(TILE_CACHE_DIR, layer_name,
                                          str(z), str(y), f'{x}.{cfg["ext"]}')
                if os.path.exists(cache_path):
                    skipped += 1
                else:
                    try:
                        url = cfg['url'].format(z=z, y=y, x=x)
                        resp = _requests.get(url, timeout=15,
                                             headers={'User-Agent': 'ExxonController/1.0 (CSU Research)'})
                        if resp.status_code == 200:
                            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                            with open(cache_path, 'wb') as f:
                                f.write(resp.content)
                        else:
                            errors += 1
                    except Exception:
                        errors += 1
                    gevent.sleep(0.02)  # ~50 tiles/sec — polite rate limit

                done += 1
                if done % 20 == 0 or done == total:
                    socketio.emit('tile_download_progress', {
                        'done': done, 'total': total,
                        'errors': errors, 'skipped': skipped,
                        'active': done < total,
                    })

            socketio.emit('tile_download_progress', {
                'done': total, 'total': total,
                'errors': errors, 'skipped': skipped, 'active': False,
            })
        except Exception as e:
            socketio.emit('tile_download_progress', {
                'done': 0, 'total': 0, 'errors': 1, 'skipped': 0,
                'active': False, 'error': str(e),
            })
        finally:
            _tile_download_active = False

    gevent.spawn(_do_download)
    return jsonify({'success': True})


@app.route('/api/tiles/status')
@login_required
def api_tiles_status():
    """Return cached tile count, disk usage, and whether a download is active."""
    total_tiles = total_bytes = 0
    if os.path.exists(TILE_CACHE_DIR):
        for dirpath, _, filenames in os.walk(TILE_CACHE_DIR):
            for fname in filenames:
                total_tiles += 1
                try:
                    total_bytes += os.path.getsize(os.path.join(dirpath, fname))
                except OSError:
                    pass
    return jsonify({
        'tile_count': total_tiles,
        'size_mb': round(total_bytes / 1_048_576, 1),
        'active': _tile_download_active,
    })


@app.route('/api/tiles/clear', methods=['POST'])
@login_required
def api_tiles_clear():
    """Delete all cached tiles."""
    global _tile_download_active
    if _tile_download_active:
        return jsonify({'error': 'Download in progress — wait for it to finish'}), 409
    if os.path.exists(TILE_CACHE_DIR):
        shutil.rmtree(TILE_CACHE_DIR)
        os.makedirs(TILE_CACHE_DIR, exist_ok=True)
    return jsonify({'success': True})


# ── Experiment HTTP Routes ────────────────────────────────────────────────────

@app.route('/api/experiments')
@login_required
def list_experiments():
    return jsonify(experiment_mgr.list_experiments())


@app.route('/api/experiments', methods=['POST'])
@login_required
def create_experiment():
    data = request.get_json() or {}
    exp_id = experiment_mgr.create_experiment(data)
    exp = experiment_mgr.get_experiment(exp_id)
    socketio.emit('experiments_update', experiment_mgr.list_experiments())
    return jsonify({'success': True, 'experiment_id': exp_id, 'experiment': exp})


@app.route('/api/experiments/<experiment_id>')
@login_required
def get_experiment(experiment_id):
    exp = experiment_mgr.get_experiment(experiment_id)
    if not exp:
        return jsonify({'error': 'Not found'}), 404
    return jsonify(exp)


@app.route('/api/experiments/<experiment_id>', methods=['PUT'])
@login_required
def update_experiment(experiment_id):
    data = request.get_json() or {}
    result = experiment_mgr.update_experiment(experiment_id, data)
    if result['success']:
        socketio.emit('experiments_update', experiment_mgr.list_experiments())
    return jsonify(result)


@app.route('/api/experiments/<experiment_id>', methods=['DELETE'])
@login_required
def delete_experiment(experiment_id):
    result = experiment_mgr.delete_experiment(experiment_id)
    if result['success']:
        socketio.emit('experiments_update', experiment_mgr.list_experiments())
    return jsonify(result)


@app.route('/api/experiments/<experiment_id>/export.json')
@login_required
def export_experiment_json(experiment_id):
    content = experiment_mgr.export_json(experiment_id)
    if content is None:
        return jsonify({'error': 'Not found'}), 404
    exp = experiment_mgr.get_experiment(experiment_id)
    safe_name = (exp['name'] if exp else experiment_id).replace(' ', '_')
    return Response(content, mimetype='application/json',
                    headers={'Content-Disposition': f'attachment; filename="{safe_name}.json"'})


@app.route('/api/experiments/<experiment_id>/export_csv/<device_name>')
@login_required
def export_device_csv(experiment_id, device_name):
    content = experiment_mgr.export_device_csv(experiment_id, device_name)
    if content is None:
        return jsonify({'error': 'Not found'}), 404
    safe = device_name.replace(' ', '_').replace('/', '-')
    return Response(content, mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename="{safe}_schedule.csv"'})


@app.route('/api/experiments/<experiment_id>/export_multi_device_csv')
@login_required
def export_multi_device_csv(experiment_id):
    """Export all device schedules as a multi-device CSV with absolute UTC timestamps."""
    content = experiment_mgr.export_multi_device_csv(experiment_id)
    if content is None:
        exp = experiment_mgr.get_experiment(experiment_id)
        if not exp:
            return jsonify({'error': 'Experiment not found'}), 404
        return jsonify({
            'error': 'No timestamp reference found. Import the experiment from a multi-device CSV first.'
        }), 400
    exp = experiment_mgr.get_experiment(experiment_id)
    safe_name = (exp['name'] if exp else experiment_id).replace(' ', '_')
    return Response(content, mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename="{safe_name}_multi_device.csv"'})


@app.route('/api/experiments/<experiment_id>/import_device_schedule', methods=['POST'])
@login_required
def import_device_schedule(experiment_id):
    """Parse an uploaded CSV and assign it to a device in the experiment."""
    device_name = request.form.get('device_name', '').strip()
    if not device_name:
        return jsonify({'error': 'device_name is required'}), 400
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    try:
        content = request.files['file'].read().decode('utf-8-sig')
    except Exception:
        return jsonify({'error': 'Could not decode file'}), 400

    parsed = experiment_mgr.parse_device_schedule_csv(content)
    if not parsed['success']:
        return jsonify({'error': parsed['error'], 'row_errors': parsed.get('row_errors', [])}), 400

    result = experiment_mgr.assign_device_schedule(experiment_id, device_name, parsed['schedule'])
    if result['success']:
        socketio.emit('experiment_updated', experiment_mgr.get_experiment(experiment_id))
    return jsonify({**result, 'count': parsed['count'], 'duration_human': parsed.get('duration_human', ''),
                    'row_errors': parsed.get('row_errors', []), 'warning': parsed.get('warning', '')})


@app.route('/api/experiments/<experiment_id>/import_multi_device_schedule', methods=['POST'])
@login_required
def import_multi_device_schedule(experiment_id):
    """Parse a multi-device CSV (Emission ID, Time (UTC), Flow (SLPM)) and assign schedules."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    try:
        content = request.files['file'].read().decode('utf-8-sig')
    except Exception:
        return jsonify({'error': 'Could not decode file'}), 400

    parsed = experiment_mgr.parse_multi_device_csv(content)
    if not parsed['success']:
        return jsonify({'error': parsed['error'], 'row_errors': parsed.get('row_errors', [])}), 400

    assigned = []
    for device_name, schedule in parsed['schedules'].items():
        result = experiment_mgr.assign_device_schedule(experiment_id, device_name, schedule)
        if result['success']:
            assigned.append({'device_name': device_name, 'steps': len(schedule)})

    if assigned:
        # Persist the global_start_iso so the experiment can be exported back to
        # multi-device CSV with correct absolute timestamps (including after a shift).
        if parsed.get('global_start_iso'):
            experiment_mgr.update_experiment(experiment_id, {'global_start_iso': parsed['global_start_iso']})
        socketio.emit('experiment_updated', experiment_mgr.get_experiment(experiment_id))

    return jsonify({
        'success': True,
        'assigned': assigned,
        'device_count': len(assigned),
        'row_errors': parsed.get('row_errors', []),
        'warning': parsed.get('warning', ''),
        'global_start_iso': parsed.get('global_start_iso', ''),
    })


@app.route('/api/parse_multi_device_csv', methods=['POST'])
@login_required
def parse_multi_device_csv_endpoint():
    """Parse-only endpoint for multi-device CSV (no experiment ID required)."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    try:
        content = request.files['file'].read().decode('utf-8-sig')
    except Exception:
        return jsonify({'error': 'Could not decode file'}), 400
    parsed = experiment_mgr.parse_multi_device_csv(content)
    if not parsed['success']:
        return jsonify({'error': parsed['error'], 'row_errors': parsed.get('row_errors', [])}), 400
    return jsonify(parsed)


@app.route('/api/import_experiment', methods=['POST'])
@login_required
def import_experiment():
    """Import an experiment from uploaded JSON."""
    if 'file' in request.files:
        try:
            content = request.files['file'].read().decode('utf-8-sig')
        except Exception:
            return jsonify({'error': 'Could not decode file'}), 400
    else:
        content = (request.get_data() or b'').decode('utf-8-sig')

    result = experiment_mgr.import_json(content)
    if result['success']:
        socketio.emit('experiments_update', experiment_mgr.list_experiments())
    return jsonify(result)


# ── Data File Routes ──────────────────────────────────────────────────────────

@app.route('/api/data/list')
@login_required
def list_data_files():
    return jsonify(experiment_mgr.list_data_files())


@app.route('/api/data/download')
@login_required
def download_data_file():
    """Download a single file. Query param: path=dir/file.csv or just file.csv"""
    rel_path = request.args.get('path', '')
    if not rel_path or '..' in rel_path:
        return jsonify({'error': 'Invalid path'}), 400
    try:
        full_path = os.path.normpath(os.path.join(DATA_DIR, rel_path))
        if not full_path.startswith(os.path.normpath(DATA_DIR)):
            return jsonify({'error': 'Access denied'}), 403
        directory = os.path.dirname(full_path)
        filename = os.path.basename(full_path)
        return send_from_directory(directory, filename, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 404


@app.route('/api/data/zip/<path:dir_name>')
@login_required
def download_data_zip(dir_name):
    """Create and stream a ZIP of all files in a data directory."""
    if '..' in dir_name:
        return jsonify({'error': 'Invalid path'}), 400
    zip_path = experiment_mgr.create_zip(dir_name)
    if not zip_path:
        return jsonify({'error': 'Directory not found or ZIP creation failed'}), 404
    return send_file(zip_path, as_attachment=True,
                      download_name=dir_name + '.zip', mimetype='application/zip')


# ── Review API Routes ─────────────────────────────────────────────────────────

@app.route('/api/review/experiments')
@login_required
def review_list_experiments():
    """List experiment folders in Data/Experiments/ with metadata."""
    import json
    exp_dir = os.path.join(DATA_DIR, 'Experiments')
    results = []
    if not os.path.isdir(exp_dir):
        return jsonify([])
    for entry in sorted(os.scandir(exp_dir), key=lambda e: e.name, reverse=True):
        if not entry.is_dir():
            continue
        meta_path = os.path.join(entry.path, 'experiment_metadata.json')
        meta = {}
        if os.path.exists(meta_path):
            try:
                with open(meta_path) as f:
                    raw = json.load(f)
                meta = raw.get('experiment', raw)
                meta.setdefault('started_at', raw.get('start_time', ''))
            except Exception:
                pass
        # Find CSV files
        csvs = [f for f in os.listdir(entry.path) if f.endswith('.csv')]
        if not csvs:
            continue
        results.append({
            'folder': entry.name,
            'name': meta.get('name', entry.name),
            'operator': meta.get('operator', ''),
            'started_at': meta.get('started_at', ''),
            'csv_file': csvs[0] if csvs else None,
        })
    return jsonify(results)


@app.route('/api/review/experiments/<path:folder>/data')
@login_required
def review_experiment_data(folder):
    """Read the long-format experiment CSV and return structured JSON."""
    import csv as csvmod, json
    exp_dir = os.path.join(DATA_DIR, 'Experiments')
    folder_path = os.path.normpath(os.path.join(exp_dir, folder))
    if not folder_path.startswith(os.path.normpath(exp_dir)):
        return jsonify({'error': 'Access denied'}), 403
    if not os.path.isdir(folder_path):
        return jsonify({'error': 'Folder not found'}), 404

    # Load metadata
    meta = {}
    meta_path = os.path.join(folder_path, 'experiment_metadata.json')
    if os.path.exists(meta_path):
        try:
            with open(meta_path) as f:
                raw = json.load(f)
            meta = raw.get('experiment', raw)
            meta.setdefault('started_at', raw.get('start_time', ''))
        except Exception:
            pass

    # Find the CSV
    csvs = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not csvs:
        return jsonify({'error': 'No CSV data file found'}), 404

    csv_path = os.path.join(folder_path, csvs[0])
    devices = {}
    try:
        with open(csv_path, newline='') as f:
            reader = csvmod.DictReader(f)
            for row in reader:
                dev = row.get('device_name', 'Unknown')
                if dev not in devices:
                    devices[dev] = []
                # Convert numeric fields
                entry = {'timestamp': row.get('timestamp', '')}
                for field in ['pressure', 'temperature', 'vol_flow', 'mass_flow',
                              'setpoint', 'accumulated_sl']:
                    v = row.get(field, '')
                    try:
                        entry[field] = float(v) if v != '' else None
                    except ValueError:
                        entry[field] = None
                devices[dev].append(entry)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({
        'meta': meta,
        'folder': folder,
        'csv_file': csvs[0],
        'device_names': list(devices.keys()),
        'data': devices,
    })


@app.route('/api/review/experiments/<path:folder>', methods=['DELETE'])
@login_required
def review_delete_experiment(folder):
    """Delete a completed experiment run folder."""
    import shutil
    exp_dir = os.path.join(DATA_DIR, 'Experiments')
    folder_path = os.path.normpath(os.path.join(exp_dir, folder))
    if not folder_path.startswith(os.path.normpath(exp_dir) + os.sep):
        return jsonify({'error': 'Access denied'}), 403
    if not os.path.isdir(folder_path):
        return jsonify({'error': 'Folder not found'}), 404
    try:
        shutil.rmtree(folder_path)
        return jsonify({'success': True})
    except OSError as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/review/experiments/<path:folder>/csv')
@login_required
def review_download_csv(folder):
    """Download the raw experiment CSV."""
    exp_dir = os.path.join(DATA_DIR, 'Experiments')
    folder_path = os.path.normpath(os.path.join(exp_dir, folder))
    if not folder_path.startswith(os.path.normpath(exp_dir)):
        return jsonify({'error': 'Access denied'}), 403
    csvs = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not csvs:
        return jsonify({'error': 'No CSV file found'}), 404
    return send_from_directory(folder_path, csvs[0], as_attachment=True)


# ── MQTT Routes & Events ──────────────────────────────────────────────────────

@app.route('/api/mqtt_config', methods=['GET', 'POST'])
@login_required
def mqtt_config():
    if request.method == 'GET':
        cfg = state.get_settings().get('mqtt', {})
        return jsonify(cfg)
    cfg = request.get_json(silent=True) or {}
    state.update_settings({'mqtt': {
        'host': cfg.get('host', ''),
        'port': int(cfg.get('port', 1883)),
        'prefix': cfg.get('prefix', 'ec'),
    }})
    return jsonify({'success': True})


@socketio.on('mqtt_connect')
@ws_login_required
def on_mqtt_connect(data):
    host   = data.get('host', '').strip()
    port   = int(data.get('port', 1883))
    prefix = data.get('prefix', 'ec').strip()
    result = mqtt_relay.connect(host, port, prefix)
    # Persist config on success
    if result.get('success'):
        state.update_settings({'mqtt': {'host': host, 'port': port, 'prefix': prefix}})
    socketio.emit('mqtt_status', mqtt_relay.get_status())
    emit('action_result', result)


@socketio.on('mqtt_disconnect')
@ws_login_required
def on_mqtt_disconnect(data):
    mqtt_relay.disconnect()
    socketio.emit('mqtt_status', mqtt_relay.get_status())
    emit('action_result', {'success': True})


# ── NAS Routes & Events ────────────────────────────────────────────────────────

@socketio.on('nas_configure')
@ws_login_required
def on_nas_configure(data):
    path = data.get('path', '').strip()
    result = nas_relay.configure(path)
    if result.get('success'):
        # Save the normalised path returned by configure(), not the raw input
        state.update_settings({'nas': {'path': result['path'], 'enabled': True}})
    socketio.emit('nas_status', nas_relay.get_status())
    emit('action_result', result)


@socketio.on('nas_disable')
@ws_login_required
def on_nas_disable(data):
    nas_relay.disable()
    saved = state.get_settings().get('nas', {})
    state.update_settings({'nas': {**saved, 'enabled': False}})
    socketio.emit('nas_status', nas_relay.get_status())
    emit('action_result', {'success': True})


# ── Experiment SocketIO Events ────────────────────────────────────────────────

@socketio.on('pre_run_check')
@ws_login_required
def on_pre_run_check(data):
    """Apply pre-run relay states, then return check result to the requesting client."""
    experiment_id = data.get('experiment_id', '')
    global_checklist = state.get_solenoid_checklist()
    result = experiment_mgr.pre_run_check(experiment_id, device_mgr, global_checklist=global_checklist)
    # Broadcast updated peripheral states so all clients see the relay changes
    for ps in device_mgr.get_all_peripheral_states():
        socketio.emit('peripheral_update', ps)
    emit('pre_run_check_result', result)


@socketio.on('start_experiment')
@ws_login_required
def on_start_experiment(data):
    experiment_id = data.get('experiment_id', '')
    exp = experiment_mgr.get_experiment(experiment_id)
    exp_name = exp.get('name', experiment_id) if exp else experiment_id
    exp_rotation = int(state.get_settings().get('exp_file_rotation_minutes', 0))
    result = experiment_mgr.start_experiment(experiment_id, device_mgr,
                                             exp_file_rotation_minutes=exp_rotation)
    if result['success']:
        socketio.emit('full_state', _build_full_state())
        socketio.emit('toast', {'type': 'success',
                                'message': f"Experiment started — logging to {result.get('data_dir', '')}"})
        _emit_log(f"[Experiment] {session.get('username','?')} started '{exp_name}' — {result.get('started_devices',0)} device(s) → {result.get('data_dir','')}")
    else:
        _emit_log(f"[Experiment] Start failed for '{exp_name}': {result.get('error','')}", 'error')
    emit('action_result', result)


@socketio.on('stop_experiment')
@ws_login_required
def on_stop_experiment(data):
    current = experiment_mgr.get_current_experiment()
    exp_name = current.get('name', '?') if current else '?'
    result = experiment_mgr.stop_experiment(device_mgr)
    if result['success']:
        socketio.emit('full_state', _build_full_state())
        socketio.emit('toast', {'type': 'info', 'message': 'Experiment stopped.'})
        _emit_log(f"[Experiment] {session.get('username','?')} stopped '{exp_name}'")
        # Post-run: auto-apply relay states, broadcast updates, warn on failures
        global_checklist = state.get_solenoid_checklist()
        post_result = experiment_mgr.post_run_check(device_mgr, global_checklist=global_checklist)
        for ps in device_mgr.get_all_peripheral_states():
            socketio.emit('peripheral_update', ps)
        if post_result.get('warnings'):
            socketio.emit('post_run_check_result', post_result)
    emit('action_result', result)


@socketio.on('create_experiment')
@ws_login_required
def on_create_experiment(data):
    exp_id = experiment_mgr.create_experiment(data)
    exp = experiment_mgr.get_experiment(exp_id)
    socketio.emit('experiments_update', experiment_mgr.list_experiments())
    emit('action_result', {'success': True, 'experiment_id': exp_id, 'experiment': exp})


@socketio.on('update_experiment')
@ws_login_required
def on_update_experiment(data):
    experiment_id = data.pop('experiment_id', '')
    result = experiment_mgr.update_experiment(experiment_id, data)
    if result['success']:
        socketio.emit('experiments_update', experiment_mgr.list_experiments())
        socketio.emit('experiment_updated', result.get('experiment', {}))
    emit('action_result', result)


@socketio.on('delete_experiment')
@ws_login_required
def on_delete_experiment(data):
    experiment_id = data.get('experiment_id', '')
    result = experiment_mgr.delete_experiment(experiment_id)
    if result['success']:
        socketio.emit('experiments_update', experiment_mgr.list_experiments())
    emit('action_result', result)


@socketio.on('remove_device_schedule')
@ws_login_required
def on_remove_device_schedule(data):
    experiment_id = data.get('experiment_id', '')
    device_name = data.get('device_name', '')
    result = experiment_mgr.remove_device_schedule(experiment_id, device_name)
    if result['success']:
        exp = experiment_mgr.get_experiment(experiment_id)
        socketio.emit('experiment_updated', exp)
    emit('action_result', result)


# ── State Builders ────────────────────────────────────────────────────────────

def _build_full_state() -> dict:
    """Build the full application state dict.

    Each subsystem is queried independently so that a failure in one
    (e.g. a device-manager timeout during reconnect) never blanks out
    unrelated fields like experiments.
    """
    with _chat_lock:
        recent_chat = list(_chat_messages[-50:])

    _s = state.get_settings()
    result: dict = {
        'devices': [],
        'peripherals': [],
        'emission_points': [],
        'sessions': [],
        'chat_messages': recent_chat,
        'crash_info': None,
        'phidget_available': False,
        'experiments': [],
        'current_experiment': None,
        'mqtt': {},
        'nas': {},
        'raw_file_rotation_minutes': _s.get('raw_file_rotation_minutes', 1440),
        'exp_file_rotation_minutes': _s.get('exp_file_rotation_minutes', 0),
    }

    # Each block is independent — a failure in one must not affect the others.
    try:
        result['emission_points'] = ep_mgr.get_all_states()
    except Exception as e:
        print(f'[full_state] emission_points error: {e}', flush=True)
    try:
        result['devices'] = device_mgr.get_all_device_states()
    except Exception as e:
        print(f'[full_state] devices error: {e}', flush=True)
    try:
        result['peripherals'] = device_mgr.get_all_peripheral_states()
    except Exception as e:
        print(f'[full_state] peripherals error: {e}', flush=True)
    try:
        result['sessions'] = _get_sessions_list()
    except Exception as e:
        print(f'[full_state] sessions error: {e}', flush=True)
    try:
        result['crash_info'] = state.get_crash_info()
    except Exception as e:
        print(f'[full_state] crash_info error: {e}', flush=True)
    try:
        result['phidget_available'] = device_mgr.phidget_available()
    except Exception as e:
        print(f'[full_state] phidget_available error: {e}', flush=True)
    try:
        result['experiments'] = experiment_mgr.list_experiments()
    except Exception as e:
        print(f'[full_state] experiments error: {e}', flush=True)
    try:
        result['current_experiment'] = experiment_mgr.get_current_experiment()
    except Exception as e:
        print(f'[full_state] current_experiment error: {e}', flush=True)
    try:
        result['mqtt'] = mqtt_relay.get_status()
    except Exception as e:
        print(f'[full_state] mqtt error: {e}', flush=True)
    try:
        result['nas'] = nas_relay.get_status()
    except Exception as e:
        print(f'[full_state] nas error: {e}', flush=True)

    return result


def _get_sessions_list() -> list:
    with _sessions_lock:
        return [
            {
                'username': info['username'],
                'ip': info['ip'],
                'connected_at': info['connected_at'],
                'session_token': info['session_token'],
            }
            for info in connected_sessions.values()
        ]


# ── Background Polling Loop ───────────────────────────────────────────────────

def _polling_loop():
    """
    Main background thread: polls all devices at exactly 1 Hz, broadcasts readings,
    and writes the heartbeat file every 5 seconds.
    Uses an absolute-deadline (fixed-rate) timer so jitter does not accumulate:
    each tick fires at t0 + N*1.0 regardless of how long the previous cycle took.
    """
    heartbeat_tick = 0
    schedule_broadcast_tick = 0
    _diag_cycle_times: list[float] = []   # rolling 60-cycle history
    _diag_next_report = time.time() + 60.0

    next_tick = time.time() + 1.0   # absolute deadline for next iteration

    while True:
        loop_start = time.time()
        _poll_heartbeat[0] += 1
        try:
            readings = device_mgr.poll_all()
            if readings.get('alicat') or readings.get('peripherals'):
                socketio.emit('readings_update', readings)

            # MQTT relay — publish each device reading if connected
            if mqtt_relay.is_connected:
                for device_id, reading in readings.get('alicat', {}).items():
                    device = device_mgr._alicat.get(device_id)
                    if device:
                        mqtt_relay.publish_reading(device.device_name, reading)

            # NAS relay — echo each device reading to CSV files.
            # Raw data is always written; experiment data is also mirrored to a
            # per-experiment subdirectory when an experiment is running.
            if nas_relay.is_enabled:
                exp_logger = device_mgr._exp_logger
                exp_subdir = None
                if exp_logger:
                    exp_subdir = os.path.join('Experiments',
                                              os.path.basename(exp_logger.data_dir))
                for device_id, reading in readings.get('alicat', {}).items():
                    device = device_mgr._alicat.get(device_id)
                    if device:
                        ep_id = getattr(device, 'emission_point_id', TEST_EP_ID)
                        ep = ep_mgr.get_ep(ep_id) or {}
                        _serial = getattr(device, 'serial_number', '') or ''
                        _ep_name = ep.get('display_name', 'TEST')
                        _nas_meta = {
                            'device_type': device.device_type,
                            'lat': device.lat if device.lat is not None else '',
                            'lon': device.lon if device.lon is not None else '',
                            'alt': device.alt if device.alt is not None else '',
                            'ep_info': ep,
                        }
                        nas_relay.write_reading(device.device_name, reading,
                                                serial=_serial, ep_name=_ep_name,
                                                meta=_nas_meta)
                        if exp_subdir:
                            nas_relay.write_reading(device.device_name, reading,
                                                    subdir=exp_subdir,
                                                    serial=_serial, ep_name=_ep_name,
                                                    meta=_nas_meta)

            heartbeat_tick += 1
            if heartbeat_tick >= 5:
                heartbeat_tick = 0
                heartbeat_state = device_mgr.get_running_state()
                heartbeat_state['_experiment'] = experiment_mgr.get_running_state_for_heartbeat()
                state.write_heartbeat(heartbeat_state)

            # Broadcast schedule progress every second so progress bars update
            schedule_broadcast_tick += 1
            if schedule_broadcast_tick >= 1:
                schedule_broadcast_tick = 0
                _broadcast_schedule_progress()

            # Auto-stop experiment when all device schedules have finished
            started_ids = experiment_mgr.get_started_device_ids()
            if started_ids and device_mgr.schedules_all_done(started_ids):
                current = experiment_mgr.get_current_experiment()
                exp_name = current.get('name', '?') if current else '?'
                result = experiment_mgr.stop_experiment(device_mgr)
                if result['success']:
                    socketio.emit('full_state', _build_full_state())
                    socketio.emit('toast', {'type': 'info', 'message': 'Experiment completed automatically.'})
                    _emit_log(f"[Experiment] '{exp_name}' completed — all schedules finished")
                    global_checklist = state.get_solenoid_checklist()
                    experiment_mgr.post_run_check(device_mgr, global_checklist=global_checklist)
                    for ps in device_mgr.get_all_peripheral_states():
                        socketio.emit('peripheral_update', ps)

        except Exception as e:
            print(f"[Polling] Error: {e}", flush=True)
            traceback.print_exc()
        except BaseException as e:
            # GreenletExit, KeyboardInterrupt, SystemExit — log and re-raise.
            # These are fatal to this greenlet and must not be swallowed.
            print(f"[Polling] Fatal {type(e).__name__} — polling loop is stopping.", flush=True)
            raise

        elapsed = time.time() - loop_start
        _diag_cycle_times.append(elapsed)
        if len(_diag_cycle_times) > 60:
            _diag_cycle_times.pop(0)

        if elapsed > 1.5:
            _emit_log(f"[Polling] SLOW cycle: {elapsed:.2f}s", 'warning')

        now = time.time()
        if now >= _diag_next_report:
            _diag_next_report = now + 60.0
            avg = sum(_diag_cycle_times) / len(_diag_cycle_times) if _diag_cycle_times else 0
            slow = sum(1 for t in _diag_cycle_times if t > 1.5)
            n_total = len(device_mgr._alicat)
            n_conn  = sum(1 for d in device_mgr._alicat.values() if d.connected)
            n_recon = len(device_mgr._reconnecting)
            pool    = gevent.get_hub().threadpool
            print(
                f"[Diag] devices={n_conn}/{n_total} reconnecting={n_recon} "
                f"poll avg={avg:.2f}s slow={slow}/{len(_diag_cycle_times)} "
                f"threadpool_hub={pool.size}",
                flush=True,
            )

        # Fixed-rate sleep: always target the next absolute 1-second boundary.
        # If this cycle ran long, sleep_time=0 and we catch up immediately,
        # but the *following* deadline is still +1.0s from the original grid.
        sleep_time = max(0.0, next_tick - time.time())
        gevent.sleep(sleep_time)
        next_tick += 1.0


def _broadcast_schedule_progress():
    """Emit per-device schedule progress for all devices with active schedules."""
    now = time.time()
    progress_updates = []
    for device_id in device_mgr._alicat:
        sched = device_mgr._schedules.get(device_id, {})
        if sched and sched.get('running'):
            elapsed = now - (sched.get('start_time') or now)
            steps = sched.get('schedule', [])
            total = steps[-1]['time'] if steps else 1
            progress_updates.append({
                'device_id': device_id,
                'elapsed': elapsed,
                'total': total,
                'pct': min(100, (elapsed / total * 100)) if total else 0,
                'current_step': sched.get('current_step', 0),
                'current_setpoint': sched.get('current_setpoint', 0.0),
            })
    if progress_updates:
        socketio.emit('schedule_progress', progress_updates)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("=" * 60)
    print("  ExxonController Web Application")
    print("=" * 60)

    # Check for crash recovery
    crash_info = state.check_crash_recovery()
    if crash_info:
        print(f"  [WARN] Crash detected!")
        print(f"         Last heartbeat : {crash_info.get('last_heartbeat', 'N/A')}")
        print(f"         Downtime        : {crash_info.get('downtime_human', 'Unknown')}")
        print(f"         UI will prompt users to resume the experiment.")

    # Restore persisted chat log
    with _chat_lock:
        _chat_messages.clear()
        _chat_messages.extend(state.load_chat_log())

    # Restore devices from saved config
    saved_config = state.get_devices()
    device_mgr.load_from_config(saved_config)

    # Auto-reconnect MQTT if previously configured
    mqtt_cfg = state.get_settings().get('mqtt', {})
    if mqtt_cfg.get('host'):
        mqtt_relay.connect(
            mqtt_cfg['host'],
            int(mqtt_cfg.get('port', 1883)),
            mqtt_cfg.get('prefix', 'ec'),
        )
    # Auto-enable NAS relay if previously configured.
    # Use restore() (no write-probe) so the relay is enabled even if the NAS
    # drive isn't mounted yet at startup; write_reading() handles OSError silently.
    nas_cfg = state.get_settings().get('nas', {})
    if nas_cfg.get('enabled') and nas_cfg.get('path'):
        nas_relay.restore(nas_cfg['path'])
        if nas_relay.is_enabled:
            print(f"  NAS File Echo: restored → {nas_cfg['path']}")
        else:
            print(f"  NAS File Echo: path invalid for this OS (kept disabled) → {nas_cfg['path']}")

    device_count = len(saved_config.get('alicat', {}))
    periph_count = len(saved_config.get('peripherals', {}))
    print(f"  Loaded {device_count} Alicat device(s), {periph_count} peripheral(s) from config.")

    # Install gevent hub error handler now that the hub and socketio exist.
    _install_gevent_error_handler()

    # Start background polling (use socketio helper so gevent schedules it properly)
    socketio.start_background_task(_polling_loop)

    print(f"  Server: http://0.0.0.0:52424")
    print(f"  Default password: admin  (change in Settings)")
    print("=" * 60)

    # ── Clean shutdown on CTRL-C or SIGTERM ────────────────────────────────
    # Use a list so the inner function can mutate it without nonlocal
    # (nonlocal only works for enclosing function scopes, not if-block scopes).
    _shutdown_requested = [False]

    # Watchdog: if the polling loop genuinely stalls (heartbeat counter stops
    # incrementing) for > 45 s, dump all thread/greenlet stacks to stderr.
    # This replaces faulthandler.dump_traceback_later(), which fired every 30 s
    # unconditionally and produced false-positive dumps whenever gevent's
    # threadpool workers were idling in acquire_with_timeout (normal behaviour).
    def _hub_watchdog():
        _last = [_poll_heartbeat[0]]
        while not _shutdown_requested[0]:
            time.sleep(45)
            if _shutdown_requested[0]:
                return
            current = _poll_heartbeat[0]
            if current == _last[0]:
                print('[Watchdog] Hub stall detected — dumping tracebacks:', flush=True)
                faulthandler.dump_traceback()
            _last[0] = current
    # Must use threadpool.spawn so this is a real OS thread, not a greenlet.
    # If the hub stalls, greenlets don't run; only real threads can detect that.
    gevent.get_hub().threadpool.spawn(_hub_watchdog)

    def _graceful_shutdown(signum, frame):
        if _shutdown_requested[0]:
            # Second Ctrl-C while cleanup is in progress: force-exit immediately.
            print('\n[Shutdown] Forcing exit.', flush=True)
            os._exit(1)
        _shutdown_requested[0] = True
        print(f'\n[Shutdown] Signal {signum} — cleaning up… (Ctrl-C again to force exit)', flush=True)
        # (no faulthandler timer to cancel — watchdog checks _shutdown_requested)

        def _do_cleanup():
            # 1. Finalise experiment logs (pure Python, no network I/O).
            try:
                experiment_mgr.stop_experiment(device_mgr)
            except Exception:
                pass
            # 2. MQTT disconnect — run in a separate greenlet with a 1.5 s timeout.
            #    paho's loop_stop() joins its background thread, which can block
            #    indefinitely when the network is disconnected (TCP timeout).
            #    Spawning a greenlet and joining with a timeout lets the watchdog
            #    preempt if MQTT hangs, while still allowing a clean disconnect
            #    when the broker is reachable.
            try:
                _g = gevent.spawn(mqtt_relay.disconnect)
                _g.join(timeout=1.5)
                if not _g.dead:
                    _g.kill()
            except Exception:
                pass
            # 3. Mark Alicat devices as disconnected and drop client refs.
            #    Do NOT call device.disconnect() / _client.close() here — pymodbus
            #    close() blocks for the full TCP timeout when the host is unreachable.
            #    The OS will reclaim the file descriptors at exit.
            for device in list(device_mgr._alicat.values()):
                try:
                    device.connected = False
                    device._client = None
                except Exception:
                    pass
            # 4. Skip periph.close() — Phidget22 C calls block on unreachable servers
            #    (confirmed by faulthandler dump showing Net.removeServer stalling).
            print('[Shutdown] Complete.', flush=True)
            os._exit(0)

        def _watchdog():
            # Hard deadline: if _do_cleanup takes > 4 s for any reason,
            # kill the process so Ctrl-C always terminates promptly.
            time.sleep(4)
            print('[Shutdown] Timeout — forcing exit.', flush=True)
            os._exit(0)

        # Signal handlers in gevent run inside the hub's event loop callback.
        # threadpool.spawn() calls semaphore.acquire() — a blocking primitive
        # that raises BlockingSwitchOutError from hub context.  gevent.spawn()
        # is non-blocking (just schedules a greenlet), and _do_cleanup is pure
        # Python (no C calls), so a greenlet is fine here.
        gevent.spawn(_do_cleanup)
        gevent.spawn(_watchdog)

    signal.signal(signal.SIGINT,  _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    try:
        socketio.run(app, host='0.0.0.0', port=52424, debug=False)
    except SystemExit:
        raise   # normal exit — let Python handle it
    except BaseException as e:
        print(f"[Fatal] socketio.run() raised {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        raise
    else:
        # socketio.run() should never return normally; if it does, we need to know.
        print("[Fatal] socketio.run() returned without exception — server has stopped.", flush=True)
