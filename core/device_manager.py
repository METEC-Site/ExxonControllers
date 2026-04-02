#!/usr/bin/env python3
"""
Device Manager
Manages all Alicat flow controllers and peripheral devices.
Handles polling, schedule execution, data logging, and history buffering.
"""

import csv
import io
import os
import threading
import traceback
import gevent
import gevent.pool
import time
import uuid
from collections import deque
from datetime import datetime, timezone

from dateutil import parser as date_parser

from core.alicat_device import AlicatDevice, DEVICE_CONFIGS
from core.data_logger import RawDataLogger, ExperimentDataLogger, PeripheralDataLogger
from core.phidget_manager import create_peripheral, PHIDGET_AVAILABLE_FLAG, check_server_health


try:
    __compiled__
    _BASE_DIR = __compiled__.containing_dir
except NameError:
    _BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(_BASE_DIR, 'Data', 'Raw')
HISTORY_MAXLEN = 3600   # 1 hour at 1 Hz


class DeviceManager:
    """
    Central manager for all connected devices and peripherals.
    Thread-safe; called from the Flask-SocketIO polling background thread
    and also from SocketIO event handlers.
    """

    def __init__(self, state_manager, socketio):
        self.state = state_manager
        self.socketio = socketio

        self._alicat: dict[str, AlicatDevice] = {}          # device_id -> AlicatDevice
        self._peripherals: dict = {}                         # peripheral_id -> peripheral obj

        self._histories: dict[str, deque] = {}              # device_id/peripheral_id -> deque
        self._raw_loggers: dict[str, RawDataLogger] = {}    # device_id -> RawDataLogger
        self._exp_logger: ExperimentDataLogger | None = None # shared experiment logger
        self._accumulated_sl: dict[str, float] = {}         # device_id -> accumulated SL (during exp)
        self._last_log_time: dict[str, float] = {}          # device_id -> last poll mono time
        self._periph_loggers: dict = {}                      # peripheral_id -> DataLogger

        self._schedules: dict[str, dict] = {}               # device_id -> schedule state
        self._last_read_ok: dict[str, bool] = {}             # device_id -> did last poll return a reading?
        self._running: dict[str, bool] = {}                  # device_id -> bool (logging active)
        self._last_reconnect: dict[str, float] = {}          # device_id -> epoch of last attempt
        self._reconnecting: set[str] = set()                 # device_ids with an active reconnect in flight
        self._disabled: dict[str, bool] = {}                 # device_id -> manually disabled
        self._was_connected: dict[str, bool | None] = {}     # None=never seen, True/False=prev state
        self._device_order: list[str] = []                   # ordered list of device_ids
        self._peripheral_order: list[str] = []               # ordered list of peripheral_ids
        self._periph_last_reconnect: dict[str, float] = {}       # peripheral_id -> epoch of last reopen attempt
        self._periph_was_opened: dict[str, bool | None] = {}   # None=never seen
        self._periph_disabled: dict[str, bool] = {}             # peripheral_id -> disabled flag
        self._periph_disconnected_since: dict[str, float | None] = {}  # when opened=True but connected=False began
        self._periph_reconnecting: set[str] = set()          # peripheral_ids with a reopen in flight

        self._auto_log_last_started: dict[str, float] = {}  # device_id -> epoch of last auto-start_device call
        self._in_experiment_devices: set[str] = set()        # device_ids currently owned by a running experiment

        self._lock = threading.Lock()

    # ── Load from Persisted Config ─────────────────────────────────────────────

    def load_from_config(self, device_configs: dict):
        """
        Restore devices and peripherals from saved config on startup.
        Attempts to connect each device.
        """
        alicat_cfgs = device_configs.get('alicat', {})
        periph_cfgs = device_configs.get('peripherals', {})

        # Restore saved order; fallback to dict insertion order
        saved_alicat_order = device_configs.get('alicat_order', list(alicat_cfgs.keys()))
        saved_periph_order = device_configs.get('peripheral_order', list(periph_cfgs.keys()))

        for device_id in saved_alicat_order:
            if device_id in alicat_cfgs:
                self._create_alicat_from_config(device_id, alicat_cfgs[device_id], connect=True)
        # Any ids not in saved order (shouldn't happen, but be safe)
        for device_id, cfg in alicat_cfgs.items():
            if device_id not in self._alicat:
                self._create_alicat_from_config(device_id, cfg, connect=True)

        for peripheral_id in saved_periph_order:
            if peripheral_id in periph_cfgs:
                self._create_peripheral_from_config(peripheral_id, periph_cfgs[peripheral_id], open_=True)
        for peripheral_id, cfg in periph_cfgs.items():
            if peripheral_id not in self._peripherals:
                self._create_peripheral_from_config(peripheral_id, cfg, open_=True)

    # ── Alicat Device Lifecycle ───────────────────────────────────────────────

    def _create_alicat_from_config(self, device_id, cfg, connect=False):
        """Internal: instantiate an AlicatDevice from config dict."""
        device = AlicatDevice(
            host=cfg['host'],
            port=cfg.get('port', 502),
            unit_id=cfg.get('unit_id', 1),
            device_type=cfg.get('device_type', 'MCP'),
            device_name=cfg.get('device_name', cfg['host']),
            max_flow=cfg.get('max_flow'),
            lat=cfg.get('lat'),
            lon=cfg.get('lon'),
            alt=cfg.get('alt'),
            expected_serial=cfg.get('expected_serial') or None,
        )
        with self._lock:
            self._alicat[device_id] = device
            self._histories[device_id] = deque(maxlen=HISTORY_MAXLEN)
            self._running[device_id] = False
            self._last_reconnect[device_id] = 0.0
            self._disabled[device_id] = bool(cfg.get('disabled', False))
            self._was_connected[device_id] = None
            self._accumulated_sl[device_id] = 0.0
            self._last_log_time[device_id] = 0.0
            if device_id not in self._device_order:
                self._device_order.append(device_id)

        if connect:
            # Stamp now so the poll loop doesn't spawn a duplicate reconnect
            # while do_connect() is still running (poll_loop checks >= 10s).
            self._last_reconnect[device_id] = time.time()
            self._reconnecting.add(device_id)
            def do_connect():
                try:
                    success = device.connect()
                    if success:
                        device.read_device_info()
                        self._check_serial_mismatch(device)
                except Exception:
                    print(f"[DeviceManager] connect error for {device_id}:", flush=True)
                    traceback.print_exc()
                finally:
                    self._last_reconnect[device_id] = time.time()
                    self._reconnecting.discard(device_id)
            threading.Thread(target=do_connect, daemon=True).start()

        return device_id, device

    def add_device(self, config: dict) -> dict:
        """
        Add a new Alicat device from UI request.
        config keys: host, port, unit_id, device_type, device_name, max_flow (optional)
        Returns {'success': bool, 'device_id': str, 'error': str}
        """
        host = config.get('host', '').strip()
        if not host:
            return {'success': False, 'error': 'Host/IP is required'}

        def _parse_coord(v):
            try:
                return float(v) if v not in (None, '', 'null') else None
            except (TypeError, ValueError):
                return None

        lat = _parse_coord(config.get('lat'))
        lon = _parse_coord(config.get('lon'))
        if lat is None or lon is None:
            return {'success': False, 'error': 'Location (lat/lon) is required'}

        expected_serial = (config.get('expected_serial') or '').strip()
        if not expected_serial:
            return {'success': False, 'error': 'Expected Serial # is required'}

        # Reject duplicate device names
        device_name = (config.get('device_name') or host).strip()
        for existing in self._alicat.values():
            if existing.device_name == device_name:
                return {'success': False, 'error': f"A device named '{device_name}' already exists"}

        device_id = str(uuid.uuid4())[:8]
        cfg = {
            'host': host,
            'port': int(config.get('port', 502)),
            'unit_id': int(config.get('unit_id', 1)),
            'device_type': config.get('device_type', 'AUTO').upper(),
            'device_name': device_name,
            'max_flow': float(config['max_flow']) if config.get('max_flow') else None,
            'lat': lat,
            'lon': lon,
            'alt': _parse_coord(config.get('alt')),
            'expected_serial': expected_serial,
        }

        _, device = self._create_alicat_from_config(device_id, cfg, connect=True)

        return {'success': True, 'device_id': device_id, 'device_name': cfg['device_name']}

    def remove_device(self, device_id: str) -> dict:
        """Disconnect and remove an Alicat device."""
        with self._lock:
            device = self._alicat.pop(device_id, None)
            self._histories.pop(device_id, None)
            self._schedules.pop(device_id, None)
            running = self._running.pop(device_id, False)
            logger = self._raw_loggers.pop(device_id, None)
            self._last_reconnect.pop(device_id, None)
            self._reconnecting.discard(device_id)
            self._disabled.pop(device_id, None)
            self._was_connected.pop(device_id, None)
            self._accumulated_sl.pop(device_id, None)
            self._last_log_time.pop(device_id, None)

        if device is None:
            return {'success': False, 'error': 'Device not found'}

        device.disconnect()
        if logger:
            logger.close()

        with self._lock:
            self._device_order = [i for i in self._device_order if i != device_id]

        return {'success': True}

    def edit_device(self, device_id: str, config: dict) -> dict:
        """
        Update device configuration in-place (name, host, port, unit_id, type, max_flow).
        Disconnects then reconnects with the new settings.
        """
        with self._lock:
            device = self._alicat.get(device_id)
            if not device:
                return {'success': False, 'error': 'Device not found'}
            # Stop any active logging/schedule while we reconfigure
            logger = self._raw_loggers.pop(device_id, None)
            sched = self._schedules.get(device_id, {})
            if sched:
                sched['running'] = False
            self._running[device_id] = False

        device.disconnect()
        if logger:
            logger.close()

        # Apply updates to the existing device object
        if config.get('host'):
            device.host = config['host'].strip()
        if config.get('port'):
            device.port = int(config['port'])
        if config.get('unit_id'):
            device.unit_id = int(config['unit_id'])
        if config.get('device_type'):
            device.device_type = config['device_type'].upper()
            # For AUTO, reset so next connect() will re-detect
            device.config = DEVICE_CONFIGS.get(device.device_type, DEVICE_CONFIGS['MCP'])
        if config.get('device_name'):
            new_name = config['device_name'].strip()
            for did, existing in self._alicat.items():
                if did != device_id and existing.device_name == new_name:
                    return {'success': False, 'error': f"A device named '{new_name}' already exists"}
            device.device_name = new_name
        if config.get('max_flow'):
            device._max_flow_user = float(config['max_flow'])
            # Effective max will be recomputed on next read_device_info (reconnect below)
            device.max_flow = float(config['max_flow'])
        elif 'max_flow' in config and not config['max_flow']:
            # Empty string / None = clear the user cap
            device._max_flow_user = None
        # lat/lon: accept empty string as "clear" (set to None)
        if 'lat' in config:
            raw = config['lat']
            device.lat = float(raw) if raw not in (None, '', 'null') else None
        if 'lon' in config:
            raw = config['lon']
            device.lon = float(raw) if raw not in (None, '', 'null') else None
        if 'alt' in config:
            raw = config['alt']
            device.alt = float(raw) if raw not in (None, '', 'null') else None
        # expected_serial: empty string clears it
        if 'expected_serial' in config:
            device.expected_serial = config['expected_serial'].strip() or None if isinstance(config['expected_serial'], str) else (config['expected_serial'] or None)

        # Reset tracking so next connection is treated as initial (no spurious toast)
        self._was_connected[device_id] = None
        # Set last_reconnect to now so the poll loop does NOT also try to reconnect
        # simultaneously — do_connect() below is already handling the reconnect.
        self._last_reconnect[device_id] = time.time()

        def do_connect():
            try:
                success = device.connect()
                if success:
                    device.read_device_info()
                    self._check_serial_mismatch(device)
            except Exception:
                print(f"[DeviceManager] do_connect error for {device_id}:", flush=True)
                traceback.print_exc()
            finally:
                self._last_reconnect[device_id] = time.time()
                # Always broadcast the final state so the UI stops showing "disconnected"
                # even if the reconnect was fast and the poll loop missed the None→True transition.
                self.socketio.emit('device_update', self.get_device_state(device_id))
        threading.Thread(target=do_connect, daemon=True).start()

        return {'success': True}

    def disable_device(self, device_id: str, disabled: bool) -> dict:
        """
        Disable or re-enable a device. Disabled devices are skipped by the poll
        loop (no reads, no reconnect attempts) but remain in the config.
        """
        if device_id not in self._alicat:
            return {'success': False, 'error': 'Device not found'}
        with self._lock:
            self._disabled[device_id] = disabled
            if disabled:
                # Stop active schedule; logging stops naturally (no more reads)
                sched = self._schedules.get(device_id, {})
                if sched:
                    sched['running'] = False
                self._running[device_id] = False
                logger = self._raw_loggers.pop(device_id, None)
            else:
                logger = None
                # Reset reconnect timer so poll loop tries immediately
                self._last_reconnect[device_id] = 0.0
                self._was_connected[device_id] = None

        if disabled and logger:
            logger.close()
        return {'success': True, 'disabled': disabled}

    def disable_peripheral(self, peripheral_id: str, disabled: bool) -> dict:
        """Disable or re-enable a peripheral. Disabled peripherals are closed and skipped."""
        periph = self._peripherals.get(peripheral_id)
        if not periph:
            return {'success': False, 'error': 'Peripheral not found'}
        with self._lock:
            self._periph_disabled[peripheral_id] = disabled
        if disabled:
            # Phidget22 close() calls Net.removeServer and ch.close(), which
            # are blocking C calls.  Must run in a real OS thread (threadpool)
            # rather than threading.Thread, which after monkey.patch_all() is a
            # gevent greenlet and would stall the entire event loop.
            gevent.get_hub().threadpool.spawn(periph.close)
        else:
            # Set last_reconnect to now so the poll loop doesn't immediately
            # spawn a concurrent _try_reopen that races with the open() below.
            self._periph_last_reconnect[peripheral_id] = time.time()
            self._periph_was_opened[peripheral_id] = None
            self._periph_disconnected_since[peripheral_id] = None
            gevent.get_hub().threadpool.spawn(periph.open)
        return {'success': True, 'disabled': disabled}

    def start_device(self, device_id: str) -> dict:
        """Start raw data logging for a device (daily-rotating CSV in Data/Raw/)."""
        with self._lock:
            device = self._alicat.get(device_id)
            if not device:
                return {'success': False, 'error': 'Device not found'}
            if not device.connected:
                return {'success': False, 'error': 'Device not connected'}
            if self._running.get(device_id):
                old_logger = self._raw_loggers.pop(device_id, None)
                if old_logger:
                    old_logger.close()
            rotation_minutes = self.state.get_settings().get('raw_file_rotation_minutes', 1440)
            device_meta = {
                'device_type': device.device_type,
                'location':    device.device_name,
                'serial':      getattr(device, 'serial_number', '') or '',
                'lat':         device.lat if device.lat is not None else '',
                'lon':         device.lon if device.lon is not None else '',
                'alt':         device.alt if device.alt is not None else '',
            }
            logger = RawDataLogger(device_name=device.device_name, data_dir=RAW_DATA_DIR,
                                   rotation_minutes=rotation_minutes, device_meta=device_meta)
            self._raw_loggers[device_id] = logger
            self._running[device_id] = True

        return {'success': True}

    def set_experiment_logger(self, exp_logger: ExperimentDataLogger):
        """Attach a shared experiment logger; resets accumulated_sl for all devices."""
        with self._lock:
            self._exp_logger = exp_logger
            for did in self._alicat:
                self._accumulated_sl[did] = 0.0
                self._last_log_time[did] = 0.0

    def clear_experiment_logger(self):
        """Detach experiment logger and clear accumulated flow state."""
        with self._lock:
            self._exp_logger = None
            for did in self._alicat:
                self._accumulated_sl[did] = 0.0
                self._last_log_time[did] = 0.0

    def stop_device(self, device_id: str) -> dict:
        """Stop data logging for a device and zero its setpoint."""
        with self._lock:
            logger = self._raw_loggers.pop(device_id, None)
            self._running[device_id] = False
            sched = self._schedules.get(device_id, {})
            if sched:
                sched['running'] = False

        if logger:
            logger.close()

        # Zero the setpoint so the flow controller stops flowing
        device = self._alicat.get(device_id)
        if device and device.connected:
            try:
                device.set_flow_rate(0.0)
            except Exception:
                pass

        return {'success': True}

    # ── Experiment ownership ──────────────────────────────────────────────────

    def mark_in_experiment(self, device_ids: list[str]):
        """Called by ExperimentManager when an experiment starts.
        Prevents auto-stop-logging from firing for these devices."""
        with self._lock:
            self._in_experiment_devices.update(device_ids)

    def unmark_in_experiment(self, device_ids: list[str]):
        """Called by ExperimentManager when an experiment stops."""
        with self._lock:
            self._in_experiment_devices.difference_update(device_ids)

    # ── Setpoint and Gas ─────────────────────────────────────────────────────

    def set_setpoint(self, device_id: str, setpoint) -> dict:
        device = self._alicat.get(device_id)
        if not device:
            return {'success': False, 'error': 'Device not found'}
        try:
            setpoint = float(setpoint)
        except (TypeError, ValueError):
            return {'success': False, 'error': 'Invalid setpoint value'}

        success, msg = device.set_flow_rate(setpoint)
        # last_reading['setpoint'] is updated inside set_flow_rate via readback,
        # so no optimistic override needed here.
        return {'success': success, 'message': msg}

    def set_gas(self, device_id: str, gas_number) -> dict:
        device = self._alicat.get(device_id)
        if not device:
            return {'success': False, 'error': 'Device not found'}
        try:
            gas_number = int(gas_number)
        except (TypeError, ValueError):
            return {'success': False, 'error': 'Invalid gas number'}

        success, msg = device.set_gas(gas_number)
        return {'success': success, 'message': msg}

    # ── Schedule Management ───────────────────────────────────────────────────

    def parse_schedule(self, csv_content: str):
        """
        Parse a schedule CSV (same formats as CLI version).
        Returns list of (time_seconds, flow_rate) tuples or None on error.
        """
        try:
            reader = csv.DictReader(io.StringIO(csv_content))
            fieldnames_lower = {k.lower().strip(): k for k in (reader.fieldnames or [])}

            time_key = fieldnames_lower.get('time')
            rate_key = next(
                (v for k, v in fieldnames_lower.items() if 'rate' in k or 'lpm' in k or 'slpm' in k),
                None
            )
            if not time_key or not rate_key:
                return None

            rows = list(reader)
            if not rows:
                return None

            # Detect ISO8601 vs seconds
            first_time = rows[0][time_key].strip()
            is_iso = 'T' in first_time or '-' in first_time

            # Two-pass for ISO: parse all entries, sort by datetime, then compute
            # relative seconds — avoids negative offsets from out-of-order rows.
            raw_entries = []
            for row in rows:
                t_raw = row[time_key].strip()
                rate = float(row[rate_key].strip())
                if is_iso:
                    dt = date_parser.parse(t_raw)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    raw_entries.append((dt, rate))
                else:
                    raw_entries.append((float(t_raw), rate))

            raw_entries.sort(key=lambda x: x[0])
            if is_iso:
                ref_time = raw_entries[0][0]
                schedule = [((dt - ref_time).total_seconds(), r) for dt, r in raw_entries]
            else:
                schedule = raw_entries

            return [{'time': t, 'rate': r} for t, r in schedule]
        except Exception as e:
            print(f"[DeviceManager] Schedule parse error: {e}")
            return None

    def load_schedule(self, device_id: str, schedule: list) -> dict:
        """Load a schedule for a device without starting it."""
        if device_id not in self._alicat:
            return {'success': False, 'error': 'Device not found'}
        if not schedule:
            return {'success': False, 'error': 'Empty schedule'}

        # Normalise step keys: UI uses 'setpoint', CSV parser uses 'rate'.
        schedule = [
            {'time': s['time'], 'rate': s.get('rate', s.get('setpoint', 0.0))}
            for s in schedule
        ]

        with self._lock:
            self._schedules[device_id] = {
                'schedule': schedule,
                'running': False,
                'start_time': None,
                'current_step': 0,
                'current_setpoint': 0.0,
            }
        return {'success': True, 'steps': len(schedule)}

    def start_schedule(self, device_id: str) -> dict:
        """Start executing the loaded schedule for a device."""
        with self._lock:
            sched = self._schedules.get(device_id)
            if not sched:
                return {'success': False, 'error': 'No schedule loaded'}
            if sched['running']:
                return {'success': False, 'error': 'Schedule already running'}
            sched['running'] = True
            sched['start_time'] = time.time()
            sched['current_step'] = 0
        return {'success': True}

    def stop_schedule(self, device_id: str) -> dict:
        """Stop the schedule for a device (device stays connected)."""
        with self._lock:
            sched = self._schedules.get(device_id)
            if sched:
                sched['running'] = False
        return {'success': True}

    def schedules_all_done(self, device_ids: list) -> bool:
        """Return True when every device in device_ids has finished its schedule.
        Returns False if device_ids is empty (nothing to check)."""
        if not device_ids:
            return False
        for did in device_ids:
            sched = self._schedules.get(did)
            if sched and sched.get('running'):
                return False
        return True

    def _tick_schedule(self, device_id: str, force_resend: bool = False):
        """
        Advance schedule for one device. Called from poll_all().
        force_resend=True skips the change-detection check and always writes the
        current scheduled setpoint — used on reconnect so a device whose internal
        state reset to 0 is immediately brought back to the correct rate.
        Returns True if a setpoint command was sent.
        """
        sched = self._schedules.get(device_id)
        if not sched or not sched['running']:
            return False

        device = self._alicat.get(device_id)
        if not device or not device.connected:
            return False

        elapsed = time.time() - sched['start_time']
        steps = sched['schedule']
        last_time = steps[-1]['time']

        if elapsed > last_time:
            sched['running'] = False
            device.set_flow_rate(0.0)
            return False

        # Find the last step where elapsed >= step time
        target_rate = sched['current_setpoint']
        for step in steps:
            if elapsed >= step['time']:
                target_rate = step['rate']
            else:
                break

        if force_resend or target_rate != sched['current_setpoint']:
            device.set_flow_rate(target_rate)
            sched['current_setpoint'] = target_rate
            sched['current_step'] = next(
                (i for i, s in enumerate(steps) if s['rate'] == target_rate),
                0
            )
            return True
        return False

    # ── Peripheral Lifecycle ─────────────────────────────────────────────────

    def _create_peripheral_from_config(self, peripheral_id, cfg, open_=False):
        """Internal: create a peripheral object from config dict."""
        periph = create_peripheral(cfg | {'peripheral_id': peripheral_id})
        if periph is None:
            return None

        # Create a data logger for this peripheral (outside the lock — does file I/O)
        channel_labels = getattr(periph, 'channel_labels', None) or ['ch0', 'ch1', 'ch2', 'ch3']
        device_meta = {
            'type':     cfg.get('type', ''),
            'location': cfg.get('name', ''),
            'hostname': cfg.get('hostname', ''),
            'port':     cfg.get('port', ''),
            'hub_port': cfg.get('hub_port', ''),
        }
        periph_logger = PeripheralDataLogger(
            peripheral_name=getattr(periph, 'name', peripheral_id),
            channel_labels=channel_labels,
            data_dir=RAW_DATA_DIR,
            device_meta=device_meta,
        )

        with self._lock:
            self._peripherals[peripheral_id] = periph
            self._periph_loggers[peripheral_id] = periph_logger
            self._histories[peripheral_id] = deque(maxlen=HISTORY_MAXLEN)
            if peripheral_id not in self._peripheral_order:
                self._peripheral_order.append(peripheral_id)
            self._periph_last_reconnect[peripheral_id] = 0.0
            self._periph_was_opened[peripheral_id] = None
            self._periph_disabled[peripheral_id] = bool(cfg.get('disabled', False))
            self._periph_disconnected_since[peripheral_id] = None

        if open_ and not self._periph_disabled.get(peripheral_id):
            # Use threadpool (real OS thread) — see disable_peripheral for why.
            gevent.get_hub().threadpool.spawn(periph.open)
        return periph

    def add_peripheral(self, config: dict) -> dict:
        """Add a new peripheral device."""
        peripheral_id = str(uuid.uuid4())[:8]
        cfg = dict(config)
        cfg['peripheral_id'] = peripheral_id

        periph = self._create_peripheral_from_config(peripheral_id, cfg, open_=True)
        if periph is None:
            return {'success': False, 'error': f"Unknown peripheral type: {config.get('type')}"}

        return {'success': True, 'peripheral_id': peripheral_id}

    def remove_peripheral(self, peripheral_id: str) -> dict:
        """Close and remove a peripheral."""
        with self._lock:
            periph = self._peripherals.pop(peripheral_id, None)
            self._histories.pop(peripheral_id, None)
            logger = self._periph_loggers.pop(peripheral_id, None)

        if periph is None:
            return {'success': False, 'error': 'Peripheral not found'}

        periph.close()
        if logger:
            logger.close()
        with self._lock:
            self._peripheral_order = [i for i in self._peripheral_order if i != peripheral_id]
            self._periph_last_reconnect.pop(peripheral_id, None)
            self._periph_was_opened.pop(peripheral_id, None)
            self._periph_disabled.pop(peripheral_id, None)
            self._periph_disconnected_since.pop(peripheral_id, None)
        return {'success': True}

    def edit_peripheral(self, peripheral_id: str, config: dict) -> dict:
        """
        Update all editable peripheral fields in-place, then close and reopen
        the Phidget connection so hardware changes take effect immediately.
        """
        with self._lock:
            periph = self._peripherals.get(peripheral_id)
            if not periph:
                return {'success': False, 'error': 'Peripheral not found'}

            if 'name' in config and config['name'].strip():
                periph.name = config['name'].strip()

            if 'hub_serial' in config:
                raw = config['hub_serial']
                periph.hub_serial = int(raw) if raw not in (None, '', 'null') else None

            if 'server_hostname' in config:
                periph.server_hostname = config['server_hostname'] or None
            if 'server_port' in config:
                periph.server_port = int(config['server_port'] or 5661)
            if 'server_password' in config:
                periph.server_password = config['server_password'] or ''

            if 'channel_offset' in config and hasattr(periph, 'channel_offset'):
                periph.channel_offset = int(config['channel_offset'] or 0)
            if 'hub_port' in config and hasattr(periph, 'hub_port'):
                raw_hp = config['hub_port']
                periph.hub_port = int(raw_hp) if raw_hp not in (None, '', 'null') else None
            if 'calibration' in config and hasattr(periph, 'calibration'):
                cal = config['calibration']
                if isinstance(cal, (list, tuple)) and len(cal) >= 2:
                    periph.calibration = (float(cal[0]), float(cal[1]))
            if 'units' in config and hasattr(periph, 'units') and config['units']:
                periph.units = config['units']

            if 'channel_labels' in config and isinstance(config['channel_labels'], list):
                periph.channel_labels = [
                    str(l) for l in config['channel_labels'][:periph.NUM_CHANNELS]
                ]

        # Reconnect in background so the handler returns immediately.
        # Must use threadpool (real OS thread) — see disable_peripheral for why.
        def do_reconnect():
            periph.close()
            periph.open()
        gevent.get_hub().threadpool.spawn(do_reconnect)
        return {'success': True}

    def set_relay(self, peripheral_id: str, channel: int, state: bool) -> dict:
        """Set a relay channel on/off."""
        periph = self._peripherals.get(peripheral_id)
        if periph is None:
            return {'success': False, 'error': 'Peripheral not found'}
        if not hasattr(periph, 'set_channel'):
            return {'success': False, 'error': 'Not a relay device'}
        success, msg = periph.set_channel(int(channel), bool(state))
        return {'success': success, 'message': msg} if success else {'success': False, 'error': msg}

    # ── Polling ───────────────────────────────────────────────────────────────

    def _poll_single_device(self, device_id: str, now_mono: float) -> tuple[str, dict | None]:
        """
        Poll one Alicat device. Runs in its own worker thread so all devices
        are read concurrently, preventing a slow/hung device from blocking the
        rest.  Returns (device_id, reading_or_None).
        """
        device = self._alicat.get(device_id)
        if device is None:
            return device_id, None

        if self._disabled.get(device_id):
            return device_id, None

        curr_connected = device.connected
        prev_connected = self._was_connected.get(device_id)

        if curr_connected != prev_connected and prev_connected is not None:
            if curr_connected:
                self.socketio.emit('toast', {'message': f'{device.device_name} reconnected', 'type': 'success'})
                self.socketio.emit('server_log', {'msg': f'[Device] {device.device_name} reconnected', 'level': 'info', 'ts': datetime.now(timezone.utc).strftime('%H:%M:%S')})
            else:
                self.socketio.emit('toast', {'message': f'{device.device_name} connection lost', 'type': 'warning'})
                self.socketio.emit('server_log', {'msg': f'[Device] {device.device_name} connection lost', 'level': 'warning', 'ts': datetime.now(timezone.utc).strftime('%H:%M:%S')})
            self.socketio.emit('device_update', self.get_device_state(device_id))

        if curr_connected != prev_connected:
            self._was_connected[device_id] = curr_connected

        if not curr_connected:
            last = self._last_reconnect.get(device_id, 0)
            if now_mono - last >= 3.0 and device_id not in self._reconnecting:
                self._last_reconnect[device_id] = now_mono
                self._reconnecting.add(device_id)
                # Spawn as a gevent greenlet (not an OS thread) so socket I/O is
                # cooperative.  Unreachable devices cannot consume threadpool slots
                # and cannot stall reads of the connected devices.
                gevent.spawn(self._try_reconnect, device_id)
            return device_id, None

        # The custom _ModbusTCPClient uses gevent-monkey-patched sockets, so
        # read_process_values() must run directly in this greenlet — not in an
        # OS threadpool thread.  Threadpool threads have no greenlet context, so
        # socket recv/sendall would fail silently (the gevent hub never delivers
        # the I/O events), causing spurious "marked disconnected after 2 failures"
        # cycles.  Within a greenlet, the patched socket yields cooperatively
        # during recv, allowing all other greenlets to run concurrently.
        try:
            reading = device.read_process_values()
        except Exception:
            reading = None

        if reading is None:
            if device.connected != curr_connected:
                self._was_connected[device_id] = device.connected
                self.socketio.emit('toast', {'message': f'{device.device_name} connection lost', 'type': 'warning'})
                self.socketio.emit('server_log', {'msg': f'[Device] {device.device_name} connection lost (read timeout)', 'level': 'warning', 'ts': datetime.now(timezone.utc).strftime('%H:%M:%S')})
                self.socketio.emit('device_update', self.get_device_state(device_id))
            return device_id, None

        if prev_connected is None:
            self._was_connected[device_id] = True

        return device_id, reading

    def poll_all(self) -> dict:
        """
        Poll all devices and peripherals concurrently.
        Each Alicat device is read in its own short-lived thread so a slow or
        hung device (e.g. Modbus TCP timeout = 1 s) cannot delay other devices.
        Results are gathered back in the main poll thread before broadcasting.
        """
        now_ts = datetime.now(timezone.utc).isoformat()
        now_mono = time.time()
        readings = {'timestamp': now_ts, 'alicat': {}, 'peripherals': {}}

        device_ids = [did for did in list(self._alicat.keys())
                      if not self._disabled.get(did)]

        # Spawn all reads concurrently as gevent greenlets so that a slow
        # or hung Modbus device cannot delay the others.  All greenlets are
        # live before we start collecting, so the total wall-clock cost is
        # roughly max(individual_times), not sum(individual_times).
        results: dict[str, dict | None] = {}

        glets = {
            did: gevent.spawn(self._poll_single_device, did, now_mono)
            for did in device_ids
        }
        t_join_start = time.time()
        gevent.joinall(list(glets.values()), timeout=1.5)
        t_join_elapsed = time.time() - t_join_start

        killed_names: list[str] = []
        for did, glet in glets.items():
            if glet.successful():
                pair = glet.value
                results[did] = pair[1] if pair else None
            else:
                results[did] = None
            if not glet.dead:
                dev = self._alicat.get(did)
                killed_names.append(dev.device_name if dev else did)
                glet.kill()  # ensure stalled greenlets don't linger
                if dev:
                    dev.connected = False
                    dev.fail_count = 0
                    # Close the pymodbus socket to interrupt the blocked OS thread.
                    # Without this, the threadpool fills with zombie connections and
                    # the device never re-attempts after WiFi reconnect.
                    try:
                        if dev._client and hasattr(dev._client, 'socket') and dev._client.socket:
                            dev._client.socket.close()
                    except Exception:
                        pass

        if killed_names:
            print(f"[PollAll] Killed {len(killed_names)} stalled greenlet(s) after "
                  f"{t_join_elapsed:.2f}s: {', '.join(killed_names)}", flush=True)

        # Post-process gathered results
        for device_id in device_ids:
            reading = results.get(device_id)
            if reading is None:
                self._last_read_ok[device_id] = False
                continue

            device = self._alicat.get(device_id)
            if device is None:
                continue

            self._histories[device_id].append(reading)
            readings['alicat'][device_id] = dict(reading)

            geo = {
                'lat': device.lat if device.lat is not None else '',
                'lon': device.lon if device.lon is not None else '',
                'alt': device.alt if device.alt is not None else '',
            }

            # Auto-start raw logging whenever the device has a non-zero setpoint.
            # Rate-limited to once every 10 s to avoid racing with manual stop/start.
            if not self._running.get(device_id) and not self._disabled.get(device_id):
                if (reading.get('setpoint') or 0) > 0:
                    _now = time.monotonic()
                    if _now - self._auto_log_last_started.get(device_id, 0) >= 10:
                        self._auto_log_last_started[device_id] = _now
                        self.start_device(device_id)
                        self.socketio.emit('device_update', self.get_device_state(device_id))

            # Auto-stop raw logging when setpoint returns to 0, provided this
            # device is not currently owned by a running experiment.
            elif (self._running.get(device_id)
                  and device_id not in self._in_experiment_devices
                  and (reading.get('setpoint') or 0) == 0):
                self.stop_device(device_id)
                self.socketio.emit('device_update', self.get_device_state(device_id))

            raw_logger = self._raw_loggers.get(device_id)
            if raw_logger:
                raw_logger.log(dict(reading) | geo)

            exp_logger = self._exp_logger
            if exp_logger:
                last_t = self._last_log_time.get(device_id, 0.0)
                if last_t > 0.0:
                    dt = now_mono - last_t
                    if dt <= 10.0:
                        mass_flow = reading.get('mass_flow') or 0.0
                        self._accumulated_sl[device_id] = (
                            self._accumulated_sl.get(device_id, 0.0) + mass_flow * dt / 60.0
                        )
                self._last_log_time[device_id] = now_mono
                exp_row = dict(reading) | geo
                exp_row['accumulated_sl'] = round(self._accumulated_sl.get(device_id, 0.0), 4)
                exp_logger.log_device(device.device_name, exp_row)

            readings['alicat'][device_id]['accumulated_sl'] = round(
                self._accumulated_sl.get(device_id, 0.0), 4
            )

            # Detect reconnect: last poll failed but this one succeeded.
            # Force-resend the scheduled setpoint so the device isn't left at
            # whatever its internal state reset to during the outage.
            just_reconnected = self._last_read_ok.get(device_id) == False
            self._last_read_ok[device_id] = True
            self._tick_schedule(device_id, force_resend=just_reconnected)

            hist = self._histories[device_id]
            if len(hist) % 10 == 0 and self._running.get(device_id):
                sched = self._schedules.get(device_id)
                # Only re-send the scheduled setpoint when the schedule is
                # actively running.  A loaded-but-not-started (or completed)
                # schedule has current_setpoint=0.0 which must NOT override a
                # manual setpoint set while no schedule is in progress.
                if sched and sched.get('running') and sched.get('current_setpoint') is not None:
                    device.set_flow_rate(sched['current_setpoint'])

        # Server-level health check: probe each unique Phidget server endpoint
        # with a raw TCP connect.  If unreachable, immediately force-disconnect
        # all peripherals on that server so the poll loop triggers close+open.
        # This catches silent TCP deaths (e.g. VINT hub power loss) where
        # on_detach never fires.  Rate-limited internally to once per 3 s per
        # endpoint and skipped entirely when all peripherals are already
        # disconnected.
        check_server_health(self._peripherals)

        for peripheral_id, periph in list(self._peripherals.items()):
            pstate = periph.get_state()

            # Skip disabled peripherals — emit static state but no reconnect
            if self._periph_disabled.get(peripheral_id):
                pstate['disabled'] = True
                readings['peripherals'][peripheral_id] = pstate
                continue

            # Use the actual Phidget attachment state (connected) for transition detection.
            # `opened` is set immediately after the non-blocking ch.open() call and stays
            # True even while waiting for hardware to attach, so it can't be used for this.
            curr_connected = pstate.get('connected', False)
            prev_connected = self._periph_was_opened.get(peripheral_id)

            if prev_connected is not None and curr_connected != prev_connected:
                if curr_connected:
                    self.socketio.emit('toast', {'message': f'Peripheral {periph.name} reconnected', 'type': 'success'})
                    self.socketio.emit('server_log', {'msg': f'[Peripheral] {periph.name} connected', 'level': 'info', 'ts': datetime.now(timezone.utc).strftime('%H:%M:%S')})
                else:
                    self.socketio.emit('toast', {'message': f'Peripheral {periph.name} connection lost', 'type': 'warning'})
                    self.socketio.emit('server_log', {'msg': f'[Peripheral] {periph.name} connection lost', 'level': 'warning', 'ts': datetime.now(timezone.utc).strftime('%H:%M:%S')})
                self.socketio.emit('peripheral_update', pstate)

            self._periph_was_opened[peripheral_id] = curr_connected

            opened = pstate.get('opened', False)

            # Track how long we've been in the opened=True but not-connected limbo.
            if opened and not curr_connected:
                if self._periph_disconnected_since.get(peripheral_id) is None:
                    self._periph_disconnected_since[peripheral_id] = now_mono
            else:
                self._periph_disconnected_since[peripheral_id] = None

            # Case 1: open() itself failed — retry every 3s.
            # Case 2: opened=True but Phidget not attached — force close+open after 2s.
            #   ChannelPersistence is disabled, so the Phidget library will not
            #   re-attach on its own.  The 2s window absorbs momentary glitches
            #   (e.g. a single missed on_attach) before we force a full
            #   close+open cycle with a fresh TCP connection.
            need_reopen = False
            reopen_interval = 3.0
            if not opened:
                need_reopen = True
            elif opened and not curr_connected:
                since = self._periph_disconnected_since.get(peripheral_id)
                if since is not None and (now_mono - since) >= 2.0:
                    need_reopen = True

            if need_reopen:
                last = self._periph_last_reconnect.get(peripheral_id, 0)
                if now_mono - last >= reopen_interval:
                    self._periph_last_reconnect[peripheral_id] = now_mono
                    self._periph_disconnected_since[peripheral_id] = None
                    # Skip if a previous _try_reopen is still in flight.
                    if peripheral_id not in self._periph_reconnecting:
                        self._periph_reconnecting.add(peripheral_id)
                        def _try_reopen(p=periph, pid=peripheral_id):
                            try:
                                p.close(for_reconnect=True)
                                p.open()
                            except BaseException:
                                pass
                            finally:
                                self._periph_reconnecting.discard(pid)
                        gevent.get_hub().threadpool.spawn(_try_reopen)

            readings['peripherals'][peripheral_id] = pstate
            # Log peripheral state to CSV when connected
            periph_logger = self._periph_loggers.get(peripheral_id)
            if periph_logger and curr_connected:
                periph_logger.log(now_ts, pstate.get('values', []))
            hist_entry = {'timestamp': now_ts, 'values': pstate.get('values', [])}
            self._histories[peripheral_id].append(hist_entry)

            # Reset the Phidget failsafe watchdog for peripherals that support it
            # (e.g. MechanicalRelayPeripheral).  This keeps the relay hardware
            # energised; if the process stalls the board will de-energise on its own.
            if curr_connected and hasattr(periph, 'heartbeat'):
                try:
                    periph.heartbeat()
                except Exception:
                    pass

        return readings

    def _check_serial_mismatch(self, device: AlicatDevice):
        """Emit a warning toast if the device's reported serial doesn't match expected."""
        expected = device.expected_serial
        if not expected:
            return  # No expectation configured — nothing to check
        reported = device.serial_number
        if reported is None:
            # Device didn't report a serial (MC series or register unavailable)
            self.socketio.emit('toast', {
                'message': (f"⚠ {device.device_name}: expected serial {expected} "
                            f"but device did not report a serial number"),
                'type': 'warning',
            })
        elif str(reported).strip() != str(expected).strip():
            self.socketio.emit('toast', {
                'message': (f"⚠ {device.device_name}: serial mismatch — "
                            f"expected {expected}, got {reported}"),
                'type': 'danger',
            })

    def _try_reconnect(self, device_id: str):
        """
        Reconnect a disconnected Alicat device.  Runs as a gevent greenlet so all
        socket I/O (TCP connect + Modbus handshake) is handled by gevent's
        monkey-patched socket layer — fully cooperative.  An unreachable device
        simply yields for up to `timeout` seconds and then moves on; it does NOT
        consume a threadpool OS thread, so it cannot starve the reads of connected
        devices.  A Timeout cap guards against any unexpected pymodbus stall.
        """
        device = self._alicat.get(device_id)
        name = device.device_name if device else device_id
        t_start = time.time()
        try:
            if not device or device.connected:
                return
            timed_out_connect = False
            try:
                with gevent.Timeout(4.0):
                    # device.connect() ultimately calls socket.create_connection,
                    # which is monkey-patched → cooperative.  No threadpool needed.
                    success = device.connect()
            except gevent.Timeout:
                success = False
                timed_out_connect = True
            elapsed = time.time() - t_start
            if success:
                try:
                    with gevent.Timeout(3.0):
                        device.read_device_info()
                except gevent.Timeout:
                    print(f"[Reconnect] <= {name} connected but read_device_info timed out "
                          f"({elapsed:.2f}s)", flush=True)
                self._check_serial_mismatch(device)
                print(f"[Reconnect] <= {name} OK ({elapsed:.2f}s)", flush=True)
        except Exception as e:
            elapsed = time.time() - t_start
            if not isinstance(e, (ConnectionRefusedError, TimeoutError)):
                print(f"[Reconnect] <= {name} error after {elapsed:.2f}s: {e}", flush=True)
        finally:
            self._reconnecting.discard(device_id)
            self._last_reconnect[device_id] = time.time()

    # ── State Queries ─────────────────────────────────────────────────────────

    def get_device_state(self, device_id: str) -> dict:
        """Return full state for a single device (for broadcasting after an action)."""
        device = self._alicat.get(device_id)
        if not device:
            return {}
        sched = self._schedules.get(device_id, {})
        return {
            'device_id': device_id,
            'device_name': device.device_name,
            'host': device.host,
            'port': device.port,
            'unit_id': device.unit_id,
            'device_type': device.device_type,
            'max_flow': device.max_flow,
            'max_flow_reported': device.max_flow_reported,
            'max_flow_user': device._max_flow_user,
            'max_flow_is_fallback': device.max_flow_is_fallback,
            'lat': device.lat,
            'lon': device.lon,
            'alt': device.alt,
            'connected': device.connected,
            'serial_number': device.serial_number,
            'expected_serial': device.expected_serial,
            'gas_number': device.gas_number,
            'last_reading': device.last_reading,
            'disabled': self._disabled.get(device_id, False),
            'logging': self._running.get(device_id, False),
            'accumulated_sl': round(self._accumulated_sl.get(device_id, 0.0), 4),
            'schedule': {
                'loaded': bool(sched),
                'running': sched.get('running', False),
                'steps': len(sched.get('schedule', [])),
                'current_step': sched.get('current_step', 0),
                'current_setpoint': sched.get('current_setpoint', 0.0),
                'start_time': sched.get('start_time'),
            } if sched else None,
        }

    def get_all_device_states(self) -> list:
        ordered = [did for did in self._device_order if did in self._alicat]
        return [self.get_device_state(did) for did in ordered]

    def get_peripheral_state(self, peripheral_id: str) -> dict:
        periph = self._peripherals.get(peripheral_id)
        if not periph:
            return {}
        state = periph.get_state()
        state['disabled'] = self._periph_disabled.get(peripheral_id, False)
        return state

    def get_all_peripheral_states(self) -> list:
        ordered = [pid for pid in self._peripheral_order if pid in self._peripherals]
        return [self.get_peripheral_state(pid) for pid in ordered]

    def get_history(self, device_id: str, limit=300) -> list:
        """Return recent history for a device (for chart initialization on connect)."""
        hist = self._histories.get(device_id, deque())
        return list(hist)[-limit:]

    def get_device_configs(self) -> dict:
        """Return serializable config dict for persistence."""
        alicat_cfgs = {}
        for device_id, device in self._alicat.items():
            alicat_cfgs[device_id] = device.to_dict() | {
                'serial_number': device.serial_number,
                'gas_number': device.gas_number,
                'disabled': self._disabled.get(device_id, False),
            }
        periph_cfgs = {}
        for peripheral_id, periph in self._peripherals.items():
            periph_cfgs[peripheral_id] = periph.to_config() | {
                'disabled': self._periph_disabled.get(peripheral_id, False),
            }

        return {
            'alicat': alicat_cfgs,
            'peripherals': periph_cfgs,
            'alicat_order': list(self._device_order),
            'peripheral_order': list(self._peripheral_order),
        }

    def reorder_devices(self, ordered_ids: list) -> dict:
        """Apply a new display order for flow controllers."""
        with self._lock:
            valid = [x for x in ordered_ids if x in self._alicat]
            remaining = [x for x in self._device_order if x not in valid]
            self._device_order = valid + remaining
        return {'success': True}

    def reorder_peripherals(self, ordered_ids: list) -> dict:
        """Apply a new display order for peripherals."""
        with self._lock:
            valid = [x for x in ordered_ids if x in self._peripherals]
            remaining = [x for x in self._peripheral_order if x not in valid]
            self._peripheral_order = valid + remaining
        return {'success': True}

    def get_running_state(self) -> dict:
        """Return running experiment state for heartbeat (crash recovery)."""
        running = {}
        for device_id, device in self._alicat.items():
            if self._running.get(device_id):
                sched = self._schedules.get(device_id, {})
                running[device_id] = {
                    'device_name': device.device_name,
                    'logging': True,
                    'last_setpoint': device.last_reading.get('setpoint', 0) if device.last_reading else 0,
                    'schedule_running': sched.get('running', False),
                    'schedule_start_time': sched.get('start_time'),
                    'schedule_current_step': sched.get('current_step', 0),
                    'schedule_data': sched.get('schedule'),
                }
        return running

    def resume_experiment(self, saved_state: dict):
        """
        Resume experiment from saved heartbeat state after crash recovery.
        Adjusts schedule start times to account for the downtime.
        """
        if not saved_state:
            return
        now = time.time()
        for device_id, state in saved_state.items():
            device = self._alicat.get(device_id)
            if not device or not device.connected:
                continue

            # Resume logging
            if state.get('logging'):
                self.start_device(device_id)

            # Resume schedule if it was running
            if state.get('schedule_running') and state.get('schedule_data'):
                schedule = state['schedule_data']
                self.load_schedule(device_id, schedule)

                # Compute where we would be in the schedule right now
                original_start = state.get('schedule_start_time', now)
                # Keep the same start time so schedule resumes from correct position
                with self._lock:
                    sched = self._schedules.get(device_id, {})
                    if sched:
                        sched['running'] = True
                        sched['start_time'] = original_start  # resume from crash point

    def phidget_available(self):
        return PHIDGET_AVAILABLE_FLAG

    def shutdown(self):
        """
        Gracefully disconnect all Alicat devices and close all peripherals.
        Safe to call from a signal handler or atexit.  Does not raise.
        """
        print('[DeviceManager] Shutting down devices...', flush=True)
        for device in list(self._alicat.values()):
            try:
                device.disconnect()
            except Exception:
                pass
        for periph in list(self._peripherals.values()):
            try:
                periph.close()
            except Exception:
                pass
        print('[DeviceManager] All devices closed.', flush=True)
