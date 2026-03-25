#!/usr/bin/env python3
"""
State Manager
Handles persistent configuration (devices, settings) and crash detection via heartbeat.
"""

import json
import os
import tempfile
import hashlib
import secrets
import threading
from datetime import datetime, timezone, timedelta


HEARTBEAT_MAX_AGE_SECONDS = 30  # If last heartbeat older than this => crash


class StateManager:
    """
    Manages persistent state:
    - config/settings.json  : auth credentials, app settings
    - config/devices.json   : saved device + peripheral configurations
    - config/heartbeat.json : written every ~5 seconds; absence or staleness indicates crash
    """

    def __init__(self, config_dir):
        self.config_dir = config_dir
        self._settings_path = os.path.join(config_dir, 'settings.json')
        self._devices_path  = os.path.join(config_dir, 'devices.json')
        self._heartbeat_path = os.path.join(config_dir, 'heartbeat.json')
        self._solenoid_checklist_path = os.path.join(config_dir, 'solenoid_checklist.json')
        self._map_config_path = os.path.join(config_dir, 'map_config.json')

        self._chat_log_path = os.path.join(config_dir, 'chat_log.json')

        self._lock = threading.Lock()
        self._settings = {}
        self._devices = {}
        self._solenoid_checklist = []
        self._map_config = {'overlays': []}
        self._crash_info = None          # populated by check_crash_recovery()
        self._crash_experiment_state = None

        self._load_settings()
        self._load_devices()
        self._load_solenoid_checklist()
        self._load_map_config()

    # ── Atomic write helper ───────────────────────────────────────────────────

    @staticmethod
    def _atomic_write_json(path: str, data) -> None:
        """Write JSON atomically: write to a temp file beside the target, then
        os.replace() so readers always see a complete file even on a crash."""
        dir_ = os.path.dirname(path) or '.'
        fd, tmp = tempfile.mkstemp(dir=dir_, prefix='.tmp_', suffix='.json')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # ── Settings ──────────────────────────────────────────────────────────────

    def _load_settings(self):
        if os.path.exists(self._settings_path):
            with open(self._settings_path, 'r') as f:
                self._settings = json.load(f)
        else:
            # Create defaults with password "admin"
            salt = secrets.token_hex(16)
            pw_hash = hashlib.sha256((salt + 'admin').encode()).hexdigest()
            self._settings = {
                'password_hash': pw_hash,
                'password_salt': salt,
                'secret_key': secrets.token_hex(32),
                'poll_interval_ms': 1000,
                'history_length': 3600,   # readings to keep in memory per device
                'heartbeat_interval_s': 5,
                'raw_file_rotation_minutes': 1440,   # daily rotation for raw data files
                'exp_file_rotation_minutes': 0,      # no rotation for experiment files
            }
            self._save_settings()

    def _save_settings(self):
        self._atomic_write_json(self._settings_path, self._settings)

    def get_settings(self):
        with self._lock:
            return dict(self._settings)

    def update_settings(self, updates: dict):
        with self._lock:
            self._settings.update(updates)
            self._save_settings()

    def get_secret_key(self):
        return self._settings.get('secret_key', secrets.token_hex(32))

    # ── Devices ───────────────────────────────────────────────────────────────

    def _load_devices(self):
        if os.path.exists(self._devices_path) and os.path.getsize(self._devices_path) > 0:
            with open(self._devices_path, 'r') as f:
                self._devices = json.load(f)
        else:
            self._devices = {'alicat': {}, 'peripherals': {}}
            self._save_devices_locked()

    def _save_devices_locked(self):
        self._atomic_write_json(self._devices_path, self._devices)

    def get_devices(self):
        with self._lock:
            return dict(self._devices)

    def save_devices(self, device_configs: dict):
        """
        Persist device configurations.
        device_configs: {'alicat': {id: config_dict, ...}, 'peripherals': {id: config_dict, ...}}
        """
        with self._lock:
            self._devices = device_configs
            self._save_devices_locked()

    # ── Solenoid Checklist ────────────────────────────────────────────────────

    def _load_solenoid_checklist(self):
        if os.path.exists(self._solenoid_checklist_path):
            try:
                with open(self._solenoid_checklist_path, 'r') as f:
                    self._solenoid_checklist = json.load(f)
            except Exception:
                self._solenoid_checklist = []

    def get_solenoid_checklist(self):
        with self._lock:
            return list(self._solenoid_checklist)

    def save_solenoid_checklist(self, checklist: list):
        with self._lock:
            self._solenoid_checklist = list(checklist)
            self._atomic_write_json(self._solenoid_checklist_path, self._solenoid_checklist)

    # ── Chat Log ──────────────────────────────────────────────────────────────

    def load_chat_log(self) -> list:
        """Load persisted chat messages from disk. Returns empty list on any error."""
        if os.path.exists(self._chat_log_path):
            try:
                with open(self._chat_log_path, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return []

    def save_chat_log(self, messages: list):
        """Persist current chat messages atomically."""
        with self._lock:
            self._atomic_write_json(self._chat_log_path, messages)

    # ── Map Configuration ─────────────────────────────────────────────────────

    def _load_map_config(self):
        if os.path.exists(self._map_config_path):
            try:
                with open(self._map_config_path, 'r') as f:
                    self._map_config = json.load(f)
            except Exception:
                self._map_config = {'overlays': []}

    def get_map_config(self) -> dict:
        with self._lock:
            return dict(self._map_config)

    def save_map_config(self, config: dict):
        with self._lock:
            self._map_config = config
            self._atomic_write_json(self._map_config_path, self._map_config)

    # ── Heartbeat / Crash Detection ───────────────────────────────────────────

    def write_heartbeat(self, running_state: dict):
        """
        Write heartbeat file with current timestamp and running experiment state.
        Called every ~5 seconds by the polling loop.
        """
        payload = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'running_state': running_state,
        }
        try:
            tmp_path = self._heartbeat_path + '.tmp'
            with open(tmp_path, 'w') as f:
                json.dump(payload, f)
            os.replace(tmp_path, self._heartbeat_path)  # atomic rename
        except Exception as e:
            print(f"[StateManager] Heartbeat write failed: {e}")

    def check_crash_recovery(self):
        """
        Check if a stale heartbeat exists, indicating a previous crash.
        Returns crash info dict if crash detected, else None.
        Stores the result in self._crash_info for later retrieval via get_crash_info().
        """
        if not os.path.exists(self._heartbeat_path):
            return None

        try:
            with open(self._heartbeat_path, 'r') as f:
                hb = json.load(f)

            last_ts_str = hb.get('timestamp', '')
            last_ts = datetime.fromisoformat(last_ts_str)

            now = datetime.now(timezone.utc)
            # Ensure comparison is timezone-aware
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=timezone.utc)

            age = (now - last_ts).total_seconds()

            if age > HEARTBEAT_MAX_AGE_SECONDS:
                running_state = hb.get('running_state', {})
                # Only treat as a crash if an experiment was actually running.
                # A stale heartbeat with no experiment (e.g. after "Start Fresh"
                # or a normal restart with no active run) should not trigger the
                # recovery dialog — the server was simply stopped intentionally.
                if not running_state.get('_experiment'):
                    return None
                crash_info = {
                    'detected': True,
                    'last_heartbeat': last_ts_str,
                    'downtime_seconds': age,
                    'downtime_human': _format_duration(age),
                    'crash_time': last_ts.isoformat(),
                    'recovery_time': now.isoformat(),
                    'running_state': running_state,
                }
                self._crash_info = crash_info
                self._crash_experiment_state = running_state
                return crash_info
        except Exception as e:
            print(f"[StateManager] Heartbeat check error: {e}")

        return None

    def get_crash_info(self):
        """Return crash info if a crash was detected on startup, else None."""
        return self._crash_info

    def get_crash_experiment_state(self):
        """Return the running experiment state captured at last heartbeat before crash."""
        return self._crash_experiment_state

    def clear_crash_state(self):
        """Clear crash state after user has acknowledged/dismissed the popup."""
        self._crash_info = None
        self._crash_experiment_state = None
        # Overwrite heartbeat with current time so it doesn't trigger again
        self.write_heartbeat({})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _format_duration(seconds: float) -> str:
    """Format a duration in seconds to a human-readable string."""
    if seconds < 60:
        return f"{seconds:.0f} seconds"
    elif seconds < 3600:
        return f"{seconds / 60:.1f} minutes"
    else:
        return f"{seconds / 3600:.1f} hours"
