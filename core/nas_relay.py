#!/usr/bin/env python3
"""
NAS File Echo Relay
Appends Alicat flow controller readings to per-device CSV files on a network share.

File naming: {output_dir}/{device_name}_{YYYY-MM-DD}.csv
"""

import csv
import os
import threading
from datetime import datetime, timezone

RELAY_FIELDS = ['pressure', 'temperature', 'vol_flow', 'mass_flow', 'setpoint', 'accumulated_sl']
CSV_HEADER = ['timestamp_utc', 'device_name'] + RELAY_FIELDS


class NasRelay:
    """Thread-safe CSV writer that echoes readings to a NAS output directory."""

    def __init__(self):
        self._path = ''
        self._enabled = False
        self._lock = threading.Lock()
        self._open_files: dict[str, tuple] = {}   # key → (file_obj, csv_writer)

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    @staticmethod
    def _normalise_path(raw: str) -> tuple[str, str | None]:
        """
        Normalise a user-supplied path for the current OS.
        Returns (normalised_path, error_string_or_None).

        Windows (os.name == 'nt'):
          - Backslashes kept (Windows native separator)
          - UNC paths (\\server\share\...) accepted as-is
          - Forward-slash-only paths converted to backslashes

        Linux / WSL (os.name == 'posix'):
          - Backslashes converted to forward slashes
          - UNC paths rejected with a helpful mount message
          - Path must be absolute (starts with /)
        """
        path = raw.strip()
        if not path:
            return '', 'Output path is required'

        if os.name == 'nt':
            # On Windows, normalise forward slashes → backslashes and let the OS handle it
            path = path.replace('/', '\\')
            # UNC paths (\\server\share) are fine on Windows
            if not os.path.isabs(path):
                return '', f'Path must be absolute, got: {path!r}'
        else:
            # On Linux/WSL, normalise backslashes → forward slashes first
            path = path.replace('\\', '/')
            if path.startswith('//'):
                return '', (
                    'Windows UNC paths (\\\\server\\share) cannot be used directly '
                    'from Linux/WSL. Mount the network share first and enter its '
                    'Linux mount point, e.g. /mnt/metec-nas/METEC2/ExxonProject'
                )
            if not os.path.isabs(path):
                return '', f'Path must be absolute (starts with /), got: {path!r}'

        return path, None

    def configure(self, path: str) -> dict:
        """Set the output path and verify it is accessible."""
        path, err = self._normalise_path(path)
        if err:
            return {'success': False, 'error': err}

        # Attempt to create the directory
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as e:
            return {'success': False, 'error': f'Cannot create/access path: {e}'}

        # Verify we can write a probe file
        probe = os.path.join(path, '.ec_probe')
        try:
            with open(probe, 'w') as f:
                f.write('ok')
            os.remove(probe)
        except OSError as e:
            return {'success': False, 'error': f'Path not writable: {e}'}

        with self._lock:
            self._close_all()
            self._path = path
            self._enabled = True

        return {'success': True, 'path': path}  # returns normalised path

    def restore(self, path: str):
        """Restore a previously saved path on startup without a write-probe.
        Normalises separators for the current OS so a path saved on Windows
        still works when the server starts on Linux and vice-versa.
        If the path is inaccessible at startup (NAS not yet mounted), write_reading()
        will fail silently until it becomes available."""
        normalised, _ = self._normalise_path(path)
        with self._lock:
            self._path = normalised or path.strip()
            self._enabled = True

    def disable(self):
        with self._lock:
            self._close_all()
            self._enabled = False

    def write_reading(self, device_name: str, reading: dict):
        """Append one reading row. Called from poll loop — must be fast."""
        if not self._enabled or not self._path:
            return
        safe_name = device_name.replace(' ', '_').replace('/', '-').replace('\\', '-')
        now = datetime.now(timezone.utc)
        date_str = now.strftime('%Y-%m-%d')
        file_key = f'{safe_name}_{date_str}'
        ts = now.strftime('%Y-%m-%dT%H:%M:%SZ')

        row = [ts, device_name] + [reading.get(f, '') for f in RELAY_FIELDS]

        with self._lock:
            if not self._enabled:
                return
            writer, needs_header = self._get_writer(file_key, safe_name, date_str)
            if writer is None:
                return
            if needs_header:
                writer.writerow(CSV_HEADER)
            writer.writerow(row)
            # Flush so data is visible on the NAS immediately.
            # If the network path has dropped, the flush raises OSError (EINVAL/EIO).
            # Close and evict the stale handle so the next write re-opens cleanly.
            try:
                self._open_files[file_key][0].flush()
            except OSError:
                try:
                    self._open_files[file_key][0].close()
                except OSError:
                    pass
                del self._open_files[file_key]

    def get_status(self) -> dict:
        return {
            'enabled': self._enabled,
            'path': self._path,
        }

    # ── Internal ─────────────────────────────────────────────────────────────

    def _get_writer(self, file_key: str, safe_name: str, date_str: str):
        """Return (writer, needs_header). Caller must hold self._lock."""
        if file_key in self._open_files:
            return self._open_files[file_key][1], False

        file_path = os.path.join(self._path, f'{safe_name}_{date_str}.csv')
        needs_header = not os.path.exists(file_path)
        try:
            fh = open(file_path, 'a', newline='', encoding='utf-8')
            writer = csv.writer(fh)
            self._open_files[file_key] = (fh, writer)
            return writer, needs_header
        except OSError:
            return None, False

    def _close_all(self):
        """Close all open file handles. Caller must hold self._lock."""
        for fh, _ in self._open_files.values():
            try:
                fh.close()
            except OSError:
                pass
        self._open_files.clear()
