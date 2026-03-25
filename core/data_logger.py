#!/usr/bin/env python3
"""
Data Logger
CSV logging for Alicat devices and peripherals.

Structure:
  Data/Raw/alicat_<name>_<period>.csv  — rotating on a configurable interval, append mode
  Data/Experiments/<folder>/<name>.csv — one file per experiment segment, long-format
"""

import csv
import json
import os
import threading
import time
from datetime import datetime, timezone


class RawDataLogger:
    """
    Appends Alicat readings to a time-rotating CSV in Data/Raw/.
    File per device per rotation period: alicat_<name>_<period>.csv

    rotation_minutes : rotation interval in minutes.
        1440 (default) → daily  → alicat_NAME_2026-03-25.csv
        720             → 12-hr  → alicat_NAME_2026-03-25T00.csv / T12.csv
        360             → 6-hr   → alicat_NAME_2026-03-25T00.csv / T06.csv / …
        60              → 1-hr   → alicat_NAME_2026-03-25T14.csv
    """

    FIELDNAMES = ['timestamp', 'pressure', 'temperature', 'vol_flow', 'mass_flow', 'setpoint', 'lat', 'lon']

    def __init__(self, device_name, data_dir='Data/Raw', rotation_minutes=1440):
        self.device_name = device_name
        self.data_dir = data_dir
        self.rotation_minutes = max(1, int(rotation_minutes))
        self._safe_name = device_name.replace(' ', '_').replace('/', '-')
        self._file = None
        self._writer = None
        self._current_period = None
        self._row_count = 0
        os.makedirs(data_dir, exist_ok=True)
        self._rotate()

    def _period_key(self):
        """Return the filename suffix for the current rotation window."""
        now = datetime.now(timezone.utc)
        if self.rotation_minutes >= 1440:
            return now.date().isoformat()
        day_minutes = now.hour * 60 + now.minute
        slot = (day_minutes // self.rotation_minutes) * self.rotation_minutes
        h, m = divmod(slot, 60)
        return f"{now.date().isoformat()}T{h:02d}{m:02d}"

    def _rotate(self):
        key = self._period_key()
        if self._current_period == key:
            return
        self._close_file()
        self._current_period = key
        fname = f"alicat_{self._safe_name}_{key}.csv"
        path = os.path.join(self.data_dir, fname)
        is_new = not os.path.exists(path)
        self._file = open(path, 'a', newline='')
        self._writer = csv.DictWriter(self._file, fieldnames=self.FIELDNAMES)
        if is_new:
            self._writer.writeheader()
        self._row_count = 0

    def log(self, reading: dict):
        self._rotate()
        row = {k: reading.get(k, '') for k in self.FIELDNAMES}
        self._writer.writerow(row)
        self._row_count += 1
        if self._row_count % 10 == 0:
            self._safe_flush()

    def _safe_flush(self):
        try:
            if self._file and not self._file.closed:
                self._file.flush()
        except OSError:
            pass

    def _close_file(self):
        try:
            if self._file and not self._file.closed:
                self._file.flush()
                self._file.close()
        except OSError:
            pass
        self._file = None
        self._writer = None

    def close(self):
        self._close_file()


class ExperimentDataLogger:
    """
    Long-format CSV — one file (or rotating segments) for the entire experiment, all devices.
    File: Data/Experiments/<folder>/<exp_name>[_partNN].csv
    Also writes experiment_metadata.json in the same folder.
    Thread-safe: log_device() acquires a lock before writing.

    rotation_minutes : 0 = no rotation (one file for whole experiment).
        Any positive value splits the output into segments at that interval:
        <name>.csv, <name>_part01.csv, <name>_part02.csv, …
    """

    FIELDNAMES = [
        'timestamp', 'device_name',
        'pressure', 'temperature', 'vol_flow', 'mass_flow', 'setpoint',
        'accumulated_sl', 'lat', 'lon'
    ]

    def __init__(self, experiment_meta: dict, devices_info: dict, data_dir: str,
                 rotation_minutes: int = 0):
        """
        experiment_meta : dict with name, operator, location, notes, etc.
        devices_info    : {device_name: device_config_dict, ...}
        data_dir        : folder to write into (e.g. Data/Experiments/Heater_Test_3_20260309T120000Z)
        rotation_minutes: 0 = single file; >0 = rotate at this interval
        """
        self.data_dir = data_dir
        self.rotation_minutes = max(0, int(rotation_minutes))
        os.makedirs(data_dir, exist_ok=True)

        exp_name = experiment_meta.get('name', 'experiment')
        self._safe_name = exp_name.replace(' ', '_').replace('/', '-')
        self._segment = 0
        self._segment_start = time.monotonic()

        self._file = None
        self._writer = None
        self._row_count = 0
        self._lock = threading.Lock()

        self._open_segment()

        # Write metadata JSON (always refers to the base name)
        start_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        meta_path = os.path.join(data_dir, 'experiment_metadata.json')
        with open(meta_path, 'w') as f:
            json.dump({
                'experiment': experiment_meta,
                'devices': devices_info,
                'start_time': start_time,
                'data_file': f"{self._safe_name}.csv",
                'rotation_minutes': self.rotation_minutes,
            }, f, indent=2)

    def _segment_filename(self):
        suffix = f"_part{self._segment:02d}" if self._segment > 0 else ""
        return os.path.join(self.data_dir, f"{self._safe_name}{suffix}.csv")

    def _open_segment(self):
        """Open (or create) the current segment file. Must be called with lock NOT held."""
        path = self._segment_filename()
        self.csv_path = path
        self._file = open(path, 'w', newline='')
        self._writer = csv.DictWriter(self._file, fieldnames=self.FIELDNAMES)
        self._writer.writeheader()
        self._row_count = 0
        self._segment_start = time.monotonic()

    def _rotate_segment(self):
        """Close current segment and open the next. Must be called with self._lock held."""
        try:
            if self._file and not self._file.closed:
                self._file.flush()
                self._file.close()
        except OSError:
            pass
        self._segment += 1
        path = self._segment_filename()
        self.csv_path = path
        self._file = open(path, 'w', newline='')
        self._writer = csv.DictWriter(self._file, fieldnames=self.FIELDNAMES)
        self._writer.writeheader()
        self._row_count = 0
        self._segment_start = time.monotonic()

    def log_device(self, device_name: str, reading: dict):
        """Thread-safe write of one device reading row."""
        row = {k: reading.get(k, '') for k in self.FIELDNAMES}
        row['device_name'] = device_name
        with self._lock:
            if self.rotation_minutes > 0:
                elapsed_min = (time.monotonic() - self._segment_start) / 60.0
                if elapsed_min >= self.rotation_minutes:
                    self._rotate_segment()
            self._writer.writerow(row)
            self._row_count += 1
            if self._row_count % 10 == 0:
                self._safe_flush()

    def _safe_flush(self):
        try:
            if self._file and not self._file.closed:
                self._file.flush()
        except OSError:
            pass

    def close(self):
        with self._lock:
            try:
                if self._file and not self._file.closed:
                    self._file.flush()
                    self._file.close()
            except OSError:
                pass


class PeripheralDataLogger:
    """
    Logs peripheral readings (thermocouples, pressure sensors) to CSV.
    """

    def __init__(self, peripheral_name, peripheral_type, channels, data_dir):
        self.peripheral_name = peripheral_name
        self.data_dir = data_dir

        os.makedirs(data_dir, exist_ok=True)

        ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        safe_name = peripheral_name.replace(' ', '_').replace('/', '-')
        self.csv_path = os.path.join(data_dir, f"peripheral_{safe_name}_{ts}.csv")

        fieldnames = ['timestamp'] + [f"ch{i}" for i in range(channels)]
        self._file = open(self.csv_path, 'w', newline='')
        self._writer = csv.DictWriter(self._file, fieldnames=fieldnames)
        self._writer.writeheader()
        self._fieldnames = fieldnames
        self._row_count = 0

    def log(self, timestamp: str, channel_values: list):
        row = {'timestamp': timestamp}
        for i, v in enumerate(channel_values):
            key = f"ch{i}"
            if key in self._fieldnames:
                row[key] = v if v is not None else ''
        self._writer.writerow(row)
        self._row_count += 1
        if self._row_count % 10 == 0:
            self._safe_flush()

    def _safe_flush(self):
        try:
            if self._file and not self._file.closed:
                self._file.flush()
        except OSError:
            pass

    def close(self):
        try:
            if self._file and not self._file.closed:
                self._file.flush()
                self._file.close()
        except OSError:
            pass
