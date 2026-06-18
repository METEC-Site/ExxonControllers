#!/usr/bin/env python3
"""
Data Logger
CSV logging for Alicat devices and peripherals.

Structure:
  Data/Raw/YYYY/MM/DD/alicat_<name>_<period>.csv  — rotating on a configurable interval, append mode
  Data/Raw/YYYY/MM/DD/alicat_<name>_<period>.txt  — metadata sidecar (human-readable)
  Data/Raw/YYYY/MM/DD/peripheral_<name>_<date>.csv — daily rotating peripheral state log
  Data/Raw/YYYY/MM/DD/peripheral_<name>_<date>.txt — metadata sidecar
  Data/Experiments/<folder>/<name>.csv             — one file per experiment segment, long-format
  Data/Experiments/<folder>/metadata.txt           — human-readable experiment metadata
"""

import csv
import json
import os
import threading
import time
from datetime import datetime, timezone


# ── Shared field definitions ───────────────────────────────────────────────────

# (csv_column_name, reading_dict_key, round_places_or_None)
_RAW_FIELD_MAP = [
    ('timestamp_utc',   'timestamp',    None),
    ('pressure_psia',   'pressure',     3),
    ('temperature_c',   'temperature',  2),
    ('vol_flow_slpm',   'vol_flow',     4),
    ('mass_flow_slpm',  'mass_flow',    4),
    ('setpoint_slpm',   'setpoint',     4),
    ('lat_deg',         'lat',          6),
    ('lon_deg',         'lon',          6),
]

_EXP_FIELD_MAP = [
    ('timestamp_utc',    'timestamp',       None),
    ('device_name',      'device_name',     None),
    ('pressure_psia',    'pressure',        3),
    ('temperature_c',    'temperature',     2),
    ('vol_flow_slpm',    'vol_flow',        4),
    ('mass_flow_slpm',   'mass_flow',       4),
    ('setpoint_slpm',    'setpoint',        4),
    ('accumulated_sl',   'accumulated_sl',  4),
    ('lat_deg',          'lat',             6),
    ('lon_deg',          'lon',             6),
]

_ALICAT_FIELD_DESCRIPTIONS = {
    'timestamp_utc':   'Timestamp (UTC, ISO 8601)',
    'pressure_psia':   'Inlet gas pressure (PSIA)',
    'temperature_c':   'Gas temperature (°C)',
    'vol_flow_slpm':   'Volumetric flow rate (SLPM)',
    'mass_flow_slpm':  'Mass flow rate (SLPM)',
    'setpoint_slpm':   'Flow setpoint (SLPM)',
    'accumulated_sl':  'Accumulated volume during experiment (SL)',
    'lat_deg':         'GPS latitude (degrees)',
    'lon_deg':         'GPS longitude (degrees)',
    'device_name':     'Device name',
}


def format_gas_info(meta: dict) -> str:
    """Human-readable gas description for a flow-controller metadata dict.
    Shared by RawDataLogger and NasRelay so both sidecars agree."""
    gas_number = meta.get('gas_number')
    gas_name = meta.get('gas_name', '')
    if gas_name:
        return f'{gas_name} (gas_number={gas_number})'
    return f'Unknown (gas_number={gas_number})'


def format_max_flow_info(meta: dict) -> str:
    """Human-readable full-scale flow description for a flow-controller
    metadata dict. Shared by RawDataLogger and NasRelay so both sidecars agree."""
    max_flow = meta.get('max_flow')
    if max_flow is None:
        return 'Unknown'
    if meta.get('max_flow_is_fallback'):
        return f'{max_flow} SLPM (user-configured — device does not report full-scale)'
    return f'{max_flow} SLPM (reported by device)'


def _round_value(v, places):
    """Round a value to `places` decimal places; return as-is if not numeric."""
    if v in ('', None):
        return v
    try:
        return round(float(v), places)
    except (TypeError, ValueError):
        return v


# ── RawDataLogger ──────────────────────────────────────────────────────────────

def _safe_filename_part(value: str, fallback: str = '') -> str:
    """Sanitise a string for use as a component of a filename."""
    return (value or fallback).replace(' ', '_').replace('/', '-').replace('\\', '-')


def safe_tc_column_name(channel_label: str) -> str:
    """Convert a thermocouple channel label to a safe CSV column name.
    E.g. 'Stack A' -> 'tc_stack_a_c'
    """
    import re
    slug = re.sub(r'[^a-z0-9]+', '_', channel_label.lower().strip()).strip('_')
    return f'tc_{slug}_c' if slug else 'tc_c'


class RawDataLogger:
    """
    Appends Alicat readings to a time-rotating CSV in Data/Raw/.
    File per device per rotation period:
        <device_name>_<serial>_<ep_name>_<period>.csv
    A sidecar .txt metadata file is written alongside each new CSV file.

    rotation_minutes : rotation interval in minutes.
        1440 (default) → daily  → NAME_SN_EP_2026-03-25.csv
        720             → 12-hr  → NAME_SN_EP_2026-03-25T00.csv / T12.csv
        360             → 6-hr   → NAME_SN_EP_2026-03-25T00.csv / T06.csv / …
        60              → 1-hr   → NAME_SN_EP_2026-03-25T14.csv
    """

    FIELDNAMES = [col for col, _, _ in _RAW_FIELD_MAP]

    def __init__(self, device_name, data_dir='Data/Raw', rotation_minutes=1440,
                 device_meta=None, tc_column_name=None):
        self.device_name = device_name
        self.data_dir = data_dir
        self.rotation_minutes = max(1, int(rotation_minutes))
        self._safe_name = _safe_filename_part(device_name, 'device')
        self._device_meta = device_meta or {}
        self._tc_column_name = tc_column_name or None
        # Serial and EP name components derived from device_meta
        self._safe_serial = _safe_filename_part(
            self._device_meta.get('serial', ''), 'NoSerial')
        self._safe_ep_name = _safe_filename_part(
            self._device_meta.get('ep_display_name', ''), 'TEST')
        # Instance-level fieldnames so the TC column can extend per-logger
        self._fieldnames = list(self.FIELDNAMES)
        if self._tc_column_name:
            self._fieldnames.append(self._tc_column_name)
        self._file = None
        self._writer = None
        self._current_period = None
        self._row_count = 0
        os.makedirs(data_dir, exist_ok=True)
        self._rotate()

    def _period_key(self, now=None):
        """Return the filename suffix for the current rotation window."""
        if now is None:
            now = datetime.now(timezone.utc)
        if self.rotation_minutes >= 1440:
            return now.date().isoformat()
        day_minutes = now.hour * 60 + now.minute
        slot = (day_minutes // self.rotation_minutes) * self.rotation_minutes
        h, m = divmod(slot, 60)
        return f"{now.date().isoformat()}T{h:02d}{m:02d}"

    def _rotate(self):
        now = datetime.now(timezone.utc)
        key = self._period_key(now)
        if self._current_period == key:
            return
        self._close_file()
        date_dir = os.path.join(self.data_dir,
                                now.strftime('%Y'),
                                now.strftime('%m'),
                                now.strftime('%d'))
        os.makedirs(date_dir, exist_ok=True)
        fname = f"{self._safe_name}_{self._safe_serial}_{self._safe_ep_name}_{key}.csv"
        path = os.path.join(date_dir, fname)
        is_new = not os.path.exists(path)
        self._file = open(path, 'a', newline='')   # may raise; _current_period unchanged
        self._writer = csv.DictWriter(self._file, fieldnames=self._fieldnames)
        self._current_period = key   # only committed after open() succeeds
        if is_new:
            self._writer.writeheader()
            self._write_metadata_txt(path)
        self._row_count = 0

    def _write_metadata_txt(self, csv_path):
        txt_path = os.path.splitext(csv_path)[0] + '.txt'
        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        meta = self._device_meta
        ep = meta.get('ep_info') or {}
        tc_desc = {}
        if self._tc_column_name:
            tc_label = meta.get('tc_channel_label', self._tc_column_name)
            tc_desc[self._tc_column_name] = f'Linked thermocouple channel "{tc_label}" (°C)'
        col_desc = {**_ALICAT_FIELD_DESCRIPTIONS, **tc_desc}
        lines = [
            '=== ExxonController Flow Controller Metadata ===',
            f'Device Name:     {self.device_name}',
            f'Device Type:     {meta.get("device_type", "Unknown")}',
            f'Serial Number:   {meta.get("serial", "")}',
            f'Modbus Unit ID:  {meta.get("unit_id", "")}',
            f'Gas:             {format_gas_info(meta)}',
            f'Full-Scale Flow: {format_max_flow_info(meta)}',
            f'File Created:    {now}',
            f'Rotation:        {self.rotation_minutes} min',
            '',
            '=== Emission Point ===',
            f'EP Name:        {ep.get("display_name", "")}',
            f'EP Description: {ep.get("description", "")}',
            f'EP Latitude:    {ep.get("lat", "")} deg',
            f'EP Longitude:   {ep.get("lon", "")} deg',
            f'EP Altitude:    {ep.get("alt", "")} m',
            f'EP Install Date:{ep.get("install_datetime", "")}',
        ]
        if self._tc_column_name:
            lines += [
                '',
                '=== Linked Temperature Sensor ===',
                f'Peripheral:    {meta.get("tc_peripheral_name", "")}',
                f'Channel:       {meta.get("tc_channel_label", "")}',
            ]
        lines += [
            '',
            '=== Column Descriptions ===',
        ] + [f'  {col}: {col_desc.get(col, col)}' for col in self._fieldnames]
        try:
            with open(txt_path, 'w') as f:
                f.write('\n'.join(lines) + '\n')
        except OSError:
            pass

    def log(self, reading: dict, tc_value=None):
        self._rotate()
        row = {}
        for col, key, places in _RAW_FIELD_MAP:
            v = reading.get(key, '')
            if places is not None:
                v = _round_value(v, places)
            row[col] = v
        if self._tc_column_name:
            row[self._tc_column_name] = _round_value(tc_value, 3) if tc_value is not None else ''
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


# ── ExperimentDataLogger ───────────────────────────────────────────────────────

class ExperimentDataLogger:
    """
    Long-format CSV — one file (or rotating segments) for the entire experiment, all devices.
    File: Data/Experiments/<folder>/<exp_name>[_partNN].csv
    Also writes experiment_metadata.json and metadata.txt in the same folder.
    Thread-safe: log_device() acquires a lock before writing.

    rotation_minutes : 0 = no rotation (one file for whole experiment).
        Any positive value splits the output into segments at that interval:
        <name>.csv, <name>_part01.csv, <name>_part02.csv, …
    """

    FIELDNAMES = [col for col, _, _ in _EXP_FIELD_MAP]

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

        # Write metadata JSON and human-readable .txt
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
        self._write_metadata_txt(experiment_meta, devices_info, start_time)

    def _write_metadata_txt(self, experiment_meta, devices_info, start_time):
        txt_path = os.path.join(self.data_dir, 'metadata.txt')
        device_lines = []
        for name, info in devices_info.items():
            device_lines.append(
                f'  {name}: {info.get("device_type", "")} @ {info.get("host", "")}'
                f' | Serial: {info.get("serial_number", "")}'
                f' | Gas: {info.get("gas_number", "")}'
            )
            ep = info.get('ep_info') or {}
            if ep:
                device_lines.append(
                    f'    EP: {ep.get("display_name", "")} — {ep.get("description", "")}'
                )
                device_lines.append(
                    f'    EP Lat/Lon/Alt: {ep.get("lat", "")} / {ep.get("lon", "")} / {ep.get("alt", "")} m'
                )
                device_lines.append(
                    f'    EP Install Date: {ep.get("install_datetime", "")}'
                )
        lines = [
            '=== ExxonController Experiment Metadata ===',
            f'Experiment:  {experiment_meta.get("name", "")}',
            f'Operator:    {experiment_meta.get("operator", "")}',
            f'Location:    {experiment_meta.get("location", "")}',
            f'Notes:       {experiment_meta.get("notes", "")}',
            f'Start Time:  {start_time}',
            '',
            '=== Devices ===',
        ] + device_lines + [
            '',
            '=== Column Descriptions ===',
        ] + [f'  {col}: {_ALICAT_FIELD_DESCRIPTIONS.get(col, col)}'
             for col in self.FIELDNAMES]
        try:
            with open(txt_path, 'w') as f:
                f.write('\n'.join(lines) + '\n')
        except OSError:
            pass

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
        row = {}
        for col, key, places in _EXP_FIELD_MAP:
            if col == 'device_name':
                v = device_name
            else:
                v = reading.get(key, '')
                if places is not None:
                    v = _round_value(v, places)
            row[col] = v
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


# ── PeripheralDataLogger ───────────────────────────────────────────────────────

class PeripheralDataLogger:
    """
    Rotating daily CSV for peripheral state data.
    One file per device per day: peripheral_<name>_<YYYY-MM-DD>.csv
    A sidecar .txt metadata file is written alongside each new CSV file.
    """

    def __init__(self, peripheral_name, channel_labels, data_dir='Data/Raw',
                 device_meta=None):
        self.peripheral_name = peripheral_name
        self.data_dir = data_dir
        self._device_meta = device_meta or {}
        self._safe_name = peripheral_name.replace(' ', '_').replace('/', '-')
        self._channel_labels = list(channel_labels) if channel_labels else []

        # Build fieldnames from channel labels
        safe_labels = [lbl.replace(' ', '_').replace('/', '_').lower()
                       for lbl in self._channel_labels]
        self._fieldnames = ['timestamp_utc'] + safe_labels

        self._file = None
        self._writer = None
        self._current_day = None
        self._row_count = 0
        os.makedirs(data_dir, exist_ok=True)
        self._rotate()

    def _rotate(self):
        now = datetime.now(timezone.utc)
        key = now.date().isoformat()
        if self._current_day == key:
            return
        self._close_file()
        date_dir = os.path.join(self.data_dir,
                                now.strftime('%Y'),
                                now.strftime('%m'),
                                now.strftime('%d'))
        os.makedirs(date_dir, exist_ok=True)
        fname = f"peripheral_{self._safe_name}_{key}.csv"
        path = os.path.join(date_dir, fname)
        is_new = not os.path.exists(path)
        self._file = open(path, 'a', newline='')   # may raise; _current_day unchanged
        self._writer = csv.DictWriter(self._file, fieldnames=self._fieldnames)
        self._current_day = key   # only committed after open() succeeds
        if is_new:
            self._writer.writeheader()
            self._write_metadata_txt(path)
        self._row_count = 0

    def _write_metadata_txt(self, csv_path):
        txt_path = os.path.splitext(csv_path)[0] + '.txt'
        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        meta = self._device_meta
        ptype = meta.get('type', 'Unknown')
        lines = [
            '=== ExxonController Peripheral Metadata ===',
            f'Peripheral Name: {self.peripheral_name}',
            f'Type:            {ptype}',
            f'Hub Serial:      {meta.get("hub_serial", "") or ""}',
            f'VINT Hub:        {meta.get("hostname", "")}:{meta.get("port", "")}',
            f'Hub Port:        {meta.get("hub_port", "")}',
        ]
        if ptype == 'pressure_vint':
            scale, offset = meta.get('calibration') or (1.0, 0.0)
            lines += [
                f'Units:           {meta.get("units", "")}',
                f'Calibration:     scale={scale}, offset={offset}',
            ]
        lines += [
            f'File Created:    {now}',
            '',
            '=== Column Descriptions ===',
            '  timestamp_utc: Timestamp (UTC, ISO 8601)',
        ] + [
            self._channel_description(ptype, fn, lbl, meta)
            for fn, lbl in zip(self._fieldnames[1:], self._channel_labels)
        ]
        try:
            with open(txt_path, 'w') as f:
                f.write('\n'.join(lines) + '\n')
        except OSError:
            pass

    @staticmethod
    def _channel_description(ptype, fieldname, label, meta):
        if ptype == 'thermocouple':
            return f'  {fieldname}: Temperature reading from channel "{label}" (°C)'
        if ptype in ('relay', 'relay_mechanical'):
            return (f'  {fieldname}: Relay state for channel "{label}" '
                    f'(1=energised/open, 0=de-energised/closed)')
        if ptype == 'pressure_vint':
            units = meta.get('units', '')
            return f'  {fieldname}: Pressure reading from channel "{label}" ({units})'
        return f'  {fieldname}: State of channel "{label}"'

    def log(self, timestamp: str, channel_values: list):
        self._rotate()
        row = {'timestamp_utc': timestamp}
        for fn, v in zip(self._fieldnames[1:], channel_values):
            if v is None:
                row[fn] = ''
            elif isinstance(v, bool):
                row[fn] = 1 if v else 0
            elif isinstance(v, float):
                row[fn] = round(v, 3)
            else:
                row[fn] = v
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
