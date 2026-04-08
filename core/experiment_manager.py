#!/usr/bin/env python3
"""
Experiment Manager
Manages multi-device experiments: creation, scheduling, execution, export/import,
and data file organisation.

Experiment JSON schema (stored in config/experiments/<id>.json):
{
  "experiment_id": "exp_20260309T120000Z_abc123",
  "name": "Oxidation Test",
  "description": "",
  "operator": "J. Smith",
  "location": "Lab B",
  "notes": "",
  "created_at": "2026-03-09T12:00:00Z",
  "started_at": null,
  "completed_at": null,
  "status": "draft",           # draft | running | completed | crashed
  "device_schedules": {
    "MFC-1": {
      "device_name": "MFC-1",
      "schedule": [{"time": 0, "setpoint": 5.0}, ...]
    }
  },
  "relay_checklist": [
    {"peripheral_name": "Valve Board", "channel": 0, "label": "N2 Supply", "expected_state": true}
  ]
}
"""

import csv
import io
import json
import os
import threading
import time
import uuid
import zipfile
from datetime import datetime, timezone, timedelta

from dateutil import parser as date_parser

from core.data_logger import ExperimentDataLogger


class ExperimentManager:

    def __init__(self, config_dir: str, data_dir: str):
        self.config_dir = config_dir
        self.data_dir = os.path.join(data_dir, 'Experiments')
        self.experiments_dir = os.path.join(config_dir, 'experiments')
        os.makedirs(self.experiments_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)

        self._current: dict | None = None   # running experiment runtime state
        self._exp_logger: ExperimentDataLogger | None = None
        self._lock = threading.Lock()

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def create_experiment(self, metadata: dict) -> str:
        """Create and save a new experiment. Returns the experiment_id."""
        ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        exp_id = f"exp_{ts}_{str(uuid.uuid4())[:6]}"
        experiment = {
            'experiment_id': exp_id,
            'name': metadata.get('name', 'Unnamed Experiment').strip() or 'Unnamed Experiment',
            'description': metadata.get('description', ''),
            'operator': metadata.get('operator', ''),
            'location': metadata.get('location', ''),
            'notes': metadata.get('notes', ''),
            'created_at': datetime.now(timezone.utc).isoformat(),
            'started_at': None,
            'completed_at': None,
            'status': 'draft',
            'device_schedules': {},
            'relay_checklist': [],
        }
        self._save_experiment(experiment)
        return exp_id

    def get_experiment(self, experiment_id: str) -> dict | None:
        path = self._exp_path(experiment_id)
        if not os.path.exists(path):
            return None
        with open(path, 'r') as f:
            return json.load(f)

    def list_experiments(self) -> list:
        experiments = []
        try:
            entries = sorted(os.listdir(self.experiments_dir), reverse=True)
        except FileNotFoundError:
            return []
        for fname in entries:
            if not fname.endswith('.json'):
                continue
            try:
                with open(os.path.join(self.experiments_dir, fname)) as f:
                    exp = json.load(f)
                step_counts = {
                    name: len(sched.get('schedule', []))
                    for name, sched in exp.get('device_schedules', {}).items()
                }
                experiments.append({
                    'experiment_id': exp['experiment_id'],
                    'name': exp['name'],
                    'status': exp['status'],
                    'operator': exp.get('operator', ''),
                    'created_at': exp.get('created_at', ''),
                    'started_at': exp.get('started_at'),
                    'completed_at': exp.get('completed_at'),
                    'device_count': len(exp.get('device_schedules', {})),
                    'step_counts': step_counts,
                    'has_global_start': bool(exp.get('global_start_iso')),
                })
            except Exception:
                pass
        return experiments

    def update_experiment(self, experiment_id: str, updates: dict) -> dict:
        exp = self.get_experiment(experiment_id)
        if not exp:
            return {'success': False, 'error': 'Experiment not found'}
        if exp['status'] == 'running':
            return {'success': False, 'error': 'Cannot edit a running experiment'}
        for field in ['name', 'description', 'operator', 'location', 'notes', 'relay_checklist']:
            if field in updates:
                exp[field] = updates[field]
        if 'device_schedules' in updates:
            exp['device_schedules'] = updates['device_schedules']
        if 'global_start_iso' in updates:
            exp['global_start_iso'] = updates['global_start_iso']
        self._save_experiment(exp)
        return {'success': True, 'experiment': exp}

    def delete_experiment(self, experiment_id: str) -> dict:
        with self._lock:
            if self._current and self._current.get('experiment_id') == experiment_id:
                return {'success': False, 'error': 'Cannot delete a running experiment'}
        path = self._exp_path(experiment_id)
        if not os.path.exists(path):
            return {'success': False, 'error': 'Experiment not found'}
        os.remove(path)
        return {'success': True}

    # ── Device Schedules ──────────────────────────────────────────────────────

    def assign_device_schedule(self, experiment_id: str, device_name: str, schedule: list) -> dict:
        """
        Assign a schedule to a device (matched by device name) within an experiment.
        schedule: list of {time: float, setpoint: float}
        """
        exp = self.get_experiment(experiment_id)
        if not exp:
            return {'success': False, 'error': 'Experiment not found'}
        if exp['status'] == 'running':
            return {'success': False, 'error': 'Cannot modify a running experiment'}
        if not schedule:
            return {'success': False, 'error': 'Schedule is empty'}

        exp['device_schedules'][device_name] = {
            'device_name': device_name,
            'schedule': schedule,
        }
        self._save_experiment(exp)
        return {'success': True, 'steps': len(schedule)}

    def remove_device_schedule(self, experiment_id: str, device_name: str) -> dict:
        exp = self.get_experiment(experiment_id)
        if not exp:
            return {'success': False, 'error': 'Experiment not found'}
        if exp['status'] == 'running':
            return {'success': False, 'error': 'Cannot modify a running experiment'}
        exp['device_schedules'].pop(device_name, None)
        self._save_experiment(exp)
        return {'success': True}

    # ── Pre-Run Check ─────────────────────────────────────────────────────────

    def pre_run_check(self, experiment_id: str, device_mgr, global_checklist: list | None = None) -> dict:
        """
        Gather pre-run information:
        - Device connectivity status for each device in the experiment
        - All relay peripheral states (for operator verification)
        - Any automated warnings (missing devices, unexpected relay states)
        Returns a dict suitable for sending to the client.
        """
        exp = self.get_experiment(experiment_id)
        if not exp:
            return {'success': False, 'error': 'Experiment not found'}

        warnings = []

        # Device connectivity checks
        device_checks = []
        for device_name, sched_info in exp.get('device_schedules', {}).items():
            device_id = self._find_device_id_by_name(device_mgr, device_name)
            device = device_mgr._alicat.get(device_id) if device_id else None
            found = device is not None
            connected = device.connected if device else False
            steps = len(sched_info.get('schedule', []))

            device_checks.append({
                'device_name': device_name,
                'found': found,
                'connected': connected,
                'steps': steps,
                'device_id': device_id,
            })
            if not found:
                warnings.append(f"Device '{device_name}' is in the experiment but not found in the system.")
            elif not connected:
                warnings.append(f"Device '{device_name}' is not connected.")

        # Auto-apply pre_state relay settings, then warn on any failures
        checklist = global_checklist if global_checklist is not None else exp.get('relay_checklist', [])
        # Normalise legacy 'expected_state' → 'pre_state' so _apply_checklist_states finds it
        normalised = [
            {**c, 'pre_state': c.get('pre_state') if c.get('pre_state') is not None else c.get('expected_state')}
            for c in checklist
        ]
        apply_warnings = self._apply_checklist_states(device_mgr, normalised, 'pre_state')
        warnings.extend(apply_warnings)

        # Snapshot relay states *after* applying so the modal shows current values
        relay_states = []
        for pid, periph in device_mgr._peripherals.items():
            if periph.TYPE in ('relay', 'relay_mechanical'):
                state_obj = periph.get_state()
                relay_states.append({
                    'peripheral_id': pid,
                    'name': periph.name,
                    'states': periph.get_states(),
                    'channel_labels': state_obj.get('channel_labels', [f'CH{i}' for i in range(4)]),
                })

        # Flow range checks: warn if any schedule step exceeds the device's max_flow
        flow_warnings = []
        for device_name, sched_info in exp.get('device_schedules', {}).items():
            device_id = self._find_device_id_by_name(device_mgr, device_name)
            device = device_mgr._alicat.get(device_id) if device_id else None
            if device is None:
                continue
            max_flow = device.max_flow
            if max_flow is None:
                flow_warnings.append(
                    f"'{device_name}': max flow not configured — cannot validate schedule range."
                )
                continue
            for step in sched_info.get('schedule', []):
                sp = step.get('setpoint', 0)
                if sp > max_flow:
                    t_human = _format_duration(step['time'])
                    flow_warnings.append(
                        f"'{device_name}': step at t={t_human} sets {sp} SLPM "
                        f"which exceeds max flow of {max_flow} SLPM."
                    )
                    break  # one warning per device is enough

        can_start = not any('not found' in w or 'not connected' in w for w in warnings)

        return {
            'success': True,
            'experiment_id': experiment_id,
            'experiment_name': exp['name'],
            'device_checks': device_checks,
            'relay_states': relay_states,
            'warnings': warnings,
            'flow_warnings': flow_warnings,
            'can_start': can_start,
        }

    def _apply_checklist_states(self, device_mgr, checklist: list, state_key: str) -> list:
        """
        Attempt to set relay states from checklist entries.
        state_key is 'pre_state' or 'post_state'.
        Returns a list of warning strings for any entries that could not be applied.
        """
        warnings = []
        # Build name→id map once
        name_to_id = {
            periph.name: pid
            for pid, periph in device_mgr._peripherals.items()
            if hasattr(periph, 'set_channel')
        }

        for check in checklist:
            desired = check.get(state_key)
            if desired is None:
                continue
            periph_name = check.get('peripheral_name', '')
            channel = check.get('channel', 0)
            label = check.get('label', f'CH{channel}')

            pid = name_to_id.get(periph_name)
            if pid is None:
                warnings.append(f"Cannot set '{periph_name}' — {label}: peripheral not found.")
                continue

            periph = device_mgr._peripherals.get(pid)
            if not periph.connected:
                warnings.append(f"Cannot set '{periph_name}' — {label}: peripheral not connected.")
                continue

            result = device_mgr.set_relay(pid, channel, desired)
            if not result.get('success'):
                state_str = 'OPEN' if desired else 'CLOSED'
                warnings.append(
                    f"Failed to set '{periph_name}' — {label} to {state_str}: "
                    f"{result.get('error', 'unknown error')}"
                )

        return warnings

    def post_run_check(self, device_mgr, global_checklist: list | None = None) -> dict:
        """
        Auto-apply post_state relay settings from checklist, then warn on any failures.
        """
        checklist = global_checklist or []
        warnings = self._apply_checklist_states(device_mgr, checklist, 'post_state')
        return {'warnings': warnings}

    # ── Start / Stop ──────────────────────────────────────────────────────────

    def start_experiment(self, experiment_id: str, device_mgr,
                         exp_file_rotation_minutes: int = 0) -> dict:
        """
        Start an experiment.
        Starts logging and schedules for all assigned devices simultaneously (T=0 = now).
        """
        with self._lock:
            if self._current:
                return {'success': False, 'error': 'Another experiment is already running. Stop it first.'}

        exp = self.get_experiment(experiment_id)
        if not exp:
            return {'success': False, 'error': 'Experiment not found'}
        if not exp.get('device_schedules'):
            return {'success': False, 'error': 'No device schedules assigned to this experiment'}

        # Create per-experiment data directory
        safe_name = exp['name'].replace(' ', '_').replace('/', '-')[:30]
        ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        exp_data_dir = os.path.join(self.data_dir, f"{safe_name}_{ts}")
        os.makedirs(exp_data_dir, exist_ok=True)

        experiment_meta = {
            'name': exp['name'],
            'operator': exp.get('operator', ''),
            'location': exp.get('location', ''),
            'notes': exp.get('notes', ''),
        }

        started_device_ids = []
        devices_info = {}

        # If the experiment has an absolute start time in the future, offset all
        # schedule times so setpoints fire at the correct wall-clock moment.
        delay_s = 0.0
        global_start_iso = exp.get('global_start_iso')
        if global_start_iso:
            try:
                global_start = datetime.fromisoformat(global_start_iso)
                if global_start.tzinfo is None:
                    global_start = global_start.replace(tzinfo=timezone.utc)
                diff = (global_start - datetime.now(timezone.utc)).total_seconds()
                if diff > 0:
                    delay_s = diff
            except Exception:
                pass  # malformed iso — ignore, start immediately

        for device_name, sched_info in exp['device_schedules'].items():
            device_id = self._find_device_id_by_name(device_mgr, device_name)
            if not device_id:
                continue
            device = device_mgr._alicat.get(device_id)
            if not device or not device.connected:
                continue

            devices_info[device_name] = device.to_dict() | {
                'serial_number': device.serial_number,
                'gas_number': device.gas_number,
                'ep_info': (
                    device_mgr._ep_mgr.get_ep(getattr(device, 'emission_point_id', '__test__'))
                    if device_mgr._ep_mgr else {}
                ) or {},
            }

            # Start raw logging (device_manager handles Data/Raw/)
            device_mgr.start_device(device_id)

            # Load and start the schedule (load_schedule normalises setpoint→rate).
            schedule = sched_info.get('schedule', [])
            if schedule:
                if delay_s > 0:
                    schedule = [
                        {**s, 'time': s['time'] + delay_s} for s in schedule
                    ]
                device_mgr.load_schedule(device_id, schedule)
                device_mgr.start_schedule(device_id)

            started_device_ids.append(device_id)

        if not started_device_ids:
            return {'success': False, 'error': 'No devices could be started (check connectivity)'}

        # Create shared experiment logger and attach to device_manager
        exp_logger = ExperimentDataLogger(
            experiment_meta=experiment_meta,
            devices_info=devices_info,
            data_dir=exp_data_dir,
            rotation_minutes=exp_file_rotation_minutes,
        )
        device_mgr.set_experiment_logger(exp_logger)
        self._exp_logger = exp_logger

        # Mark experiment as running
        exp['status'] = 'running'
        exp['started_at'] = datetime.now(timezone.utc).isoformat()
        self._save_experiment(exp)

        with self._lock:
            self._current = {
                'experiment_id': experiment_id,
                'experiment': exp,
                'data_dir': exp_data_dir,
                'start_time': time.time(),
                'started_device_ids': started_device_ids,
            }

        # Prevent auto-stop-logging from firing for these devices while the
        # experiment is running (setpoint may legitimately hit 0 mid-schedule).
        device_mgr.mark_in_experiment(started_device_ids)

        return {
            'success': True,
            'data_dir': exp_data_dir,
            'started_devices': len(started_device_ids),
        }

    def stop_experiment(self, device_mgr) -> dict:
        with self._lock:
            current = self._current
            if not current:
                return {'success': False, 'error': 'No experiment is currently running'}

        device_mgr.unmark_in_experiment(current['started_device_ids'])

        for device_id in current['started_device_ids']:
            device_mgr.set_setpoint(device_id, 0)
            device_mgr.stop_schedule(device_id)
            device_mgr.stop_device(device_id)

        device_mgr.clear_experiment_logger()

        with self._lock:
            exp_logger = self._exp_logger
            self._exp_logger = None

        if exp_logger:
            exp_logger.close()

        exp = current['experiment']
        exp['status'] = 'completed'
        exp['completed_at'] = datetime.now(timezone.utc).isoformat()
        self._save_experiment(exp)

        with self._lock:
            self._current = None

        return {'success': True}

    def get_current_experiment(self) -> dict | None:
        with self._lock:
            if not self._current:
                return None
            elapsed = time.time() - self._current['start_time']
            return {
                'experiment_id': self._current['experiment_id'],
                'name': self._current['experiment']['name'],
                'operator': self._current['experiment'].get('operator', ''),
                'started_at': self._current['experiment'].get('started_at', ''),
                'elapsed_seconds': elapsed,
                'elapsed_human': _format_duration(elapsed),
                'data_dir': os.path.basename(self._current['data_dir']),
                'device_count': len(self._current['started_device_ids']),
            }

    def get_started_device_ids(self) -> list | None:
        """Return the device IDs started by the current experiment, or None if nothing is running."""
        with self._lock:
            if not self._current:
                return None
            return list(self._current['started_device_ids'])

    def get_running_state_for_heartbeat(self) -> dict | None:
        """Return experiment state to include in the heartbeat file."""
        with self._lock:
            if not self._current:
                return None
            return {
                'experiment_id': self._current['experiment_id'],
                'experiment_name': self._current['experiment']['name'],
                'start_time': self._current['start_time'],
                'data_dir': self._current['data_dir'],
                'started_device_ids': self._current['started_device_ids'],
            }

    def mark_crashed(self):
        """Called on crash recovery — mark the running experiment as crashed."""
        with self._lock:
            if not self._current:
                return
            exp = self._current['experiment']
            exp['status'] = 'crashed'
            self._save_experiment(exp)
            self._current = None
            exp_logger = self._exp_logger
            self._exp_logger = None
        if exp_logger:
            exp_logger.close()

    # ── Import / Export ───────────────────────────────────────────────────────

    def export_json(self, experiment_id: str) -> str | None:
        exp = self.get_experiment(experiment_id)
        if not exp:
            return None
        return json.dumps({
            'format_version': '1.0',
            'experiment': {k: v for k, v in exp.items() if k != 'experiment_id'},
            'device_schedules': exp.get('device_schedules', {}),
            'relay_checklist': exp.get('relay_checklist', []),
        }, indent=2)

    def export_device_csv(self, experiment_id: str, device_name: str) -> str | None:
        """Export one device's schedule as CSV (same format as CLI version)."""
        exp = self.get_experiment(experiment_id)
        if not exp:
            return None
        sched_info = exp.get('device_schedules', {}).get(device_name)
        if not sched_info:
            return None
        lines = ['time,rate(SLPM)']
        for step in sched_info.get('schedule', []):
            lines.append(f"{step['time']},{step['setpoint']}")
        return '\n'.join(lines)

    def export_multi_device_csv(self, experiment_id: str) -> str | None:
        """
        Export all device schedules as a multi-device CSV with absolute UTC timestamps.
        Requires global_start_iso to have been saved on the experiment (set during import).
        Returns None if not found or if no timestamp reference exists.
        """
        exp = self.get_experiment(experiment_id)
        if not exp:
            return None
        global_start_iso = exp.get('global_start_iso')
        if not global_start_iso:
            return None
        try:
            global_start = datetime.fromisoformat(global_start_iso)
            if global_start.tzinfo is None:
                global_start = global_start.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

        rows = []
        for device_name, sched_info in exp.get('device_schedules', {}).items():
            for step in sched_info.get('schedule', []):
                abs_time = global_start + timedelta(seconds=step['time'])
                rows.append((abs_time, device_name, step['setpoint']))
        rows.sort(key=lambda r: r[0])

        lines = ['Emission ID,Time (UTC),Flow (SLPM)']
        for abs_time, device_name, flow in rows:
            # ISO 8601 with millisecond precision, always UTC
            ts = abs_time.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
            lines.append(f'{device_name},{ts},{flow}')
        return '\n'.join(lines)

    def import_json(self, json_str: str) -> dict:
        """Import an experiment from a JSON string. Creates a new local experiment."""
        try:
            data = json.loads(json_str)
            # Support both the export wrapper format and a raw experiment dict
            if 'experiment' in data and isinstance(data['experiment'], dict):
                meta = data['experiment']
                device_schedules = data.get('device_schedules', meta.get('device_schedules', {}))
                relay_checklist = data.get('relay_checklist', meta.get('relay_checklist', []))
            else:
                meta = data
                device_schedules = data.get('device_schedules', {})
                relay_checklist = data.get('relay_checklist', [])

            exp_id = self.create_experiment(meta)
            exp = self.get_experiment(exp_id)
            exp['device_schedules'] = device_schedules
            exp['relay_checklist'] = relay_checklist
            self._save_experiment(exp)
            return {'success': True, 'experiment_id': exp_id, 'name': exp['name']}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def parse_device_schedule_csv(self, csv_content: str) -> dict:
        """
        Parse a device schedule CSV (same formats as CLI version):
        - Relative seconds: columns "time", "rate(LPM)" or similar
        - ISO8601: first time cell contains 'T' or '-'
        Returns {'success': bool, 'schedule': [...], 'count': int, 'error': str}
        Row-level errors are collected and returned as 'row_errors' list.
        """
        try:
            reader = csv.DictReader(io.StringIO(csv_content.strip()))
            fieldnames_lower = {k.lower().strip(): k for k in (reader.fieldnames or [])}

            time_key = fieldnames_lower.get('time')
            rate_key = next(
                (v for k, v in fieldnames_lower.items()
                 if any(word in k for word in ['rate', 'lpm', 'slpm', 'setpoint', 'flow'])),
                None
            )
            if not time_key:
                return {'success': False, 'error': 'CSV must have a "time" column'}
            if not rate_key:
                return {'success': False, 'error': 'CSV must have a flow rate column (rate, lpm, slpm, setpoint, or flow)'}

            rows = list(reader)
            if not rows:
                return {'success': False, 'error': 'CSV has no data rows'}

            first_time = rows[0][time_key].strip()
            is_iso = 'T' in first_time or (first_time.count('-') >= 2)

            # For ISO mode: two-pass so out-of-order rows don't corrupt ref_time.
            # First pass: parse all (datetime, rate) pairs.
            raw_entries = []
            row_errors = []
            for row_num, row in enumerate(rows, start=2):  # start=2: row 1 is header
                t_raw = row.get(time_key, '').strip()
                r_raw = row.get(rate_key, '').strip()
                try:
                    rate = float(r_raw)
                except ValueError:
                    row_errors.append({'row': row_num, 'content': str(dict(row)),
                                       'message': f'Invalid flow rate: {r_raw!r}'})
                    continue
                try:
                    if is_iso:
                        dt = date_parser.parse(t_raw)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        raw_entries.append((dt, rate))
                    else:
                        raw_entries.append((float(t_raw), rate))
                except Exception as e:
                    row_errors.append({'row': row_num, 'content': str(dict(row)),
                                       'message': f'Invalid time {t_raw!r}: {e}'})

            if not raw_entries:
                msg = 'CSV has no valid data rows'
                if row_errors:
                    msg += f' ({len(row_errors)} errors found)'
                return {'success': False, 'error': msg, 'row_errors': row_errors}

            # Second pass: sort by time, then compute relative seconds for ISO.
            raw_entries.sort(key=lambda x: x[0])
            if is_iso:
                ref_time = raw_entries[0][0]
                schedule = [{'time': (dt - ref_time).total_seconds(), 'setpoint': rate}
                            for dt, rate in raw_entries]
            else:
                schedule = [{'time': t, 'setpoint': rate} for t, rate in raw_entries]

            schedule.sort(key=lambda x: x['time'])
            duration = schedule[-1]['time'] if schedule else 0
            result = {
                'success': True,
                'schedule': schedule,
                'count': len(schedule),
                'duration_seconds': duration,
                'duration_human': _format_duration(duration),
            }
            if row_errors:
                result['row_errors'] = row_errors
                result['warning'] = f'{len(row_errors)} row(s) skipped due to errors'
            return result
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def parse_multi_device_csv(self, csv_content: str) -> dict:
        """
        Parse a multi-device experiment CSV with columns:
          Emission ID, Time (UTC), Flow (SLPM)
        Groups rows by Emission ID and returns a schedule per device.
        Returns {'success': bool, 'schedules': {emission_id: [...]}, 'row_errors': [...]}
        """
        try:
            # Strip BOM if present
            csv_content = csv_content.lstrip('\ufeff')
            reader = csv.DictReader(io.StringIO(csv_content.strip()))
            # Strip BOM + whitespace from each field name before lowercasing
            fieldnames_lower = {
                k.lstrip('\ufeff').lower().strip(): k
                for k in (reader.fieldnames or [])
            }

            id_key   = (fieldnames_lower.get('emission id') or fieldnames_lower.get('emission_id')
                        or fieldnames_lower.get('device') or fieldnames_lower.get('device name')
                        or fieldnames_lower.get('device_name'))
            time_key = fieldnames_lower.get('time (utc)') or fieldnames_lower.get('time(utc)') or fieldnames_lower.get('time')
            flow_key = fieldnames_lower.get('flow (slpm)') or fieldnames_lower.get('flow(slpm)') or fieldnames_lower.get('flow')

            if not id_key:
                return {'success': False, 'error': 'CSV must have an "Emission ID" column'}
            if not time_key:
                return {'success': False, 'error': 'CSV must have a "Time (UTC)" column'}
            if not flow_key:
                return {'success': False, 'error': 'CSV must have a "Flow (SLPM)" column'}

            rows = list(reader)
            if not rows:
                return {'success': False, 'error': 'CSV has no data rows'}

            # Collect (emission_id, datetime, flow) triples
            groups: dict[str, list] = {}
            row_errors = []
            for row_num, row in enumerate(rows, start=2):
                eid   = row.get(id_key, '').strip()
                t_raw = row.get(time_key, '').strip()
                f_raw = row.get(flow_key, '').strip()
                if not eid:
                    row_errors.append({'row': row_num, 'content': str(dict(row)),
                                       'message': 'Missing Emission ID'})
                    continue
                try:
                    flow = float(f_raw)
                except ValueError:
                    row_errors.append({'row': row_num, 'content': str(dict(row)),
                                       'message': f'Invalid flow: {f_raw!r}'})
                    continue
                try:
                    dt = date_parser.parse(t_raw)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                except Exception as e:
                    row_errors.append({'row': row_num, 'content': str(dict(row)),
                                       'message': f'Invalid time {t_raw!r}: {e}'})
                    continue
                groups.setdefault(eid, []).append((dt, flow))

            if not groups:
                return {'success': False, 'error': 'No valid rows parsed', 'row_errors': row_errors}

            if row_errors:
                msgs = '; '.join(f'Row {e["row"]}: {e["message"]}' for e in row_errors)
                return {
                    'success': False,
                    'error': f'{len(row_errors)} row(s) have errors — fix the file and re-upload',
                    'row_errors': row_errors,
                    'detail': msgs,
                }

            # Use a single global zero reference across all devices so inter-device
            # timing offsets are preserved (e.g. device B starts 1hr after device A).
            all_times = [dt for entries in groups.values() for dt, _ in entries]
            global_earliest = min(all_times)

            schedules = {}
            for eid, entries in groups.items():
                entries.sort(key=lambda x: x[0])
                schedules[eid] = [
                    {'time': (dt - global_earliest).total_seconds(), 'setpoint': flow}
                    for dt, flow in entries
                ]

            return {
                'success': True,
                'schedules': schedules,
                'device_count': len(schedules),
                'global_start_iso': global_earliest.isoformat(),
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}

    # ── Data File Browser ─────────────────────────────────────────────────────

    def list_data_files(self) -> list:
        """
        List all data files in the Data/ directory, grouped by experiment/session.
        Returns list of directory/file entries with metadata.
        """
        results = []
        if not os.path.exists(self.data_dir):
            return results

        try:
            entries = sorted(os.scandir(self.data_dir), key=lambda e: e.name, reverse=True)
        except Exception:
            return results

        for entry in entries:
            if entry.is_dir():
                info = self._scan_data_dir(entry.path, entry.name)
                if info:
                    results.append(info)
            elif entry.is_file() and entry.name.endswith('.csv'):
                size = entry.stat().st_size
                results.append({
                    'type': 'file',
                    'name': entry.name,
                    'path': entry.name,
                    'size': size,
                    'size_human': _format_size(size),
                })

        return results

    def _scan_data_dir(self, dir_path: str, dir_name: str) -> dict | None:
        """Scan a data subdirectory and return its summary."""
        files = []
        total_size = 0
        metadata = None

        meta_path = os.path.join(dir_path, 'experiment_metadata.json')
        if os.path.exists(meta_path):
            try:
                with open(meta_path) as f:
                    raw = json.load(f)
                # New format: {'experiment': {...}, 'devices': {...}, 'start_time': ...}
                if 'experiment' in raw and isinstance(raw['experiment'], dict):
                    metadata = raw['experiment']
                    metadata.setdefault('started_at', raw.get('start_time', ''))
                else:
                    metadata = raw
            except Exception:
                pass

        try:
            for fname in sorted(os.listdir(dir_path)):
                if fname.endswith('.csv'):
                    fpath = os.path.join(dir_path, fname)
                    try:
                        size = os.path.getsize(fpath)
                        total_size += size
                        files.append({
                            'filename': fname,
                            'path': dir_name + '/' + fname,
                            'size': size,
                            'size_human': _format_size(size),
                        })
                    except Exception:
                        pass
        except Exception:
            return None

        if not files:
            return None

        return {
            'type': 'directory',
            'dir_name': dir_name,
            'experiment_name': metadata['name'] if metadata else dir_name,
            'operator': metadata.get('operator', '') if metadata else '',
            'started_at': metadata.get('started_at', '') if metadata else '',
            'files': files,
            'file_count': len(files),
            'total_size': total_size,
            'total_size_human': _format_size(total_size),
        }

    def create_zip(self, dir_name: str) -> str | None:
        """
        Create a ZIP of all CSVs in a data directory.
        Returns the path to the created ZIP file, or None on error.
        """
        dir_path = os.path.join(self.data_dir, dir_name)
        if not os.path.isdir(dir_path):
            return None

        zip_path = dir_path + '.zip'
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for fname in os.listdir(dir_path):
                    fpath = os.path.join(dir_path, fname)
                    if os.path.isfile(fpath):
                        zf.write(fpath, fname)
            return zip_path
        except Exception:
            return None

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _exp_path(self, experiment_id: str) -> str:
        return os.path.join(self.experiments_dir, experiment_id + '.json')

    def _save_experiment(self, experiment: dict):
        path = self._exp_path(experiment['experiment_id'])
        tmp = path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(experiment, f, indent=2)
        os.replace(tmp, path)

    def _find_device_id_by_name(self, device_mgr, device_name: str) -> str | None:
        for device_id, device in device_mgr._alicat.items():
            if device.device_name == device_name:
                return device_id
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _format_duration(seconds: float) -> str:
    if seconds < 0:
        return '0s'
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s}s"
    elif seconds < 86400:
        h, rem = divmod(int(seconds), 3600)
        m = rem // 60
        return f"{h}h {m}m"
    else:
        d, rem = divmod(int(seconds), 86400)
        h = rem // 3600
        return f"{d}d {h}h"


def _format_size(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size //= 1024
    return f"{size:.1f} TB"
