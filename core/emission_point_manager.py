#!/usr/bin/env python3
"""
Emission Point Manager
Manages a global pool of emission points. Each Alicat flow controller
must be assigned to exactly one emission point (or the built-in TEST point).

Versioning: v1 display_name == base_name; v2+ == "{base_name}_{version}".
"""

import threading
import uuid


# ── Test Emission Point (hardcoded, never persisted) ──────────────────────────

TEST_EP_ID = '__test__'

TEST_EP = {
    'ep_id': TEST_EP_ID,
    'base_name': 'DEFAULT',
    'version': 1,
    'display_name': 'DEFAULT',
    'description': 'Default — assign a real emission point before production use',
    'lat': 0.0,
    'lon': 0.0,
    'alt': 0.0,
    'install_datetime': '2000-01-01T00:00:00',
    'photo_filename': None,
    'is_test': True,
}


class EmissionPointManager:
    """
    Thread-safe CRUD manager for emission points.
    The DEFAULT emission point is never stored on disk; it is injected at runtime.
    """

    def __init__(self):
        self._eps: dict = {}       # ep_id -> config dict
        self._ep_order: list = []  # ordered list of ep_ids (TEST not included)
        self._lock = threading.Lock()

    # ── Config I/O ────────────────────────────────────────────────────────────

    def load_from_config(self, config: dict):
        """Restore emission points from a persisted config dict."""
        with self._lock:
            self._eps = config.get('emission_points', {})
            self._ep_order = config.get('ep_order', list(self._eps.keys()))

    def get_configs(self) -> dict:
        """Serializable config dict for persistence (excludes TEST EP)."""
        with self._lock:
            return {
                'emission_points': dict(self._eps),
                'ep_order': list(self._ep_order),
            }

    # ── State Queries ─────────────────────────────────────────────────────────

    def get_all_states(self) -> list:
        """Return all EPs as a list for broadcasting (TEST EP always first)."""
        with self._lock:
            result = [dict(TEST_EP)]
            for ep_id in self._ep_order:
                if ep_id in self._eps:
                    ep = dict(self._eps[ep_id])
                    ep['ep_id'] = ep_id
                    ep['is_test'] = False
                    result.append(ep)
            return result

    def get_ep(self, ep_id: str) -> dict | None:
        """Return a single EP by ID, or None if not found."""
        if ep_id == TEST_EP_ID:
            return dict(TEST_EP)
        with self._lock:
            ep = self._eps.get(ep_id)
            if ep:
                d = dict(ep)
                d['ep_id'] = ep_id
                d['is_test'] = False
                return d
            return None

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def add_ep(self, config: dict) -> dict:
        """
        Add a new emission point.
        Required: base_name, description, lat, lon, install_datetime
        Optional: alt, photo_filename
        Returns {'success': bool, 'ep_id': str, ...} or {'success': False, 'error': str}
        """
        base_name = (config.get('base_name') or '').strip()
        if not base_name:
            return {'success': False, 'error': 'Name is required'}

        description = (config.get('description') or '').strip()
        if not description:
            return {'success': False, 'error': 'Description is required'}

        install_datetime = (config.get('install_datetime') or '').strip()
        if not install_datetime:
            return {'success': False, 'error': 'Install date/time is required'}

        try:
            lat = float(config['lat'])
            lon = float(config['lon'])
        except (TypeError, ValueError, KeyError):
            return {'success': False, 'error': 'Location (lat/lon) is required'}

        alt_raw = config.get('alt')
        alt = float(alt_raw) if alt_raw not in (None, '', 'null') else None

        with self._lock:
            for ep in self._eps.values():
                if ep['base_name'].lower() == base_name.lower():
                    return {'success': False, 'error': f"An emission point named '{base_name}' already exists"}

        ep_id = str(uuid.uuid4())[:8]
        ep_data = {
            'base_name': base_name,
            'version': 1,
            'display_name': base_name,
            'description': description,
            'lat': lat,
            'lon': lon,
            'alt': alt,
            'install_datetime': install_datetime,
            'photo_filename': config.get('photo_filename') or None,
        }
        with self._lock:
            self._eps[ep_id] = ep_data
            self._ep_order.append(ep_id)

        return {
            'success': True,
            'ep_id': ep_id,
            'display_name': base_name,
            'lat': lat,
            'lon': lon,
            'alt': alt,
        }

    def edit_ep(self, ep_id: str, config: dict) -> dict:
        """
        Edit an existing emission point, incrementing its version.
        Returns {'success': True, 'ep': <updated EP dict>} or error.
        """
        if ep_id == TEST_EP_ID:
            return {'success': False, 'error': 'Cannot edit the default emission point'}

        with self._lock:
            ep = self._eps.get(ep_id)
            if not ep:
                return {'success': False, 'error': 'Emission point not found'}

            description = (config.get('description') or '').strip()
            if not description:
                return {'success': False, 'error': 'Description is required'}

            install_datetime = (config.get('install_datetime') or ep.get('install_datetime', '')).strip()
            if not install_datetime:
                return {'success': False, 'error': 'Install date/time is required'}

            try:
                lat = float(config['lat'])
                lon = float(config['lon'])
            except (TypeError, ValueError, KeyError):
                return {'success': False, 'error': 'Location (lat/lon) is required'}

            alt_raw = config.get('alt')
            alt = float(alt_raw) if alt_raw not in (None, '', 'null') else ep.get('alt')

            new_version = ep['version'] + 1
            new_display = f"{ep['base_name']}_{new_version}"
            photo_filename = config.get('photo_filename', ep.get('photo_filename'))

            ep.update({
                'version': new_version,
                'display_name': new_display,
                'description': description,
                'lat': lat,
                'lon': lon,
                'alt': alt,
                'install_datetime': install_datetime,
                'photo_filename': photo_filename or None,
            })

            result_ep = dict(ep)
            result_ep['ep_id'] = ep_id
            result_ep['is_test'] = False
            return {'success': True, 'ep': result_ep}

    def delete_ep(self, ep_id: str) -> dict:
        """Delete an emission point. Returns {'success': bool}."""
        if ep_id == TEST_EP_ID:
            return {'success': False, 'error': 'Cannot delete the default emission point'}
        with self._lock:
            if ep_id not in self._eps:
                return {'success': False, 'error': 'Emission point not found'}
            del self._eps[ep_id]
            self._ep_order = [x for x in self._ep_order if x != ep_id]
        return {'success': True}

    def reorder_eps(self, ordered_ids: list) -> dict:
        """Apply a new display order for emission points."""
        with self._lock:
            valid = [x for x in ordered_ids if x in self._eps]
            remaining = [x for x in self._ep_order if x not in valid]
            self._ep_order = valid + remaining
        return {'success': True}
