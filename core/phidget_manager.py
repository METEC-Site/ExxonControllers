#!/usr/bin/env python3
"""
Phidget Device Manager
Handles Phidgets 4x Thermocouple, Relay, and VINT pressure sensors.
Phidget22 is optional - gracefully degrades if not installed.
"""

import _thread as _cthread   # C-level, never monkey-patched by gevent
import threading
import time
from datetime import datetime, timezone

try:
    from Phidget22.Phidget import Phidget
    from Phidget22.Devices.TemperatureSensor import TemperatureSensor
    from Phidget22.Devices.DigitalOutput import DigitalOutput
    from Phidget22.Devices.VoltageRatioInput import VoltageRatioInput
    from Phidget22.PhidgetException import PhidgetException
    from Phidget22.PhidgetSupport import PhidgetSupport
    from Phidget22.Net import Net, PhidgetServerType
    PhidgetSupport.getDll()   # force-load the native library now; raises OSError if deps missing
    PHIDGET_AVAILABLE = True
except (ImportError, OSError):
    PHIDGET_AVAILABLE = False

# ChannelPersistence is intentionally NOT used.  The Phidget C library's
# internal persistence layer holds stale TCP state for DigitalOutput channels
# after network drops and never fires on_attach again.  Our own close+open
# cycle (driven by the poll loop in device_manager) handles reconnection
# reliably for all channel types.


# Shared server registry — all peripherals pointing at the same
# (host, port, password) endpoint share ONE Net.addServer registration,
# reference-counted across them.  This avoids exhausting the Phidget network
# server's connection-slot limit when several peripherals (e.g. 5 on the same
# PhidgetSBC) would otherwise each open their own TCP connection.
#
# Per-peripheral reconnect operates only at the channel level
# (ch.close()/ch.open(), see close(for_reconnect=True)) and never touches the
# shared registration's refcount.  Only a full close()/open() (refcount
# decrement to 0 / re-acquire) or check_server_health()'s _shared_server_reset
# cycles the underlying Net server registration.
#
# A *bounded* modular generation counter (mod _SERVER_GEN_MODULUS) is
# embedded in the server name so consecutive registrations always get a
# distinct name, while the total number of unique names per host:port stays
# small.  This prevents unbounded accumulation of stale server registrations
# in the Phidget C library during long outages.
#
# A C-level lock is used so it is never monkey-patched by gevent.

_server_lock = _cthread.allocate_lock()  # C-level lock
_net_add_lock = _cthread.allocate_lock()  # serialises concurrent Net.addServer() calls
_shared_servers: dict[tuple, dict] = {}   # (host, port, password) -> {'server_name': str, 'refcount': int}
_server_periph_map: dict[str, list] = {}   # server_name -> [peripheral, ...]
_server_events_registered = False
_server_gen = 0  # modular generation counter
_SERVER_GEN_MODULUS = 10  # names rotate every 10 generations


def _next_server_gen_locked():
    """Return the next generation number (mod _SERVER_GEN_MODULUS).
    Caller must already hold _server_lock (which is not reentrant)."""
    global _server_gen
    _server_gen = (_server_gen + 1) % _SERVER_GEN_MODULUS
    return _server_gen


def _add_server(server_name, hostname, port, password):
    """Call Net.addServer(), retrying once if the name is still registered
    from a not-yet-finished async removeServer."""
    with _net_add_lock:
        try:
            Net.addServer(server_name, hostname, port, password, 0)
        except Exception:
            try:
                Net.removeServer(server_name)
            except Exception:
                pass
            Net.addServer(server_name, hostname, port, password, 0)


def _remove_server_async(server_name):
    """Remove a Net server by name asynchronously in a daemon thread so
    callers never block (Net.removeServer can stall 30+ s on an unreachable
    server)."""
    def _do_remove():
        try:
            Net.removeServer(server_name)
        except Exception:
            pass
    t = threading.Thread(target=_do_remove, daemon=True)
    t.start()


def _shared_server_acquire(hostname, port, password, peripheral):
    """Join (or create) the shared Net server registration for
    (hostname, port, password).  Returns the current server_name.  Each
    successful call must be matched by exactly one _shared_server_release()."""
    _register_server_events_once()
    key = (hostname, port, password)
    with _server_lock:
        entry = _shared_servers.get(key)
        if entry is not None:
            entry['refcount'] += 1
            _server_periph_map.setdefault(entry['server_name'], []).append(peripheral)
            return entry['server_name']
        gen = _next_server_gen_locked()
        server_name = f"phidget_{hostname.replace('.', '_')}_{port}_g{gen}"
        _shared_servers[key] = {'server_name': server_name, 'refcount': 1}
        _server_periph_map[server_name] = [peripheral]
    _add_server(server_name, hostname, port, password)
    return server_name


def _shared_server_release(hostname, port, password, peripheral):
    """Release this peripheral's share of the (host, port, password)
    registration.  When the refcount reaches zero, the underlying Net server
    is removed."""
    key = (hostname, port, password)
    server_name = None
    with _server_lock:
        entry = _shared_servers.get(key)
        if entry is None:
            return
        periphs = _server_periph_map.get(entry['server_name'])
        if periphs is not None:
            try:
                periphs.remove(peripheral)
            except ValueError:
                pass
        entry['refcount'] -= 1
        if entry['refcount'] <= 0:
            server_name = entry['server_name']
            del _shared_servers[key]
            _server_periph_map.pop(server_name, None)
    if server_name is not None:
        _remove_server_async(server_name)


def _shared_server_reset(hostname, port, password):
    """Cycle the underlying Net server registration for (host, port, password)
    without changing its refcount.  Used by check_server_health() when an
    endpoint is confirmed unreachable, so all peripherals sharing it get a
    fresh TCP connection via a single removeServer/addServer pair."""
    key = (hostname, port, password)
    with _server_lock:
        entry = _shared_servers.get(key)
        if entry is None:
            return
        old_name = entry['server_name']
        gen = _next_server_gen_locked()
        new_name = f"phidget_{hostname.replace('.', '_')}_{port}_g{gen}"
        entry['server_name'] = new_name
        _server_periph_map[new_name] = _server_periph_map.pop(old_name, [])
    _add_server(new_name, hostname, port, password)
    _remove_server_async(old_name)


def _register_server_events_once():
    """Register Net server added/removed handlers exactly once."""
    global _server_events_registered
    with _server_lock:
        if _server_events_registered:
            return
        _server_events_registered = True
    try:
        Net.setOnServerAddedHandler(_on_net_server_added)
        Net.setOnServerRemovedHandler(_on_net_server_removed)
    except Exception:
        pass


def _on_net_server_added(server):
    """Phidget network server appeared.  Channel persistence handles re-attach;
    no action needed here — on_attach callbacks update _connected flags."""
    pass


def _on_net_server_removed(server):
    """Phidget network server disappeared.  Immediately invalidate _connected
    flags for every peripheral sharing that server so the poll loop detects
    the disconnect on the next cycle without waiting for on_detach to fire."""
    server_name = getattr(server, 'name', None)
    if not server_name:
        return
    with _server_lock:
        periphs = list(_server_periph_map.get(server_name, []))
    for periph in periphs:
        try:
            periph._on_server_removed()
        except Exception:
            pass


class ThermocouplePeripheral:
    """
    Phidgets 4x Thermocouple device (e.g., TMP1101 / 1048).
    Reads up to 4 temperature channels in degrees Celsius.
    """

    TYPE = 'thermocouple'
    NUM_CHANNELS = 4

    def __init__(self, peripheral_id, name, hub_serial=None, channel_offset=0,
                 server_hostname=None, server_port=5661, server_password='',
                 channel_labels=None, hub_port=None):
        """
        hub_serial      : Phidget Hub serial number (None = any)
        hub_port        : Physical VINT hub port (0–5) to restrict attachment to.
                          When set, uses setHubPort() so multiple same-type devices
                          on one hub can be distinguished. None = any port.
        channel_offset  : Channel index offset within the device (usually 0)
        server_hostname : IP or hostname of network Phidget server (None = USB/local)
        server_port     : Network server port (default 5661)
        server_password : Network server password (default empty)
        channel_labels  : Optional list of 4 label strings for each channel
        """
        self.peripheral_id = peripheral_id
        self.name = name
        self.hub_serial = hub_serial
        self.hub_port = int(hub_port) if hub_port is not None else None
        self.channel_offset = channel_offset
        self.server_hostname = server_hostname
        self.server_port = server_port
        self.server_password = server_password
        self.channel_labels = list(channel_labels) if channel_labels else [f'CH{i}' for i in range(self.NUM_CHANNELS)]

        self._channels = []      # list of TemperatureSensor objects
        self._values = [None] * self.NUM_CHANNELS
        self._connected = [False] * self.NUM_CHANNELS
        self._lock = threading.Lock()
        self._op_lock = _cthread.allocate_lock()  # serialises open() / close()
        self.opened = False
        self.error = None
        self._server_name = None  # assigned by _server_acquire

    def open(self):
        """Open all thermocouple channels."""
        if not PHIDGET_AVAILABLE:
            self.error = "Phidget22 library not installed"
            return False
        with self._op_lock:
            if self.opened:
                return True  # already open; caller must close() first to re-open

            try:
                if self.server_hostname:
                    self._server_name = _shared_server_acquire(
                        self.server_hostname, self.server_port, self.server_password, self
                    )

                for i in range(self.NUM_CHANNELS):
                    ch = TemperatureSensor()
                    if self.server_hostname:
                        ch.setIsRemote(True)
                    if self.hub_serial is not None:
                        ch.setDeviceSerialNumber(self.hub_serial)
                    if self.hub_port is not None:
                        ch.setHubPort(self.hub_port)
                    ch.setChannel(self.channel_offset + i)

                    # Closure capture
                    idx = i
                    def on_attach(c, index=idx):
                        with self._lock:
                            self._connected[index] = True

                    def on_detach(c, index=idx):
                        with self._lock:
                            self._connected[index] = False
                            self._values[index] = None

                    def on_error(c, code, description, index=idx):
                        # Thermocouple boards fire sensor-level errors (open probe,
                        # out-of-range, short) that do NOT indicate a channel
                        # disconnection — the board is still attached.  Clear the
                        # reading but leave _connected alone; on_detach handles
                        # actual channel disconnections.
                        with self._lock:
                            self._values[index] = None

                    def on_temp(c, temp, index=idx):
                        with self._lock:
                            self._values[index] = temp

                    ch.setOnAttachHandler(on_attach)
                    ch.setOnDetachHandler(on_detach)
                    ch.setOnErrorHandler(on_error)
                    ch.setOnTemperatureChangeHandler(on_temp)
                    ch.open()
                    self._channels.append(ch)

                self.opened = True
                self.error = None
                return True
            except Exception as e:
                self.error = str(e)
                self.opened = False
                return False

    @property
    def connected(self):
        return self.opened and any(self._connected)

    def close(self, for_reconnect=False):
        with self._op_lock:
            self.opened = False
            with self._lock:
                self._connected = [False] * self.NUM_CHANNELS
            for ch in self._channels:
                try:
                    ch.close()
                except Exception:
                    pass
            self._channels.clear()
            if not for_reconnect and self._server_name:
                self._server_name = None
                _shared_server_release(
                    self.server_hostname, self.server_port, self.server_password, self
                )

    def _on_server_removed(self):
        """Called by the Net server-removed event handler.  Immediately clears
        connection state so the poll loop detects the disconnect on the next
        cycle without waiting for on_detach to fire."""
        with self._lock:
            self._connected = [False] * self.NUM_CHANNELS
            self._values = [None] * self.NUM_CHANNELS

    def read(self):
        """Return list of 4 temperature values (°C), None for unavailable channels."""
        if not self.opened:
            return [None] * self.NUM_CHANNELS
        # Attempt direct reads in case callbacks haven't fired recently
        values = []
        for i, ch in enumerate(self._channels):
            try:
                values.append(ch.getTemperature())
            except Exception:
                with self._lock:
                    values.append(self._values[i])
        return values

    def _sync_attachment(self):
        """Probe getAttached() for each channel — the C library updates this
        flag when on_detach fires.  Catches any missed lifecycle events."""
        for i, ch in enumerate(self._channels):
            try:
                if not ch.getAttached():
                    with self._lock:
                        self._connected[i] = False
                        self._values[i] = None
            except Exception:
                pass

    def get_state(self):
        self._sync_attachment()
        return {
            'peripheral_id': self.peripheral_id,
            'name': self.name,
            'type': self.TYPE,
            'opened': self.opened,
            'connected': self.connected,
            'error': self.error,
            'values': self.read(),
            'units': '°C',
            'channel_labels': self.channel_labels,
            'hub_serial': self.hub_serial,
            'hub_port': self.hub_port,
            'channel_offset': self.channel_offset,
            'server_hostname': self.server_hostname,
            'server_port': self.server_port,
            'server_password': self.server_password,
        }

    def to_config(self):
        return {
            'peripheral_id': self.peripheral_id,
            'name': self.name,
            'type': self.TYPE,
            'hub_serial': self.hub_serial,
            'hub_port': self.hub_port,
            'channel_offset': self.channel_offset,
            'server_hostname': self.server_hostname,
            'server_port': self.server_port,
            'server_password': self.server_password,
            'channel_labels': self.channel_labels,
        }


class RelayPeripheral:
    """
    Phidgets solid-state relay (e.g. REL1100 / REL1101).
    Controls up to 4 digital output channels.
    """

    TYPE = 'relay'
    NUM_CHANNELS = 4

    def __init__(self, peripheral_id, name, hub_serial=None, channel_offset=0,
                 server_hostname=None, server_port=5661, server_password='',
                 channel_labels=None, hub_port=None):
        self.peripheral_id = peripheral_id
        self.name = name
        self.hub_serial = hub_serial
        self.hub_port = int(hub_port) if hub_port is not None else None
        self.channel_offset = channel_offset
        self.server_hostname = server_hostname
        self.server_port = server_port
        self.server_password = server_password
        self.channel_labels = list(channel_labels) if channel_labels else [f'Relay {i}' for i in range(self.NUM_CHANNELS)]

        self._channels = []
        self._states = [False] * self.NUM_CHANNELS
        self._connected = [False] * self.NUM_CHANNELS
        self._lock = threading.Lock()
        self._op_lock = _cthread.allocate_lock()  # serialises open() / close()
        self.opened = False
        self.error = None
        self._server_name = None  # assigned by _server_acquire

    @property
    def connected(self):
        return self.opened and any(self._connected)

    def open(self):
        if not PHIDGET_AVAILABLE:
            self.error = "Phidget22 library not installed"
            return False
        with self._op_lock:
            if self.opened:
                return True  # already open; caller must close() first to re-open
            try:
                if self.server_hostname:
                    self._server_name = _shared_server_acquire(
                        self.server_hostname, self.server_port, self.server_password, self
                    )
                for i in range(self.NUM_CHANNELS):
                    ch = DigitalOutput()
                    if self.server_hostname:
                        ch.setIsRemote(True)
                    if self.hub_serial is not None:
                        ch.setDeviceSerialNumber(self.hub_serial)
                    if self.hub_port is not None:
                        ch.setHubPort(self.hub_port)
                    ch.setChannel(self.channel_offset + i)

                    idx = i
                    def on_attach(c, index=idx):
                        with self._lock:
                            self._connected[index] = True
                        # Restore desired relay state after reconnection so a
                        # close+open cycle does not lose active relay settings.
                        desired = self._states[index]
                        if desired:
                            try:
                                c.setState(True)
                            except Exception:
                                pass

                    def on_detach(c, index=idx):
                        with self._lock:
                            self._connected[index] = False

                    def on_error(c, code, description, index=idx):
                        # Do NOT clear _connected here.  On a power cycle, the hub
                        # fires error events (unknown relay state, failsafe triggered)
                        # immediately after on_attach, which would leave the channel
                        # stuck as disconnected.  on_detach and _sync_attachment()
                        # handle genuine disconnections; on_error is registered only
                        # so the library delivers the event rather than suppressing it.
                        pass

                    ch.setOnAttachHandler(on_attach)
                    ch.setOnDetachHandler(on_detach)
                    ch.setOnErrorHandler(on_error)
                    ch.open()   # non-blocking; on_attach fires when device connects
                    self._channels.append(ch)

                self.opened = True
                self.error = None
                return True
            except Exception as e:
                self.error = str(e)
                self.opened = False
                return False

    def close(self, for_reconnect=False):
        # Mark as closed immediately so concurrent set_channel() calls
        # fail fast ("Device not opened") instead of hitting 0x34.
        with self._op_lock:
            self.opened = False
            with self._lock:
                self._connected = [False] * self.NUM_CHANNELS
            for ch in self._channels:
                try:
                    if not for_reconnect:
                        ch.setState(False)  # safe shutdown — de-energise relays
                    ch.close()
                except Exception:
                    pass
            self._channels.clear()
            if not for_reconnect and self._server_name:
                self._server_name = None
                _shared_server_release(
                    self.server_hostname, self.server_port, self.server_password, self
                )

    def _on_server_removed(self):
        """Called by the Net server-removed event handler.  Immediately clears
        connection state so the poll loop detects the disconnect on the next
        cycle without waiting for on_detach to fire."""
        with self._lock:
            self._connected = [False] * self.NUM_CHANNELS

    def set_channel(self, channel: int, state: bool):
        """Set a relay channel on or off. Returns (success, message)."""
        if not self.opened:
            return False, "Device not opened"
        if channel < 0 or channel >= self.NUM_CHANNELS:
            return False, f"Channel {channel} out of range"
        try:
            if not self._channels[channel].getAttached():
                return False, f"Channel {channel} not yet attached — the device may still be connecting"
        except Exception:
            return False, f"Channel {channel} not yet attached — the device may still be connecting"
        try:
            self._channels[channel].setState(state)
            with self._lock:
                self._states[channel] = state
            return True, f"Channel {channel} set to {'ON' if state else 'OFF'}"
        except Exception as e:
            # If the channel detached between our check and the setState call,
            # correct the _connected flag so the UI reflects the true state.
            err = str(e)
            if '0x34' in err or 'not Attached' in err or 'NotAttached' in err:
                with self._lock:
                    self._connected[channel] = False
            return False, err

    def get_states(self):
        states = []
        for i, ch in enumerate(self._channels):
            try:
                states.append(ch.getState())
            except Exception:
                with self._lock:
                    states.append(self._states[i])
        return states

    def _sync_attachment(self):
        """Probe getAttached() for each channel — catches any missed events."""
        for i, ch in enumerate(self._channels):
            try:
                if not ch.getAttached():
                    with self._lock:
                        self._connected[i] = False
            except Exception:
                pass

    def get_state(self):
        self._sync_attachment()
        return {
            'peripheral_id': self.peripheral_id,
            'name': self.name,
            'type': self.TYPE,
            'opened': self.opened,
            'connected': self.connected,
            'error': self.error,
            'values': self.get_states(),
            'channel_labels': self.channel_labels,
            'hub_serial': self.hub_serial,
            'hub_port': self.hub_port,
            'channel_offset': self.channel_offset,
            'server_hostname': self.server_hostname,
            'server_port': self.server_port,
            'server_password': self.server_password,
        }

    def to_config(self):
        return {
            'peripheral_id': self.peripheral_id,
            'name': self.name,
            'type': self.TYPE,
            'hub_serial': self.hub_serial,
            'hub_port': self.hub_port,
            'channel_offset': self.channel_offset,
            'server_hostname': self.server_hostname,
            'server_port': self.server_port,
            'server_password': self.server_password,
            'channel_labels': self.channel_labels,
        }


class MechanicalRelayPeripheral(RelayPeripheral):
    """
    Phidgets REL1000 mechanical relay board (4 channels).
    Identical wiring to RelayPeripheral (DigitalOutput) but typed separately
    so the UI can label it correctly.

    Adds hardware failsafe: each channel is configured with a 10-second
    Phidget failsafe timer so that the relays de-energise automatically if
    the host process stops calling heartbeat() within that window.
    """
    TYPE = 'relay_mechanical'
    _FAILSAFE_MS = 10_000  # relays open (safe) if heartbeat() not called within 10 s

    def open(self):
        if not PHIDGET_AVAILABLE:
            self.error = "Phidget22 library not installed"
            return False
        with self._op_lock:
            if self.opened:
                return True  # already open; caller must close() first to re-open
            try:
                if self.server_hostname:
                    self._server_name = _shared_server_acquire(
                        self.server_hostname, self.server_port, self.server_password, self
                    )
                for i in range(self.NUM_CHANNELS):
                    ch = DigitalOutput()
                    if self.server_hostname:
                        ch.setIsRemote(True)
                    if self.hub_serial is not None:
                        ch.setDeviceSerialNumber(self.hub_serial)
                    if self.hub_port is not None:
                        ch.setHubPort(self.hub_port)
                    ch.setChannel(self.channel_offset + i)

                    idx = i
                    def on_attach(c, index=idx):
                        with self._lock:
                            self._connected[index] = True
                        # Enable hardware failsafe after attach so the relay board
                        # de-energises automatically if the host process dies or
                        # the network link drops.
                        try:
                            c.setFailsafeTime(MechanicalRelayPeripheral._FAILSAFE_MS)
                        except Exception:
                            pass  # older firmware may not support failsafe; non-fatal
                        # Restore desired relay state after reconnection
                        desired = self._states[index]
                        if desired:
                            try:
                                c.setState(True)
                            except Exception:
                                pass

                    def on_detach(c, index=idx):
                        with self._lock:
                            self._connected[index] = False

                    def on_error(c, code, description, index=idx):
                        # Do NOT clear _connected here — same reasoning as
                        # RelayPeripheral.  Mechanical relay boards additionally
                        # fire a failsafe-triggered error on cold boot which would
                        # clear _connected immediately after on_attach sets it.
                        pass

                    ch.setOnAttachHandler(on_attach)
                    ch.setOnDetachHandler(on_detach)
                    ch.setOnErrorHandler(on_error)
                    ch.open()   # non-blocking; failsafe is configured in on_attach
                    self._channels.append(ch)

                self.opened = True
                self.error = None
                return True
            except Exception as e:
                self.error = str(e)
                self.opened = False
                return False

    def heartbeat(self):
        """
        Reset the Phidget failsafe watchdog on all attached channels.
        Must be called at least once every _FAILSAFE_MS milliseconds while
        the relays are in use, otherwise the hardware will de-energise them.
        """
        for ch in self._channels:
            try:
                ch.resetFailsafe()
            except Exception:
                pass


class PressureVINTPeripheral:
    """
    Single-channel pressure sensor wired directly to a bare VINT hub port.
    Uses VoltageRatioInput with setIsHubPortDevice(True).
    Applies linear calibration: pressure = (voltage_ratio * scale) + offset
    """

    TYPE = 'pressure_vint'
    NUM_CHANNELS = 1

    def __init__(self, peripheral_id, name, hub_serial=None, hub_port=0,
                 calibration=None, units='psia',
                 server_hostname=None, server_port=5661, server_password='',
                 channel_labels=None):
        """
        hub_serial  : Serial number of the VINT hub (None = any)
        hub_port    : Physical port on the VINT hub (0–5)
        calibration : (scale, offset) tuple; None = raw voltage ratio
        units       : Engineering unit label (e.g. 'psia', 'bar', 'kPa')
        """
        self.peripheral_id = peripheral_id
        self.name = name
        self.hub_serial = hub_serial
        self.hub_port = int(hub_port) if hub_port is not None else 0
        self.calibration = tuple(calibration) if calibration else (1.0, 0.0)
        self.units = units
        self.server_hostname = server_hostname
        self.server_port = server_port
        self.server_password = server_password
        self.channel_labels = [channel_labels[0]] if channel_labels else ['Pressure']

        self._channels = []
        self._values = [None]
        self._channel_connected = False
        self._lock = threading.Lock()
        self.opened = False
        self.error = None
        self._server_name = None  # assigned by _server_acquire
        self._op_lock = _cthread.allocate_lock()  # serialises open() / close()

    @property
    def connected(self):
        return self.opened and self._channel_connected

    def open(self):
        if not PHIDGET_AVAILABLE:
            self.error = "Phidget22 library not installed"
            return False
        with self._op_lock:
            if self.opened:
                return True  # already open; caller must close() first to re-open
            try:
                if self.server_hostname:
                    self._server_name = _shared_server_acquire(
                        self.server_hostname, self.server_port, self.server_password, self
                    )
                ch = VoltageRatioInput()
                if self.server_hostname:
                    ch.setIsRemote(True)
                if self.hub_serial is not None:
                    ch.setDeviceSerialNumber(self.hub_serial)
                ch.setHubPort(self.hub_port)
                ch.setIsHubPortDevice(True)
                ch.setChannel(0)

                scale, offset = self.calibration

                def on_attach(c):
                    with self._lock:
                        self._channel_connected = True

                def on_detach(c):
                    with self._lock:
                        self._channel_connected = False
                        self._values[0] = None

                def on_error(c, code, description):
                    # Do not clear _channel_connected — on_detach and
                    # _sync_attachment() handle genuine disconnections.
                    pass

                def on_ratio(c, ratio):
                    with self._lock:
                        self._values[0] = ratio * scale + offset

                ch.setOnAttachHandler(on_attach)
                ch.setOnDetachHandler(on_detach)
                ch.setOnErrorHandler(on_error)
                ch.setOnVoltageRatioChangeHandler(on_ratio)
                ch.open()
                self._channels.append(ch)

                self.opened = True
                self.error = None
                return True
            except Exception as e:
                self.error = str(e)
                self.opened = False
                return False

    def close(self, for_reconnect=False):
        with self._op_lock:
            self.opened = False
            with self._lock:
                self._channel_connected = False
            for ch in self._channels:
                try:
                    ch.close()
                except Exception:
                    pass
            self._channels.clear()
            if not for_reconnect and self._server_name:
                self._server_name = None
                _shared_server_release(
                    self.server_hostname, self.server_port, self.server_password, self
                )

    def _on_server_removed(self):
        """Called by the Net server-removed event handler.  Immediately clears
        connection state so the poll loop detects the disconnect on the next
        cycle without waiting for on_detach to fire."""
        with self._lock:
            self._channel_connected = False
            self._values = [None]

    def read(self):
        if not self._channels:
            return [None]
        ch = self._channels[0]
        try:
            ratio = ch.getVoltageRatio()
            scale, offset = self.calibration
            return [ratio * scale + offset]
        except Exception:
            with self._lock:
                return list(self._values)

    def _sync_attachment(self):
        """Probe getAttached() — catches any missed lifecycle events."""
        if not self._channels:
            return
        try:
            if not self._channels[0].getAttached():
                with self._lock:
                    self._channel_connected = False
        except Exception:
            pass

    def get_state(self):
        self._sync_attachment()
        return {
            'peripheral_id': self.peripheral_id,
            'name': self.name,
            'type': self.TYPE,
            'opened': self.opened,
            'connected': self.connected,
            'error': self.error,
            'values': self.read(),
            'units': self.units,
            'channel_labels': self.channel_labels,
            'hub_serial': self.hub_serial,
            'hub_port': self.hub_port,
            'calibration': list(self.calibration),
            'server_hostname': self.server_hostname,
            'server_port': self.server_port,
            'server_password': self.server_password,
        }

    def to_config(self):
        return {
            'peripheral_id': self.peripheral_id,
            'name': self.name,
            'type': self.TYPE,
            'hub_serial': self.hub_serial,
            'hub_port': self.hub_port,
            'calibration': list(self.calibration),
            'units': self.units,
            'server_hostname': self.server_hostname,
            'server_port': self.server_port,
            'server_password': self.server_password,
            'channel_labels': self.channel_labels,
        }



# ── Factory ───────────────────────────────────────────────────────────────────

def create_peripheral(config: dict):
    """
    Factory function to create a peripheral object from a config dict.
    Returns the peripheral object or None if type is unknown.
    """
    ptype = config.get('type', '')
    pid   = config.get('peripheral_id', '')
    name  = config.get('name', 'Peripheral')
    hub   = config.get('hub_serial')
    ch_off = config.get('channel_offset', 0)
    srv_host = config.get('server_hostname') or None
    srv_port = int(config.get('server_port') or 5661)
    srv_pass = config.get('server_password') or ''

    labels = config.get('channel_labels') or None
    hub_port_raw = config.get('hub_port')
    hub_port_common = int(hub_port_raw) if hub_port_raw not in (None, '', 'null') else None
    if ptype == ThermocouplePeripheral.TYPE:
        return ThermocouplePeripheral(pid, name, hub, ch_off, srv_host, srv_port, srv_pass, labels, hub_port=hub_port_common)
    elif ptype == RelayPeripheral.TYPE:
        return RelayPeripheral(pid, name, hub, ch_off, srv_host, srv_port, srv_pass, labels, hub_port=hub_port_common)
    elif ptype == MechanicalRelayPeripheral.TYPE:
        return MechanicalRelayPeripheral(pid, name, hub, ch_off, srv_host, srv_port, srv_pass, labels, hub_port=hub_port_common)
    elif ptype == PressureVINTPeripheral.TYPE:
        hub_port = int(config.get('hub_port', 0))
        # Support both new single `calibration` and legacy `calibrations` list-of-4
        cal_new = config.get('calibration')
        cal_old = config.get('calibrations')
        if cal_new:
            calibration = tuple(cal_new[:2])
        elif cal_old:
            calibration = tuple(cal_old[0][:2])
        else:
            calibration = (1.0, 0.0)
        units = config.get('units', 'psia')
        return PressureVINTPeripheral(pid, name, hub, hub_port, calibration, units, srv_host, srv_port, srv_pass, labels)
    return None


# ── Server-level TCP health monitor ──────────────────────────────────────────
#
# The Phidget C library's on_detach fires reliably for graceful disconnects,
# but when the VINT hub loses power (hard power cycle), the TCP connection
# dies silently.  The C library's internal keepalive may take 30+ seconds to
# notice — or may never fire for DigitalOutput channels.
#
# This monitor is *completely decoupled* from individual peripherals.  It
# probes each unique server host:port with a raw TCP connect (2 s timeout).
# If the probe fails, it calls _on_server_removed() on every peripheral
# registered to that server, immediately clearing their _connected flags so
# the poll loop can trigger a close+open cycle.
#
# The monitor is meant to be called once every few seconds from the poll loop
# in device_manager.

import socket as _socket

_server_health_last: dict[tuple[str, int], float] = {}   # (host, port) -> last_check monotonic
_server_health_fail_count: dict[tuple[str, int], int] = {}  # (host, port) -> consecutive probe failures
_SERVER_HEALTH_INTERVAL = 3.0  # seconds between probes for the same endpoint
_SERVER_HEALTH_TIMEOUT = 2.0   # TCP connect timeout
_SERVER_HEALTH_FAIL_THRESHOLD = 3  # consecutive failed probes before declaring the endpoint down


def check_server_health(peripherals: dict):
    """Probe each unique Phidget server endpoint used by the given peripherals.

    For each unique (hostname, port), attempt a raw TCP connect.  If it fails
    _SERVER_HEALTH_FAIL_THRESHOLD times in a row, call _on_server_removed() on
    every peripheral using that endpoint so the poll loop detects the
    disconnect.  Requiring consecutive failures avoids force-disconnecting a
    healthy group of peripherals over a single transient probe error.

    Parameters
    ----------
    peripherals : dict
        peripheral_id -> peripheral object mapping (from DeviceManager._peripherals)
    """
    if not PHIDGET_AVAILABLE:
        return

    now = time.monotonic()

    # Group peripherals by server endpoint
    endpoints: dict[tuple[str, int], list] = {}
    for periph in peripherals.values():
        host = getattr(periph, 'server_hostname', None)
        if not host:
            continue
        port = getattr(periph, 'server_port', 5661)
        key = (host, port)
        endpoints.setdefault(key, []).append(periph)

    for (host, port), periphs in endpoints.items():
        # Rate-limit probes — no more than once per _SERVER_HEALTH_INTERVAL
        last = _server_health_last.get((host, port), 0)
        if now - last < _SERVER_HEALTH_INTERVAL:
            continue
        _server_health_last[(host, port)] = now

        # Only probe if at least one peripheral thinks it's connected.
        # If all are already disconnected, the poll loop is already handling
        # reconnection — no need to add probe overhead.
        any_connected = any(getattr(p, 'connected', False) for p in periphs)
        if not any_connected:
            continue

        # Raw TCP connect probe
        reachable = False
        try:
            sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            sock.settimeout(_SERVER_HEALTH_TIMEOUT)
            sock.connect((host, port))
            reachable = True
        except Exception:
            reachable = False
        finally:
            try:
                sock.close()
            except Exception:
                pass

        if not reachable:
            # Require several consecutive failed probes before acting, so a
            # single transient connect error (e.g. a momentary local socket
            # abort) doesn't force-disconnect every peripheral on this
            # endpoint.
            fail_count = _server_health_fail_count.get((host, port), 0) + 1
            _server_health_fail_count[(host, port)] = fail_count
            if fail_count >= _SERVER_HEALTH_FAIL_THRESHOLD:
                # Server unreachable — cycle the shared Net server registration
                # once for this endpoint, then force-disconnect all peripherals
                # on it so the poll loop reattaches each one's channels
                # individually against the fresh connection.
                password = getattr(periphs[0], 'server_password', '') or ''
                _shared_server_reset(host, port, password)
                for p in periphs:
                    try:
                        p._on_server_removed()
                    except Exception:
                        pass
                _server_health_fail_count[(host, port)] = 0
        else:
            _server_health_fail_count[(host, port)] = 0


PHIDGET_AVAILABLE_FLAG = PHIDGET_AVAILABLE
