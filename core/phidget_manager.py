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


# No global lock around Net.addServer / Net.removeServer.
# Each peripheral has its own _op_lock that serialises its open() and close()
# calls, and each uses a unique server_name ("phidget_<id>"), so concurrent
# Net operations for *different* peripherals target different server names and
# cannot trigger PhidgetException 0x1b (Duplicate).  A global lock previously
# serialised ALL peripherals behind a single Net.removeServer() C call that
# blocks for 30+ seconds when the Phidget server is unreachable, starving the
# gevent threadpool and killing WebSocket responsiveness.


def _net_add_server(server_name, hostname, port, password):
    """Register a Phidget network server.
    Uses a non-blocking remove first (same fire-and-forget approach as
    _net_remove_server) so this function never blocks on TCP teardown.
    If the remove hasn't completed by the time addServer runs, the
    Duplicate error is silently ignored — the existing registration has
    the same host/port, so reusing it is correct for reconnect scenarios."""
    _net_remove_server(server_name)   # fire-and-forget; never blocks
    try:
        Net.addServer(server_name, hostname, port, password, 0)
    except Exception as e:
        # Duplicate = remove not yet done but existing entry is fine; ignore.
        if 'Duplicate' not in str(e):
            raise


def _net_remove_server(server_name):
    """Unregister a Phidget network server in a daemon thread so callers never
    block.  Net.removeServer() can stall for 30+ s waiting for TCP teardown on
    an unreachable server; running it in the background keeps close() fast so a
    subsequent open() is called immediately when reconnecting."""
    def _do_remove():
        try:
            Net.removeServer(server_name)
        except Exception:
            pass
    t = threading.Thread(target=_do_remove, daemon=True)
    t.start()


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
        self._server_name = f"phidget_{self.peripheral_id}"

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
                    _net_add_server(
                        self._server_name,
                        self.server_hostname, self.server_port, self.server_password
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

                    def on_temp(c, temp, index=idx):
                        with self._lock:
                            self._values[index] = temp

                    ch.setOnAttachHandler(on_attach)
                    ch.setOnDetachHandler(on_detach)
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

    def close(self):
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
            if self.server_hostname:
                _net_remove_server(self._server_name)

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

    def get_state(self):
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
        self._server_name = f"phidget_{self.peripheral_id}"

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
                    _net_add_server(
                        self._server_name,
                        self.server_hostname, self.server_port, self.server_password
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

                    def on_detach(c, index=idx):
                        with self._lock:
                            self._connected[index] = False

                    ch.setOnAttachHandler(on_attach)
                    ch.setOnDetachHandler(on_detach)
                    ch.open()   # non-blocking; on_attach fires when device connects
                    self._channels.append(ch)

                self.opened = True
                self.error = None
                return True
            except Exception as e:
                self.error = str(e)
                self.opened = False
                return False

    def close(self):
        # Mark as closed immediately so concurrent set_channel() calls
        # fail fast ("Device not opened") instead of hitting 0x34.
        with self._op_lock:
            self.opened = False
            with self._lock:
                self._connected = [False] * self.NUM_CHANNELS
            for ch in self._channels:
                try:
                    ch.setState(False)
                    ch.close()
                except Exception:
                    pass
            self._channels.clear()
            if self.server_hostname:
                _net_remove_server(self._server_name)

    def set_channel(self, channel: int, state: bool):
        """Set a relay channel on or off. Returns (success, message)."""
        if not self.opened:
            return False, "Device not opened"
        if channel < 0 or channel >= self.NUM_CHANNELS:
            return False, f"Channel {channel} out of range"
        with self._lock:
            if not self._connected[channel]:
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

    def get_state(self):
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
                    _net_add_server(
                        self._server_name,
                        self.server_hostname, self.server_port, self.server_password
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

                    def on_detach(c, index=idx):
                        with self._lock:
                            self._connected[index] = False

                    ch.setOnAttachHandler(on_attach)
                    ch.setOnDetachHandler(on_detach)
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
        self._server_name = f"phidget_{self.peripheral_id}"
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
                    _net_add_server(
                        self._server_name,
                        self.server_hostname, self.server_port, self.server_password
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

                def on_ratio(c, ratio):
                    with self._lock:
                        self._values[0] = ratio * scale + offset

                ch.setOnAttachHandler(on_attach)
                ch.setOnDetachHandler(on_detach)
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

    def close(self):
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
            if self.server_hostname:
                _net_remove_server(self._server_name)

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

    def get_state(self):
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


PHIDGET_AVAILABLE_FLAG = PHIDGET_AVAILABLE
