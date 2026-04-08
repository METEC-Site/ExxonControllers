#!/usr/bin/env python3
"""
Alicat Flow Controller Device Interface
Communicates with Alicat MCP/MCR and MC series flow controllers via Modbus TCP/IP.
MCR responds identically to MCP (same register map and encoding).
Ported and adapted from the CLI version for use with the web application.
"""

import socket
import struct
import threading
import time
from datetime import datetime, timezone

# Modbus TCP is implemented directly over socket — no C extension dependency,
# gevent-safe, and no segfault risk on connection timeout.


# ── Gas Table ─────────────────────────────────────────────────────────────────
GAS_TABLE = {
    0: 'Air',     1: 'Ar',      2: 'CH4',     3: 'CO',      4: 'CO2',
    5: 'C2H6',    6: 'H2',      7: 'He',       8: 'N2',      9: 'N2O',
    10: 'Ne',     11: 'O2',     12: 'C3H8',   13: 'nC4H10', 14: 'C2H2',
    15: 'C2H4',   16: 'iC4H10', 17: 'Kr',     18: 'Xe',     19: 'SF6',
    20: 'C-25',   21: 'C-10',   22: 'C-8',    23: 'C-2',    24: 'C-75',
    25: 'A-75',   26: 'A-25',   27: 'A1025',  28: 'Star29', 29: 'P-5',
}

# ── Device Type Configs ────────────────────────────────────────────────────────
DEVICE_CONFIGS = {
    'MCP': {
        'format': 'sint32',          # SInt32 with x0.01 scaling
        'register_type': 'input',
        'registers': {
            'setpoint':       1299,   # 0-indexed (docs ref 1300-1301)
            'pressure':       1303,
            'temperature':    1309,
            'vol_flow':       1311,
            'mass_flow':      1313,
            'serial_number':  1093,
            'gas_number':     1346,
            'max_flow_raw':   1643,   # 32-bit unsigned int (docs ref 1644-1645)
            'max_flow_dec':   1650,   # decimal places: scale = 10^(-value) (docs ref 1651)
        },
        'setpoint_holding': 1299,    # MCP setpoint write uses input registers too
        'use_holding_for_setpoint': False,
    },
    # MCR uses the same Modbus register map and encoding as MCP
    'MCR': {
        'format': 'sint32',
        'register_type': 'input',
        'registers': {
            'setpoint':       1299,
            'pressure':       1303,
            'temperature':    1309,
            'vol_flow':       1311,
            'mass_flow':      1313,
            'serial_number':  1093,
            'gas_number':     1346,
            'max_flow_raw':   1643,
            'max_flow_dec':   1650,
        },
        'use_holding_for_setpoint': False,
    },
    'MC': {
        'format': 'float32',         # IEEE 754 32-bit float
        'register_type': 'input',
        'registers': {
            'setpoint_write': 1009,   # Holding register for writes (docs ref 1010-1011)
            'gas_number':     1199,
            'status':         1200,
            'pressure':       1202,
            'temperature':    1204,
            'vol_flow':       1206,
            'mass_flow':      1208,
            'setpoint':       1210,   # Input register for reading
            'serial_number':  None,   # MC series may not have serial in Modbus
        },
        'use_holding_for_setpoint': True,
    },
}

# Detection probes: (device_type, register, count, decoder)
# Try MCP/MCR first (most common in this deployment), then MC.
# A "sane" mass flow is any non-NaN float in [-1, 500].
_DETECTION_PROBES = [
    ('MCP', 1313, 2, 'sint32'),   # MCP mass_flow register
    ('MC',  1208, 2, 'float32'),  # MC  mass_flow register
]


# ── Pure-Python Modbus TCP Client ─────────────────────────────────────────────

class _ModbusTCPClient:
    """
    Minimal Modbus TCP client built on Python's socket module.
    Implements only the three function codes used by AlicatDevice:
      FC 0x03 — Read Holding Registers
      FC 0x04 — Read Input Registers
      FC 0x10 — Write Multiple Registers
    Timeouts raise a plain Python exception — no C-level mutex, no segfault.
    """

    def __init__(self, host, port=502, timeout=1.0, connect_timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connect_timeout = connect_timeout if connect_timeout is not None else timeout
        self._sock = None
        self._tid = 0

    def connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.connect_timeout)
        try:
            sock.connect((self.host, self.port))
            # Switch to the (shorter) read/write timeout for all subsequent I/O.
            sock.settimeout(self.timeout)
            self._sock = sock
            return True
        except Exception:
            try:
                sock.close()
            except Exception:
                pass
            return False

    def close(self):
        sock, self._sock = self._sock, None
        if sock:
            try:
                sock.close()
            except Exception:
                pass

    def _next_tid(self):
        self._tid = (self._tid + 1) & 0xFFFF
        return self._tid

    def _transact(self, pdu, unit_id):
        """Send one Modbus TCP request and return the response PDU, or None."""
        if self._sock is None:
            return None
        tid = self._next_tid()
        mbap = struct.pack('>HHHB', tid, 0, 1 + len(pdu), unit_id)
        try:
            self._sock.sendall(mbap + pdu)
            header = self._recv_all(7)
            if header is None:
                return None
            resp_tid, _pid, length, _unit = struct.unpack('>HHHB', header)
            if resp_tid != tid or length < 1:
                return None
            return self._recv_all(length - 1)
        except Exception:
            return None

    def _recv_all(self, n):
        buf = bytearray()
        while len(buf) < n:
            try:
                chunk = self._sock.recv(n - len(buf))
            except Exception:
                return None
            if not chunk:
                return None
            buf += chunk
        return bytes(buf)

    def _read_registers(self, fc, address, count, unit_id):
        pdu = struct.pack('>BHH', fc, address, count)
        resp = self._transact(pdu, unit_id)
        if resp is None or len(resp) < 2 or resp[0] != fc:
            return None
        byte_count = resp[1]
        if len(resp) < 2 + byte_count or byte_count != count * 2:
            return None
        regs = list(struct.unpack(f'>{count}H', resp[2:2 + byte_count]))
        return _ModbusReadResult(regs)

    def read_input_registers(self, address, count, unit_id):
        return self._read_registers(0x04, address, count, unit_id)

    def read_holding_registers(self, address, count, unit_id):
        return self._read_registers(0x03, address, count, unit_id)

    def write_registers(self, address, values, unit_id):
        count = len(values)
        data = struct.pack(f'>{count}H', *values)
        pdu = struct.pack('>BHHB', 0x10, address, count, count * 2) + data
        resp = self._transact(pdu, unit_id)
        if resp is None or len(resp) < 1 or resp[0] != 0x10:
            return None
        return _ModbusWriteResult()


class _ModbusReadResult:
    __slots__ = ('registers',)

    def __init__(self, registers):
        self.registers = registers


class _ModbusWriteResult:
    def isError(self):
        return False


class AlicatDevice:
    """
    Interface to a single Alicat flow controller via Modbus TCP/IP.
    Thread-safe: all public methods acquire an internal lock.
    """

    def __init__(self, host, port=502, unit_id=1, device_type='MCP',
                 device_name=None, max_flow=None, lat=None, lon=None, alt=None,
                 expected_serial=None, emission_point_id='__test__'):
        self.host = host
        self.port = port
        self.unit_id = unit_id
        # 'AUTO' means probe on first connect; stored as 'AUTO' until resolved
        self.device_type = device_type.upper() if device_type else 'AUTO'
        self.device_name = device_name or host
        self._max_flow_user = float(max_flow) if max_flow is not None else None  # user-supplied cap
        self.max_flow = self._max_flow_user  # effective max; updated after device read
        self.max_flow_reported = None        # as read from device registers
        self.max_flow_is_fallback = False    # True only when register read failed entirely
        self.emission_point_id = emission_point_id or '__test__'
        self.lat = lat            # Cached from assigned emission point
        self.lon = lon            # Cached from assigned emission point
        self.alt = alt            # Cached from assigned emission point (metres)
        self.expected_serial = expected_serial or None  # None = not checked

        self.config = DEVICE_CONFIGS.get(self.device_type, DEVICE_CONFIGS['MCP'])
        self._client = None
        self._lock = threading.Lock()
        self.connected = False
        self.serial_number = None
        self.gas_number = None
        self.fail_count = 0
        self.last_reading = None

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self):
        """Attempt to connect. Returns True on success."""
        with self._lock:
            try:
                # Close any existing client before creating a new one to prevent socket leaks.
                if self._client is not None:
                    try:
                        self._client.close()
                    except Exception:
                        pass
                    self._client = None
                # Connect timeout (2 s) is generous for initial TCP handshake;
                # read/write timeout (0.25 s) is tight because a healthy device
                # on a local network responds in < 50 ms.  Keeping reads fast
                # prevents a single unresponsive device from blocking the 1 Hz
                # poll cycle.  Five consecutive failures (≈5 s) are required
                # before the device is marked disconnected.
                self._client = _ModbusTCPClient(self.host, port=self.port,
                                                timeout=0.25, connect_timeout=2.0)
                result = self._client.connect()
                self.connected = result
                if result:
                    self.fail_count = 0
                    # If device_type is AUTO, probe now (lock already held; pass client directly)
                    if self.device_type == 'AUTO':
                        detected = self._probe_device_type_locked()
                        if detected:
                            self.device_type = detected
                            self.config = DEVICE_CONFIGS[detected]
                            print(f"[AlicatDevice] {self.device_name}: auto-detected as {detected}", flush=True)
                        else:
                            # Fall back to MCP so polling still works
                            self.device_type = 'MCP'
                            self.config = DEVICE_CONFIGS['MCP']
                            print(f"[AlicatDevice] {self.device_name}: detection failed, defaulting to MCP", flush=True)
                return result
            except Exception as e:
                self.connected = False
                return False

    def _probe_device_type_locked(self):
        """
        Identify device type by probing discriminating registers.
        Must be called with self._lock held and self._client already connected.

        Problem: at zero flow both MCP (sint32 at 1313) and MC (float32 at 1208)
        return 0x00000000, which decodes to 0.0 in either format — a "sane"
        value.  So mass-flow alone is ambiguous.

        Discriminator: MCP/MCR devices expose a max_flow_raw register at 1643
        (a non-zero unsigned 32-bit int encoding the device full-scale range).
        MC devices do not define register 1643; it returns 0 or garbage.
        We only accept an MCP/MCR match when max_flow_raw is 1–10,000,000.
        """
        import math

        for dtype, reg, count, fmt in _DETECTION_PROBES:
            try:
                data = self._read_input_registers(reg, count)
                if not data or len(data) < count:
                    continue
                if fmt == 'sint32':
                    val = self._sint32_to_float(data[0], data[1])
                else:
                    val = self._float32_from_registers(data[0], data[1])
                if math.isnan(val) or math.isinf(val) or not (-1.0 <= val <= 2000.0):
                    continue

                # MCP/MCR extra check: read max_flow_raw (register 1643).
                # A genuine MCP/MCR device reports a non-zero full-scale value.
                # MC devices return 0 here, which would be a false positive.
                if dtype in ('MCP', 'MCR'):
                    mfr = self._read_input_registers(1643, 2)
                    if not mfr or len(mfr) < 2:
                        continue
                    max_flow_raw = (mfr[0] << 16) | mfr[1]
                    if not (1 <= max_flow_raw <= 10_000_000):
                        continue  # looks like MC at zero flow — keep trying

                return dtype
            except Exception:
                continue
        return None

    def disconnect(self):
        """Disconnect from device."""
        with self._lock:
            if self._client:
                try:
                    self._client.close()
                except Exception:
                    pass
                self._client = None
            self.connected = False

    # ── Low-Level Modbus I/O ──────────────────────────────────────────────────

    def _modbus_call(self, fn_name, address, count):
        """Call a register-read method on the client."""
        if not self._client or not self.connected:
            return None
        try:
            result = getattr(self._client, fn_name)(address, count, self.unit_id)
            return result.registers if result is not None else None
        except Exception:
            return None

    def _read_input_registers(self, address, count):
        """Read input registers with pymodbus version compatibility."""
        return self._modbus_call('read_input_registers', address, count)

    def _read_holding_registers(self, address, count):
        """Read holding registers with pymodbus version compatibility."""
        return self._modbus_call('read_holding_registers', address, count)

    def _write_registers(self, address, values):
        """Write multiple holding registers."""
        if not self._client or not self.connected:
            return False
        try:
            result = self._client.write_registers(address, values, self.unit_id)
            return result is not None and not result.isError()
        except Exception:
            return False

    # ── Data Conversion ───────────────────────────────────────────────────────

    def _sint32_to_float(self, high, low):
        """Convert two 16-bit registers (SInt32 x0.01 scaled) to float. Used by MCP."""
        combined = (high << 16) | low
        if combined >= 0x80000000:
            combined -= 0x100000000
        return combined / 100.0

    def _float32_from_registers(self, high, low):
        """Convert two 16-bit registers to IEEE 754 float. Used by MC."""
        raw = struct.pack('>HH', high, low)
        return struct.unpack('>f', raw)[0]

    def _float_to_registers(self, value):
        """Convert float to two 16-bit register values (IEEE 754 big-endian)."""
        raw = struct.pack('>f', float(value))
        high, low = struct.unpack('>HH', raw)
        return [high, low]

    def _sint32_from_float(self, value):
        """Convert float to SInt32 x0.01 register pair. Used by MCP setpoint writes."""
        scaled = int(round(value * 100))
        if scaled < 0:
            scaled += 0x100000000
        high = (scaled >> 16) & 0xFFFF
        low = scaled & 0xFFFF
        return [high, low]

    # ── Device Info ───────────────────────────────────────────────────────────

    def read_device_info(self):
        """
        Read serial number, gas number, and auto-detect max_flow if needed.
        Returns dict with info, or None on failure.
        """
        with self._lock:
            regs = self.config['registers']
            info = {}

            # Serial number — read as plain unsigned 32-bit integer (no flow scaling)
            if regs.get('serial_number') is not None:
                data = self._read_input_registers(regs['serial_number'], 2)
                if data and len(data) >= 2:
                    sn = (data[0] << 16) | data[1]
                    if 0 < sn < 10_000_000:  # sanity check: valid serial range
                        info['serial_number'] = str(sn)
                        self.serial_number = info['serial_number']

            # Gas number
            gas_reg = regs.get('gas_number')
            if gas_reg is not None:
                data = self._read_input_registers(gas_reg, 1)
                if data and len(data) >= 1:
                    info['gas_number'] = data[0]
                    self.gas_number = data[0]

            # Always read max_flow from device registers; user value (if set) caps it downward.
            raw_reg = regs.get('max_flow_raw')
            dec_reg = regs.get('max_flow_dec')
            device_reported = None
            if raw_reg is not None:
                raw_data = self._read_input_registers(raw_reg, 2)
                dec_data = self._read_input_registers(dec_reg, 1) if dec_reg is not None else None
                if raw_data and len(raw_data) >= 2:
                    raw_val = (raw_data[0] << 16) | raw_data[1]
                    decimals = dec_data[0] if (dec_data and len(dec_data) >= 1) else 2
                    device_reported = raw_val * (10 ** -decimals)

            if device_reported is not None:
                self.max_flow_is_fallback = False
                self.max_flow_reported = device_reported
                # User cap takes effect if it is lower than device-reported max
                if self._max_flow_user is not None and self._max_flow_user < device_reported:
                    self.max_flow = self._max_flow_user
                else:
                    self.max_flow = device_reported
            else:
                # No max_flow register on this device type (e.g. MC series).
                # Use only the user-supplied value; leave None if user hasn't set one.
                self.max_flow_is_fallback = True
                self.max_flow_reported = None
                self.max_flow = self._max_flow_user  # None if never set by user

            info['max_flow'] = self.max_flow
            return info if info else None

    # ── Process Values ────────────────────────────────────────────────────────

    def _read_process_values_locked(self):
        """Read all process values. Must be called with self._lock held."""
        regs = self.config['registers']
        fmt = self.config['format']

        if fmt == 'sint32':
            # MCP/MCR: single read spanning setpoint through mass_flow (1299-1314 = 16 regs).
            # One request instead of two prevents a second connect-timeout when the
            # device has just dropped off the network, halving disconnect-detection time.
            # Offsets from base 1299:
            #   [0,1]=setpoint, [4,5]=pressure, [10,11]=temp,
            #   [12,13]=vol_flow, [14,15]=mass_flow
            block = self._read_input_registers(regs['setpoint'], 16)
            if not block or len(block) < 16:
                return None
            return {
                'setpoint':    self._sint32_to_float(block[0],  block[1]),
                'pressure':    self._sint32_to_float(block[4],  block[5]),
                'temperature': self._sint32_to_float(block[10], block[11]),
                'vol_flow':    self._sint32_to_float(block[12], block[13]),
                'mass_flow':   self._sint32_to_float(block[14], block[15]),
            }
        else:
            # MC: read from status(1200) through setpoint(1210) = 12 registers
            block = self._read_input_registers(regs['status'], 12)
            if not block or len(block) < 12:
                return None
            return {
                'pressure':    round(self._float32_from_registers(block[2], block[3]), 3),
                'temperature': round(self._float32_from_registers(block[4], block[5]), 2),
                'vol_flow':    round(self._float32_from_registers(block[6], block[7]), 3),
                'mass_flow':   round(self._float32_from_registers(block[8], block[9]), 3),
                'setpoint':    round(self._float32_from_registers(block[10], block[11]), 2),
            }

    def read_process_values(self):
        """
        Read all process values (pressure, temp, vol_flow, mass_flow, setpoint).
        Returns dict with ISO8601 timestamp, or None on failure.
        """
        with self._lock:
            if not self.connected:
                return None
            try:
                values = self._read_process_values_locked()
                if values is None:
                    self.fail_count += 1
                    if self.fail_count >= 5:
                        self.connected = False
                        print(f"[AlicatDevice] {self.device_name} marked disconnected "
                              f"after {self.fail_count} failures")
                    return None
                self.fail_count = 0
                values['timestamp'] = datetime.now(timezone.utc).isoformat()
                values['gas_number'] = self.gas_number
                self.last_reading = values
                return values
            except Exception:
                self.fail_count += 1
                if self.fail_count >= 5:
                    self.connected = False
                return None

    # ── Setpoint Control ─────────────────────────────────────────────────────

    def set_flow_rate(self, flow_rate):
        """
        Set the flow setpoint. Validates against max_flow.
        Returns (success, message).
        """
        if self.max_flow and flow_rate > self.max_flow:
            return False, f"Flow rate {flow_rate} exceeds max {self.max_flow}"
        if flow_rate < 0:
            return False, "Flow rate cannot be negative"

        with self._lock:
            if not self.connected:
                return False, "Device not connected"
            try:
                regs = self.config['registers']
                fmt = self.config['format']
                use_holding = self.config.get('use_holding_for_setpoint', False)

                if fmt == 'sint32':
                    reg_values = self._sint32_from_float(flow_rate)
                    reg_addr = regs['setpoint']
                else:
                    reg_values = self._float_to_registers(flow_rate)
                    reg_addr = regs['setpoint_write']

                success = self._write_registers(reg_addr, reg_values)
                if not success:
                    return False, "Write failed"

                # Readback: immediately re-read the setpoint register to verify
                # the device actually applied the command (some firmware accepts
                # the Modbus write but silently ignores it, e.g. when in local
                # control mode).
                if fmt == 'sint32':
                    rb = self._read_input_registers(regs['setpoint'], 2)
                    actual = self._sint32_to_float(rb[0], rb[1]) if rb and len(rb) >= 2 else None
                else:
                    rb = self._read_input_registers(regs['setpoint'], 2)
                    actual = self._float32_from_registers(rb[0], rb[1]) if rb and len(rb) >= 2 else None

                # Update last_reading with the verified value so the UI reflects
                # reality immediately rather than waiting for the next poll.
                # When the difference is within device precision (±0.1 SLPM) use
                # the commanded value — the device rounds to 0.01 SLPM internally
                # and sub-resolution noise (499.99 vs 500.00) would confuse operators.
                if actual is not None and self.last_reading is not None:
                    self.last_reading['setpoint'] = (
                        flow_rate if abs(actual - flow_rate) <= 0.1 else actual
                    )

                if actual is None:
                    return True, f"Setpoint written (readback unavailable)"
                if abs(actual - flow_rate) > 0.5:
                    return True, (f"Write acknowledged but device reports {actual:.2f} SLPM "
                                  f"(commanded {flow_rate}). Check device type configuration.")
                return True, f"Setpoint set to {flow_rate}"
            except Exception as e:
                return False, str(e)

    # ── Gas Selection ─────────────────────────────────────────────────────────

    def set_gas(self, gas_number):
        """
        Set the gas number on the device.
        Returns (success, message).
        """
        with self._lock:
            if not self.connected:
                return False, "Device not connected"
            try:
                regs = self.config['registers']
                gas_reg = regs.get('gas_number')
                if gas_reg is None:
                    return False, "Gas selection not supported for this device type"
                success = self._write_registers(gas_reg, [int(gas_number)])
                if success:
                    self.gas_number = gas_number
                    return True, f"Gas set to {gas_number}"
                else:
                    return False, "Write failed"
            except Exception as e:
                return False, str(e)

    # ── State Serialization ───────────────────────────────────────────────────

    def to_dict(self):
        """Return serializable configuration dict for persistence."""
        return {
            'host': self.host,
            'port': self.port,
            'unit_id': self.unit_id,
            'device_type': self.device_type,
            'device_name': self.device_name,
            'max_flow': self._max_flow_user,  # persist only the user-supplied cap (None = no cap)
            'emission_point_id': self.emission_point_id,
            'lat': self.lat,   # cached from emission point; kept for data logging
            'lon': self.lon,
            'alt': self.alt,
            'expected_serial': self.expected_serial,
        }

    def get_state(self):
        """Return current runtime state dict for broadcasting."""
        return {
            'connected': self.connected,
            'serial_number': self.serial_number,
            'gas_number': self.gas_number,
            'max_flow': self.max_flow,
            'max_flow_reported': self.max_flow_reported,
            'max_flow_user': self._max_flow_user,
            'max_flow_is_fallback': self.max_flow_is_fallback,
            'emission_point_id': self.emission_point_id,
            'lat': self.lat,
            'lon': self.lon,
            'last_reading': self.last_reading,
            'fail_count': self.fail_count,
        }
