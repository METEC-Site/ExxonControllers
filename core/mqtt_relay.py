#!/usr/bin/env python3
"""
MQTT Data Relay
Publishes Alicat flow controller readings to an MQTT broker in real time.

Topic format:  {prefix}/{device_name}/{field}
Example:       ec/MFC_1/mass_flow  →  "5.32"
"""

import threading

try:
    import paho.mqtt.client as mqtt
    PAHO_AVAILABLE = True
except ImportError:
    PAHO_AVAILABLE = False

RELAY_FIELDS = ['pressure', 'temperature', 'vol_flow', 'mass_flow', 'setpoint', 'accumulated_sl']


class MqttRelay:
    """Thread-safe MQTT publisher for device readings."""

    def __init__(self):
        self._client = None
        self._connected = False
        self._host = ''
        self._port = 1883
        self._prefix = 'ec'
        self._lock = threading.Lock()

    @property
    def available(self) -> bool:
        return PAHO_AVAILABLE

    @property
    def is_connected(self) -> bool:
        return self._connected and self._client is not None

    def connect(self, host: str, port: int = 1883, topic_prefix: str = 'ec') -> dict:
        if not PAHO_AVAILABLE:
            return {'success': False, 'error': 'paho-mqtt not installed. Run: pip install paho-mqtt'}

        host = host.strip()
        if not host:
            return {'success': False, 'error': 'Broker host is required'}

        with self._lock:
            # Tear down any existing connection
            if self._client:
                try:
                    self._client.loop_stop()
                    self._client.disconnect()
                except Exception:
                    pass
                self._client = None
                self._connected = False

            try:
                # Support both paho-mqtt 1.x and 2.x
                try:
                    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
                except AttributeError:
                    client = mqtt.Client()

                client.on_connect    = self._on_connect
                client.on_disconnect = self._on_disconnect

                client.connect(host, int(port), keepalive=60)
                client.loop_start()

                self._client = client
                self._host   = host
                self._port   = int(port)
                self._prefix = topic_prefix.strip().rstrip('/') or 'ec'

                # Give the async connect a moment to complete
                import time
                time.sleep(0.6)

                return {'success': True, 'connected': self._connected}

            except Exception as e:
                self._client    = None
                self._connected = False
                return {'success': False, 'error': str(e)}

    def disconnect(self):
        with self._lock:
            if self._client:
                try:
                    self._client.loop_stop()
                    self._client.disconnect()
                except Exception:
                    pass
                self._client    = None
                self._connected = False

    def publish_reading(self, device_name: str, reading: dict):
        """Publish one device reading. Called from the poll loop — must be fast."""
        if not self._connected or not self._client:
            return
        safe_name = device_name.replace(' ', '_').replace('/', '-')
        prefix = f"{self._prefix}/{safe_name}"
        for field in RELAY_FIELDS:
            val = reading.get(field)
            if val is not None and val != '':
                try:
                    self._client.publish(f"{prefix}/{field}", str(val), qos=0, retain=False)
                except Exception:
                    pass

    def get_status(self) -> dict:
        return {
            'available': PAHO_AVAILABLE,
            'connected': self._connected,
            'host': self._host,
            'port': self._port,
            'prefix': self._prefix,
        }

    # ── Internal callbacks ────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        # reason_code is int in paho 1.x, ReasonCode object in paho 2.x
        try:
            rc = reason_code.value if hasattr(reason_code, 'value') else reason_code
        except Exception:
            rc = 1
        self._connected = (rc == 0)

    def _on_disconnect(self, client, userdata, flags=None, reason_code=None, properties=None):
        self._connected = False
