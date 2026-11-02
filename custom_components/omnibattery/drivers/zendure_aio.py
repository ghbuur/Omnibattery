"""Zendure AIO 2400 local MQTT driver for Omnibattery.

Legacy SolarFlow AIO protocol (not zenSDK HTTP):
  - Subscribe:  /{productKey}/{deviceId}/#  and  iot/{productKey}/{deviceId}/#
  - Publish:    iot/{productKey}/{deviceId}/properties/read
                iot/{productKey}/{deviceId}/function/invoke
  - Control:    properties/write  outputLimit  (while autoModel == 0)
                deviceAutomation autoModel=0 is only used to leave a stale
                automation mode — autoModel=8 makes this unit idle.

Uses Home Assistant's MQTT integration (same broker as Mosquitto) —
no separate broker credentials.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import Any, Callable, Optional

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant, callback

from .base import (
    BatteryDriver,
    DriverCapabilities,
    ReadGroup,
    SetpointResult,
    TelemetrySnapshot,
)

_LOGGER = logging.getLogger(__name__)

# Control path: properties/write outputLimit while autoModel == 0.
#
# deviceAutomation (autoModel=8) is accepted by the AIO 2400 (reply success=1)
# but puts it into IDLE — verified on-device: after autoModel 0 → 8 the unit
# reported outputLimit=0, packState=0, remainOutTime=13536 and stopped feeding
# the house. Writing outputLimit in manual mode (autoModel=0) is the path that
# demonstrably tracks the setpoint.
#
# outputLimit persists once written, so the re-assert is only a safety net
# against the device forgetting it — not a short keep-alive.
_KEEP_ALIVE_S = 120.0
_SETPOINT_TOL_W = 15
_POLL_MIN_INTERVAL_S = 5.0

# Wire property → logical key
_PROP_MAP: dict[str, str] = {
    "electricLevel": "battery_soc",
    "solarInputPower": "solar_power",
    "outputPackPower": "output_pack_power",
    "packInputPower": "pack_input_power",
    "outputHomePower": "output_home_power",
    "invOutputPower": "inv_output_power",
    "outputInversePower": "output_inverse_power",
    "outputLimit": "output_limit",
    "inputLimit": "input_limit",
    "socSet": "soc_set",
    "minSoc": "min_soc",
    "inverseMaxPower": "inverse_max_power",
    "autoModel": "auto_model",
    "remainOutTime": "remain_discharge_time",
    "solarPower1": "mppt1_power",
    "solarPower2": "mppt2_power",
    "packNum": "pack_count",
    "hubState": "hub_state",
    "packState": "pack_state",
    "masterSwitch": "master_switch",
}

# SOC keys reported as deci-percent on legacy AIO (100 → 10 %)
_SOC_DECI_KEYS = frozenset({"battery_soc", "soc_set", "min_soc"})

# Per-pack cell data arrives in the "packData" array alongside "properties".
# All four values are reported in hundredths (3094 → 30.94 °C, 341 → 3.41 V).
# Fields appear individually — a packData entry may carry only "totalVol" — so
# each key is handled on its own and missing ones leave the cache untouched.
_PACK_MAP: dict[str, tuple[str, str]] = {
    # wire key: (logical key, aggregation across packs)
    "maxTemp": ("internal_temperature", "max"),
    "totalVol": ("battery_voltage", "max"),
    "maxVol": ("max_cell_voltage", "max"),
    "minVol": ("min_cell_voltage", "min"),
}
_PACK_SCALE = 0.01

SENSOR_DEFINITIONS: list[dict] = [
    {"key": "battery_soc", "name": "Battery SOC", "unit": "%",
     "device_class": "battery", "state_class": "measurement", "scale": 1, "precision": 0,
     "scan_interval": "medium", "enabled_by_default": True},
    {"key": "solar_power", "name": "Solar Power", "unit": "W",
     "device_class": "power", "state_class": "measurement", "scale": 1, "precision": 0,
     "icon": "mdi:solar-power", "scan_interval": "high", "enabled_by_default": True},
    {"key": "battery_power", "name": "Battery Power", "unit": "W",
     "device_class": "power", "state_class": "measurement", "scale": 1, "precision": 0,
     "scan_interval": "high", "enabled_by_default": True},
    {"key": "output_home_power", "name": "Output Home Power", "unit": "W",
     "device_class": "power", "state_class": "measurement", "scale": 1, "precision": 0,
     "scan_interval": "high", "enabled_by_default": True},
    {"key": "output_limit", "name": "Output Limit", "unit": "W",
     "device_class": "power", "state_class": "measurement", "scale": 1, "precision": 0,
     "scan_interval": "medium", "enabled_by_default": False},
    {"key": "auto_model", "name": "Auto Model", "unit": None,
     "device_class": None, "state_class": None, "scale": 1, "precision": 0,
     "scan_interval": "high", "enabled_by_default": False},
    {"key": "remain_discharge_time", "name": "Remaining Discharge Time", "unit": "min",
     "device_class": "duration", "state_class": "measurement", "scale": 1, "precision": 0,
     "scan_interval": "medium", "enabled_by_default": False},
    {"key": "mppt1_power", "name": "MPPT1 Power", "unit": "W",
     "device_class": "power", "state_class": "measurement", "scale": 1, "precision": 0,
     "scan_interval": "high", "enabled_by_default": True},
    {"key": "mppt2_power", "name": "MPPT2 Power", "unit": "W",
     "device_class": "power", "state_class": "measurement", "scale": 1, "precision": 0,
     "scan_interval": "high", "enabled_by_default": True},
    {"key": "pack_count", "name": "Battery Pack Count", "unit": None,
     "device_class": None, "state_class": None, "scale": 1, "precision": 0,
     "scan_interval": "low", "enabled_by_default": False},
    # --- Cell data, decoded from the packData array (see _ingest_pack_data) ---
    {"key": "battery_voltage", "name": "Battery Voltage", "unit": "V",
     "device_class": "voltage", "state_class": "measurement", "scale": 1, "precision": 2,
     "scan_interval": "medium", "enabled_by_default": True},
    {"key": "internal_temperature", "name": "Internal Temperature", "unit": "°C",
     "device_class": "temperature", "state_class": "measurement", "scale": 1, "precision": 1,
     "scan_interval": "medium", "enabled_by_default": True},
    {"key": "max_cell_voltage", "name": "Max Cell Voltage", "unit": "V",
     "device_class": "voltage", "state_class": "measurement", "scale": 1, "precision": 3,
     "scan_interval": "medium", "enabled_by_default": True},
    {"key": "min_cell_voltage", "name": "Min Cell Voltage", "unit": "V",
     "device_class": "voltage", "state_class": "measurement", "scale": 1, "precision": 3,
     "scan_interval": "medium", "enabled_by_default": True},
]

NUMBER_DEFINITIONS: list[dict] = [
    {"key": "soc_set", "name": "Target SOC", "unit": "%",
     "device_class": "battery", "min": 70, "max": 100, "step": 1,
     "scale": 1, "precision": 0, "scan_interval": "low", "enabled_by_default": True},
    {"key": "min_soc", "name": "Minimum SOC", "unit": "%",
     "device_class": "battery", "min": 5, "max": 50, "step": 1,
     "scale": 1, "precision": 0, "scan_interval": "low", "enabled_by_default": True},
]


class ZendureAioDriver(BatteryDriver):
    """Local MQTT driver for Zendure AIO 2400 (legacy deviceAutomation)."""

    def __init__(
        self,
        hass: HomeAssistant,
        product_key: str,
        device_id: str,
        *,
        max_charge_power_w: int = 0,
        max_discharge_power_w: int = 800,
    ) -> None:
        self.hass = hass
        self.product_key = product_key
        self.device_id = device_id
        self._max_charge_w = max(0, int(max_charge_power_w))
        self._max_discharge_w = max(100, int(max_discharge_power_w))
        self._capabilities = self._make_capabilities()
        self._cache: dict[str, Any] = {}
        self._connected = False
        self._shutting_down = False
        self._unsubscribers: list[Callable[[], Any]] = []
        self._write_lock = asyncio.Lock()
        self._last_commanded: int = 0
        self._last_publish_mono: float = 0.0
        self._last_poll_mono: float = 0.0
        self._msg_id = 1
        self._product: Optional[str] = "solarFlowAIO"

        self._definitions: dict[str, list[dict]] = {
            "sensor": SENSOR_DEFINITIONS,
            "number": NUMBER_DEFINITIONS,
            "select": [],
            "switch": [],
            "binary_sensor": [],
            "button": [],
            "all": SENSOR_DEFINITIONS + NUMBER_DEFINITIONS,
        }
        self._read_groups = [
            ReadGroup(
                scan_interval="high",
                keys=tuple(d["key"] for d in self._definitions["all"])
                + ("battery_power", "output_home_power", "auto_model"),
            )
        ]

    # --- entity definitions ---
    @property
    def sensor_definitions(self) -> list[dict]:
        return self._definitions["sensor"]

    @property
    def number_definitions(self) -> list[dict]:
        return self._definitions["number"]

    @property
    def select_definitions(self) -> list[dict]:
        return self._definitions["select"]

    @property
    def switch_definitions(self) -> list[dict]:
        return self._definitions["switch"]

    @property
    def binary_sensor_definitions(self) -> list[dict]:
        return self._definitions["binary_sensor"]

    @property
    def button_definitions(self) -> list[dict]:
        return self._definitions["button"]

    @property
    def all_definitions(self) -> list[dict]:
        return self._definitions["all"]

    @property
    def control_dependency_keys(self) -> frozenset[str]:
        return frozenset({
            "battery_soc",
            "battery_power",
            "auto_model",
            "min_soc",
            "soc_set",
            "output_limit",
            "commanded_net_power",
        })

    def net_power_from_data(self, data: dict) -> Optional[int]:
        """Signed net power for skip-if-unchanged (+charge / -discharge).

        Always prefer the last command we published so Omnibattery does not
        re-issue every cycle (AIO drops automation if flooded).
        """
        if self._last_commanded is not None and self._last_publish_mono > 0:
            return int(self._last_commanded)
        commanded = data.get("commanded_net_power")
        if commanded is not None:
            try:
                return int(commanded)
            except (TypeError, ValueError):
                pass
        # Nothing commanded yet: in manual mode (autoModel 0) the device runs on a
        # pre-existing outputLimit, so report what it is actually doing rather than
        # assuming idle.
        am = data.get("auto_model")
        try:
            am_i = int(am) if am is not None else None
        except (TypeError, ValueError):
            am_i = None
        if am_i not in (None, 0):
            # Automation mode — the AIO idles there; nothing meaningful to report.
            return None
        lim = data.get("output_limit")
        if isinstance(lim, (int, float)) and lim > 0:
            return -int(lim)
        bp = data.get("battery_power")
        if isinstance(bp, (int, float)):
            return int(bp)
        return 0


    def _make_capabilities(self) -> DriverCapabilities:
        """Build capabilities using only fields present in this Omnibattery version."""
        kwargs = dict(
            hardware_soc_cutoff=True,
            has_force_mode=False,
            push_telemetry=True,
            max_charge_power_w=self._max_charge_w,
            max_discharge_power_w=self._max_discharge_w,
            # NOTE: must stay False. sensor.py adds SOLAR_POWER_SENSOR_DEFINITIONS
            # when has_mppt_pv is True, which declares key "solar_power" — the same
            # key this driver already publishes from solarInputPower. Both build
            # unique_id as f"{device_key}_{key}", so HA drops one entity with
            # "Platform omnibattery does not generate unique IDs". The AIO 2400 does
            # have two MPPT strings (mppt1_power / mppt2_power are reported), so a
            # proper fix upstream would be preferable to this workaround.
            has_mppt_pv=False,
            has_alarm_registers=False,
            has_rs485_control=False,
            has_energy_counters=False,
        )
        # Optional fields (newer Omnibattery); ignore if dataclass has no such field
        import dataclasses
        field_names = {f.name for f in dataclasses.fields(DriverCapabilities)}
        optional = {
            "has_daily_energy_counters": False,
            "has_nominal_capacity": False,
            "setpoint_confirm_reliable": False,
            "actuator_latency_s": 8.0,
        }
        for k, v in optional.items():
            if k in field_names:
                kwargs[k] = v
        return DriverCapabilities(**kwargs)

    # --- identity ---
    @property
    def capabilities(self) -> DriverCapabilities:
        return self._capabilities

    @property
    def model_label(self) -> Optional[str]:
        return self._product or "AIO 2400"

    @property
    def serial(self) -> Optional[str]:
        return self.device_id

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def read_groups(self) -> list[ReadGroup]:
        return self._read_groups

    def set_shutting_down(self, value: bool) -> None:
        self._shutting_down = value

    # --- topics ---
    def _topic_iot(self, *parts: str) -> str:
        return "/".join(("iot", self.product_key, self.device_id, *parts))

    def _topic_root(self, *parts: str) -> str:
        return "/" + "/".join((self.product_key, self.device_id, *parts))

    # --- connection ---
    async def connect(self) -> bool:
        # The coordinator's "fresh reconnection" recovery calls this on the live
        # object. Returning early on self._connected would make that a silent
        # no-op — no re-subscribe, no new getAll — and the recovery loop would
        # spin forever (observed: 242 attempts). Always rebuild instead.
        if self._connected:
            await self._unsubscribe_all()
            self._connected = False
        try:
            for pattern in (
                self._topic_root("#"),
                self._topic_iot("#"),
            ):
                unsub = await mqtt.async_subscribe(
                    self.hass, pattern, self._on_message, qos=0
                )
                self._unsubscribers.append(unsub)
            # Request full snapshot
            await mqtt.async_publish(
                self.hass,
                self._topic_iot("properties", "read"),
                self._read_all_payload(),
                qos=0,
                retain=False,
            )
            self._connected = True
            self._last_poll_mono = time.monotonic()

            # Wait for the first report before declaring success. Without this the
            # coordinator polls an empty cache, logs "all read attempts failed" and
            # marks the battery unavailable for the first few cycles.
            #
            # The AIO answers getAll as ~15 fragments at roughly 1/s.
            # Succeed as soon as ANY property is decoded so reconnect is not
            # blocked by a slow fragment sequence; continue collecting the rest.
            for _ in range(160):  # up to ~40 s
                await asyncio.sleep(0.25)
                if self._cache:
                    break
            else:
                _LOGGER.warning(
                    "Zendure AIO %s: no telemetry within 40s of subscribing — "
                    "check that the device points at this broker",
                    self.device_id,
                )

            _LOGGER.info(
                "Zendure AIO MQTT subscribed product=%s device=%s (cache=%d keys: %s)",
                self.product_key,
                self.device_id,
                len(self._cache),
                ", ".join(sorted(self._cache)) or "none",
            )
            return True
        except Exception as err:
            _LOGGER.error("Zendure AIO MQTT connect failed: %s", err)
            await self._unsubscribe_all()
            self._connected = False
            return False

    async def close(self) -> None:
        await self._unsubscribe_all()
        self._connected = False

    async def _unsubscribe_all(self) -> None:
        for unsub in self._unsubscribers:
            try:
                result = unsub()
                if hasattr(result, "__await__"):
                    await result
            except Exception:
                pass
        self._unsubscribers.clear()

    # --- MQTT message handling ---
    @callback
    def _on_message(self, message) -> None:
        try:
            payload = json.loads(message.payload)
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        if payload.get("product"):
            self._product = str(payload["product"])
        props = payload.get("properties")
        if isinstance(props, dict):
            if _LOGGER.isEnabledFor(logging.DEBUG):
                _LOGGER.debug(
                    "Zendure AIO rx %s: %s",
                    getattr(message, "topic", "?"),
                    ", ".join(sorted(props)) or "empty",
                )
            self._ingest_properties(props)
        packs = payload.get("packData")
        if isinstance(packs, list) and packs:
            self._ingest_pack_data(packs)

    def _ingest_pack_data(self, packs: list) -> None:
        """Decode cell voltages and temperature from the packData array.

        Values are hundredths of a volt / degree. Entries are partial — a report
        may carry only {"totalVol": ..., "sn": ...} — so each wire key is
        aggregated only over the packs that actually contain it, and keys absent
        from this message leave the cached value alone.
        """
        for wire, (logical, mode) in _PACK_MAP.items():
            values = [
                p[wire]
                for p in packs
                if isinstance(p, dict)
                and isinstance(p.get(wire), (int, float))
            ]
            if not values:
                continue
            raw = min(values) if mode == "min" else max(values)
            self._cache[logical] = round(raw * _PACK_SCALE, 3)

    def _ingest_properties(self, props: dict) -> None:
        for wire, logical in _PROP_MAP.items():
            if wire not in props:
                continue
            val = props[wire]
            if not isinstance(val, (int, float)):
                continue
            if logical in _SOC_DECI_KEYS and val > 100:
                val = int(val) // 10
            elif logical in _SOC_DECI_KEYS and logical == "min_soc" and val >= 100:
                # minSoc often reported as 100 meaning 10 %
                val = int(val) // 10
            self._cache[logical] = int(val) if logical != "battery_soc" else int(val)

        # Signed battery power: +charge / -discharge (Omnibattery convention).
        # Only recompute when this message carries a power field — pure SOC
        # reports must not zero out the last known power.
        power_wires = (
            "outputHomePower",
            "packInputPower",
            "outputPackPower",
            "invOutputPower",
            "outputInversePower",
            "solarInputPower",
        )
        if any(w in props for w in power_wires):
            home = int(self._cache.get("output_home_power") or 0)
            pack_out = int(self._cache.get("output_pack_power") or 0)
            pack_in = int(self._cache.get("pack_input_power") or 0)
            inv_out = int(self._cache.get("inv_output_power") or 0)
            if home > 0:
                bp = -home
            elif inv_out > 0:
                bp = -inv_out
            elif pack_in > 0 and pack_out == 0:
                bp = -pack_in
            elif pack_out > 0 and pack_in == 0:
                bp = pack_out
            else:
                bp = pack_out - pack_in
            self._cache["battery_power"] = bp
            # Some control paths also look at ac_power
            self._cache["ac_power"] = bp
            _LOGGER.debug(
                "Zendure AIO power home=%s pack_in=%s pack_out=%s → battery_power=%s",
                home,
                pack_in,
                pack_out,
                bp,
            )

        inv = self._cache.get("inverse_max_power")
        if isinstance(inv, (int, float)) and inv > 0:
            self._max_discharge_w = int(inv)
            self._capabilities = self._make_capabilities()

    # --- telemetry ---
    async def read_telemetry(self, keys: Optional[list[str]] = None) -> TelemetrySnapshot:
        """Return cache; occasionally request getAll so idle telemetry stays fresh.

        The AIO pushes reports when values change, but when idle it can go quiet.
        A rate-limited getAll (with full deviceId/messageId/timestamp) keeps the
        stream alive without flooding the broker. No long sleep — coordinator
        must not block; the next poll cycle picks up the reply.
        """
        now = time.monotonic()
        if self._connected and (now - self._last_poll_mono) >= _POLL_MIN_INTERVAL_S:
            self._last_poll_mono = now
            try:
                await mqtt.async_publish(
                    self.hass,
                    self._topic_iot("properties", "read"),
                    self._read_all_payload(),
                    qos=0,
                    retain=False,
                )
            except Exception as err:
                if not self._shutting_down:
                    _LOGGER.debug("Zendure AIO poll failed: %s", err)

        data = dict(self._cache)
        if keys is None:
            return data
        return {k: data[k] for k in keys if k in data}

    # --- control ---
    def _clamp(self, net_power_w: int) -> int:
        return max(-self._max_discharge_w, min(self._max_charge_w, int(round(net_power_w))))

    def _read_all_payload(self) -> str:
        """Full getAll request — AIO may ignore bare {properties:[getAll]}."""
        self._msg_id += 1
        return json.dumps(
            {
                "properties": ["getAll"],
                "messageId": self._msg_id,
                "deviceId": self.device_id,
                "timestamp": int(time.time()),
            }
        )

    def _write_props_payload(self, props: dict) -> str:
        """properties/write with the full envelope the AIO replies to."""
        self._msg_id += 1
        return json.dumps(
            {
                "properties": props,
                "messageId": self._msg_id,
                "deviceId": self.device_id,
                "timestamp": int(time.time()),
            }
        )

    def _automation_payload(self, out_power: int, auto_model: int) -> str:
        self._msg_id += 1
        return json.dumps(
            {
                "arguments": [
                    {
                        "autoModelProgram": 2 if auto_model == 8 else 0,
                        "autoModelValue": {
                            "chargingType": 0,
                            "chargingPower": 0,
                            "freq": 0,
                            "outPower": abs(int(out_power)),
                        },
                        "msgType": 1,
                        "autoModel": auto_model,
                    }
                ],
                "function": "deviceAutomation",
                "messageId": self._msg_id,
                "deviceId": self.device_id,
                "timestamp": int(time.time()),
            }
        )

    async def apply_setpoint(
        self,
        net_power_w: int,
        *,
        mode_hint: Optional[str] = None,
        read_back: bool = True,
    ) -> SetpointResult:
        if not self._connected:
            return SetpointResult(False, 0, False, failure_reason="not_connected")

        applied = self._clamp(net_power_w)
        # AIO: no AC charge path in this driver → only discharge or idle
        if applied > 0:
            applied = 0

        # Manual mode only — see _KEEP_ALIVE_S comment for why autoModel 8 is unusable.
        auto_model = 0
        out_power = abs(applied)
        now = time.monotonic()

        changed = abs(applied - self._last_commanded) > _SETPOINT_TOL_W
        # Force publish when leaving a non-zero command (idle)
        force_idle = applied == 0 and self._last_commanded != 0
        # Only re-assert a *non-zero* setpoint. An idle keep-alive would publish
        # net=0 every _KEEP_ALIVE_S, and Omnibattery writes that echo back into the
        # config entry — resetting manual_force_mode to "None" a few ms later and
        # making every UI change appear to snap back. A 0 W command needs no
        # refresh: the device is already standing still.
        due_keepalive = (
            applied != 0
            and self._last_publish_mono > 0
            and (now - self._last_publish_mono) >= _KEEP_ALIVE_S
        )
        first = self._last_publish_mono == 0.0
        # The device silently ignores outputLimit while in automation mode, so a
        # stale autoModel must be cleared even if the setpoint itself is unchanged.
        needs_manual = self._cache.get("auto_model") not in (None, 0)
        should_publish = changed or force_idle or due_keepalive or first or needs_manual

        if should_publish:
            try:
                async with self._write_lock:
                    if needs_manual:
                        # Leave deviceAutomation before writing outputLimit.
                        await mqtt.async_publish(
                            self.hass,
                            self._topic_iot("function", "invoke"),
                            self._automation_payload(0, 0),
                            qos=0,
                            retain=False,
                        )
                        await asyncio.sleep(0.3)

                    await mqtt.async_publish(
                        self.hass,
                        self._topic_iot("properties", "write"),
                        self._write_props_payload({"outputLimit": out_power}),
                        qos=0,
                        retain=False,
                    )
                    self._last_commanded = applied
                    self._last_publish_mono = now
                    self._cache["commanded_net_power"] = applied
                    _LOGGER.debug(
                        "Zendure AIO outputLimit=%sW (net=%sW) manual_reset=%s "
                        "keepalive=%s changed=%s",
                        out_power,
                        applied,
                        needs_manual,
                        due_keepalive,
                        changed,
                    )
            except Exception as err:
                if not self._shutting_down:
                    _LOGGER.warning("Zendure AIO apply_setpoint failed: %s", err)
                return SetpointResult(
                    False, applied, False, failure_reason="mqtt_publish_failed"
                )
        else:
            _LOGGER.debug(
                "Zendure AIO skip publish (net=%sW, last=%sW, age=%.1fs)",
                applied,
                self._last_commanded,
                now - self._last_publish_mono,
            )

        confirmed = False
        battery_power_w: Optional[int] = None
        if read_back and should_publish:
            # Do NOT sleep here: Omnibattery holds _control_lock during apply_setpoint.
            # A long await blocks Manual Mode off and freezes the control loop.
            # Confirm from whatever the push cache already has; next poll refines it.
            snap = dict(self._cache)
            bp = snap.get("battery_power")
            home = snap.get("output_home_power")
            lim = snap.get("output_limit")
            if isinstance(bp, (int, float)):
                battery_power_w = int(bp)

            # In manual mode the device echoes the written value back as
            # outputLimit — that is the real setpoint confirmation. The power
            # readings lag by a second or two and only corroborate it.
            if isinstance(lim, (int, float)) and abs(lim - out_power) <= _SETPOINT_TOL_W:
                confirmed = True
            elif applied == 0:
                if isinstance(bp, (int, float)) and abs(bp) < 30:
                    confirmed = True
            else:
                if isinstance(bp, (int, float)) and bp <= -20:
                    confirmed = True
                elif isinstance(home, (int, float)) and home >= 20:
                    confirmed = True
        elif not should_publish:
            # Unchanged command — optimistically confirmed
            confirmed = True
            bp = self._cache.get("battery_power")
            if isinstance(bp, (int, float)):
                battery_power_w = int(bp)

        return SetpointResult(
            ok=True,
            net_power_w=applied,
            confirmed=confirmed,
            battery_power_w=battery_power_w,
            applied={
                "auto_model": auto_model,
                "output_limit": out_power if applied < 0 else 0,
                "commanded_net_power": applied,
            },
        )


    async def write_control(self, key: str, value: int) -> bool:
        """Write min_soc / soc_set via properties/write (deci-percent)."""
        prop_map = {
            "min_soc": "minSoc",
            "soc_set": "socSet",
            "output_limit": "outputLimit",
        }
        wire = prop_map.get(key)
        if wire is None:
            return False
        val = int(value)
        if key in ("min_soc", "soc_set"):
            val = val * 10  # percent → deci-percent
        try:
            await mqtt.async_publish(
                self.hass,
                self._topic_iot("properties", "write"),
                json.dumps({"properties": {wire: val}}),
                qos=0,
                retain=False,
            )
            return True
        except Exception as err:
            _LOGGER.debug("write_control %s failed: %s", key, err)
            return False

    async def apply_config(
        self,
        *,
        max_soc_pct: float,
        min_soc_pct: float,
        max_charge_power_w: int,
        max_discharge_power_w: int,
    ) -> bool:
        ok1 = await self.write_control("soc_set", int(max_soc_pct))
        ok2 = await self.write_control("min_soc", int(min_soc_pct))
        return ok1 and ok2

    async def set_rs485_control(self, enabled: bool) -> bool:
        # No external-control toggle on this hardware; capabilities report
        # has_rs485_control=False so callers gate on that. Answered as a no-op
        # rather than omitted, so a coordinator path that skips the gate raises
        # nothing.
        return True

    async def get_rs485_control(self) -> Optional[bool]:
        return None

    async def set_charge_cutoff(self, soc_pct: float) -> bool:
        return await self.write_control("soc_set", int(soc_pct))

    async def standby(self) -> bool:
        result = await self.apply_setpoint(0, read_back=False)
        return result.ok

    @classmethod
    async def probe(
        cls,
        hass: HomeAssistant,
        product_key: str,
        device_id: str,
        timeout: float = 8.0,
        meta_timeout: float = 20.0,
    ) -> tuple[bool, dict]:
        """Subscribe briefly and wait for a properties/report.

        Two separate waits, because the AIO answers getAll as ~15 fragments over
        several seconds and the fields the caller needs are not in the first one:

          1. `reachable` — any properties payload or a success:1 ack. Proves the
             device is on this broker; this is what gates the config flow.
          2. `complete`  — inverseMaxPower has arrived. It sits late in the
             fragment sequence (measured ~6 s in), so returning as soon as the
             ack lands would leave meta empty and the caller would silently fall
             back to a hardcoded default — which then gets frozen into the config
             entry and survives reconfiguration.

        The second wait is best-effort: if it times out we still report success,
        just without the ceiling.
        """
        reachable = asyncio.Event()
        complete = asyncio.Event()
        meta: dict = {}

        @callback
        def _handler(message) -> None:
            try:
                p = json.loads(message.payload)
            except Exception:
                return
            if not isinstance(p, dict):
                return
            props = p.get("properties")
            if isinstance(props, dict) and props:
                meta["seen"] = True
                if "electricLevel" in props:
                    meta["soc"] = props["electricLevel"]
                if "inverseMaxPower" in props:
                    meta["inverseMaxPower"] = props["inverseMaxPower"]
                    complete.set()
                reachable.set()
            if p.get("success") == 1:
                reachable.set()

        unsubs = []
        try:
            root = f"/{product_key}/{device_id}/#"
            iot = f"iot/{product_key}/{device_id}/#"
            unsubs.append(await mqtt.async_subscribe(hass, root, _handler, qos=0))
            unsubs.append(await mqtt.async_subscribe(hass, iot, _handler, qos=0))
            await mqtt.async_publish(
                hass,
                f"iot/{product_key}/{device_id}/properties/read",
                json.dumps({
                    "properties": ["getAll"],
                    "messageId": 1,
                    "deviceId": device_id,
                    "timestamp": int(time.time()),
                }),
                qos=0,
            )
            await asyncio.wait_for(reachable.wait(), timeout)
            # Device is on the broker — that alone is enough to report success.
            # Now give the fragment sequence a chance to deliver the ceiling.
            if not complete.is_set():
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(complete.wait(), meta_timeout)
            return True, meta
        except Exception:
            return False, meta
        finally:
            for u in unsubs:
                try:
                    r = u()
                    if hasattr(r, "__await__"):
                        await r
                except Exception:
                    pass
