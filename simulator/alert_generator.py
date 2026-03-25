"""
alert_generator.py — Evaluate readings against clinical thresholds and emit alerts.

AlertGenerator scans a day's readings for threshold breaches and produces
alert dicts in the canonical output schema. Multiple consecutive breaches of
the same metric produce one alert (the worst single reading is used as the
representative value).
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Optional

from simulator.patient_loader import Patient

logger = logging.getLogger(__name__)


# ── Threshold definitions ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class Threshold:
    metric_code: str
    severity: str          # "warning" | "critical"
    direction: str         # "above" | "below"
    value: float
    description: str


# Order matters: thresholds are evaluated in order; the most severe match wins.
THRESHOLDS: list[Threshold] = [
    # Heart rate — critical
    Threshold("heart_rate", "critical", "above", 150.0, "Severe tachycardia"),
    Threshold("heart_rate", "critical", "below",  40.0, "Severe bradycardia"),
    # Heart rate — warning
    Threshold("heart_rate", "warning",  "above", 120.0, "Tachycardia"),
    Threshold("heart_rate", "warning",  "below",  50.0, "Bradycardia"),
    # SpO2 — critical
    Threshold("spo2",        "critical", "below",  90.0, "Severe hypoxemia"),
    # SpO2 — warning
    Threshold("spo2",        "warning",  "below",  93.0, "Hypoxemia"),
    # Blood glucose — critical (hypoglycemia)
    Threshold("blood_glucose", "critical", "below",  70.0, "Hypoglycemia"),
    # Blood glucose — warning (hyperglycemia)
    Threshold("blood_glucose", "warning",  "above", 180.0, "Hyperglycemia"),
]

# Group thresholds by metric code for fast lookup
_THRESHOLDS_BY_METRIC: dict[str, list[Threshold]] = {}
for _t in THRESHOLDS:
    _THRESHOLDS_BY_METRIC.setdefault(_t.metric_code, []).append(_t)


class AlertGenerator:
    """Evaluates a list of reading dicts and returns alert dicts.

    Design choice: one alert per (metric, severity) breach-window rather than
    one per reading. This avoids flooding the alert table with thousands of
    identical warnings during a sustained tachycardia episode, while still
    capturing the worst value observed and the first trigger time.
    """

    def generate_alerts(
        self,
        readings: list[dict],
        patient: Patient,
    ) -> list[dict]:
        """Evaluate *readings* and return a list of alert dicts.

        Args:
            readings: Output of WearableGenerator.generate_day — list of reading dicts.
            patient:  The patient these readings belong to.

        Returns:
            List of alert dicts (may be empty).
        """
        alerts: list[dict] = []

        # Group readings by metric code for efficient per-metric processing
        by_metric: dict[str, list[dict]] = {}
        for r in readings:
            by_metric.setdefault(r["metric_code"], []).append(r)

        for metric_code, metric_readings in by_metric.items():
            thresholds = _THRESHOLDS_BY_METRIC.get(metric_code, [])
            if not thresholds:
                continue

            metric_alerts = self._evaluate_metric(
                metric_code, thresholds, metric_readings, patient
            )
            alerts.extend(metric_alerts)

        logger.debug(
            "Generated %d alerts for patient %s", len(alerts), patient.patient_id
        )
        return alerts

    # ── Private helpers ───────────────────────────────────────────────────────

    def _evaluate_metric(
        self,
        metric_code: str,
        thresholds: list[Threshold],
        readings: list[dict],
        patient: Patient,
    ) -> list[dict]:
        """Emit one alert per (direction, severity) breach window.

        A "window" is a contiguous run of readings that all breach the same
        threshold. We report the first breach time and the worst (most extreme)
        value within the window.
        """
        alerts: list[dict] = []

        # Track open windows: key = (severity, direction) → worst reading so far
        open_windows: dict[tuple[str, str], dict] = {}

        for reading in sorted(readings, key=lambda r: r["measured_at"]):
            value = reading["value"]
            matching = self._match_thresholds(thresholds, value)

            for threshold in matching:
                key = (threshold.severity, threshold.direction)
                if key not in open_windows:
                    # Open a new breach window
                    open_windows[key] = {
                        "threshold": threshold,
                        "first_reading": reading,
                        "worst_reading": reading,
                        "worst_value": value,
                    }
                else:
                    # Update worst reading in the existing window
                    current = open_windows[key]
                    if self._is_worse(value, current["worst_value"], threshold):
                        open_windows[key]["worst_reading"] = reading
                        open_windows[key]["worst_value"] = value

            # Close windows for thresholds no longer breaching
            for key in list(open_windows.keys()):
                threshold = open_windows[key]["threshold"]
                still_breaching = any(
                    t.severity == threshold.severity and t.direction == threshold.direction
                    for t in self._match_thresholds(thresholds, value)
                )
                if not still_breaching:
                    alert = self._build_alert(open_windows.pop(key), patient)
                    alerts.append(alert)

        # Flush any still-open windows at end of day
        for window in open_windows.values():
            alerts.append(self._build_alert(window, patient))

        return alerts

    def _match_thresholds(
        self, thresholds: list[Threshold], value: float
    ) -> list[Threshold]:
        """Return all thresholds breached by *value*, highest severity first."""
        matched: list[Threshold] = []
        for t in thresholds:
            if t.direction == "above" and value > t.value:
                matched.append(t)
            elif t.direction == "below" and value < t.value:
                matched.append(t)
        # Sort: critical before warning
        return sorted(matched, key=lambda t: (0 if t.severity == "critical" else 1))

    def _is_worse(
        self, candidate: float, current_worst: float, threshold: Threshold
    ) -> bool:
        """Return True if *candidate* is a more extreme breach than *current_worst*."""
        if threshold.direction == "above":
            return candidate > current_worst
        return candidate < current_worst

    def _build_alert(self, window: dict, patient: Patient) -> dict:
        """Construct the canonical alert dict from a closed breach window."""
        threshold: Threshold = window["threshold"]
        first_reading: dict = window["first_reading"]
        worst_reading: dict = window["worst_reading"]

        device_serial = first_reading.get("device_serial", "")

        return {
            "alert_id": str(uuid.uuid4()),
            "patient_id": patient.patient_id,
            "device_serial": device_serial,
            "alert_type": "threshold_breach",
            "severity": threshold.severity,
            "metric_code": threshold.metric_code,
            "threshold_value": threshold.value,
            "actual_value": round(float(window["worst_value"]), 2),
            "triggered_at": first_reading["measured_at"],
            "reading_id": worst_reading["reading_id"],
        }
