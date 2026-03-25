"""
wearable_generator.py — Generate realistic daily wearable readings.

The WearableGenerator is patient-aware, temporally realistic, and produces
fully deterministic output when given a fixed seed. All randomness flows
through numpy's Generator (new-style API, seeded per patient-day).

Key design principles:
  - Circadian rhythms: HR ~55-65 bpm overnight (00:00-06:00), 60-90 during day
  - Patient conditions affect baselines: hypertensives run 10-20 bpm higher,
    diabetics show post-meal glucose spikes
  - Anomalies are injected probabilistically: sustained tachycardia or
    gradual SpO2 drops, each with a realistic temporal signature
  - Blood glucose readings are only generated for diabetic patients
  - Seeding strategy: combine the global seed, patient index, and date to
    produce reproducible but independent per-patient-day randomness
"""

from __future__ import annotations

import datetime
import logging
import uuid
from typing import Optional

import numpy as np

from simulator.config import (
    SLEEP_STAGE_LABELS,
    SimConfig,
)
from simulator.patient_loader import Patient

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# Sleep window: patients go to sleep between 22:00 and 23:00, wake 06:00-07:00
SLEEP_START_H = 22
SLEEP_END_H = 6

# Activity labels and their typical step rates (steps/5-min interval)
_ACTIVITIES = ["resting", "walking", "running", "driving"]
_LOCATION_TYPES = ["home", "outdoor", "gym", "office"]


class WearableGenerator:
    """Generates a day's worth of readings for one patient.

    Args:
        config: SimConfig instance controlling seeds and anomaly probability.
    """

    def __init__(self, config: SimConfig) -> None:
        self._config = config

    # ── Public API ────────────────────────────────────────────────────────────

    def generate_day(
        self,
        patient: Patient,
        date: datetime.date,
        seed: int,
    ) -> list[dict]:
        """Generate all readings for *patient* on *date*.

        Returns:
            List of reading dicts in the canonical output schema.
        """
        rng = np.random.default_rng(seed)
        readings: list[dict] = []

        # Decide whether today is an anomaly day for this patient
        is_anomaly_day = rng.random() < self._config.anomaly_probability
        anomaly_type: Optional[str] = None
        if is_anomaly_day:
            anomaly_type = rng.choice(["tachycardia", "hypoxemia"])
            logger.debug(
                "Anomaly day for patient %s on %s: %s",
                patient.patient_id,
                date,
                anomaly_type,
            )

        # Anomaly window: 2-6 hour block, starting in daytime hours (08-20)
        anomaly_start_h: int = int(rng.integers(8, 18))
        anomaly_duration_h: float = float(rng.uniform(2.0, 6.0))

        # ── Heart rate + SpO2 (every 5 min) ──────────────────────────────────
        for minute in range(0, 1440, 5):  # 288 readings / day
            ts = _ts(date, minute)
            hour = minute // 60

            in_anomaly = (
                is_anomaly_day
                and anomaly_start_h <= hour < anomaly_start_h + anomaly_duration_h
            )

            hr = self._sample_heart_rate(rng, patient, hour, in_anomaly, anomaly_type)
            readings.append(
                self._build_reading(
                    patient=patient,
                    metric_code="heart_rate",
                    value=round(float(hr), 1),
                    unit="bpm",
                    measured_at=ts,
                    rng=rng,
                )
            )

            spo2 = self._sample_spo2(rng, patient, hour, in_anomaly, anomaly_type)
            readings.append(
                self._build_reading(
                    patient=patient,
                    metric_code="spo2",
                    value=round(float(spo2), 1),
                    unit="%",
                    measured_at=ts,
                    rng=rng,
                )
            )

        # ── Steps (hourly) ────────────────────────────────────────────────────
        for hour in range(24):
            ts = _ts(date, hour * 60)
            steps = self._sample_steps(rng, patient, hour, date)
            readings.append(
                self._build_reading(
                    patient=patient,
                    metric_code="steps",
                    value=int(steps),
                    unit="count",
                    measured_at=ts,
                    rng=rng,
                )
            )

        # ── Skin temperature (every 15 min) ───────────────────────────────────
        for minute in range(0, 1440, 15):  # 96 readings / day
            ts = _ts(date, minute)
            temp = self._sample_skin_temp(rng, patient, minute // 60)
            readings.append(
                self._build_reading(
                    patient=patient,
                    metric_code="skin_temperature",
                    value=round(float(temp), 2),
                    unit="°C",
                    measured_at=ts,
                    rng=rng,
                )
            )

        # ── Blood glucose (diabetic patients only, every 15 min) ──────────────
        if patient.has_diabetes and patient.cgm_serial:
            for minute in range(0, 1440, 15):
                ts = _ts(date, minute)
                glucose = self._sample_glucose(rng, patient, minute)
                readings.append(
                    self._build_reading(
                        patient=patient,
                        metric_code="blood_glucose",
                        value=round(float(glucose), 1),
                        unit="mg/dL",
                        measured_at=ts,
                        rng=rng,
                        use_cgm=True,
                    )
                )

        # ── Sleep stage (30s epochs during sleep hours) ───────────────────────
        # Sleep window: 22:00 previous day equivalently → we model sleep from
        # SLEEP_START_H:00 to the end of day, plus the start of day through SLEEP_END_H.
        sleep_minutes: list[int] = list(range(0, SLEEP_END_H * 60, 1))  # 00:00-06:00
        sleep_minutes += list(range(SLEEP_START_H * 60, 1440, 1))       # 22:00-00:00

        for minute in sleep_minutes:
            # 30-second epochs → two per minute, but we emit one per minute to
            # keep the output manageable while still representing sleep stages
            ts = _ts(date, minute)
            stage = self._sample_sleep_stage(rng, minute)
            stage_label = SLEEP_STAGE_LABELS[int(stage)]
            readings.append(
                self._build_reading(
                    patient=patient,
                    metric_code="sleep_stage",
                    value=int(stage),
                    unit="stage",
                    measured_at=ts,
                    rng=rng,
                    extra_context={"stage_label": stage_label},
                )
            )

        return readings

    # ── Sampling functions ────────────────────────────────────────────────────

    def _sample_heart_rate(
        self,
        rng: np.random.Generator,
        patient: Patient,
        hour: int,
        in_anomaly: bool,
        anomaly_type: Optional[str],
    ) -> float:
        """Return a single HR sample, honouring circadian rhythm and conditions."""

        # Circadian baseline
        if 0 <= hour < 6:
            # Deep sleep / overnight — lowest HR
            base = rng.normal(loc=58.0, scale=4.0)
        elif 6 <= hour < 8:
            # Wake-up ramp
            base = rng.normal(loc=65.0, scale=5.0)
        elif 8 <= hour < 22:
            # Active daytime
            base = rng.normal(loc=74.0, scale=8.0)
        else:
            # Wind-down evening
            base = rng.normal(loc=68.0, scale=6.0)

        # Condition-based offset
        if patient.has_hypertension:
            base += rng.uniform(8.0, 15.0)
        if patient.has_cardiac_condition:
            base += rng.uniform(-5.0, 10.0)

        # Anomaly injection
        if in_anomaly and anomaly_type == "tachycardia":
            base = rng.uniform(110.0, 150.0)

        return float(np.clip(base, 30.0, 220.0))

    def _sample_spo2(
        self,
        rng: np.random.Generator,
        patient: Patient,
        hour: int,
        in_anomaly: bool,
        anomaly_type: Optional[str],
    ) -> float:
        """Return a single SpO2 sample."""
        base = rng.normal(loc=97.5, scale=0.8)

        # Mild overnight dip (normal physiology)
        if 0 <= hour < 6:
            base -= rng.uniform(0.5, 1.5)

        # Cardiac patients have slightly lower baseline
        if patient.has_cardiac_condition:
            base -= rng.uniform(0.5, 1.5)

        # Anomaly: gradual SpO2 drop
        if in_anomaly and anomaly_type == "hypoxemia":
            base = rng.uniform(88.0, 93.0)

        return float(np.clip(base, 70.0, 100.0))

    def _sample_steps(
        self,
        rng: np.random.Generator,
        patient: Patient,
        hour: int,
        date: datetime.date,
    ) -> int:
        """Return hourly step count, applying time-of-day and weekday patterns."""
        weekday = date.weekday()  # 0=Monday, 6=Sunday
        is_weekend = weekday >= 5

        # No steps during sleep hours
        if hour < 6 or hour >= 22:
            return 0

        # Peak activity hours: 08-10, 12-13, 17-19
        if hour in (8, 9, 12, 17, 18):
            mean_steps = 800.0 if not is_weekend else 600.0
        elif hour in (10, 11, 13, 14, 15, 16, 19, 20, 21):
            mean_steps = 400.0 if not is_weekend else 350.0
        else:
            mean_steps = 100.0

        # Cardiac patients are slightly less active
        if patient.has_cardiac_condition:
            mean_steps *= 0.7

        steps = rng.normal(loc=mean_steps, scale=mean_steps * 0.3)
        return max(0, int(round(steps)))

    def _sample_skin_temp(
        self,
        rng: np.random.Generator,
        patient: Patient,
        hour: int,
    ) -> float:
        """Return skin temperature. Slightly lower during sleep, higher at peak activity."""
        if 0 <= hour < 6:
            base = rng.normal(loc=34.2, scale=0.3)
        elif 14 <= hour < 18:
            # Afternoon peak
            base = rng.normal(loc=35.5, scale=0.4)
        else:
            base = rng.normal(loc=34.8, scale=0.4)

        return float(np.clip(base, 30.0, 40.0))

    def _sample_glucose(
        self,
        rng: np.random.Generator,
        patient: Patient,
        minute: int,
    ) -> float:
        """Return blood glucose for a diabetic patient.

        Post-meal spikes occur ~30 minutes after typical meal times:
          - Breakfast: 07:30 → spike window 08:00-09:30
          - Lunch:     12:30 → spike window 13:00-14:30
          - Dinner:    18:30 → spike window 19:00-20:30
        """
        hour = minute // 60

        # Fasting baseline
        if 0 <= hour < 7:
            base = rng.normal(loc=105.0, scale=10.0)
        # Post-breakfast spike
        elif 8 <= hour < 10:
            base = rng.normal(loc=165.0, scale=20.0)
        # Pre-lunch dip
        elif 10 <= hour < 13:
            base = rng.normal(loc=120.0, scale=12.0)
        # Post-lunch spike
        elif 13 <= hour < 15:
            base = rng.normal(loc=160.0, scale=18.0)
        # Mid-afternoon
        elif 15 <= hour < 19:
            base = rng.normal(loc=130.0, scale=15.0)
        # Post-dinner spike
        elif 19 <= hour < 21:
            base = rng.normal(loc=170.0, scale=25.0)
        # Evening descent
        else:
            base = rng.normal(loc=115.0, scale=12.0)

        return float(np.clip(base, 40.0, 400.0))

    def _sample_sleep_stage(
        self,
        rng: np.random.Generator,
        minute: int,
    ) -> int:
        """Return a sleep stage integer (0=awake, 1=light, 2=deep, 3=rem).

        Approximate 90-minute ultradian cycle: light → deep → REM → repeat.
        """
        hour = minute // 60

        # During sleep hours, cycle through stages
        if 0 <= hour < 6 or hour >= SLEEP_START_H:
            cycle_minute = minute % 90  # 90-minute sleep cycle
            if cycle_minute < 15:
                # Light sleep / transition
                stage = int(rng.choice([1, 0], p=[0.85, 0.15]))
            elif cycle_minute < 45:
                # Deep sleep
                stage = int(rng.choice([2, 1], p=[0.70, 0.30]))
            elif cycle_minute < 75:
                # REM
                stage = int(rng.choice([3, 1], p=[0.65, 0.35]))
            else:
                # Brief arousal / light
                stage = int(rng.choice([1, 0], p=[0.80, 0.20]))
        else:
            stage = 0  # awake

        return stage

    # ── Reading builder ───────────────────────────────────────────────────────

    def _build_reading(
        self,
        patient: Patient,
        metric_code: str,
        value: float | int,
        unit: str,
        measured_at: str,
        rng: np.random.Generator,
        use_cgm: bool = False,
        extra_context: Optional[dict] = None,
    ) -> dict:
        """Assemble the canonical reading dict."""

        device_serial = patient.cgm_serial if use_cgm else patient.device_serial

        quality_flag = self._quality_flag(rng)
        activity = self._activity(rng, metric_code, measured_at)
        location = self._location(rng, activity)

        context: dict = {
            "activity": activity,
            "location_type": location,
        }
        if extra_context:
            context.update(extra_context)

        return {
            "reading_id": str(uuid.uuid4()),
            "patient_id": patient.patient_id,
            "device_serial": device_serial,
            "metric_code": metric_code,
            "value": value,
            "unit": unit,
            "quality_flag": quality_flag,
            "measured_at": measured_at,
            "context": context,
        }

    # ── Context helpers ───────────────────────────────────────────────────────

    def _quality_flag(self, rng: np.random.Generator) -> str:
        """Randomly assign a quality flag. 95% good, rest noisy/interpolated."""
        roll = rng.random()
        if roll < 0.95:
            return "good"
        elif roll < 0.98:
            return "noisy"
        else:
            return "interpolated"

    def _activity(
        self, rng: np.random.Generator, metric_code: str, measured_at: str
    ) -> str:
        """Infer likely activity from time of day and metric."""
        hour = int(measured_at[11:13])
        if 0 <= hour < 6:
            return "sleeping"
        if metric_code == "steps":
            # Higher chance of walking/running when steps are being tracked
            return str(rng.choice(["walking", "resting", "running", "driving"], p=[0.4, 0.35, 0.1, 0.15]))
        return str(rng.choice(_ACTIVITIES, p=[0.45, 0.30, 0.10, 0.15]))

    def _location(self, rng: np.random.Generator, activity: str) -> str:
        """Infer location from activity."""
        if activity in ("sleeping", "resting"):
            return str(rng.choice(["home", "office"], p=[0.90, 0.10]))
        if activity == "running":
            return str(rng.choice(["outdoor", "gym"], p=[0.65, 0.35]))
        return str(rng.choice(_LOCATION_TYPES, p=[0.35, 0.30, 0.15, 0.20]))


# ── Utilities ─────────────────────────────────────────────────────────────────

def _ts(date: datetime.date, minute: int) -> str:
    """Return an ISO-8601 UTC timestamp string for *date* at *minute* minutes past midnight."""
    h = minute // 60
    m = minute % 60
    dt = datetime.datetime(date.year, date.month, date.day, h, m, 0, tzinfo=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_seed(base_seed: int, patient_idx: int, date: datetime.date) -> int:
    """Combine base seed, patient index, and date into a single integer seed.

    This ensures each patient-day gets independent, reproducible randomness
    while sharing the same top-level seed.
    """
    date_int = date.year * 10000 + date.month * 100 + date.day
    # XOR-based mixing — keeps the values within int64 range
    return (base_seed * 1_000_003 ^ patient_idx * 999_983 ^ date_int) & 0xFFFF_FFFF
