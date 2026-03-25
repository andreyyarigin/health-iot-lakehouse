"""
config.py — Simulation parameters and metric/device definitions.

All tuneable knobs for the wearable simulator live here. Import
SimConfig and the metric/device constants from this module.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


# ── Metric definitions ────────────────────────────────────────────────────────
# Each entry describes one health metric:
#   code          — canonical identifier used throughout the system
#   unit          — physical unit string
#   sample_period — seconds between readings (used to compute expected count/day)
#   normal_low    — lower bound of "normal" range
#   normal_high   — upper bound of "normal" range
#   description   — human-readable label

METRIC_DEFINITIONS: list[dict] = [
    {
        "code": "heart_rate",
        "unit": "bpm",
        "sample_period": 300,   # every 5 min
        "normal_low": 50.0,
        "normal_high": 100.0,
        "description": "Heart rate",
    },
    {
        "code": "spo2",
        "unit": "%",
        "sample_period": 300,   # every 5 min
        "normal_low": 95.0,
        "normal_high": 100.0,
        "description": "Blood oxygen saturation (SpO2)",
    },
    {
        "code": "steps",
        "unit": "count",
        "sample_period": 3600,  # hourly sum
        "normal_low": 0.0,
        "normal_high": 2000.0,
        "description": "Step count (hourly total)",
    },
    {
        "code": "skin_temperature",
        "unit": "°C",
        "sample_period": 900,   # every 15 min
        "normal_low": 33.0,
        "normal_high": 37.0,
        "description": "Skin surface temperature",
    },
    {
        "code": "blood_glucose",
        "unit": "mg/dL",
        "sample_period": 900,   # every 15 min (CGM)
        "normal_low": 70.0,
        "normal_high": 140.0,
        "description": "Interstitial blood glucose (CGM)",
    },
    {
        "code": "sleep_stage",
        "unit": "stage",
        "sample_period": 30,    # per 30-second epoch (sleep hours only)
        "normal_low": 0.0,      # 0=awake, 1=light, 2=deep, 3=rem
        "normal_high": 3.0,
        "description": "Sleep stage (awake/light/deep/rem)",
    },
]

# Mapping from metric code → definition dict for quick lookup
METRIC_BY_CODE: dict[str, dict] = {m["code"]: m for m in METRIC_DEFINITIONS}

# Sleep stage integer → label
SLEEP_STAGE_LABELS: dict[int, str] = {
    0: "awake",
    1: "light",
    2: "deep",
    3: "rem",
}


# ── Device models ─────────────────────────────────────────────────────────────
# Each entry describes a wearable device model:
#   model              — device model name (used in device_status records)
#   manufacturer       — manufacturer string
#   metrics_supported  — list of metric codes this device can capture
#   type               — "smartwatch" | "cgm" | "pulse_oximeter"

DEVICE_MODELS: list[dict] = [
    {
        "model": "Garmin Venu 3",
        "manufacturer": "Garmin",
        "type": "smartwatch",
        "metrics_supported": [
            "heart_rate",
            "spo2",
            "steps",
            "skin_temperature",
            "sleep_stage",
        ],
    },
    {
        "model": "Apple Watch Ultra 2",
        "manufacturer": "Apple",
        "type": "smartwatch",
        "metrics_supported": [
            "heart_rate",
            "spo2",
            "steps",
            "skin_temperature",
            "sleep_stage",
        ],
    },
    {
        "model": "Dexcom G7",
        "manufacturer": "Dexcom",
        "type": "cgm",
        "metrics_supported": [
            "blood_glucose",
        ],
    },
    {
        "model": "Withings ScanWatch 2",
        "manufacturer": "Withings",
        "type": "smartwatch",
        "metrics_supported": [
            "heart_rate",
            "spo2",
            "steps",
            "skin_temperature",
            "sleep_stage",
        ],
    },
]

# Device model name → definition dict for quick lookup
DEVICE_BY_MODEL: dict[str, dict] = {d["model"]: d for d in DEVICE_MODELS}

# Smartwatch models (assigned to non-diabetic patients)
SMARTWATCH_MODELS: list[dict] = [d for d in DEVICE_MODELS if d["type"] == "smartwatch"]

# CGM models (assigned to diabetic patients — they get both a smartwatch AND a CGM)
CGM_MODELS: list[dict] = [d for d in DEVICE_MODELS if d["type"] == "cgm"]


# ── SNOMED condition codes ────────────────────────────────────────────────────
SNOMED_DIABETES: set[str] = {
    "44054006",       # Diabetes mellitus type 2
    "15777000",       # Prediabetes
    "368581000119106",  # Prediabetes (alternate)
}

SNOMED_HYPERTENSION: set[str] = {
    "38341003",       # Hypertensive disorder
}

SNOMED_CARDIAC: set[str] = {
    "53741008",       # Coronary artery disease
    "413844008",      # Chronic ischemic heart disease
    "44784217",       # Cardiac arrhythmia
}


# ── Main configuration dataclass ──────────────────────────────────────────────

@dataclass
class SimConfig:
    """Top-level simulation configuration.

    All parameters have sensible defaults. Override only what you need:

        cfg = SimConfig(seed=7, n_patients_limit=10)
    """

    # Reproducibility
    seed: int = 42

    # Synthea input files
    patients_file: str = "synthea/output/csv/patients.csv"
    conditions_file: str = "synthea/output/csv/conditions.csv"

    # Local output directory (used when --upload is not specified)
    output_dir: str = "data/output"

    # MinIO / S3 connection details
    minio_endpoint: str = os.environ.get("MINIO_ENDPOINT", "http://localhost:9010")
    minio_access_key: str = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.environ.get("MINIO_SECRET_KEY", "")
    minio_bucket_raw: str = "raw"

    # Anomaly simulation
    anomaly_probability: float = 0.05  # probability of an anomaly event per patient-day

    # Development convenience — set to a small number to run on a subset
    n_patients_limit: int | None = None

    # ── Read-only derived references (not user-configurable) ──────────────────
    # Access metric and device definitions through module-level constants:
    #   from simulator.config import METRIC_DEFINITIONS, DEVICE_MODELS

    def __post_init__(self) -> None:
        if not 0.0 <= self.anomaly_probability <= 1.0:
            raise ValueError(
                f"anomaly_probability must be in [0, 1], got {self.anomaly_probability}"
            )
        if self.n_patients_limit is not None and self.n_patients_limit < 1:
            raise ValueError(
                f"n_patients_limit must be >= 1, got {self.n_patients_limit}"
            )
