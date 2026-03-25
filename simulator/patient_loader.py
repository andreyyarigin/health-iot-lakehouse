"""
patient_loader.py — Load and enrich Synthea patient data.

Reads patients.csv and conditions.csv produced by Synthea, enriches each
patient with condition flags and an assigned wearable device, and returns a
list of Patient dataclass instances ready for the wearable generator.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

from simulator.config import (
    CGM_MODELS,
    DEVICE_MODELS,
    SMARTWATCH_MODELS,
    SNOMED_CARDIAC,
    SNOMED_DIABETES,
    SNOMED_HYPERTENSION,
    SimConfig,
)

logger = logging.getLogger(__name__)


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Patient:
    """A single Synthea-sourced patient enriched with device assignment."""

    # Core identity
    patient_id: str          # Synthea UUID (business key)
    birth_date: str          # ISO date string: YYYY-MM-DD
    gender: str              # "M" or "F"
    race: str
    ethnicity: str
    city: str
    state: str
    bmi: Optional[float]

    # Chronic conditions as a list of SNOMED codes (strings)
    chronic_conditions: list[str] = field(default_factory=list)

    # Condition flags (derived from chronic_conditions)
    has_diabetes: bool = False
    has_hypertension: bool = False
    has_cardiac_condition: bool = False

    # Device assignment
    device_serial: str = ""       # e.g. "GRM-W0042"
    device_model: str = ""        # e.g. "Garmin Venu 3"
    device_manufacturer: str = "" # e.g. "Garmin"

    # CGM device (only for diabetic patients)
    cgm_serial: Optional[str] = None
    cgm_model: Optional[str] = None
    cgm_manufacturer: Optional[str] = None

    @property
    def metrics_supported(self) -> list[str]:
        """All metric codes this patient's device(s) can capture."""
        device = next(
            (d for d in DEVICE_MODELS if d["model"] == self.device_model), None
        )
        codes: list[str] = []
        if device:
            codes.extend(device["metrics_supported"])
        if self.has_diabetes and self.cgm_model:
            cgm = next(
                (d for d in DEVICE_MODELS if d["model"] == self.cgm_model), None
            )
            if cgm:
                for c in cgm["metrics_supported"]:
                    if c not in codes:
                        codes.append(c)
        return codes


# ── Loader ────────────────────────────────────────────────────────────────────

class PatientLoader:
    """Reads Synthea CSV exports and produces a list of enriched Patient objects.

    Args:
        config: SimConfig instance with file paths and optional patient limit.
    """

    def __init__(self, config: SimConfig) -> None:
        self._config = config

    # ── Public API ────────────────────────────────────────────────────────────

    def load(self) -> list[Patient]:
        """Load patients and conditions, enrich, and return."""
        patients_path = Path(self._config.patients_file)
        conditions_path = Path(self._config.conditions_file)

        self._validate_paths(patients_path, conditions_path)

        patients_df = self._read_patients(patients_path)
        conditions_map = self._read_conditions(conditions_path)

        patients: list[Patient] = []
        for idx, row in enumerate(patients_df.itertuples(index=False)):
            patient = self._build_patient(idx, row, conditions_map)
            patients.append(patient)

        logger.info("Loaded %d patients", len(patients))
        return patients

    # ── Private helpers ───────────────────────────────────────────────────────

    def _validate_paths(
        self, patients_path: Path, conditions_path: Path
    ) -> None:
        missing = [p for p in (patients_path, conditions_path) if not p.exists()]
        if missing:
            raise FileNotFoundError(
                f"Required Synthea files not found: {missing}. "
                "Run synthea/run_synthea.sh first."
            )

    def _read_patients(self, path: Path) -> pd.DataFrame:
        """Read patients.csv and apply optional patient limit."""
        df = pd.read_csv(path, dtype=str)
        df.columns = [c.strip().lower() for c in df.columns]

        limit = self._config.n_patients_limit
        if limit is not None:
            df = df.head(limit)
            logger.debug("Patient limit applied: using %d of available patients", len(df))

        return df

    def _read_conditions(self, path: Path) -> dict[str, list[str]]:
        """Return a map of patient_id → [snomed_code, ...] from conditions.csv."""
        df = pd.read_csv(path, dtype=str)
        df.columns = [c.strip().lower() for c in df.columns]

        # Synthea conditions.csv uses 'patient' for the patient UUID column
        patient_col = "patient"
        code_col = "code"

        if patient_col not in df.columns or code_col not in df.columns:
            logger.warning(
                "conditions.csv missing expected columns %r or %r — "
                "condition detection will be skipped. Found: %s",
                patient_col,
                code_col,
                list(df.columns),
            )
            return {}

        conditions_map: dict[str, list[str]] = {}
        for patient_id, group in df.groupby(patient_col):
            conditions_map[str(patient_id)] = group[code_col].dropna().tolist()

        return conditions_map

    def _build_patient(
        self,
        idx: int,
        row: object,  # pandas namedtuple from itertuples
        conditions_map: dict[str, list[str]],
    ) -> Patient:
        """Construct a single Patient from a patients.csv row."""

        # Synthea uses 'id' for the patient UUID
        patient_id = _get(row, "id", f"patient-{idx:06d}")

        chronic_conditions = conditions_map.get(patient_id, [])

        has_diabetes = bool(SNOMED_DIABETES & set(chronic_conditions))
        has_hypertension = bool(SNOMED_HYPERTENSION & set(chronic_conditions))
        has_cardiac = bool(SNOMED_CARDIAC & set(chronic_conditions))

        bmi_raw = _get(row, "bmi", None)
        bmi = float(bmi_raw) if bmi_raw not in (None, "", "nan") else None

        # ── Device assignment ─────────────────────────────────────────────────
        # Diabetic patients get both a smartwatch (for HR/SpO2/steps) and a CGM.
        # The smartwatch model alternates across the catalogue for variety.
        smartwatch_model = SMARTWATCH_MODELS[idx % len(SMARTWATCH_MODELS)]
        device_serial = f"GRM-W{idx:04d}"

        cgm_serial: Optional[str] = None
        cgm_model: Optional[str] = None
        cgm_manufacturer: Optional[str] = None

        if has_diabetes and CGM_MODELS:
            cgm_def = CGM_MODELS[idx % len(CGM_MODELS)]
            cgm_serial = f"CGM-{idx:04d}"
            cgm_model = cgm_def["model"]
            cgm_manufacturer = cgm_def["manufacturer"]

        return Patient(
            patient_id=patient_id,
            birth_date=_get(row, "birthdate", "1970-01-01"),
            gender=_get(row, "gender", "U"),
            race=_get(row, "race", "unknown"),
            ethnicity=_get(row, "ethnicity", "unknown"),
            city=_get(row, "city", ""),
            state=_get(row, "state", ""),
            bmi=bmi,
            chronic_conditions=chronic_conditions,
            has_diabetes=has_diabetes,
            has_hypertension=has_hypertension,
            has_cardiac_condition=has_cardiac,
            device_serial=device_serial,
            device_model=smartwatch_model["model"],
            device_manufacturer=smartwatch_model["manufacturer"],
            cgm_serial=cgm_serial,
            cgm_model=cgm_model,
            cgm_manufacturer=cgm_manufacturer,
        )


# ── Utility ───────────────────────────────────────────────────────────────────

def _get(row: object, attr: str, default: object) -> object:
    """Safe attribute access on a pandas namedtuple row."""
    val = getattr(row, attr, default)
    if val is None or (isinstance(val, float) and str(val) == "nan"):
        return default
    return val
