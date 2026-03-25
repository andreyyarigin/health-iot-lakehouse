"""
cli.py — Command-line interface for the wearable simulator.

Entry points:
    python -m simulator.cli generate-day --date 2026-03-25 [--seed N] [--upload]
    python -m simulator.cli backfill --start-date 2026-01-01 --end-date 2026-03-25
    python -m simulator.cli seed-devices

All commands write newline-delimited JSON to the local output directory unless
--upload is specified, in which case files are pushed to MinIO.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
from pathlib import Path

from simulator.config import DEVICE_MODELS, SimConfig
from simulator.patient_loader import Patient, PatientLoader
from simulator.wearable_generator import WearableGenerator, make_seed
from simulator.alert_generator import AlertGenerator
from simulator.uploader import MinIOUploader

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def _parse_date(value: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {value!r}. Expected YYYY-MM-DD."
        )


def _save_ndjson(path: Path, records: list[dict]) -> None:
    """Write *records* to *path* as newline-delimited JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _make_device_status(patients: list[Patient], date: datetime.date) -> list[dict]:
    """Generate a device_status record for every device (smartwatch + CGM)."""
    import random
    import hashlib

    status_records: list[dict] = []
    for patient in patients:
        # Use a stable seed per device so battery level drifts slowly over time
        device_seed = int(
            hashlib.md5(
                f"{patient.device_serial}{date.isoformat()}".encode()
            ).hexdigest()[:8],
            16,
        )
        rng = random.Random(device_seed)

        status_records.append({
            "device_serial": patient.device_serial,
            "patient_id": patient.patient_id,
            "device_model": patient.device_model,
            "manufacturer": patient.device_manufacturer,
            "battery_level_pct": rng.randint(15, 100),
            "firmware_version": "3.2.1",
            "last_sync_at": f"{date.isoformat()}T06:00:00Z",
            "status": "active",
        })

        if patient.cgm_serial:
            cgm_seed = int(
                hashlib.md5(
                    f"{patient.cgm_serial}{date.isoformat()}".encode()
                ).hexdigest()[:8],
                16,
            )
            cgm_rng = random.Random(cgm_seed)
            status_records.append({
                "device_serial": patient.cgm_serial,
                "patient_id": patient.patient_id,
                "device_model": patient.cgm_model,
                "manufacturer": patient.cgm_manufacturer,
                "battery_level_pct": cgm_rng.randint(20, 100),
                "firmware_version": "1.6.4",
                "last_sync_at": f"{date.isoformat()}T06:00:00Z",
                "status": "active",
            })

    return status_records


# ── Sub-commands ──────────────────────────────────────────────────────────────

def cmd_generate_day(args: argparse.Namespace) -> int:
    """Generate readings, alerts, and device status for a single date."""
    date = args.date
    config = SimConfig(
        seed=args.seed,
        output_dir=args.output_dir,
        n_patients_limit=args.limit_patients,
    )

    _configure_logging(args.verbose)
    logger.info("Generating data for %s (seed=%d)", date, config.seed)

    loader = PatientLoader(config)
    patients = loader.load()

    generator = WearableGenerator(config)
    alert_gen = AlertGenerator()

    all_readings: list[dict] = []
    all_alerts: list[dict] = []

    for idx, patient in enumerate(patients):
        patient_seed = make_seed(config.seed, idx, date)
        readings = generator.generate_day(patient, date, patient_seed)
        alerts = alert_gen.generate_alerts(readings, patient)
        all_readings.extend(readings)
        all_alerts.extend(alerts)

    device_status = _make_device_status(patients, date)

    print(
        f"Generated: {len(patients)} patients, "
        f"{len(all_readings)} readings, "
        f"{len(all_alerts)} alerts, "
        f"{len(device_status)} device status records"
    )

    if args.upload:
        uploader = MinIOUploader(config)
        if not uploader.check_connection():
            print(
                "ERROR: Cannot connect to MinIO at "
                f"{config.minio_endpoint}. Is it running?",
                file=sys.stderr,
            )
            return 1
        paths = uploader.upload_day(date, all_readings, all_alerts, device_status)
        print("Uploaded to MinIO:")
        for label, uri in paths.items():
            print(f"  {label}: {uri}")
    else:
        out_root = Path(config.output_dir) / f"{date.year:04d}" / f"{date.month:02d}" / f"{date.day:02d}"
        _save_ndjson(out_root / "readings.json", all_readings)
        _save_ndjson(out_root / "alerts.json", all_alerts)
        _save_ndjson(out_root / "device_status.json", device_status)
        print(f"Saved to: {out_root}")

    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    """Run generate-day for every date in [start_date, end_date]."""
    _configure_logging(args.verbose)

    start = args.start_date
    end = args.end_date
    if end < start:
        print("ERROR: --end-date must be >= --start-date", file=sys.stderr)
        return 1

    current = start
    total_days = (end - start).days + 1
    completed = 0

    while current <= end:
        print(f"[backfill] {current} ({completed + 1}/{total_days})")

        # Re-use generate-day logic by constructing a synthetic namespace
        day_args = argparse.Namespace(
            date=current,
            seed=args.seed,
            upload=args.upload,
            output_dir=args.output_dir,
            limit_patients=args.limit_patients,
            verbose=args.verbose,
        )
        rc = cmd_generate_day(day_args)
        if rc != 0:
            print(f"[backfill] Failed on {current}, aborting.", file=sys.stderr)
            return rc

        current += datetime.timedelta(days=1)
        completed += 1

    print(f"[backfill] Done. Generated {completed} day(s).")
    return 0


def cmd_seed_devices(args: argparse.Namespace) -> int:
    """Generate device_status.json for all devices (today's snapshot)."""
    _configure_logging(args.verbose)
    date = datetime.date.today()

    config = SimConfig(
        seed=args.seed,
        output_dir=args.output_dir,
        n_patients_limit=args.limit_patients,
    )

    loader = PatientLoader(config)
    patients = loader.load()
    device_status = _make_device_status(patients, date)

    if args.upload:
        uploader = MinIOUploader(config)
        if not uploader.check_connection():
            print(
                "ERROR: Cannot connect to MinIO at "
                f"{config.minio_endpoint}. Is it running?",
                file=sys.stderr,
            )
            return 1
        paths = uploader.upload_day(date, [], [], device_status)
        print(f"Device status uploaded: {paths.get('device_status')}")
    else:
        out_root = Path(config.output_dir)
        out_path = out_root / "device_status.json"
        _save_ndjson(out_path, device_status)
        print(f"Saved {len(device_status)} device records to: {out_path}")

    return 0


# ── Argument parser ───────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m simulator.cli",
        description="Wearable health data simulator for health-iot-lakehouse.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging.",
    )

    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    # ── Common args shared by multiple sub-commands ───────────────────────────
    def _add_common(sub: argparse.ArgumentParser) -> None:
        sub.add_argument(
            "--seed",
            type=int,
            default=42,
            metavar="N",
            help="Random seed for reproducibility (default: 42).",
        )
        sub.add_argument(
            "--upload",
            action="store_true",
            help="Upload output to MinIO instead of saving locally.",
        )
        sub.add_argument(
            "--output-dir",
            default="data/output",
            metavar="DIR",
            help="Local output directory when not uploading (default: data/output).",
        )
        sub.add_argument(
            "--limit-patients",
            type=int,
            default=None,
            metavar="N",
            help="Only process the first N patients (useful for testing).",
        )

    # ── generate-day ─────────────────────────────────────────────────────────
    gen = subparsers.add_parser(
        "generate-day",
        help="Generate readings + alerts for a single date.",
    )
    gen.add_argument(
        "--date",
        type=_parse_date,
        required=True,
        metavar="YYYY-MM-DD",
        help="Date to generate data for.",
    )
    _add_common(gen)
    gen.set_defaults(func=cmd_generate_day)

    # ── backfill ──────────────────────────────────────────────────────────────
    bf = subparsers.add_parser(
        "backfill",
        help="Generate data for a range of dates.",
    )
    bf.add_argument(
        "--start-date",
        type=_parse_date,
        required=True,
        metavar="YYYY-MM-DD",
        help="First date (inclusive).",
    )
    bf.add_argument(
        "--end-date",
        type=_parse_date,
        required=True,
        metavar="YYYY-MM-DD",
        help="Last date (inclusive).",
    )
    _add_common(bf)
    bf.set_defaults(func=cmd_backfill)

    # ── seed-devices ─────────────────────────────────────────────────────────
    sd = subparsers.add_parser(
        "seed-devices",
        help="Generate device_status.json for all patient devices.",
    )
    _add_common(sd)
    sd.set_defaults(func=cmd_seed_devices)

    return parser


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
