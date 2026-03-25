"""
simulator — Wearable health data generator for health-iot-lakehouse.

Generates synthetic daily readings, alerts, and device status events
for a population of Synthea-generated patients. Output is newline-delimited
JSON, ready for upload to the MinIO raw landing zone.

Usage:
    python -m simulator.cli generate-day --date 2026-03-25
    python -m simulator.cli backfill --start-date 2026-01-01 --end-date 2026-03-25
    python -m simulator.cli seed-devices
"""

__version__ = "0.1.0"
