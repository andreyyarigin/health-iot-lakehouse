"""
uploader.py — Upload simulator output to MinIO (S3-compatible).

Uses boto3 with path-style addressing so it works against MinIO's
S3-compatible API without the AWS SDK trying to resolve bucket subdomains.

Upload layout in the raw bucket:
    s3://raw/wearable/YYYY/MM/DD/readings.json
    s3://raw/wearable/YYYY/MM/DD/alerts.json
    s3://raw/wearable/YYYY/MM/DD/device_status.json
"""

from __future__ import annotations

import datetime
import json
import logging
from io import BytesIO

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from simulator.config import SimConfig

logger = logging.getLogger(__name__)


class MinIOUploader:
    """Uploads newline-delimited JSON files to the MinIO raw landing zone.

    Args:
        config: SimConfig instance with MinIO connection details.
    """

    def __init__(self, config: SimConfig) -> None:
        self._config = config
        self._client = self._build_client()

    # ── Public API ────────────────────────────────────────────────────────────

    def upload_day(
        self,
        date: datetime.date,
        readings: list[dict],
        alerts: list[dict],
        device_status: list[dict],
    ) -> dict[str, str]:
        """Upload readings, alerts, and device_status for *date*.

        Each list is serialised as newline-delimited JSON (one object per line).

        Returns:
            Dict mapping file type → S3 URI, e.g.:
            {
                "readings":      "s3://raw/wearable/2026/03/25/readings.json",
                "alerts":        "s3://raw/wearable/2026/03/25/alerts.json",
                "device_status": "s3://raw/wearable/2026/03/25/device_status.json",
            }
        """
        prefix = self._date_prefix(date)
        bucket = self._config.minio_bucket_raw

        self._ensure_bucket(bucket)

        uploads = {
            "readings": (f"{prefix}/readings.json", readings),
            "alerts": (f"{prefix}/alerts.json", alerts),
            "device_status": (f"{prefix}/device_status.json", device_status),
        }

        result: dict[str, str] = {}
        for label, (key, records) in uploads.items():
            self._upload_ndjson(bucket, key, records)
            s3_uri = f"s3://{bucket}/{key}"
            result[label] = s3_uri
            logger.info("Uploaded %d records → %s", len(records), s3_uri)

        return result

    def check_connection(self) -> bool:
        """Return True if the MinIO endpoint is reachable and credentials work."""
        try:
            self._client.list_buckets()
            return True
        except (BotoCoreError, ClientError) as exc:
            logger.warning("MinIO connection check failed: %s", exc)
            return False

    # ── Private helpers ───────────────────────────────────────────────────────

    def _build_client(self):  # type: ignore[return]
        """Build a boto3 S3 client configured for MinIO path-style access."""
        return boto3.client(
            "s3",
            endpoint_url=self._config.minio_endpoint,
            aws_access_key_id=self._config.minio_access_key,
            aws_secret_access_key=self._config.minio_secret_key,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
            region_name="us-east-1",  # required but ignored by MinIO
        )

    def _ensure_bucket(self, bucket: str) -> None:
        """Create the bucket if it doesn't exist yet."""
        try:
            self._client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket"):
                logger.info("Creating bucket: %s", bucket)
                self._client.create_bucket(Bucket=bucket)
            else:
                raise

    def _upload_ndjson(
        self, bucket: str, key: str, records: list[dict]
    ) -> None:
        """Serialise *records* as newline-delimited JSON and PUT to S3."""
        body = "\n".join(json.dumps(record, ensure_ascii=False) for record in records)
        if body:
            body += "\n"  # trailing newline for clean appending

        data = body.encode("utf-8")
        self._client.put_object(
            Bucket=bucket,
            Key=key,
            Body=BytesIO(data),
            ContentType="application/x-ndjson",
        )

    @staticmethod
    def _date_prefix(date: datetime.date) -> str:
        """Return the S3 key prefix for a given date: wearable/YYYY/MM/DD"""
        return f"wearable/{date.year:04d}/{date.month:02d}/{date.day:02d}"
