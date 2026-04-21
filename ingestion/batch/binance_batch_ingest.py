"""Batch ingestion helpers for Binance historical kline data."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests
from requests import Response

from ingestion.gcp_credentials import ensure_service_account_file_exists_if_configured

LOGGER = logging.getLogger(__name__)
BINANCE_KLINES_ENDPOINT = "https://api.binance.com/api/v3/klines"
KLINE_API_MAX_LIMIT = 1000
MINUTE_MS = 60 * 1000
SUPPORTED_INTERVAL_MS = {
    "1m": 1 * MINUTE_MS,
    "3m": 3 * MINUTE_MS,
    "5m": 5 * MINUTE_MS,
    "15m": 15 * MINUTE_MS,
    "30m": 30 * MINUTE_MS,
    "1h": 60 * MINUTE_MS,
    "2h": 2 * 60 * MINUTE_MS,
    "4h": 4 * 60 * MINUTE_MS,
    "6h": 6 * 60 * MINUTE_MS,
    "8h": 8 * 60 * MINUTE_MS,
    "12h": 12 * 60 * MINUTE_MS,
    "1d": 24 * 60 * MINUTE_MS,
}


def normalize_kline(
    symbol: str,
    kline: list,
    interval: str,
    ingest_time_ms: int | None = None,
) -> dict[str, Any]:
    """Map a Binance kline array to the project raw record schema."""
    if len(kline) < 7:
        raise ValueError(
            f"Malformed kline payload: expected at least 7 fields, got {len(kline)}"
        )

    ingest_time = int(ingest_time_ms) if ingest_time_ms is not None else int(time.time() * 1000)
    return {
        "symbol": symbol.upper(),
        "interval": interval,
        "open_time": int(kline[0]),
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "volume": float(kline[5]),
        "close_time": int(kline[6]),
        "ingest_time": ingest_time,
    }


def build_kline_params(
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    limit: int = KLINE_API_MAX_LIMIT,
) -> dict[str, Any]:
    """Build standard Binance klines query parameters."""
    return {
        "symbol": symbol.upper(),
        "interval": interval,
        "startTime": int(start_time_ms),
        "endTime": int(end_time_ms),
        "limit": int(limit),
    }


def fetch_klines(
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    *,
    limit: int = KLINE_API_MAX_LIMIT,
    timeout_seconds: int = 20,
    max_retries: int = 3,
) -> list[list[Any]]:
    """Fetch klines with bounded retries for transient failures."""
    params = build_kline_params(symbol, interval, start_time_ms, end_time_ms, limit=limit)

    for attempt in range(1, max_retries + 1):
        try:
            response: Response = requests.get(
                BINANCE_KLINES_ENDPOINT,
                params=params,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError("Unexpected Binance response payload for klines")
            return payload
        except (requests.RequestException, ValueError) as exc:
            if attempt == max_retries:
                raise
            backoff_seconds = attempt
            LOGGER.warning(
                "Retrying Binance kline request for %s/%s after error: %s",
                symbol,
                interval,
                exc,
            )
            time.sleep(backoff_seconds)

    return []


def interval_to_milliseconds(interval: str) -> int:
    """Convert Binance interval string to milliseconds."""
    interval_value = interval.strip().lower()
    interval_ms = SUPPORTED_INTERVAL_MS.get(interval_value)
    if interval_ms is None:
        raise ValueError(f"Unsupported BATCH_INTERVAL '{interval}'.")
    return interval_ms


def fetch_klines_for_time_range(
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    *,
    timeout_seconds: int = 20,
    max_retries: int = 3,
) -> list[list[Any]]:
    """Fetch full kline history for a time window using paginated API calls."""
    interval_ms = interval_to_milliseconds(interval)
    chunk_span_ms = interval_ms * KLINE_API_MAX_LIMIT
    all_klines: list[list[Any]] = []
    current_start_ms = int(start_time_ms)
    upper_end_ms = int(end_time_ms)

    while current_start_ms <= upper_end_ms:
        chunk_end_ms = min(current_start_ms + chunk_span_ms - 1, upper_end_ms)
        klines = fetch_klines(
            symbol=symbol,
            interval=interval,
            start_time_ms=current_start_ms,
            end_time_ms=chunk_end_ms,
            limit=KLINE_API_MAX_LIMIT,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
        if not klines:
            break
        all_klines.extend(klines)
        last_open_time_ms = int(klines[-1][0])
        next_start_ms = last_open_time_ms + interval_ms
        if next_start_ms <= current_start_ms:
            break
        current_start_ms = next_start_ms
        if len(klines) < KLINE_API_MAX_LIMIT:
            # No more data returned in this range.
            break

    return all_klines


def rows_to_records(
    symbol: str,
    interval: str,
    klines: list[list[Any]],
    ingest_time_ms: int | None = None,
) -> list[dict[str, Any]]:
    """Convert raw kline rows to normalized dictionaries."""
    return [
        normalize_kline(symbol, kline, interval, ingest_time_ms=ingest_time_ms)
        for kline in klines
    ]


def build_gcs_object_name(
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
) -> str:
    """Build deterministic GCS object name for a kline batch window."""
    return (
        "binance/klines/"
        f"symbol={symbol.upper()}/"
        f"interval={interval}/"
        f"start={int(start_time_ms)}_end={int(end_time_ms)}.jsonl"
    )


def build_gcs_date_object_name(
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
) -> str:
    """Build UTC date-partitioned GCS object name for batch writes."""
    start_dt = datetime.fromtimestamp(int(start_time_ms) / 1000, tz=timezone.utc)
    deterministic_name = build_gcs_object_name(
        symbol=symbol,
        interval=interval,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
    )
    return (
        f"raw/batch/{start_dt.year:04d}/{start_dt.month:02d}/{start_dt.day:02d}/"
        f"{deterministic_name}"
    )


def run_batch_extract_transform(
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    *,
    ingest_time_ms: int | None = None,
    timeout_seconds: int = 20,
    max_retries: int = 3,
) -> tuple[list[dict[str, Any]], str]:
    """Run fetch -> normalize path and return records plus object name."""
    klines = fetch_klines_for_time_range(
        symbol=symbol,
        interval=interval,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
    records = rows_to_records(
        symbol=symbol,
        interval=interval,
        klines=klines,
        ingest_time_ms=ingest_time_ms,
    )
    object_name = build_gcs_date_object_name(
        symbol=symbol,
        interval=interval,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
    )
    return records, object_name


def records_to_jsonl(records: list[dict[str, Any]]) -> str:
    """Convert normalized records to JSONL text."""
    return "\n".join(json.dumps(record, sort_keys=True) for record in records)


def upload_jsonl_to_gcs(
    bucket_name: str,
    object_path: str,
    jsonl_payload: str,
    *,
    project: str | None = None,
    max_retries: int = 3,
) -> str:
    """Upload JSONL payload to GCS and return its URI."""
    from google.cloud import storage

    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    for attempt in range(1, max_retries + 1):
        try:
            blob.upload_from_string(jsonl_payload, content_type="application/json")
            break
        except Exception as exc:
            if attempt == max_retries:
                raise
            LOGGER.warning(
                "Retrying GCS upload for gs://%s/%s after error: %s",
                bucket_name,
                object_path,
                exc,
            )
            time.sleep(attempt)
    return f"gs://{bucket_name}/{object_path}"


def load_to_bigquery_raw_table(
    table_id: str,
    records: list[dict[str, Any]],
    *,
    project: str | None = None,
    write_disposition: str = "WRITE_APPEND",
    max_retries: int = 3,
) -> int:
    """Load normalized records to BigQuery raw table."""
    from google.cloud import bigquery

    # Default WRITE_APPEND keeps ingestion idempotent-ish when upstream object keys are deterministic.
    # Callers can override to WRITE_TRUNCATE or WRITE_EMPTY for controlled backfills.
    write_mode = getattr(bigquery.WriteDisposition, write_disposition, write_disposition)
    client = bigquery.Client(project=project)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_mode,
    )
    for attempt in range(1, max_retries + 1):
        try:
            job = client.load_table_from_json(records, table_id, job_config=job_config)
            job.result()
            return len(records)
        except Exception as exc:
            if attempt == max_retries:
                raise
            LOGGER.warning(
                "Retrying BigQuery load for %s after error: %s",
                table_id,
                exc,
            )
            time.sleep(attempt)

    return len(records)


def run_batch_pipeline(
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    bucket_name: str,
    table_id: str,
    *,
    ingest_time_ms: int | None = None,
    project: str | None = None,
    write_disposition: str = "WRITE_APPEND",
    timeout_seconds: int = 20,
    max_retries: int = 3,
) -> dict[str, Any]:
    """Run fetch -> normalize -> jsonl -> gcs upload -> bigquery load."""
    records, object_name = run_batch_extract_transform(
        symbol=symbol,
        interval=interval,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        ingest_time_ms=ingest_time_ms,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
    jsonl_payload = records_to_jsonl(records)
    gcs_uri = upload_jsonl_to_gcs(
        bucket_name=bucket_name,
        object_path=object_name,
        jsonl_payload=jsonl_payload,
        project=project,
        max_retries=max_retries,
    )
    loaded_rows = load_to_bigquery_raw_table(
        table_id=table_id,
        records=records,
        project=project,
        write_disposition=write_disposition,
        max_retries=max_retries,
    )
    return {
        "symbol": symbol.upper(),
        "interval": interval,
        "record_count": len(records),
        "object_name": object_name,
        "gcs_uri": gcs_uri,
        "loaded_rows": loaded_rows,
    }


def main() -> None:
    """Simple local runner for one symbol/time window."""
    logging.basicConfig(level=logging.INFO)
    symbols_env = os.getenv("BINANCE_SYMBOLS", "BTCUSDT")
    symbols = [symbol.strip().upper() for symbol in symbols_env.split(",") if symbol.strip()]
    if not symbols:
        raise ValueError("BINANCE_SYMBOLS must contain at least one symbol")

    interval = os.getenv("BATCH_INTERVAL", "1m").strip()
    if not interval:
        raise ValueError("BATCH_INTERVAL must be non-empty when set")
    lookback_days = float(os.getenv("BATCH_LOOKBACK_DAYS", "0").strip() or "0")
    lookback_hours = float(os.getenv("BATCH_LOOKBACK_HOURS", "1").strip() or "1")
    if lookback_days <= 0 and lookback_hours <= 0:
        raise ValueError("Set BATCH_LOOKBACK_DAYS or BATCH_LOOKBACK_HOURS to a value > 0")
    lookback_ms = int((lookback_days * 24 + lookback_hours) * 60 * 60 * 1000)
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - lookback_ms
    bucket_name = os.getenv("GCS_BUCKET")
    project_id = os.getenv("GCP_PROJECT_ID")
    table_id = os.getenv("BQ_RAW_TABLE")
    if not table_id and project_id and os.getenv("BQ_DATASET_RAW"):
        table_id = f"{project_id}.{os.getenv('BQ_DATASET_RAW')}.raw_batch_ohlc"
    should_run_pipeline = os.getenv("RUN_BATCH_PIPELINE", "").lower() in {"1", "true", "yes"}

    if should_run_pipeline and bucket_name and table_id:
        ensure_service_account_file_exists_if_configured()
        for symbol in symbols:
            summary = run_batch_pipeline(
                symbol=symbol,
                interval=interval,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
                bucket_name=bucket_name,
                table_id=table_id,
                project=project_id,
            )
            LOGGER.info("Pipeline summary: %s", summary)
        return

    LOGGER.info("Dry run only. Set RUN_BATCH_PIPELINE=1 + cloud env vars to execute writes.")
    LOGGER.info(
        "Using lookback window: %.2f days (%.2f hours)",
        lookback_days if lookback_days > 0 else lookback_hours / 24,
        lookback_hours if lookback_days <= 0 else lookback_days * 24 + lookback_hours,
    )
    for symbol in symbols:
        records, object_name = run_batch_extract_transform(
            symbol=symbol,
            interval=interval,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
        )
        LOGGER.info("Fetched and normalized %d kline rows for %s", len(records), symbol)
        LOGGER.info("Object name preview for %s: %s", symbol, object_name)


if __name__ == "__main__":
    main()
