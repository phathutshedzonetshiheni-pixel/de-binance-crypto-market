"""Pub/Sub to BigQuery consumer for Binance trade stream events."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any
from datetime import datetime, timezone

from google.auth.exceptions import DefaultCredentialsError

from ingestion.gcp_credentials import ADC_DOCKER_HINT, ensure_service_account_file_exists_if_configured

LOGGER = logging.getLogger(__name__)
DEFAULT_RAW_STREAM_TABLE = "raw_stream_trades"


def parse_trade_event(event: dict) -> dict[str, Any]:
    """Map a Binance trade event payload to the raw stream table schema."""
    ingest_time_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    return {
        "symbol": str(event["s"]).upper(),
        "trade_id": int(event["t"]),
        "price": float(event["p"]),
        "qty": float(event["q"]),
        "event_time": int(event["E"]),
        "ingest_time": ingest_time_ms,
    }


def parse_pubsub_data(message_data: bytes) -> dict[str, Any]:
    """Parse Pub/Sub payload bytes into normalized trade record."""
    decoded = message_data.decode("utf-8")
    payload = json.loads(decoded)
    event = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
    return parse_trade_event(event)


def chunk_records(records: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
    """Split records into deterministic batches."""
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    return [records[index : index + batch_size] for index in range(0, len(records), batch_size)]


def insert_rows_with_retry(
    client: Any,
    table_id: str,
    rows: list[dict[str, Any]],
    *,
    max_retries: int = 3,
) -> int:
    """Insert one batch into BigQuery with simple bounded retry behavior."""
    if not rows:
        return 0

    for attempt in range(1, max_retries + 1):
        try:
            errors = client.insert_rows_json(table_id, rows)
            if errors:
                raise RuntimeError(f"BigQuery insert_rows_json returned errors: {errors}")
            return len(rows)
        except Exception as exc:
            if attempt == max_retries:
                raise
            LOGGER.warning("Retrying BigQuery stream insert after error: %s", exc)
            time.sleep(attempt)

    return 0


def chunk_with_ack_ids(
    records_with_ack_ids: list[tuple[dict[str, Any], str]],
    batch_size: int,
) -> list[list[tuple[dict[str, Any], str]]]:
    """Split record+ack tuples into deterministic batches."""
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    return [
        records_with_ack_ids[index : index + batch_size]
        for index in range(0, len(records_with_ack_ids), batch_size)
    ]


def process_pubsub_messages(
    subscriber: Any,
    subscription_path: str,
    bq_client: Any,
    table_id: str,
    *,
    pull_max_messages: int = 100,
    bq_batch_size: int = 200,
    max_retries: int = 3,
) -> int:
    """Pull Pub/Sub messages, insert to BigQuery in batches, ack successful subsets."""
    response = subscriber.pull(subscription=subscription_path, max_messages=pull_max_messages)
    received = list(response.received_messages)
    if not received:
        return 0

    records_with_ack_ids: list[tuple[dict[str, Any], str]] = []
    for item in received:
        records_with_ack_ids.append((parse_pubsub_data(item.message.data), item.ack_id))

    inserted_count = 0
    for batch in chunk_with_ack_ids(records_with_ack_ids, bq_batch_size):
        rows = [record for record, _ack_id in batch]
        ack_ids = [ack_id for _record, ack_id in batch]
        try:
            inserted_count += insert_rows_with_retry(
                client=bq_client,
                table_id=table_id,
                rows=rows,
                max_retries=max_retries,
            )
            subscriber.acknowledge(subscription=subscription_path, ack_ids=ack_ids)
        except Exception as exc:
            LOGGER.warning("Skipping ack for failed batch of %d messages: %s", len(batch), exc)

    return inserted_count


def resolve_subscription_path(subscription_value: str, project_id: str | None) -> str:
    """Build full subscription path when only a name is provided."""
    if subscription_value.startswith("projects/"):
        return subscription_value
    if not project_id:
        raise ValueError("GCP_PROJECT_ID required when PUBSUB_SUBSCRIPTION is not a full path")
    return f"projects/{project_id}/subscriptions/{subscription_value}"


def resolve_table_id(
    table_id_env: str | None,
    *,
    project_id: str | None,
    dataset_raw: str | None,
) -> str:
    """Resolve target BigQuery stream table id with backward-compatible env fallbacks."""
    if table_id_env:
        return table_id_env
    if project_id and dataset_raw:
        return f"{project_id}.{dataset_raw}.{DEFAULT_RAW_STREAM_TABLE}"
    raise ValueError("Provide BQ_RAW_STREAM_TABLE or both GCP_PROJECT_ID and BQ_DATASET_RAW")


def run_consumer_loop(
    subscription_path: str,
    table_id: str,
    *,
    pull_max_messages: int = 100,
    bq_batch_size: int = 200,
    max_retries: int = 3,
    idle_sleep_seconds: float = 1.0,
) -> None:
    """Run a simple long-lived consumer loop."""
    from google.cloud import bigquery
    from google.cloud import pubsub_v1

    try:
        subscriber = pubsub_v1.SubscriberClient()
        bq_client = bigquery.Client()
    except (DefaultCredentialsError, IsADirectoryError) as exc:
        raise RuntimeError(
            f"{ADC_DOCKER_HINT} "
            "If you see 'Is a directory', use docker/service-account.json as a file, not a folder (see docker/README.txt)."
        ) from exc

    while True:
        inserted = process_pubsub_messages(
            subscriber=subscriber,
            subscription_path=subscription_path,
            bq_client=bq_client,
            table_id=table_id,
            pull_max_messages=pull_max_messages,
            bq_batch_size=bq_batch_size,
            max_retries=max_retries,
        )
        if inserted == 0:
            time.sleep(idle_sleep_seconds)


def main() -> None:
    """CLI entrypoint."""
    logging.basicConfig(level=logging.INFO)
    ensure_service_account_file_exists_if_configured()
    project_id = os.getenv("GCP_PROJECT_ID")
    subscription_value = os.getenv("PUBSUB_SUBSCRIPTION")
    if not subscription_value:
        raise ValueError("PUBSUB_SUBSCRIPTION is required")
    subscription_path = resolve_subscription_path(subscription_value, project_id)
    table_id = resolve_table_id(
        os.getenv("BQ_RAW_STREAM_TABLE"),
        project_id=project_id,
        dataset_raw=os.getenv("BQ_DATASET_RAW"),
    )
    run_consumer_loop(subscription_path=subscription_path, table_id=table_id)


if __name__ == "__main__":
    main()
