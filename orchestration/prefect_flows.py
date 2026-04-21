"""Prefect orchestration for batch ingestion, stream ingestion, and transforms."""

from __future__ import annotations

import argparse
import os
import subprocess
import time
from collections.abc import Sequence
from typing import Any

from prefect import flow, get_run_logger, task

from ingestion.batch.binance_batch_ingest import run_batch_pipeline
from ingestion.stream.binance_ws_producer import (
    build_multi_symbol_stream_url,
    resolve_topic_path,
    stream_to_pubsub,
)
from ingestion.stream.pubsub_to_bq_consumer import (
    resolve_subscription_path,
    resolve_table_id,
    run_consumer_loop,
)


@task(name="run_batch_ingestion_task", retries=2, retry_delay_seconds=10)
def run_batch_ingestion_task(
    *,
    symbol: str,
    interval: str,
    lookback_minutes: int,
    bucket_name: str,
    table_id: str,
    project_id: str | None = None,
) -> dict[str, Any]:
    """Execute one batch ingestion window and return the summary payload."""
    logger = get_run_logger()
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - (lookback_minutes * 60 * 1000)
    logger.info(
        "Starting batch ingestion for %s %s (lookback=%s minutes)",
        symbol,
        interval,
        lookback_minutes,
    )
    summary = run_batch_pipeline(
        symbol=symbol,
        interval=interval,
        start_time_ms=start_ms,
        end_time_ms=end_ms,
        bucket_name=bucket_name,
        table_id=table_id,
        project=project_id,
    )
    logger.info("Batch ingestion completed. loaded_rows=%s", summary["loaded_rows"])
    return summary


@task(name="run_stream_producer_task", retries=2, retry_delay_seconds=5)
def run_stream_producer_task(*, topic_path: str, stream_url: str) -> None:
    """Run Binance websocket producer publishing to Pub/Sub."""
    logger = get_run_logger()
    logger.info("Starting stream producer for topic=%s stream_url=%s", topic_path, stream_url)
    import asyncio

    asyncio.run(stream_to_pubsub(topic_path=topic_path, stream_url=stream_url))


@task(name="run_stream_consumer_task", retries=2, retry_delay_seconds=5)
def run_stream_consumer_task(*, subscription_path: str, table_id: str) -> None:
    """Run Pub/Sub consumer loop writing normalized rows to BigQuery."""
    logger = get_run_logger()
    logger.info(
        "Starting stream consumer for subscription=%s table=%s", subscription_path, table_id
    )
    run_consumer_loop(subscription_path=subscription_path, table_id=table_id)


@task(name="run_dbt_command_task", retries=1, retry_delay_seconds=5)
def run_dbt_command_task(args: Sequence[str]) -> str:
    """Run a dbt command and return stdout for logging/debugging."""
    logger = get_run_logger()
    logger.info("Running dbt command: %s", " ".join(args))
    result = subprocess.run(args, check=True, capture_output=True, text=True)
    if result.stdout.strip():
        logger.info("dbt stdout:\n%s", result.stdout.strip())
    if result.stderr.strip():
        logger.warning("dbt stderr:\n%s", result.stderr.strip())
    return result.stdout


@flow(name="batch_ingestion_flow")
def batch_ingestion_flow(
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    lookback_minutes: int = 60,
) -> dict[str, Any]:
    """Orchestrate one batch ingestion run."""
    bucket_name = os.getenv("GCS_BUCKET")
    table_id = os.getenv("BQ_RAW_TABLE")
    if not bucket_name or not table_id:
        raise ValueError("Set GCS_BUCKET and BQ_RAW_TABLE before running batch_ingestion_flow")
    project_id = os.getenv("GCP_PROJECT_ID")
    return run_batch_ingestion_task(
        symbol=symbol,
        interval=interval,
        lookback_minutes=lookback_minutes,
        bucket_name=bucket_name,
        table_id=table_id,
        project_id=project_id,
    )


@flow(name="stream_ingestion_flow")
def stream_ingestion_flow(run_mode: str = "consumer") -> None:
    """Orchestrate streaming ingestion in either producer or consumer mode."""
    if run_mode not in {"producer", "consumer"}:
        raise ValueError("run_mode must be 'producer' or 'consumer'")

    project_id = os.getenv("GCP_PROJECT_ID")
    if run_mode == "producer":
        topic_path = resolve_topic_path(
            topic_path_env=os.getenv("PUBSUB_TOPIC_PATH"),
            topic_env=os.getenv("PUBSUB_TOPIC"),
            project_id=project_id,
        )
        stream_url = os.getenv("BINANCE_STREAM_URL") or build_multi_symbol_stream_url(
            os.getenv("BINANCE_SYMBOLS", "BTCUSDT")
        )
        run_stream_producer_task(topic_path=topic_path, stream_url=stream_url)
        return

    subscription_value = os.getenv("PUBSUB_SUBSCRIPTION")
    if not subscription_value:
        raise ValueError("Set PUBSUB_SUBSCRIPTION before running consumer mode")
    subscription_path = resolve_subscription_path(subscription_value, project_id)
    table_id = resolve_table_id(
        os.getenv("BQ_RAW_STREAM_TABLE"),
        project_id=project_id,
        dataset_raw=os.getenv("BQ_DATASET_RAW"),
    )
    run_stream_consumer_task(subscription_path=subscription_path, table_id=table_id)


@flow(name="transform_flow")
def transform_flow(
    dbt_project_dir: str = "dbt",
    dbt_profiles_dir: str = "dbt",
    run_tests: bool = True,
) -> None:
    """Run dbt transformations and optional tests."""
    run_dbt_command_task(
        [
            "python",
            "-m",
            "dbt",
            "run",
            "--project-dir",
            dbt_project_dir,
            "--profiles-dir",
            dbt_profiles_dir,
        ]
    )
    if run_tests:
        run_dbt_command_task(
            [
                "python",
                "-m",
                "dbt",
                "test",
                "--project-dir",
                dbt_project_dir,
                "--profiles-dir",
                dbt_profiles_dir,
            ]
        )


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Prefect project flows")
    subparsers = parser.add_subparsers(dest="flow_name", required=True)

    batch_parser = subparsers.add_parser("batch", help="Run batch ingestion flow")
    batch_parser.add_argument("--symbol", default="BTCUSDT")
    batch_parser.add_argument("--interval", default="1m")
    batch_parser.add_argument("--lookback-minutes", type=int, default=60)

    stream_parser = subparsers.add_parser("stream", help="Run stream ingestion flow")
    stream_parser.add_argument(
        "--run-mode",
        choices=["producer", "consumer"],
        default="consumer",
    )

    transform_parser = subparsers.add_parser("transform", help="Run transform flow")
    transform_parser.add_argument("--dbt-project-dir", default="dbt")
    transform_parser.add_argument("--dbt-profiles-dir", default="dbt")
    transform_parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Run dbt run only",
    )
    return parser


def main() -> None:
    parser = _build_cli_parser()
    args = parser.parse_args()
    if args.flow_name == "batch":
        batch_ingestion_flow(
            symbol=args.symbol,
            interval=args.interval,
            lookback_minutes=args.lookback_minutes,
        )
    elif args.flow_name == "stream":
        stream_ingestion_flow(run_mode=args.run_mode)
    else:
        transform_flow(
            dbt_project_dir=args.dbt_project_dir,
            dbt_profiles_dir=args.dbt_profiles_dir,
            run_tests=not args.skip_tests,
        )


if __name__ == "__main__":
    main()
