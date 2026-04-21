"""Binance websocket producer that publishes raw events to Pub/Sub."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import websockets

from google.auth.exceptions import DefaultCredentialsError

from ingestion.gcp_credentials import ADC_DOCKER_HINT, ensure_service_account_file_exists_if_configured

LOGGER = logging.getLogger(__name__)
DEFAULT_BINANCE_STREAM_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"


def publish_message(publisher: Any, topic_path: str, payload: str, timeout: int = 30) -> None:
    """Publish one event payload to Pub/Sub."""
    future = publisher.publish(topic_path, payload.encode("utf-8"))
    future.result(timeout=timeout)


def build_multi_symbol_stream_url(symbols: str) -> str:
    """Build Binance combined stream URL from comma-separated symbols."""
    symbol_parts = [symbol.strip().lower() for symbol in symbols.split(",") if symbol.strip()]
    if not symbol_parts:
        raise ValueError("BINANCE_SYMBOLS must contain at least one symbol")
    if len(symbol_parts) == 1:
        return f"wss://stream.binance.com:9443/ws/{symbol_parts[0]}@trade"
    streams = "/".join(f"{symbol}@trade" for symbol in symbol_parts)
    return f"wss://stream.binance.com:9443/stream?streams={streams}"


def resolve_topic_path(
    topic_path_env: str | None,
    topic_env: str | None,
    project_id: str | None,
) -> str:
    """Resolve full Pub/Sub topic path with compatibility for topic name env."""
    if topic_path_env:
        return topic_path_env
    if topic_env and topic_env.startswith("projects/"):
        return topic_env
    if topic_env and project_id:
        return f"projects/{project_id}/topics/{topic_env}"
    raise ValueError("Set PUBSUB_TOPIC_PATH or PUBSUB_TOPIC (+ GCP_PROJECT_ID)")


async def stream_to_pubsub(
    topic_path: str,
    *,
    stream_url: str = DEFAULT_BINANCE_STREAM_URL,
    max_retries: int = 5,
) -> None:
    """Connect to Binance websocket stream and publish each message to Pub/Sub."""
    from google.cloud import pubsub_v1

    try:
        publisher = pubsub_v1.PublisherClient()
    except (DefaultCredentialsError, IsADirectoryError) as exc:
        raise RuntimeError(
            f"{ADC_DOCKER_HINT} "
            "If you see 'Is a directory', you likely have a folder where a .json file should be — "
            "use docker/service-account.json as a file (see docker/README.txt)."
        ) from exc
    attempt = 0

    while True:
        try:
            async with websockets.connect(stream_url) as ws:
                LOGGER.info("Connected to Binance stream: %s", stream_url)
                attempt = 0
                async for message in ws:
                    publish_message(publisher, topic_path, message)
        except Exception as exc:
            attempt += 1
            if attempt > max_retries:
                raise RuntimeError(f"Exceeded websocket retry budget ({max_retries})") from exc
            backoff = min(attempt, 10)
            LOGGER.warning("Websocket error (%s). Reconnecting in %ss", exc, backoff)
            await asyncio.sleep(backoff)


def main() -> None:
    """CLI entrypoint."""
    logging.basicConfig(level=logging.INFO)
    ensure_service_account_file_exists_if_configured()
    topic_path = resolve_topic_path(
        topic_path_env=os.getenv("PUBSUB_TOPIC_PATH"),
        topic_env=os.getenv("PUBSUB_TOPIC"),
        project_id=os.getenv("GCP_PROJECT_ID"),
    )
    stream_url = os.getenv("BINANCE_STREAM_URL")
    if not stream_url:
        stream_url = build_multi_symbol_stream_url(os.getenv("BINANCE_SYMBOLS", "BTCUSDT"))
    asyncio.run(stream_to_pubsub(topic_path=topic_path, stream_url=stream_url))


if __name__ == "__main__":
    main()
