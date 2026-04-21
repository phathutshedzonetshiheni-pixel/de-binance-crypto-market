import pytest

from ingestion.stream.pubsub_to_bq_consumer import (
    insert_rows_with_retry,
    parse_pubsub_data,
    parse_trade_event,
    process_pubsub_messages,
)


def test_parse_trade_event_maps_and_casts_fields() -> None:
    event = {
        "s": "btcusdt",
        "t": "123456",
        "p": "63123.45",
        "q": "0.015",
        "E": "1710000000123",
    }

    result = parse_trade_event(event)

    assert result["symbol"] == "BTCUSDT"
    assert result["trade_id"] == 123456
    assert result["price"] == 63123.45
    assert result["qty"] == 0.015
    assert result["event_time"] == 1710000000123
    assert isinstance(result["ingest_time"], int)


def test_insert_rows_with_retry_retries_transient_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    class DummyBigQueryClient:
        def insert_rows_json(self, table_id: str, rows: list[dict]):  # type: ignore[no-untyped-def]
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("temporary insert error")
            return []

    monkeypatch.setattr("ingestion.stream.pubsub_to_bq_consumer.time.sleep", lambda _: None)
    client = DummyBigQueryClient()
    rows = [
        {
            "symbol": "BTCUSDT",
            "trade_id": 1,
            "price": 10.0,
            "qty": 0.1,
            "event_time": 1710000000123,
        }
    ]

    inserted = insert_rows_with_retry(
        client=client,
        table_id="project.dataset.raw_stream_trades",
        rows=rows,
        max_retries=2,
    )

    assert inserted == 1
    assert calls["count"] == 2


def test_parse_pubsub_data_supports_combined_stream_payload() -> None:
    payload = b'{"stream":"btcusdt@trade","data":{"s":"btcusdt","t":"123","p":"10.5","q":"0.2","E":"1710000000123"}}'

    result = parse_pubsub_data(payload)

    assert result["symbol"] == "BTCUSDT"
    assert result["trade_id"] == 123
    assert result["price"] == 10.5
    assert result["qty"] == 0.2
    assert result["event_time"] == 1710000000123
    assert isinstance(result["ingest_time"], int)


def test_process_pubsub_messages_batches_and_acks_per_successful_batch() -> None:
    class DummyMessage:
        def __init__(self, data: bytes) -> None:
            self.data = data

    class DummyReceived:
        def __init__(self, ack_id: str, payload: bytes) -> None:
            self.ack_id = ack_id
            self.message = DummyMessage(payload)

    class DummyPullResponse:
        def __init__(self, received_messages: list[DummyReceived]) -> None:
            self.received_messages = received_messages

    class DummySubscriber:
        def __init__(self) -> None:
            self.ack_calls: list[list[str]] = []

        def pull(self, subscription: str, max_messages: int):  # type: ignore[no-untyped-def]
            assert subscription == "projects/p/subscriptions/s"
            assert max_messages == 10
            return DummyPullResponse(
                [
                    DummyReceived("ack-1", b'{"s":"btcusdt","t":"1","p":"1.0","q":"0.1","E":"11"}'),
                    DummyReceived("ack-2", b'{"s":"ethusdt","t":"2","p":"2.0","q":"0.2","E":"22"}'),
                    DummyReceived("ack-3", b'{"s":"solusdt","t":"3","p":"3.0","q":"0.3","E":"33"}'),
                ]
            )

        def acknowledge(self, subscription: str, ack_ids: list[str]) -> None:
            assert subscription == "projects/p/subscriptions/s"
            self.ack_calls.append(ack_ids)

    class DummyBigQueryClient:
        def __init__(self) -> None:
            self.inserted_batches: list[list[dict]] = []

        def insert_rows_json(self, table_id: str, rows: list[dict]):  # type: ignore[no-untyped-def]
            assert table_id == "p.raw.stream"
            self.inserted_batches.append(rows)
            if len(self.inserted_batches) == 2:
                raise RuntimeError("batch 2 failed")
            return []

    subscriber = DummySubscriber()
    bq_client = DummyBigQueryClient()

    inserted = process_pubsub_messages(
        subscriber=subscriber,
        subscription_path="projects/p/subscriptions/s",
        bq_client=bq_client,
        table_id="p.raw.stream",
        pull_max_messages=10,
        bq_batch_size=2,
        max_retries=1,
    )

    assert inserted == 2
    assert len(bq_client.inserted_batches) == 2
    assert len(bq_client.inserted_batches[0]) == 2
    assert len(bq_client.inserted_batches[1]) == 1
    assert subscriber.ack_calls == [["ack-1", "ack-2"]]


def test_process_pubsub_messages_multiple_batches_all_success_ack_each_batch() -> None:
    class DummyMessage:
        def __init__(self, data: bytes) -> None:
            self.data = data

    class DummyReceived:
        def __init__(self, ack_id: str, payload: bytes) -> None:
            self.ack_id = ack_id
            self.message = DummyMessage(payload)

    class DummyPullResponse:
        def __init__(self, received_messages: list[DummyReceived]) -> None:
            self.received_messages = received_messages

    class DummySubscriber:
        def __init__(self) -> None:
            self.ack_calls: list[list[str]] = []

        def pull(self, subscription: str, max_messages: int):  # type: ignore[no-untyped-def]
            return DummyPullResponse(
                [
                    DummyReceived("ack-1", b'{"s":"btcusdt","t":"1","p":"1.0","q":"0.1","E":"11"}'),
                    DummyReceived("ack-2", b'{"s":"ethusdt","t":"2","p":"2.0","q":"0.2","E":"22"}'),
                    DummyReceived("ack-3", b'{"s":"solusdt","t":"3","p":"3.0","q":"0.3","E":"33"}'),
                ]
            )

        def acknowledge(self, subscription: str, ack_ids: list[str]) -> None:
            self.ack_calls.append(ack_ids)

    class DummyBigQueryClient:
        def __init__(self) -> None:
            self.inserted_batches: list[list[dict]] = []

        def insert_rows_json(self, table_id: str, rows: list[dict]):  # type: ignore[no-untyped-def]
            self.inserted_batches.append(rows)
            return []

    subscriber = DummySubscriber()
    bq_client = DummyBigQueryClient()

    inserted = process_pubsub_messages(
        subscriber=subscriber,
        subscription_path="projects/p/subscriptions/s",
        bq_client=bq_client,
        table_id="p.raw.stream",
        pull_max_messages=10,
        bq_batch_size=2,
        max_retries=1,
    )

    assert inserted == 3
    assert len(bq_client.inserted_batches) == 2
    assert subscriber.ack_calls == [["ack-1", "ack-2"], ["ack-3"]]
