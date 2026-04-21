import sys
import types

import pytest
import requests

from ingestion.batch.binance_batch_ingest import (
    build_gcs_date_object_name,
    build_gcs_object_name,
    build_kline_params,
    fetch_klines,
    fetch_klines_for_time_range,
    interval_to_milliseconds,
    load_to_bigquery_raw_table,
    normalize_kline,
    run_batch_pipeline,
    run_batch_extract_transform,
    upload_jsonl_to_gcs,
)


def test_normalize_kline_maps_fields_and_casts_values() -> None:
    kline = [
        1710000000000,
        "63450.10",
        "63500.55",
        "63310.01",
        "63490.20",
        "123.456",
        1710000059999,
    ]

    result = normalize_kline("BTCUSDT", kline, "1m", ingest_time_ms=1710009999999)

    assert result == {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "open_time": 1710000000000,
        "open": 63450.10,
        "high": 63500.55,
        "low": 63310.01,
        "close": 63490.20,
        "volume": 123.456,
        "close_time": 1710000059999,
        "ingest_time": 1710009999999,
    }


def test_build_kline_params_includes_required_query_fields() -> None:
    result = build_kline_params(
        symbol="ethusdt",
        interval="1m",
        start_time_ms=1710000000000,
        end_time_ms=1710003600000,
    )

    assert result == {
        "symbol": "ETHUSDT",
        "interval": "1m",
        "startTime": 1710000000000,
        "endTime": 1710003600000,
        "limit": 1000,
    }


def test_normalize_kline_raises_for_malformed_input() -> None:
    with pytest.raises(ValueError, match="at least 7 fields"):
        normalize_kline("BTCUSDT", [1710000000000, "1.0"], "1m")


def test_build_gcs_object_name_is_deterministic() -> None:
    object_name = build_gcs_object_name(
        symbol="btcusdt",
        interval="1m",
        start_time_ms=1710000000000,
        end_time_ms=1710003600000,
    )

    assert (
        object_name
        == "binance/klines/symbol=BTCUSDT/interval=1m/start=1710000000000_end=1710003600000.jsonl"
    )


def test_build_gcs_date_object_name_uses_utc_date_path() -> None:
    object_name = build_gcs_date_object_name(
        symbol="btcusdt",
        interval="1m",
        start_time_ms=1710000000000,
        end_time_ms=1710003600000,
    )

    assert (
        object_name
        == "raw/batch/2024/03/09/binance/klines/symbol=BTCUSDT/interval=1m/start=1710000000000_end=1710003600000.jsonl"
    )


def test_fetch_klines_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    expected_payload = [[1710000000000, "1", "2", "0.5", "1.5", "100", 1710000059999]]
    calls = {"count": 0}

    class DummyResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[list[str | int]]:
            return expected_payload

    def fake_get(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls["count"] += 1
        if calls["count"] < 3:
            raise requests.RequestException("transient network issue")
        return DummyResponse()

    monkeypatch.setattr("ingestion.batch.binance_batch_ingest.requests.get", fake_get)
    monkeypatch.setattr("ingestion.batch.binance_batch_ingest.time.sleep", lambda _: None)

    result = fetch_klines("BTCUSDT", "1m", 1710000000000, 1710003600000, max_retries=3)

    assert result == expected_payload
    assert calls["count"] == 3


def test_interval_to_milliseconds_returns_expected_values() -> None:
    assert interval_to_milliseconds("1m") == 60_000
    assert interval_to_milliseconds("1h") == 3_600_000


def test_interval_to_milliseconds_raises_for_unsupported_interval() -> None:
    with pytest.raises(ValueError, match="Unsupported BATCH_INTERVAL"):
        interval_to_milliseconds("13m")


def test_fetch_klines_for_time_range_paginates_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int]] = []
    first_page = [[index * 60_000, "1", "2", "0.5", "1.5", "100", (index * 60_000) + 59_999] for index in range(1000)]
    second_page = [[60_000_000, "1.1", "2.2", "0.6", "1.6", "110", 60_059_999]]

    def fake_fetch_klines(**kwargs):  # type: ignore[no-untyped-def]
        calls.append((kwargs["start_time_ms"], kwargs["end_time_ms"]))
        if kwargs["start_time_ms"] == 0:
            return first_page
        if kwargs["start_time_ms"] == 60_000_000:
            return second_page
        return []

    monkeypatch.setattr("ingestion.batch.binance_batch_ingest.fetch_klines", fake_fetch_klines)

    result = fetch_klines_for_time_range(
        symbol="BTCUSDT",
        interval="1m",
        start_time_ms=0,
        end_time_ms=60_059_999,
    )

    assert result == first_page + second_page
    assert calls == [(0, 59_999_999), (60_000_000, 60_059_999)]


def test_load_to_bigquery_raw_table_retries_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [
        {
            "symbol": "BTCUSDT",
            "open_time": 1710000000000,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100.0,
            "close_time": 1710000059999,
        }
    ]
    calls = {"count": 0}

    class DummyJob:
        def result(self) -> None:
            return None

    class DummyClient:
        def load_table_from_json(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls["count"] += 1
            if calls["count"] < 2:
                raise RuntimeError("temporary bq failure")
            return DummyJob()

    class DummyBigQueryModule:
        class SourceFormat:
            NEWLINE_DELIMITED_JSON = "NEWLINE_DELIMITED_JSON"

        class WriteDisposition:
            WRITE_APPEND = "WRITE_APPEND"

        class LoadJobConfig:
            def __init__(self, source_format: str, write_disposition: str) -> None:
                self.source_format = source_format
                self.write_disposition = write_disposition

        @staticmethod
        def Client(project=None):  # type: ignore[no-untyped-def]
            return DummyClient()

    google_module = types.ModuleType("google")
    cloud_module = types.ModuleType("google.cloud")
    bigquery_module = DummyBigQueryModule()
    cloud_module.bigquery = bigquery_module
    google_module.cloud = cloud_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)
    monkeypatch.setattr("ingestion.batch.binance_batch_ingest.time.sleep", lambda _: None)

    loaded_count = load_to_bigquery_raw_table("project.dataset.raw_klines", records)

    assert loaded_count == 1
    assert calls["count"] == 2


def test_upload_jsonl_to_gcs_retries_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    class DummyBlob:
        def upload_from_string(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            calls["count"] += 1
            if calls["count"] < 2:
                raise RuntimeError("temporary gcs upload failure")

    class DummyBucket:
        def blob(self, _object_path: str) -> DummyBlob:
            return DummyBlob()

    class DummyClient:
        def bucket(self, _bucket_name: str) -> DummyBucket:
            return DummyBucket()

    class DummyStorageModule:
        @staticmethod
        def Client(project=None):  # type: ignore[no-untyped-def]
            return DummyClient()

    google_module = types.ModuleType("google")
    cloud_module = types.ModuleType("google.cloud")
    storage_module = DummyStorageModule()
    cloud_module.storage = storage_module
    google_module.cloud = cloud_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_module)
    monkeypatch.setattr("ingestion.batch.binance_batch_ingest.time.sleep", lambda _: None)

    uri = upload_jsonl_to_gcs("my-bucket", "raw/klines.jsonl", '{"x":1}\n')

    assert uri == "gs://my-bucket/raw/klines.jsonl"
    assert calls["count"] == 2


def test_run_batch_pipeline_orchestrates_steps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [{"symbol": "BTCUSDT", "interval": "1m", "open_time": 1, "ingest_time": 2}]
    calls = {"extract": 0, "jsonl": 0, "upload": 0, "load": 0}

    def fake_extract(**kwargs):  # type: ignore[no-untyped-def]
        calls["extract"] += 1
        return records, "raw/batch/2024/03/09/test.jsonl"

    def fake_jsonl(input_records):  # type: ignore[no-untyped-def]
        calls["jsonl"] += 1
        assert input_records == records
        return '{"symbol":"BTCUSDT"}\n'

    def fake_upload(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls["upload"] += 1
        return "gs://my-bucket/raw/batch/2024/03/09/test.jsonl"

    def fake_load(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls["load"] += 1
        return 1

    monkeypatch.setattr("ingestion.batch.binance_batch_ingest.run_batch_extract_transform", fake_extract)
    monkeypatch.setattr("ingestion.batch.binance_batch_ingest.records_to_jsonl", fake_jsonl)
    monkeypatch.setattr("ingestion.batch.binance_batch_ingest.upload_jsonl_to_gcs", fake_upload)
    monkeypatch.setattr("ingestion.batch.binance_batch_ingest.load_to_bigquery_raw_table", fake_load)

    result = run_batch_pipeline(
        symbol="BTCUSDT",
        interval="1m",
        start_time_ms=1710000000000,
        end_time_ms=1710003600000,
        bucket_name="my-bucket",
        table_id="proj.raw.klines",
    )

    assert result == {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "record_count": 1,
        "object_name": "raw/batch/2024/03/09/test.jsonl",
        "gcs_uri": "gs://my-bucket/raw/batch/2024/03/09/test.jsonl",
        "loaded_rows": 1,
    }
    assert calls == {"extract": 1, "jsonl": 1, "upload": 1, "load": 1}
