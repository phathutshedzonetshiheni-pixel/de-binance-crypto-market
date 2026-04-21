.PHONY: setup test lint run-batch run-stream-producer run-stream-consumer dbt-run dbt-test

ifeq ($(OS),Windows_NT)
VENV_BIN := .venv/Scripts
else
VENV_BIN := .venv/bin
endif

VENV_PYTHON := $(VENV_BIN)/python

setup:
	python -m venv .venv
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -r requirements.txt

test:
	$(VENV_PYTHON) -m pytest -q

lint:
	$(VENV_PYTHON) -m py_compile ingestion/batch/binance_batch_ingest.py ingestion/stream/binance_ws_producer.py ingestion/stream/pubsub_to_bq_consumer.py orchestration/prefect_flows.py

run-batch:
	$(VENV_PYTHON) ingestion/batch/binance_batch_ingest.py

run-stream-producer:
	$(VENV_PYTHON) ingestion/stream/binance_ws_producer.py

run-stream-consumer:
	$(VENV_PYTHON) ingestion/stream/pubsub_to_bq_consumer.py

dbt-run:
	$(VENV_PYTHON) -m dbt run --project-dir transform --profiles-dir transform

dbt-test:
	$(VENV_PYTHON) -m dbt test --project-dir transform --profiles-dir transform
