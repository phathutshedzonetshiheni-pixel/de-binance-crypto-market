# Crypto Market Reliability & Volatility Dashboard - Design

## 1) Problem Statement and Goals

This project builds an end-to-end data engineering pipeline on GCP to monitor crypto market behavior with both historical and near-real-time data.

Primary goals:
- Demonstrate hybrid ingestion (batch + stream) in one coherent architecture.
- Build a warehouse model optimized for analytical queries.
- Produce a dashboard with at least two meaningful tiles for distribution and temporal trends.
- Keep the stack open-source/free-first and reproducible for peer review.

Success criteria:
- End-to-end flow runs from source to dashboard.
- Project satisfies Zoomcamp rubric categories with evidence in code and README.
- Setup and run instructions work from a clean clone.

## 2) Scope and Non-Goals

### In scope
- GCP storage and warehouse setup (GCS + BigQuery).
- Batch ingestion for historical market data.
- Streaming ingestion for live trades.
- dbt Core models and tests.
- Looker Studio dashboard with required tiles.
- Basic quality, testing, and CI checks.

### Out of scope
- Low-latency trading systems.
- Multi-region high-availability production deployment.
- Advanced ML forecasting pipelines.

## 3) Data Sources and Domain Rationale

Dataset choice: Binance public market data (historical + live stream).

Rationale:
- Naturally supports both batch and streaming ingestion modes.
- Industry relevance for event-driven and time-series analytics patterns.
- High event volume helps demonstrate partitioning, clustering, and incremental processing.

Initial symbol set:
- BTCUSDT
- ETHUSDT
- SOLUSDT

The symbol set is intentionally small for one-week delivery and predictable costs.

## 4) High-Level Architecture

### Batch path
1. Python ingestion job fetches historical OHLC/trade data.
2. Files are written to GCS raw zone with date-based paths.
3. Load job writes to BigQuery raw batch tables.
4. dbt run/test materializes staging and marts.

### Streaming path
1. Python websocket producer receives live trades from Binance.
2. Events are published to Pub/Sub topic.
3. Consumer job writes micro-batches to BigQuery raw stream table.
4. Incremental dbt model refreshes dashboard-ready marts.

### Serving path
1. Looker Studio connects to BigQuery marts.
2. Dashboard visualizes categorical and temporal distributions.

## 5) Technology Stack (Free/Open-Source First)

- Cloud: GCP
- Data lake: GCS
- Data warehouse: BigQuery
- Orchestration: Prefect OSS (Airflow OSS is acceptable substitute)
- Streaming transport: Pub/Sub
- Transformations: dbt Core
- Dashboard: Looker Studio
- IaC: Terraform OSS
- Language: Python + SQL
- CI: GitHub Actions

Why this stack:
- Aligns with approved project constraints and one-week timeline.
- Matches common industry data platform patterns.
- Balances operational simplicity with modern DE skill coverage.

## 6) Data Model Design

### Raw tables
- `raw_batch_ohlc`
  - symbol, interval, open, high, low, close, volume, open_time, close_time, ingest_time
- `raw_stream_trades`
  - symbol, trade_id, price, qty, event_time, ingest_time, source_ts

### Staging models (dbt)
- Type normalization and timestamp standardization to UTC.
- Deduplication by natural keys (`symbol`, `trade_id`, `event_time` where applicable).
- Basic data quality guards (non-null and uniqueness tests).

### Mart models (dbt)
- `mart_volume_distribution_daily`
  - daily volume aggregates and share by symbol.
- `mart_price_volatility_hourly`
  - hourly returns and rolling volatility metrics.

## 7) Warehouse Optimization Strategy

BigQuery table optimization:
- Partition by event date / close date.
- Cluster by symbol (and interval for OHLC when needed).

Expected benefit:
- Lower scan volume for date/symbol filtered dashboard queries.
- Better query performance and reduced costs.

## 8) Orchestration and Scheduling

Prefect flows:
- `batch_ingestion_flow` (daily schedule)
  - extract -> land in GCS -> load BigQuery -> trigger dbt.
- `stream_ingestion_flow` (continuous or frequent schedule)
  - consume events -> batch writes -> freshness checks.
- `transform_flow` (hourly or on-demand)
  - dbt run + dbt test.

Operational expectations:
- Retry with exponential backoff for external API/websocket failures.
- Structured logs for each task.
- Alerting hooks can be lightweight (log-based in v1).

## 9) Data Quality and Reliability

Minimum reliability controls:
- Idempotent batch loads (use deterministic file/object names and merge-safe loads).
- Duplicate protection for stream writes (dedupe logic in staging).
- Freshness checks for stream lag and batch completion.
- Graceful handling of upstream API issues.

Minimum dbt tests:
- Not null on required keys and timestamps.
- Unique test on trade identifiers where valid.
- Accepted values tests for known symbols.

## 10) Dashboard Specification

Tool: Looker Studio.

Required tiles:
1. Categorical distribution tile:
   - "Volume share by symbol (24h/7d)" as bar or donut chart.
2. Temporal distribution tile:
   - "Volatility trend over time" as line chart.

Optional enhancements:
- Symbol and date-range filters.
- Volatility spike threshold marker.

## 11) Repository and Reproducibility Requirements

Target structure:
- `ingestion/` batch and stream jobs
- `orchestration/` Prefect flows
- `transform/` dbt project
- `infra/terraform/` GCP resources
- `dashboard/` screenshots and notes
- `tests/` ingestion and transformation tests
- `Makefile` common commands
- `.env.example` configuration template

Reproducibility checklist:
- Clear prerequisites and setup instructions.
- No hardcoded local paths or personal project IDs.
- Run instructions validated from clean clone.

## 12) CI and Optional Extra-Mile Features

Planned optional additions:
- Unit tests (pytest) for ingestion utilities.
- Make targets for setup/run/test/lint.
- GitHub Actions pipeline:
  - lint + unit tests
  - dbt dependency install and tests

## 13) Risks and Mitigations

- Streaming instability from external source:
  - Mitigation: reconnect logic + buffered writes + health checks.
- Scope overrun within one week:
  - Mitigation: strict MVP boundaries and small symbol set.
- Unexpected GCP cost:
  - Mitigation: partition pruning, query limits, low-frequency refresh during development.

## 14) Delivery Plan (1 Week)

- Day 1: Scaffold repo + Terraform + GCS/BigQuery setup.
- Day 2: Batch ingestion path end-to-end.
- Day 3: Streaming ingestion path end-to-end.
- Day 4: dbt staging and marts + tests.
- Day 5: Looker Studio dashboard completion.
- Day 6: CI, tests, Makefile, hardening.
- Day 7: README polish and clean-clone validation.

## 15) Rubric Mapping

- Problem description: documented in this design and README.
- Cloud: GCP resources provisioned and used.
- Ingestion: hybrid batch and streaming workflows.
- Data warehouse: optimized BigQuery schema and table design.
- Transformations: dbt Core models/tests.
- Dashboard: two required tiles in Looker Studio.
- Reproducibility: complete setup/run guide and validation steps.

## 16) Final Decisions Locked for Implementation

- Dataset/domain: Binance crypto market data.
- Cloud: GCP.
- Architecture: hybrid batch + stream.
- Transformation framework: dbt Core.
- Dashboard: Looker Studio.
- Orchestration: Prefect OSS (Airflow OSS fallback if needed).

