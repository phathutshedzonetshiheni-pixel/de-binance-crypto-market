# Looker Studio Dashboard Deliverable

This folder tracks the dashboard deliverable for crypto market reliability and volatility.

## Required Tiles

- **KPI scorecards:** peak volatility, peak hourly return, top average price, and top volume share.
- **Categorical comparison:** volume share by symbol (daily).
- **Temporal trends:** volatility and average price by hour with symbol breakdown.

## Data Sources (dbt Marts)

- **`mart_volume_distribution_daily`**
  - Fields: `trade_date`, `symbol`, `total_volume`, `market_total_volume`, `volume_share`
  - Suggested tile mapping:
    - Category: `symbol`
    - Metric: `sum(total_volume)` and/or `avg(volume_share)`
    - Time filter: last 1 day (24h proxy) and last 7 days

- **`mart_price_volatility_hourly`**
  - Fields: `event_hour`, `symbol`, `volatility_24h`, `hourly_return`, `avg_price`
  - Suggested tile mapping:
    - Time dimension: `event_hour`
    - Series/breakdown: `symbol`
    - Metric: `volatility_24h`

## Exact Looker Studio Configuration (5 Blocks)

Use a single page with these defaults:
- **Default date range:** last 30 days
- **Filters:** date range control + symbol filter control (multi-select, default all)
- **Timezone:** UTC

1) **KPI Scorecards (4 Cards)**
- **Data source:** `mart_price_volatility_hourly` for cards A-C, `mart_volume_distribution_daily` for card D
- **Card A (Most Volatile Symbol):**
  - Chart: Scorecard
  - Metric: `volatility_24h` (Aggregation: `MAX`)
  - Optional secondary metric: `symbol` via calculated field `symbol_of_max_volatility` using `CASE` in blended table
  - Sort: `volatility_24h DESC`
- **Card B (Highest Avg Hourly Return):**
  - Metric: `hourly_return` (Aggregation: `MAX`)
  - Sort: `hourly_return DESC`
- **Card C (Highest Avg Price):**
  - Metric: `avg_price` (Aggregation: `MAX`)
  - Sort: `avg_price DESC`
- **Card D (Top Symbol by Volume Share):**
  - Metric: `volume_share` (Aggregation: `MAX`)
  - Sort: `volume_share DESC`

2) **Volume Share by Symbol (Comparison)**
- **Data source:** `mart_volume_distribution_daily`
- **Chart:** Bar chart (horizontal, sorted)
- **Dimension:** `symbol`
- **Metric:** `volume_share`
- **Aggregation:** `AVG(volume_share)`
- **Sort:** `AVG(volume_share) DESC`
- **Date dimension:** `trade_date`
- **Style:** show percent axis and data labels

3) **Average Price Trend by Hour (Comparison)**
- **Data source:** `mart_price_volatility_hourly`
- **Chart:** Time series (multi-line)
- **Dimension:** `event_hour`
- **Breakdown dimension:** `symbol`
- **Metric:** `avg_price`
- **Aggregation:** `AVG(avg_price)`
- **Sort:** `event_hour ASC`
- **Date dimension:** `event_hour`

4) **Volatility Trend by Hour (Comparison)**
- **Data source:** `mart_price_volatility_hourly`
- **Chart:** Time series (multi-line)
- **Dimension:** `event_hour`
- **Breakdown dimension:** `symbol`
- **Metric:** `volatility_24h`
- **Aggregation:** `AVG(volatility_24h)`
- **Sort:** `event_hour ASC`
- **Date dimension:** `event_hour`

5) **Symbol Comparison Table (Compact Decision Table)**
- **Data source:** `mart_price_volatility_hourly` (primary) + `mart_volume_distribution_daily` (optional blend for volume share)
- **Chart:** Table
- **Dimension:** `symbol`
- **Metrics (recommended):**
  - `AVG(avg_price)`
  - `AVG(hourly_return)`
  - `AVG(volatility_24h)`
  - `AVG(volume_share)` (from blend or second table using `mart_volume_distribution_daily`)
- **Sort:** `AVG(volatility_24h) DESC`, then `AVG(hourly_return) DESC`
- **Style:** enable heatmap/conditional formatting on volatility and return columns

## Submission Evidence (Screenshots)

Saved under `dashboard/screenshots/` for peer review and the rubric.

| File | Tile / Purpose |
|------|----------------|
| `binance-architecture-pipeline.png` | **Architecture visual:** detailed platform view (sources, ingestion, bronze/silver/gold, serving, and control plane). |
| `tile-dashboard-volume-share-and-price-trend.png` | **Full dashboard (latest):** KPI scorecards + symbol ranking table + symbol filter + daily volume share comparison chart. |
| `tile-average-price-trend-by-hour.png` | **Trend panel (latest):** Date/symbol controls + volatility trend by hour + average price trend by hour + daily volume share chart. |

## Finalization Checklist

- [x] Confirm Looker Studio dashboard URL is final and accessible to reviewers.
- [x] Add dashboard URL to this file under a `Dashboard URL` section.
- [x] Export and save screenshots for required tiles into `dashboard/screenshots/`.
- [x] Name screenshots clearly (see table above).
- [x] Verify screenshot paths render on GitHub before submission.

## Dashboard URL

- [de-binance-crypto-report](https://datastudio.google.com/reporting/0dfef084-7d0a-4f63-badf-b9978a9179e6)
