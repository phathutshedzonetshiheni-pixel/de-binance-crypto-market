# Looker Studio Dashboard Deliverable

This folder tracks the dashboard deliverable for crypto market reliability and volatility.

## Required tiles

- **Categorical distribution:** volume share by symbol for trailing `24h` and `7d`.
- **Temporal trend:** volatility over time by symbol.

## Data sources (dbt marts)

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

## Submission evidence (screenshots)

Saved under `dashboard/screenshots/` for peer review and the rubric.

| File | Tile / purpose |
|------|----------------|
| `tile-dashboard-volume-share-and-price-trend.png` | **Full dashboard:** Volume Share by Symbol (Daily) + Average Price Trend by Hour (categorical + temporal in one capture). |
| `tile-average-price-trend-by-hour.png` | **Temporal (detail):** Average price by hour only. |

## Finalization checklist

- [x] Confirm Looker Studio dashboard URL is final and accessible to reviewers.
- [x] Add dashboard URL to this file under a `Dashboard URL` section.
- [x] Export and save screenshots for required tiles into `dashboard/screenshots/`.
- [x] Name screenshots clearly (see table above).
- [ ] Verify screenshot paths render on GitHub before submission.

## Dashboard URL

- [de-binance-crypto-report](https://datastudio.google.com/reporting/0dfef084-7d0a-4f63-badf-b9978a9179e6)
