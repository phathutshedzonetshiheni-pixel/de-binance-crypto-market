# Docker Guide

Run the project services with Docker Compose from the repository root.

## Quick Start

1. Copy env template:
   - `cp .env.example .env`
2. Fill required values in `.env` (`GCP_*`, `PUBSUB_*`, `BQ_*`, etc.).
3. Add your GCP service-account key under `docker/` (details below).
4. Build the image:
   - `docker compose build`

## Compose Profiles

- CI tests:
  - `docker compose --profile ci run --rm test`
- Batch ingestion:
  - `docker compose --profile batch run --rm batch`
- Streaming (producer + consumer):
  - `docker compose --profile stream up`
- dbt run:
  - `docker compose --profile dbt run --rm dbt`

## GCP Credentials in Containers

The host `docker/` folder is mounted read-only at `/run/gcp` in each service container.

- Default in-container path:
  - `/run/gcp/service-account.json`
- Override via `.env`:
  - `GCP_CREDENTIALS_CONTAINER_PATH=/run/gcp/...`

### Recommended Host Layout

Store the key as:

- `docker/service-account.json`

Then no `.env` override is needed.

### Nested Host Layout (also supported)

If your key is inside a subfolder, for example:

- `docker/sa.json/binance-crypto-market-e25d3ad95776.json`

set:

- `GCP_CREDENTIALS_CONTAINER_PATH=/run/gcp/sa.json/binance-crypto-market-e25d3ad95776.json`

## Windows Check (File vs Folder)

If auth fails with file/path errors, verify your key path is a file:

- `Get-Item .\docker\service-account.json`
- `Get-Item ".\docker\sa.json\binance-crypto-market-e25d3ad95776.json"`

In PowerShell, the key should have file mode like `-a---`, not directory mode like `d----`.

## Notes

- Keep keys out of Git (already ignored by `.gitignore`).
- Prefer `docker/service-account.json` to avoid accidental folder naming issues on Windows.
