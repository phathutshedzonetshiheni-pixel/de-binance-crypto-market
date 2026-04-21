GCP service account (Docker Compose)

The host folder docker/ is mounted at /run/gcp in the container.

A — Recommended layout: one file next to this README:

   docker/service-account.json

B — Nested layout (your case): docker/sa.json is a folder and the key file is inside it, e.g.

   docker/sa.json/binance-crypto-market-e25d3ad95776.json

   Add to .env (forward slashes; path is inside the container):

   GCP_CREDENTIALS_CONTAINER_PATH=/run/gcp/sa.json/binance-crypto-market-e25d3ad95776.json

C — Verify a path is a file in PowerShell (Mode should be -a--- not d----):

   Get-Item .\docker\service-account.json
   Get-Item ".\docker\sa.json\binance-crypto-market-e25d3ad95776.json"
