# Manual Setup Guide: GCP Info and Credentials

This guide walks you through all manual setup needed before running Terraform, ingestion, and dbt with real GCP resources.

## What You Need to Collect

Before running the pipeline, you need these values:

- GCP project ID
- GCP region
- Billing-enabled project
- A service account with required permissions
- Service account key JSON (or local ADC auth)
- Enabled APIs
- Values for `.env` and Terraform variables
- dbt profile auth setup

---

## 1) Create or Select a GCP Project

1. Open [Google Cloud Console](https://console.cloud.google.com/).
2. In the top project selector, create a new project (or pick an existing one).
3. Copy the **Project ID** exactly.
4. Put it in:
   - `.env` -> `GCP_PROJECT_ID`
   - Terraform vars (`project_id`)

Notes:
- Use a unique project ID (global namespace).
- Keep one dedicated project for this capstone to avoid confusion.

---

## 2) Enable Billing on the Project

1. Go to **Billing** in GCP Console.
2. Link a billing account to your selected project.
3. Confirm billing is active for that project.

Why this matters:
- BigQuery jobs and Pub/Sub usage can fail with permission/quota errors if billing is not linked.

---

## 3) Enable Required GCP APIs

Enable these APIs for your project:

- BigQuery API
- Cloud Storage API
- Pub/Sub API
- IAM Service Account Credentials API (recommended)

Quick method (Cloud Shell or local `gcloud`):

```bash
gcloud services enable bigquery.googleapis.com storage.googleapis.com pubsub.googleapis.com iamcredentials.googleapis.com --project <YOUR_PROJECT_ID>
```

---

## 4) Install and Authenticate `gcloud`

If `gcloud` is not installed:
- Install Google Cloud SDK: [Install Guide](https://cloud.google.com/sdk/docs/install)

Authenticate:

```bash
gcloud auth login
gcloud config set project <YOUR_PROJECT_ID>
gcloud auth application-default login
```

`application-default login` helps local tools (including dbt with oauth method) access BigQuery.

---

## 5) Create a Service Account (Recommended)

Create one service account dedicated to this project (example name: `de-zoomcamp-runner`).

Required roles (minimum practical set):

- `roles/storage.admin` (or narrower if you prefer)
- `roles/bigquery.admin` (or dataset/job-scoped alternatives)
- `roles/pubsub.admin`
- `roles/iam.serviceAccountTokenCreator` (optional, useful in some auth flows)

CLI example:

```bash
gcloud iam service-accounts create de-zoomcamp-runner --display-name "DE Zoomcamp Runner" --project <YOUR_PROJECT_ID>
```

Then bind roles to that service account.

---

## 6) Create and Store Service Account Key (Optional if using OAuth locally)

If you want JSON-key auth for scripts/dbt:

```bash
gcloud iam service-accounts keys create "gcp-key.json" --iam-account de-zoomcamp-runner@<YOUR_PROJECT_ID>.iam.gserviceaccount.com --project <YOUR_PROJECT_ID>
```

Important:
- Never commit key files.
- Keep keys outside the repo when possible.
- If leaked, revoke immediately.

For this project, you can use either:
- OAuth/local ADC (recommended for local dev), or
- Service account JSON.

---

## 7) Fill `.env` from `.env.example`

Copy:

```bash
cp .env.example .env
```

On PowerShell:

```powershell
Copy-Item .env.example .env
```

Set at least:

- `GCP_PROJECT_ID=<YOUR_PROJECT_ID>`
- `GCP_REGION=us-central1` (or your chosen region)
- `GCS_BUCKET=<UNIQUE_BUCKET_NAME>`
- `BQ_DATASET_RAW=crypto_raw`
- `BQ_DATASET_ANALYTICS=crypto_analytics`
- `BQ_RAW_STREAM_TABLE=<YOUR_PROJECT_ID>.crypto_raw.raw_stream_trades`
- `PUBSUB_TOPIC=crypto-trades-topic`
- `PUBSUB_SUBSCRIPTION=crypto-trades-sub`

Keep symbols/interval as needed:
- `BINANCE_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT`
- `BATCH_INTERVAL=1m` (must match the kline interval in `raw_batch_ohlc` and dbt staging filters)

---

## 8) Prepare Terraform Variables

Create `infra/terraform/terraform.tfvars` from example:

```bash
cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
```

Fill values:

- `project_id`
- `region`
- `bucket_name`
- `dataset_raw`
- `dataset_analytics`
- `topic_name`
- `subscription_name`

Then validate:

```bash
terraform -chdir=infra/terraform init
terraform -chdir=infra/terraform plan -var-file=terraform.tfvars
```

---

## 9) Prepare dbt Profile

Copy profile template:

```bash
cp transform/profiles.yml.example transform/profiles.yml
```

Auth options:

- **OAuth/ADC**: use `gcloud auth application-default login`
- **Service account JSON**: set `GOOGLE_APPLICATION_CREDENTIALS_JSON` if profile uses json content

Sanity check (no model run yet):

```bash
dbt parse --project-dir transform --profiles-dir transform
```

---

## 10) Verify Credentials and Permissions

Run quick checks:

```bash
terraform -chdir=infra/terraform validate
dbt parse --project-dir transform --profiles-dir transform
```

Then credentialed checks:

```bash
dbt deps --project-dir transform --profiles-dir transform
dbt test --project-dir transform --profiles-dir transform
```

If `dbt run/test` fails with permission errors:
- confirm billing is enabled
- confirm active project is correct
- confirm service account roles include BigQuery job execution permissions

---

## Troubleshooting Checklist

- Wrong project ID in `.env` or Terraform vars
- Billing not enabled on the selected project
- APIs not enabled
- Missing IAM roles for service account/user
- Bucket name not globally unique
- `profiles.yml` not in `transform/` (or wrong `--profiles-dir`)
- Local shell using stale env vars

