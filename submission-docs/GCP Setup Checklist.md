# GCP Setup Checklist (Fill-In Template)

Use this checklist while completing manual cloud setup.  
Do not commit secrets or raw credential files.

## Legend

- Status: `Not started` | `In progress` | `Done` | `Blocked`

---

## 1) Project and Billing

- [ ] **GCP project selected/created**
  - Status:
  - Project name:
  - Project ID:
  - Verified by:
  - Notes:

- [ ] **Billing linked to project**
  - Status:
  - Billing account name/id:
  - Verified by:
  - Notes:

---

## 2) Required APIs Enabled

- [ ] **BigQuery API**
  - Status:
  - Verification command/output:
  - Notes:

- [ ] **Cloud Storage API**
  - Status:
  - Verification command/output:
  - Notes:

- [ ] **Pub/Sub API**
  - Status:
  - Verification command/output:
  - Notes:

- [ ] **IAM Credentials API**
  - Status:
  - Verification command/output:
  - Notes:

---

## 3) Authentication Setup

- [ ] **`gcloud` installed**
  - Status:
  - Version (`gcloud --version`):
  - Notes:

- [ ] **User login complete (`gcloud auth login`)**
  - Status:
  - Active account:
  - Notes:

- [ ] **Project configured (`gcloud config set project`)**
  - Status:
  - Active project:
  - Notes:

- [ ] **Application default credentials (`gcloud auth application-default login`)**
  - Status:
  - Verified by:
  - Notes:

---

## 4) Service Account and IAM

- [ ] **Service account created**
  - Status:
  - Service account email:
  - Notes:

- [ ] **IAM roles assigned**
  - Status:
  - Roles granted:
    - [ ] `roles/storage.admin`
    - [ ] `roles/bigquery.admin`
    - [ ] `roles/pubsub.admin`
    - [ ] `roles/iam.serviceAccountTokenCreator` (optional)
  - Verified by:
  - Notes:

- [ ] **Service account key strategy decided**
  - Status:
  - Choice: `OAuth/ADC only` | `JSON key file`
  - Key file path (if used, local only):
  - Notes:

---

## 5) Repository Configuration

- [ ] **`.env` created from `.env.example`**
  - Status:
  - Verified by:
  - Notes:

- [ ] **`.env` values filled**
  - Status:
  - `GCP_PROJECT_ID`:
  - `GCP_REGION`:
  - `GCS_BUCKET`:
  - `BQ_DATASET_RAW`:
  - `BQ_DATASET_ANALYTICS`:
  - `BQ_RAW_STREAM_TABLE`:
  - `PUBSUB_TOPIC`:
  - `PUBSUB_SUBSCRIPTION`:
  - `BINANCE_SYMBOLS`:
  - `BATCH_INTERVAL`:
  - Notes:

- [ ] **No secrets committed**
  - Status:
  - Quick check performed:
  - Notes:

---

## 6) Terraform Setup

- [ ] **`terraform.tfvars` created from example**
  - Status:
  - File path:
  - Notes:

- [ ] **Terraform init successful**
  - Status:
  - Command:
  - Result:
  - Notes:

- [ ] **Terraform plan successful**
  - Status:
  - Command:
  - Result:
  - Notes:

- [ ] **Terraform apply successful**
  - Status:
  - Command:
  - Result:
  - Notes:

---

## 7) dbt Setup

- [ ] **`transform/profiles.yml` created**
  - Status:
  - Source template:
  - Notes:

- [ ] **dbt parse successful**
  - Status:
  - Command:
  - Result:
  - Notes:

- [ ] **dbt deps successful (credentialed)**
  - Status:
  - Command:
  - Result:
  - Notes:

- [ ] **dbt test successful (credentialed)**
  - Status:
  - Command:
  - Result:
  - Notes:

---

## 8) End-to-End Readiness Gate

- [ ] **Batch ingestion can run**
  - Status:
  - Command:
  - Result:
  - Notes:

- [ ] **Stream producer/consumer can run**
  - Status:
  - Commands:
  - Result:
  - Notes:

- [ ] **BigQuery tables receiving data**
  - Status:
  - Verified by query/screenshot:
  - Notes:

- [ ] **Ready to move to dashboard evidence**
  - Status:
  - Notes:

