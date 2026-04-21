terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "raw_bucket" {
  name     = var.bucket_name
  location = var.region
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = var.dataset_raw
  location   = var.region
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = var.dataset_analytics
  location   = var.region
}

resource "google_pubsub_topic" "trades" {
  name = var.topic_name
}

resource "google_pubsub_subscription" "trades_sub" {
  name  = var.subscription_name
  topic = google_pubsub_topic.trades.name
}
