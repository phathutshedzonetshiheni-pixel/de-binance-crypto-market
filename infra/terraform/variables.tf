variable "project_id" {
  description = "GCP project ID where resources will be provisioned."
  type        = string

  validation {
    condition     = length(trimspace(var.project_id)) > 0
    error_message = "project_id must be a non-empty string."
  }
}

variable "region" {
  description = "GCP region for regional resources."
  type        = string

  validation {
    condition     = length(trimspace(var.region)) > 0
    error_message = "region must be a non-empty string."
  }
}

variable "bucket_name" {
  description = "Name of the GCS bucket for raw market data."
  type        = string

  validation {
    condition     = length(trimspace(var.bucket_name)) > 0
    error_message = "bucket_name must be a non-empty string."
  }
}

variable "dataset_raw" {
  description = "BigQuery dataset ID for raw ingested data."
  type        = string

  validation {
    condition     = length(trimspace(var.dataset_raw)) > 0
    error_message = "dataset_raw must be a non-empty string."
  }
}

variable "dataset_analytics" {
  description = "BigQuery dataset ID for analytics-ready tables."
  type        = string

  validation {
    condition     = length(trimspace(var.dataset_analytics)) > 0
    error_message = "dataset_analytics must be a non-empty string."
  }
}

variable "topic_name" {
  description = "Pub/Sub topic name for trade events."
  type        = string

  validation {
    condition     = length(trimspace(var.topic_name)) > 0
    error_message = "topic_name must be a non-empty string."
  }
}

variable "subscription_name" {
  description = "Pub/Sub subscription name for trade event consumers."
  type        = string

  validation {
    condition     = length(trimspace(var.subscription_name)) > 0
    error_message = "subscription_name must be a non-empty string."
  }
}
