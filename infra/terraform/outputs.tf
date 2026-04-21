output "bucket_name" {
  value = google_storage_bucket.raw_bucket.name
}

output "raw_dataset" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "analytics_dataset" {
  value = google_bigquery_dataset.analytics.dataset_id
}

output "topic" {
  value = google_pubsub_topic.trades.name
}

output "subscription" {
  value = google_pubsub_subscription.trades_sub.name
}
