output "webhook_base_url" {
  description = "Point each gateway's webhook URL at <this>/webhooks/<a|b|c>."
  value       = aws_apigatewayv2_api.webhooks.api_endpoint
}

output "raw_archive_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "dedup_table" {
  value = aws_dynamodb_table.dedup.name
}

output "events_queue_url" {
  value = aws_sqs_queue.events.url
}

output "events_dlq_url" {
  value = aws_sqs_queue.events_dlq.url
}
