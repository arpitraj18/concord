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

output "payments_table" {
  value = aws_dynamodb_table.payments.name
}

output "payments_stream_arn" {
  value = aws_dynamodb_table.payments.stream_arn
}

output "state_worker_policy_arn" {
  value = aws_iam_policy.state_worker.arn
}

output "delivery_dlq_url" {
  value = aws_sqs_queue.delivery_dlq.url
}

output "delivery_worker_policy_arn" {
  value = aws_iam_policy.delivery_worker.arn
}
