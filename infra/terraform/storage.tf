locals {
  name = "concord-${var.environment}"
}

data "aws_caller_identity" "current" {}

# ---------- Raw event archive (S3) ----------
# Every delivery is stored forever under raw/; rejected (bad signature) payloads expire after 30 days.

resource "aws_s3_bucket" "raw" {
  bucket        = "${local.name}-raw-events-${data.aws_caller_identity.current.account_id}"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "expire-rejected"
    status = "Enabled"
    filter {
      prefix = "rejected/"
    }
    expiration {
      days = 30
    }
  }
}

# ---------- Dedup table (DynamoDB) ----------

resource "aws_dynamodb_table" "dedup" {
  name         = "${local.name}-dedup"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "dedup_key"

  attribute {
    name = "dedup_key"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# ---------- Event queue (SQS FIFO + DLQ) ----------
# MessageGroupId = gateway#payment_id gives per-payment order; dedup ids are set explicitly by ingestion.

resource "aws_sqs_queue" "events_dlq" {
  name                      = "${local.name}-events-dlq.fifo"
  fifo_queue                = true
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "events" {
  name                        = "${local.name}-events.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  deduplication_scope         = "messageGroup"
  fifo_throughput_limit       = "perMessageGroupId"
  visibility_timeout_seconds  = 60
  message_retention_seconds   = 345600

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.events_dlq.arn
    maxReceiveCount     = 5
  })
}
