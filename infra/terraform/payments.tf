# ---------- Payment state store (DynamoDB, single table) ----------
# PK = PAY#<gateway>#<paymentId>; SK = STATE | EVT#<dedupKey> | OUTBOX#<version>
# GSI1 (gsi1pk = STATE#<state>, gsi1sk = updated_at) finds payments stuck in a state.
# GSI2 (sparse: gsi2pk = VERIFY | PARKED) holds only payments needing attention.
# Index sort keys are fixed-width timestamps (9 fraction digits) so text order = time order.
# The stream carries OUTBOX items to the Phase 3 delivery worker (change data capture).

resource "aws_dynamodb_table" "payments" {
  name         = "${local.name}-payments"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "gsi1pk"
    type = "S"
  }

  attribute {
    name = "gsi1sk"
    type = "S"
  }

  attribute {
    name = "gsi2pk"
    type = "S"
  }

  attribute {
    name = "gsi2sk"
    type = "S"
  }

  global_secondary_index {
    name            = "gsi2"
    hash_key        = "gsi2pk"
    range_key       = "gsi2sk"
    projection_type = "ALL"
  }

  global_secondary_index {
    name            = "gsi1"
    hash_key        = "gsi1pk"
    range_key       = "gsi1sk"
    projection_type = "ALL"
  }

  stream_enabled   = true
  stream_view_type = "NEW_IMAGE"

  point_in_time_recovery {
    enabled = var.environment == "prod"
  }
}

# Policy for the state worker (attached to its ECS task role in Phase 6).
data "aws_iam_policy_document" "state_worker" {
  statement {
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.events.arn]
  }
  statement {
    actions   = ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:PutItem", "dynamodb:ConditionCheckItem"]
    resources = [aws_dynamodb_table.payments.arn, "${aws_dynamodb_table.payments.arn}/index/*"]
  }
}

resource "aws_iam_policy" "state_worker" {
  name   = "${local.name}-state-worker"
  policy = data.aws_iam_policy_document.state_worker.json
}
