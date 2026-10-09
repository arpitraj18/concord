# ---------- Resolver: a Lambda run every minute by EventBridge Scheduler ----------

variable "resolver_jar" {
  description = "Path to the shaded resolver jar (mvn package)."
  type        = string
  default     = "../../resolver/target/resolver-0.1.0-SNAPSHOT-lambda.jar"
}

variable "gateway_status_urls" {
  description = "Status API base URL per gateway (a, b, c)."
  type        = map(string)
  default     = {}
}

variable "gateway_api_keys" {
  description = "Read-only status API key per gateway. Concord never holds keys that can move money."
  type        = map(string)
  sensitive   = true
  default     = {}
}

variable "merchant_orders_url" {
  description = "Merchant order adapter base URL (GET /orders/{id})."
  type        = string
  default     = "http://localhost:8090"
}

resource "aws_cloudwatch_log_group" "resolver" {
  name              = "/aws/lambda/${local.name}-resolver"
  retention_in_days = 7
}

resource "aws_iam_role" "resolver" {
  name = "${local.name}-resolver"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "resolver" {
  name = "resolver"
  role = aws_iam_role.resolver.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:PutItem", "dynamodb:ConditionCheckItem"]
        Resource = [aws_dynamodb_table.payments.arn, "${aws_dynamodb_table.payments.arn}/index/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.resolver.arn}:*"
      }
    ]
  })
}

resource "aws_lambda_function" "resolver" {
  function_name    = "${local.name}-resolver"
  role             = aws_iam_role.resolver.arn
  runtime          = "java21"
  handler          = "dev.concord.resolver.ResolverLambda::handleRequest"
  filename         = var.resolver_jar
  source_code_hash = filebase64sha256(var.resolver_jar)
  memory_size      = 1024
  timeout          = 55
  architectures    = ["arm64"]

  # One sweep at a time: overlapping sweeps would only duplicate status-API calls.
  reserved_concurrent_executions = 1

  environment {
    variables = merge(
      {
        PAYMENTS_TABLE      = aws_dynamodb_table.payments.name
        MERCHANT_ORDERS_URL = var.merchant_orders_url
      },
      { for g, url in var.gateway_status_urls : "GATEWAY_${upper(g)}_STATUS_URL" => url },
      { for g, key in var.gateway_api_keys : "GATEWAY_${upper(g)}_API_KEY" => key }
    )
  }

  depends_on = [aws_cloudwatch_log_group.resolver]
}

resource "aws_iam_role" "scheduler" {
  name = "${local.name}-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "invoke-resolver"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.resolver.arn
    }]
  })
}

resource "aws_scheduler_schedule" "resolver" {
  count               = var.localstack ? 0 : 1
  name                = "${local.name}-resolver-sweep"
  schedule_expression = "rate(1 minute)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.resolver.arn
    role_arn = aws_iam_role.scheduler.arn
  }
}

# ---------- Delivery: dead-letter queue and the worker's permissions (ECS task in Phase 6) ----------

resource "aws_sqs_queue" "delivery_dlq" {
  name                      = "${local.name}-delivery-dlq"
  message_retention_seconds = 1209600
}

data "aws_iam_policy_document" "delivery_worker" {
  statement {
    actions   = ["dynamodb:DescribeStream", "dynamodb:GetShardIterator", "dynamodb:GetRecords", "dynamodb:ListStreams"]
    resources = [aws_dynamodb_table.payments.stream_arn]
  }
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.delivery_dlq.arn]
  }
}

resource "aws_iam_policy" "delivery_worker" {
  name   = "${local.name}-delivery-worker"
  policy = data.aws_iam_policy_document.delivery_worker.json
}
