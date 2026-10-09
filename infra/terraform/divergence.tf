# ---------- Divergence detector: a Lambda run every 2 minutes by EventBridge Scheduler ----------
# Each run: read the merchant's change feed, run due checks, then the windowed sweep.

variable "detector_jar" {
  description = "Path to the shaded divergence detector jar (mvn package)."
  type        = string
  default     = "../../divergence/target/divergence-0.1.0-SNAPSHOT-lambda.jar"
}

resource "aws_cloudwatch_log_group" "detector" {
  name              = "/aws/lambda/${local.name}-detector"
  retention_in_days = 7
}

resource "aws_iam_role" "detector" {
  name = "${local.name}-detector"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "detector" {
  name = "detector"
  role = aws_iam_role.detector.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # reads payments, writes incidents / detector state, and can flag a payment for the resolver
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:PutItem", "dynamodb:ConditionCheckItem"]
        Resource = [aws_dynamodb_table.payments.arn, "${aws_dynamodb_table.payments.arn}/index/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.detector.arn}:*"
      }
    ]
  })
}

resource "aws_lambda_function" "detector" {
  function_name    = "${local.name}-detector"
  role             = aws_iam_role.detector.arn
  runtime          = "java21"
  handler          = "dev.concord.divergence.DetectorLambda::handleRequest"
  filename         = var.detector_jar
  source_code_hash = filebase64sha256(var.detector_jar)
  memory_size      = 1024
  timeout          = 110
  architectures    = ["arm64"]

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

  depends_on = [aws_cloudwatch_log_group.detector]
}

resource "aws_iam_role_policy" "scheduler_detector" {
  name = "invoke-detector"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.detector.arn
    }]
  })
}

resource "aws_scheduler_schedule" "detector" {
  count               = var.localstack ? 0 : 1
  name                = "${local.name}-divergence-sweep"
  schedule_expression = "rate(2 minutes)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.detector.arn
    role_arn = aws_iam_role.scheduler.arn
  }
}
