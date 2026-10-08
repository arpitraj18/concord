# ---------- Ingest Lambda ----------

resource "aws_iam_role" "ingest" {
  name = "${local.name}-ingest"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Least privilege: write-only to the archive, item-level ops on one table, send-only to one queue.
resource "aws_iam_role_policy" "ingest" {
  name = "ingest"
  role = aws_iam_role.ingest.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${aws_s3_bucket.raw.arn}/raw/*", "${aws_s3_bucket.raw.arn}/rejected/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:GetItem"]
        Resource = aws_dynamodb_table.dedup.arn
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.ingest.arn}:*"
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/aws/lambda/${local.name}-ingest"
  retention_in_days = 7
}

resource "aws_lambda_function" "ingest" {
  function_name    = "${local.name}-ingest"
  role             = aws_iam_role.ingest.arn
  runtime          = "java21"
  handler          = "dev.concord.ingest.lambda.IngestHandler::handleRequest"
  filename         = var.lambda_jar
  source_code_hash = filebase64sha256(var.lambda_jar)
  memory_size      = 1024
  timeout          = 10
  architectures    = ["arm64"]
  publish          = true

  # SnapStart restores a pre-initialised JVM snapshot, cutting Java cold starts.
  dynamic "snap_start" {
    for_each = var.localstack ? [] : [1]
    content {
      apply_on = "PublishedVersions"
    }
  }

  environment {
    variables = merge(
      {
        ARCHIVE_BUCKET              = aws_s3_bucket.raw.bucket
        DEDUP_TABLE                 = aws_dynamodb_table.dedup.name
        QUEUE_URL                   = aws_sqs_queue.events.url
        DEDUP_TTL_HOURS             = tostring(var.dedup_ttl_hours)
        SIGNATURE_TOLERANCE_SECONDS = "300"
        JAVA_TOOL_OPTIONS           = "-XX:+TieredCompilation -XX:TieredStopAtLevel=1"
      },
      { for gateway, secrets in var.gateway_secrets : "GATEWAY_${upper(gateway)}_SECRETS" => secrets }
    )
  }

  depends_on = [aws_cloudwatch_log_group.ingest]
}

resource "aws_lambda_alias" "live" {
  name             = "live"
  function_name    = aws_lambda_function.ingest.function_name
  function_version = aws_lambda_function.ingest.version
}

# ---------- HTTP API ----------

resource "aws_apigatewayv2_api" "webhooks" {
  name          = "${local.name}-webhooks"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "ingest" {
  api_id                 = aws_apigatewayv2_api.webhooks.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_alias.live.invoke_arn
  payload_format_version = "2.0"
  timeout_milliseconds   = 10000
}

resource "aws_apigatewayv2_route" "webhook" {
  api_id    = aws_apigatewayv2_api.webhooks.id
  route_key = "POST /webhooks/{gateway}"
  target    = "integrations/${aws_apigatewayv2_integration.ingest.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.webhooks.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_rate_limit  = var.api_rate_limit
    throttling_burst_limit = var.api_burst_limit
  }
}

resource "aws_lambda_permission" "api" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  qualifier     = aws_lambda_alias.live.name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.webhooks.execution_arn}/*/*/webhooks/*"
}
