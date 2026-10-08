variable "aws_region" {
  description = "AWS region."
  type        = string
  default     = "ap-south-1"
}

variable "environment" {
  description = "Environment name, used in resource names."
  type        = string
  default     = "dev"
}

variable "localstack" {
  description = "Deploy to LocalStack on localhost:4566 instead of AWS."
  type        = bool
  default     = false
}

variable "lambda_jar" {
  description = "Path to the shaded ingestion jar (mvn package)."
  type        = string
  default     = "../../ingestion/target/ingestion-0.1.0-SNAPSHOT-lambda.jar"
}

variable "gateway_secrets" {
  description = "Webhook signing secrets per gateway, comma-separated to allow rotation. A gateway with no entry is disabled."
  type        = map(string)
  sensitive   = true
  default     = {}
}

variable "dedup_ttl_hours" {
  description = "How long dedup records live. Must outlast the longest gateway retry schedule."
  type        = number
  default     = 168
}

variable "api_rate_limit" {
  description = "Steady-state requests per second allowed on the webhook API (cost and abuse guard)."
  type        = number
  default     = 100
}

variable "api_burst_limit" {
  description = "Burst requests allowed on the webhook API."
  type        = number
  default     = 200
}

variable "budget_email" {
  description = "If set, a monthly AWS budget alert is sent to this address."
  type        = string
  default     = ""
}

variable "monthly_budget_usd" {
  description = "Monthly budget that triggers the alert."
  type        = number
  default     = 5
}
