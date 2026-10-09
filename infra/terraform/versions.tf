terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.70, < 7.0"
    }
  }
}

locals {
  localstack_endpoint = "http://localhost:4566"
  endpoint            = var.localstack ? local.localstack_endpoint : null
}

provider "aws" {
  region = var.aws_region

  # With localstack = true every service points at LocalStack and fake credentials are accepted.
  access_key                  = var.localstack ? "test" : null
  secret_key                  = var.localstack ? "test" : null
  skip_credentials_validation = var.localstack
  skip_metadata_api_check     = var.localstack
  skip_requesting_account_id  = var.localstack
  s3_use_path_style           = var.localstack

  endpoints {
    apigateway   = local.endpoint
    apigatewayv2 = local.endpoint
    cloudwatch   = local.endpoint
    dynamodb     = local.endpoint
    iam          = local.endpoint
    lambda       = local.endpoint
    logs         = local.endpoint
    s3           = local.endpoint
    scheduler    = local.endpoint
    sqs          = local.endpoint
    sts          = local.endpoint
  }

  default_tags {
    tags = {
      project     = "concord"
      environment = var.environment
      managed_by  = "terraform"
    }
  }
}
