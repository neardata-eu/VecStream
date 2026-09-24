provider "aws" {
  region = var.aws_region
}

# S3 bucket for Lambda code (replace with your bucket name if needed)
resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket        = var.lambda_code_bucket_name
  force_destroy = true
}

# Upload code.zip to S3
resource "aws_s3_object" "lambda_code_zip" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key    = "code.zip"
  source = "../deployment/code.zip"
  etag   = filemd5("../deployment/code.zip")
}

# ---------------------------------------------------------------------------
# VPC resolution (only runs in VPC mode, i.e. when vpc_id is set)
# ---------------------------------------------------------------------------
# Default is no VPC. Setting `vpc_id` (and optionally `subnet_ids`,
# `security_group_ids`, route-table overrides) opts the deployment into VPC
# mode: data lookups run, the Gateway endpoints are created, and every lambda
# gets a `vpc_config` block. When `vpc_id == ""` none of this happens.

locals {
  vpc_mode = var.vpc_id != ""

  vpc_id = local.vpc_mode ? (var.vpc_id != "" ? var.vpc_id : data.aws_vpc.default[0].id) : null
  subnet_ids = local.vpc_mode ? (
    length(var.subnet_ids) > 0 ? var.subnet_ids : data.aws_subnets.default[0].ids
  ) : []
  security_group_ids = local.vpc_mode ? (
    length(var.security_group_ids) > 0 ? var.security_group_ids : [aws_security_group.lambda[0].id]
  ) : []
}

data "aws_vpc" "default" {
  count   = local.vpc_mode && var.vpc_id == "" ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = local.vpc_mode && length(var.subnet_ids) == 0 ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [local.vpc_id]
  }
}

# Dedicated security group for Lambda ENIs with outbound all-traffic egress.
# Created only in VPC mode. Used as the default when `security_group_ids` is
# not overridden, replacing the VPC's default SG which may lack egress rules.
resource "aws_security_group" "lambda" {
  count       = local.vpc_mode ? 1 : 0
  name        = "vecstream-lambda-sg"
  description = "VecStream Lambda outbound egress"
  vpc_id      = local.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "vecstream-lambda-sg"
  })
}

# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

resource "aws_iam_role" "lambda_exec" {
  name = "lambda_exec_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# Managed policy: required for VPC-attached lambdas (creates ENIs in the VPC).
# Harmless to attach even when not in VPC mode.
resource "aws_iam_role_policy_attachment" "lambda_vpc_access" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_custom_policy" {
  name = "lambda_custom_policy"
  role = aws_iam_role.lambda_exec.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:*",
          "s3-object-lambda:*",
          "s3express:*",
          "lambda:*",
          "ec2:*",
          "ecr:*",
          "sts:GetCallerIdentity",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Lambda functions: L1, L2, Kafka
# ---------------------------------------------------------------------------
# L1 and L2 share the same handler (vecstream.map_lambda.event_handler); the
# LAMBDA_TIER env var is set for ops visibility. When `vpc_id` is set the
# lambdas are attached to the resolved VPC; otherwise they run with no VPC
# config (the AWS default). Kafka lambdas follow the same pattern.

resource "aws_lambda_function" "l1_quicksvdb" {
  count = var.lambda_count_l1

  function_name = "${var.l1_lambda_function_name_prefix}${count.index}"
  handler       = var.l1_lambda_handler
  runtime       = "python3.13"
  role          = aws_iam_role.lambda_exec.arn

  s3_bucket        = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key           = aws_s3_object.lambda_code_zip.key
  source_code_hash = aws_s3_object.lambda_code_zip.etag

  layers        = var.lambda_layers
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout
  architectures = [var.lambda_architecture]


  environment {
    variables = {
      LAMBDA_TIER = "l1"
    }
  }

  tags = merge(var.tags, {
    Name = "${var.l1_lambda_function_name_prefix}${count.index}"
    Tier = "l1"
  })
}

resource "aws_lambda_function_event_invoke_config" "l1_quicksvdb_invoke" {
  count                  = var.lambda_count_l1
  function_name          = aws_lambda_function.l1_quicksvdb[count.index].function_name
  maximum_retry_attempts = 0
}

resource "aws_lambda_function" "l2_quicksvdb" {
  count = var.lambda_count_l2

  function_name = "${var.l2_lambda_function_name_prefix}${count.index}"
  handler       = var.l2_lambda_handler
  runtime       = "python3.13"
  role          = aws_iam_role.lambda_exec.arn

  s3_bucket        = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key           = aws_s3_object.lambda_code_zip.key
  source_code_hash = aws_s3_object.lambda_code_zip.etag

  layers        = var.lambda_layers
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout
  architectures = [var.lambda_architecture]

  environment {
    variables = {
      LAMBDA_TIER = "l2"
    }
  }

  tags = merge(var.tags, {
    Name = "${var.l2_lambda_function_name_prefix}${count.index}"
    Tier = "l2"
  })
}

resource "aws_lambda_function_event_invoke_config" "l2_quicksvdb_invoke" {
  count                  = var.lambda_count_l2
  function_name          = aws_lambda_function.l2_quicksvdb[count.index].function_name
  maximum_retry_attempts = 0
}

resource "aws_lambda_function" "kafka_quicksvdb" {
  count = var.lambda_count_kafka

  function_name = "${var.kafka_lambda_function_name_prefix}${count.index}"
  handler       = var.kafka_lambda_handler
  runtime       = "python3.13"
  role          = aws_iam_role.lambda_exec.arn

  s3_bucket        = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key           = aws_s3_object.lambda_code_zip.key
  source_code_hash = aws_s3_object.lambda_code_zip.etag

  layers        = var.lambda_layers
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout
  architectures = [var.lambda_architecture]

  dynamic "vpc_config" {
    for_each = local.vpc_mode ? [1] : []
    content {
      subnet_ids         = local.subnet_ids
      security_group_ids = local.security_group_ids
    }
  }

  environment {
    variables = {
      LAMBDA_KIND = "kafka"
    }
  }

  tags = merge(var.tags, {
    Name = "${var.kafka_lambda_function_name_prefix}${count.index}"
    Kind = "kafka"
  })
}

resource "aws_lambda_function_event_invoke_config" "kafka_quicksvdb_invoke" {
  count                  = var.lambda_count_kafka
  function_name          = aws_lambda_function.kafka_quicksvdb[count.index].function_name
  maximum_retry_attempts = 0
}

# ---------------------------------------------------------------------------
# Async index creation (S3-triggered FAISS indexer)
# ---------------------------------------------------------------------------
# Triggered by ObjectCreated events on the Kafka tiered-storage bucket: the
# handler trains a FAISS IVF index from the new log segment and uploads it to
# S3. IVF training needs more memory and a longer timeout than the query
# lambdas. The tiered-storage bucket is created by a different Terraform root
# (deployment/kafka_on_eks), so its name is a
# variable here; leaving it empty skips the notification wiring. No Function
# URL (nothing calls this Lambda over HTTP) and no invoke config: the handler
# is idempotent (the registry merge skips keys already present), so
# re-invocation is safe, but it returns normally even when individual records
# fail; failed segments surface only in CloudWatch logs. Add an on-failure
# destination if strict delivery guarantees are needed.

resource "aws_lambda_function" "async_index_creation" {
  count = var.async_index_lambda_enabled ? 1 : 0

  function_name = var.async_index_lambda_function_name
  handler       = var.async_index_lambda_handler
  runtime       = "python3.13"
  role          = aws_iam_role.lambda_exec.arn

  s3_bucket        = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key           = aws_s3_object.lambda_code_zip.key
  source_code_hash = aws_s3_object.lambda_code_zip.etag

  layers        = var.lambda_layers
  memory_size   = var.async_index_lambda_memory_size
  timeout       = var.async_index_lambda_timeout
  architectures = [var.lambda_architecture]

  dynamic "vpc_config" {
    for_each = local.vpc_mode ? [1] : []
    content {
      subnet_ids         = local.subnet_ids
      security_group_ids = local.security_group_ids
    }
  }

  environment {
    variables = {
      INDEX_NLIST             = "150"
      INDEX_NPROBE            = "15"
      INDEX_METRIC            = "euclidean"
      INDEX_STORAGE_BUCKET    = ""
      INDEX_PREFIX            = ""
      FAISS_NUM_THREADS       = "6"
      KAFKA_BOOTSTRAP_SERVERS = var.async_index_kafka_bootstrap_servers
      KAFKA_COMMIT_GROUP_ID   = var.async_index_kafka_commit_group_id
    }
  }

  tags = merge(var.tags, {
    Name = var.async_index_lambda_function_name
    Kind = "async-index"
  })
}

resource "aws_lambda_permission" "allow_s3_async_index" {
  count = var.async_index_lambda_enabled && var.async_index_tiered_storage_bucket_name != "" ? 1 : 0

  action        = "lambda:InvokeFunction"
  principal     = "s3.amazonaws.com"
  function_name = aws_lambda_function.async_index_creation[0].function_name
  source_arn    = "arn:aws:s3:::${var.async_index_tiered_storage_bucket_name}"
}

# The permission must exist before the notification is created, otherwise S3
# invokes fail with permission errors.
resource "aws_s3_bucket_notification" "async_index_tiered_storage" {
  count = var.async_index_lambda_enabled && var.async_index_tiered_storage_bucket_name != "" ? 1 : 0

  bucket = var.async_index_tiered_storage_bucket_name

  lambda_function {
    lambda_function_arn = aws_lambda_function.async_index_creation[0].arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".log"
  }

  depends_on = [aws_lambda_permission.allow_s3_async_index]
}

# ---------------------------------------------------------------------------
# Function URLs
# ---------------------------------------------------------------------------
# auth=NONE matches the previous deployment style. Clients in `vecstream/urls.py`
# hardcode these URLs (see AGENTS.md anti-patterns).

resource "aws_lambda_function_url" "l1_quicksvdb_url" {
  count              = var.lambda_count_l1
  function_name      = aws_lambda_function.l1_quicksvdb[count.index].function_name
  authorization_type = "NONE"
}

resource "aws_lambda_function_url" "l2_quicksvdb_url" {
  count              = var.lambda_count_l2
  function_name      = aws_lambda_function.l2_quicksvdb[count.index].function_name
  authorization_type = "NONE"
}

resource "aws_lambda_function_url" "kafka_quicksvdb_url" {
  count              = var.lambda_count_kafka
  function_name      = aws_lambda_function.kafka_quicksvdb[count.index].function_name
  authorization_type = "NONE"
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "l1_lambda_function_names" {
  value = [for f in aws_lambda_function.l1_quicksvdb : f.function_name]
}

output "l1_lambda_function_urls" {
  value = [for u in aws_lambda_function_url.l1_quicksvdb_url : u.function_url]
}

output "l2_lambda_function_names" {
  value = [for f in aws_lambda_function.l2_quicksvdb : f.function_name]
}

output "l2_lambda_function_urls" {
  value = [for u in aws_lambda_function_url.l2_quicksvdb_url : u.function_url]
}

output "kafka_lambda_function_names" {
  value = [for f in aws_lambda_function.kafka_quicksvdb : f.function_name]
}

output "kafka_lambda_function_urls" {
  value = [for u in aws_lambda_function_url.kafka_quicksvdb_url : u.function_url]
}

output "async_index_lambda_function_name" {
  value = var.async_index_lambda_enabled ? aws_lambda_function.async_index_creation[0].function_name : ""
}

output "vpc_mode" {
  value = local.vpc_mode
}

output "resolved_vpc_id" {
  value = local.vpc_id
}

output "resolved_subnet_ids" {
  value = local.subnet_ids
}

output "resolved_security_group_ids" {
  value = local.security_group_ids
}
