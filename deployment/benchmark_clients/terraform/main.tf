terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.88"
    }
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.0"
    }
    local = {
      source  = "hashicorp/local"
      version = ">= 2.0"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  suite = terraform.workspace

  suite_config = {
    ingestion = {
      project_tag = "ingestion-bench"
      name        = "bench-client-ingestion"
      key_name    = "bench-key-ingestion"
    }
    static-queries = {
      project_tag = "static-queries-bench"
      name        = "bench-client"
      key_name    = "bench-key"
    }
    streaming-queries = {
      project_tag = "streaming-queries-bench"
      name        = "streaming-bench-client"
      key_name    = "streaming-bench-key"
    }
  }

  # Fail-loud guard: the default workspace and any unknown workspace name cause
  # terraform validate to error because the regex cannot match.
  suite_guard = regex("^(ingestion|static-queries|streaming-queries)$", local.suite)

  selected_config = local.suite_config[local.suite_guard[0]]
}

resource "tls_private_key" "bench" {
  count     = var.enable_client ? 1 : 0
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "bench" {
  count      = var.enable_client ? 1 : 0
  key_name   = local.selected_config.key_name
  public_key = tls_private_key.bench[0].public_key_openssh
}

resource "local_file" "bench_key" {
  count           = var.enable_client ? 1 : 0
  content         = tls_private_key.bench[0].private_key_pem
  filename        = "${path.module}/bench-key-${terraform.workspace}.pem"
  file_permission = "0600"
}

module "ec2_client" {
  count  = var.enable_client ? 1 : 0
  source = "./modules/ec2"

  name              = local.selected_config.name
  instance_type     = var.instance_type
  availability_zone = var.availability_zone
  key_name          = aws_key_pair.bench[0].key_name
  vpc_id            = var.vpc_id
  subnet_id         = var.subnet_id
  tags = {
    Project = local.selected_config.project_tag
  }
}

module "s3" {
  count  = var.enable_s3 ? 1 : 0
  source = "./modules/s3"

  bucket_name                    = var.s3_bucket_name
  express_bucket_name            = var.express_bucket_name
  availability_zone              = var.availability_zone
  availability_zone_abbreviation = var.availability_zone_abbreviation
  tags = {
    Project = local.selected_config.project_tag
  }
}

resource "aws_iam_role_policy" "client_s3_access" {
  count = var.enable_client && var.enable_s3 ? 1 : 0
  name  = "${local.selected_config.name}-s3-access"
  role  = module.ec2_client[0].iam_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        module.s3[0].bucket_arn,
        "${module.s3[0].bucket_arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "client_express_access" {
  count = var.enable_client && var.enable_s3 ? 1 : 0
  name  = "${local.selected_config.name}-express-access"
  role  = module.ec2_client[0].iam_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3express:CreateSession"
      ]
      Resource = [
        module.s3[0].express_bucket_arn,
        "${module.s3[0].express_bucket_arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "client_dataset_access" {
  count = var.enable_client ? 1 : 0
  name  = "${local.selected_config.name}-dataset-access"
  role  = module.ec2_client[0].iam_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:*"
      ]
      Resource = [
        "arn:aws:s3:::${var.dataset_s3_bucket_name}",
        "arn:aws:s3:::${var.dataset_s3_bucket_name}/*"
      ]
    }]
  })
}
