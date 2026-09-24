# S3 Bucket for Tiered Storage
resource "aws_s3_bucket" "tiered_storage" {
  count         = var.enable_tiered_storage ? 1 : 0
  bucket        = coalesce(var.tiered_storage_bucket_name, "${var.name}-tiered-storage-${var.deployment_id}")
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "tiered_storage" {
  count                   = var.enable_tiered_storage ? 1 : 0
  bucket                  = aws_s3_bucket.tiered_storage[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM Policy for S3 Access
resource "aws_iam_policy" "tiered_storage_policy" {
  count       = var.enable_tiered_storage ? 1 : 0
  name        = "${var.name}-tiered-storage-policy-${var.deployment_id}"
  description = "Policy for Kafka Tiered Storage to access S3"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.tiered_storage[0].arn,
          "${aws_s3_bucket.tiered_storage[0].arn}/*"
        ]
      }
    ]
  })
}

# IAM Role for Kafka Pods via EKS Pod Identity
resource "aws_iam_role" "kafka_tiered_storage" {
  count = var.enable_tiered_storage ? 1 : 0
  name  = "${var.name}-kafka-tiered-storage-${var.deployment_id}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "pods.eks.amazonaws.com"
        }
        Action = [
          "sts:AssumeRole",
          "sts:TagSession"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "kafka_tiered_storage" {
  count      = var.enable_tiered_storage ? 1 : 0
  role       = aws_iam_role.kafka_tiered_storage[0].name
  policy_arn = aws_iam_policy.tiered_storage_policy[0].arn
}

# EKS Pod Identity Association: maps IAM role to Kafka service account
resource "aws_eks_pod_identity_association" "kafka_tiered_storage" {
  count           = var.enable_tiered_storage ? 1 : 0
  cluster_name    = module.eks.cluster_name
  namespace       = "kafka"
  service_account = "kafka-on-eks-kafka"
  role_arn        = aws_iam_role.kafka_tiered_storage[0].arn
}

# ECR Repository for the Custom Kafka Docker Image
resource "aws_ecr_repository" "kafka_custom" {
  count                = var.enable_tiered_storage ? 1 : 0
  name                 = lower("${var.name}-tiered-${var.deployment_id}")
  force_delete         = true
  image_tag_mutability = "MUTABLE"
}
