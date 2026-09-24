output "bucket_arn" {
  description = "S3 bucket ARN"
  value       = aws_s3_bucket.this.arn
}

output "bucket_name" {
  description = "S3 bucket name"
  value       = aws_s3_bucket.this.bucket
}

output "bucket_id" {
  description = "S3 bucket ID"
  value       = aws_s3_bucket.this.id
}

output "express_bucket_arn" {
  description = "S3 Express bucket ARN"
  value       = aws_s3_directory_bucket.express.arn
}

output "express_bucket_name" {
  description = "S3 Express bucket name"
  value       = aws_s3_directory_bucket.express.bucket
}

output "express_bucket_id" {
  description = "S3 Express bucket ID"
  value       = aws_s3_directory_bucket.express.id
}