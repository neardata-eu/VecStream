variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type for the benchmark client"
  type        = string
  default     = "m6i.large"
}

variable "availability_zone" {
  description = "Availability zone for the client EC2 instance"
  type        = string
  default     = "us-east-1a"
}

variable "vpc_id" {
  description = "VPC ID for the benchmark client (uses default VPC if empty)"
  type        = string
  default     = ""
}

variable "subnet_id" {
  description = "Subnet ID for the benchmark client (uses default subnet matching availability_zone if empty)"
  type        = string
  default     = ""
}

variable "enable_client" {
  description = "Deploy the benchmark client EC2 instance"
  type        = bool
  default     = true
}

variable "enable_s3" {
  description = "Deploy an S3 bucket and S3 Express One Zone bucket for plain S3 benchmarks"
  type        = bool
  default     = false
}

variable "s3_bucket_name" {
  description = "S3 bucket name for plain S3 benchmarks"
  type        = string
  default     = "ingestion-vector-store"
}

variable "express_bucket_name" {
  description = "Base name for S3 Express One Zone bucket"
  type        = string
  default     = "ingestion-vector-express"
}

variable "availability_zone_abbreviation" {
  description = "Availability zone abbreviation for S3 Express One Zone bucket"
  type        = string
  default     = "use1-az6"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "dataset_s3_bucket_name" {
  description = "S3 bucket name where datasets are stored"
  type        = string
  default     = "vecstream-benchmarks-data"
}
