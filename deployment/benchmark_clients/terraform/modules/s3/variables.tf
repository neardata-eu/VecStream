variable "bucket_name" {
  description = "S3 bucket name"
  type        = string
}

variable "express_bucket_name" {
  description = "Base name for S3 Express One Zone bucket"
  type        = string
}

variable "availability_zone_abbreviation" {
  description = "Availability zone abbreviation for S3 Express One Zone bucket"
  type        = string
}

variable "availability_zone" {
  description = "Availability zone for S3 Express One Zone bucket"
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}