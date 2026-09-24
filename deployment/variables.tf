variable "lambda_code_bucket_name" {
  description = "S3 bucket name for Lambda deployment package"
  type        = string
  default     = "vecstream-lambda-code"
}

variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

variable "lambda_architecture" {
  description = "Lambda function architecture (arm64 or x86_64)"
  type        = string
  default     = "x86_64"
}

variable "lambda_count_l1" {
  description = "Number of L1 cache Lambda functions to create"
  type        = number
  default     = 500
}

variable "lambda_count_l2" {
  description = "Number of L2 cache Lambda functions to create"
  type        = number
  default     = 500
}

variable "lambda_count_kafka" {
  description = "Number of Kafka search Lambda functions to create"
  type        = number
  default     = 25
}

variable "l1_lambda_function_name_prefix" {
  description = "Prefix for L1 Lambda function names"
  type        = string
  default     = "vecstream_lambda_l1_"
}

variable "l2_lambda_function_name_prefix" {
  description = "Prefix for L2 Lambda function names"
  type        = string
  default     = "vecstream_lambda_l2_"
}

variable "kafka_lambda_function_name_prefix" {
  description = "Prefix for Kafka Lambda function names"
  type        = string
  default     = "vecstream_lambda_kafka_"
}

variable "lambda_source_code" {
  description = "Path to the Lambda deployment package (ZIP file)"
  type        = string
  default     = "code.zip"
}

variable "lambda_layers" {
  description = "List of Lambda layer ARNs to attach"
  type        = list(string)
  default = [
    "arn:aws:lambda:us-east-1:012345678910:layer:vecstream:1", # Base vecstream layer
  ]
}

variable "lambda_memory_size" {
  description = "Memory size (MB) for Lambda functions"
  type        = number
  default     = 1769
}

variable "lambda_timeout" {
  description = "Timeout (seconds) for Lambda functions"
  type        = number
  default     = 60
}

variable "l1_lambda_handler" {
  description = "Handler for the L1 Lambda function"
  type        = string
  default     = "vecstream.map_lambda.event_handler"
}

variable "l2_lambda_handler" {
  description = "Handler for the L2 Lambda function (shared with L1 by default)"
  type        = string
  default     = "vecstream.map_lambda.event_handler"
}

variable "kafka_lambda_handler" {
  description = "Handler for the Kafka Lambda function"
  type        = string
  default     = "vecstream.kafka_lambda.event_handler"
}

variable "async_index_lambda_enabled" {
  description = "Create the S3-triggered async index-creation Lambda (FAISS indexer)."
  type        = bool
  default     = true
}

variable "async_index_lambda_function_name" {
  description = "Function name for the async index-creation Lambda"
  type        = string
  default     = "vecstream_lambda_async_index"
}

variable "async_index_lambda_handler" {
  description = "Handler for the async index-creation Lambda"
  type        = string
  default     = "vecstream.async_index_creation.event_handler"
}

variable "async_index_lambda_memory_size" {
  description = "Memory size (MB) for the async index-creation Lambda (FAISS IVF training needs more than the 1769 default)"
  type        = number
  default     = 4096
}

variable "async_index_lambda_timeout" {
  description = "Timeout (seconds) for the async index-creation Lambda"
  type        = number
  default     = 300
}

variable "async_index_kafka_bootstrap_servers" {
  description = "Kafka bootstrap servers for offset commits in the async index-creation Lambda. Leave empty to disable offset commits."
  type        = string
  default     = ""
}

variable "async_index_kafka_commit_group_id" {
  description = "Kafka consumer group ID used for offset commits in the async index-creation Lambda"
  type        = string
  default     = "vecstream_indexing"
}

variable "async_index_tiered_storage_bucket_name" {
  description = "Kafka tiered-storage bucket whose ObjectCreated events trigger the async index-creation Lambda. Leave empty to skip the notification wiring."
  type        = string
  default     = ""
}

variable "vpc_id" {
  description = "VPC ID for the Lambda functions. Leave empty to use the account's default VPC."
  type        = string
  default     = ""
}

variable "subnet_ids" {
  description = "Subnet IDs for the Lambda VPC config. Leave empty to use all subnets of the resolved VPC."
  type        = list(string)
  default     = []
}

variable "security_group_ids" {
  description = "Security group IDs for the Lambda VPC config. Leave empty to use the VPC's default security group."
  type        = list(string)
  default     = []
}

variable "enable_s3_gateway_endpoint" {
  description = "Create a Gateway VPC endpoint for standard S3 (com.amazonaws.<region>.s3)."
  type        = bool
  default     = true
}

variable "enable_s3express_gateway_endpoint" {
  description = "Create a Gateway VPC endpoint for S3 Express One Zone (com.amazonaws.<region>.s3express)."
  type        = bool
  default     = true
}

variable "s3_gateway_route_table_ids" {
  description = "Route table IDs to associate with the S3 Gateway endpoint. Leave empty to use the main route tables of the resolved VPC."
  type        = list(string)
  default     = []
}

variable "s3express_gateway_route_table_ids" {
  description = "Route table IDs to associate with the S3 Express Gateway endpoint. Leave empty to use the main route tables of the resolved VPC."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags applied to all Lambda functions and VPC endpoints."
  type        = map(string)
  default     = {}
}
