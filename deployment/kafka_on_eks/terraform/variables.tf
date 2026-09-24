variable "name" {
  description = "Name to be used on all the resources as identifier"
  default     = "kafka-on-eks"
  type        = string

  validation {
    condition     = length(var.name) > 0 && length(var.name) <= 63
    error_message = "Name must be between 1 and 63 characters."
  }
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "tags" {
  description = "A map of tags to add to all resources"
  type        = map(string)
  default     = {}
}

variable "deployment_id" {
  description = "Unique ID used to tag all AWS resources for this deployment. Enables identification of orphaned resources and cleanup in case of Terraform state loss. Auto-generated on first deploy."
  type        = string
  default     = "DO-NOT-EDIT-AUTO-GENERATED"
}

#---------------------------------------------------------------
# VPC
#---------------------------------------------------------------

variable "vpc_cidr" {
  description = "The CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"

  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "VPC CIDR must be a valid IPv4 CIDR block."
  }

  validation {
    condition     = tonumber(split("/", var.vpc_cidr)[1]) >= 16 && tonumber(split("/", var.vpc_cidr)[1]) <= 28
    error_message = "VPC CIDR must have a prefix length between /16 and /28."
  }
}

variable "secondary_cidrs" {
  description = "List of secondary CIDR blocks to associate with the VPC"
  type        = list(string)
  default = [
    "100.64.0.0/16",
    "100.65.0.0/16",
    "100.66.0.0/16",
  ]

  validation {
    condition = alltrue([
      for cidr in var.secondary_cidrs : can(cidrhost(cidr, 0))
    ])
    error_message = "All secondary CIDRs must be valid IPv4 CIDR blocks."
  }

  validation {
    condition = alltrue([
      for cidr in var.secondary_cidrs : tonumber(split("/", cidr)[1]) >= 16 && tonumber(split("/", cidr)[1]) <= 28
    ])
    error_message = "All secondary CIDRs must have a prefix length between /16 and /28."
  }
}

variable "public_subnet_tags" {
  description = "Additional tags for the public subnets"
  type        = map(string)
  default     = {}
}

variable "private_subnet_tags" {
  description = "Additional tags for the private subnets"
  type        = map(string)
  default     = {}
}

#---------------------------------------------------------------
# EKS
#---------------------------------------------------------------

variable "eks_cluster_version" {
  description = "Kubernetes `<major>.<minor>` version to use for the EKS cluster (i.e.: `1.31`)"
  type        = string
  default     = "1.34"

  validation {
    condition     = can(regex("^[0-9]+\\.[0-9]+$", var.eks_cluster_version))
    error_message = "EKS cluster version must be in format 'major.minor' (e.g., '1.31')."
  }
}

variable "cluster_endpoint_public_access" {
  description = "Indicates whether or not the Amazon EKS public API server endpoint is enabled"
  type        = bool
  default     = true
}

variable "kms_key_admin_roles" {
  description = "A list of IAM roles that will have admin access to the KMS key used by the cluster"
  type        = list(string)
  default     = []
}
#---------------------------------------------------------------
# Optional Features
#---------------------------------------------------------------

variable "enable_monitoring" {
  description = "Enable Prometheus, Grafana, and Kafka Exporter for monitoring."
  type        = bool
  default     = false
}

variable "enable_tiered_storage" {
  description = "Enable Tiered Storage for Kafka backed by Amazon S3"
  type        = bool
  default     = true
}

variable "tiered_storage_bucket_name" {
  description = "Name of the S3 bucket for Kafka Tiered Storage. If empty, a generated name is used."
  type        = string
  default     = ""
}
variable "kafka_az" {
  description = "The specific Availability Zone to deploy the Kafka brokers into (e.g., 'us-east-1a'). This ensures all brokers are in the same AZ to eliminate cross-AZ latency and costs."
  type        = string
  default     = "us-east-1a"
}