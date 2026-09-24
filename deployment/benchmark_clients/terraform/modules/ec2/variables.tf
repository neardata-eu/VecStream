variable "name" {
  description = "Name prefix for all resources"
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "m6i.large"
}

variable "availability_zone" {
  description = "Availability zone for the instance"
  type        = string
  default     = "us-east-1a"
}

variable "key_name" {
  description = "SSH key pair name"
  type        = string
}

variable "ingress_source_sg_rules" {
  description = "Additional ingress rules from a source security group"
  type = list(object({
    from_port                = number
    to_port                  = number
    protocol                 = string
    source_security_group_id = string
  }))
  default = []
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "vpc_id" {
  description = "VPC ID for the instance (defaults to default VPC if null)"
  type        = string
  default     = null
}

variable "subnet_id" {
  description = "Subnet ID for the instance (defaults to subnet in the VPC matching availability_zone if null)"
  type        = string
  default     = null
}