data "aws_availability_zones" "available" {}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 3)
}

#---------------------------------------------------------------
# VPC
#---------------------------------------------------------------
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 6.0"

  name = var.name
  cidr = var.vpc_cidr

  azs = local.azs

  secondary_cidr_blocks = var.secondary_cidrs

  private_subnets = concat(
    [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 4, k)],
    [for k in range(length(local.azs)) : var.secondary_cidrs[k]]
  )
  public_subnets = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 8, k + 48)]

  private_subnet_names = concat(
    [for k, v in local.azs : "${var.name}-private-${v}"],
    [for k, v in local.azs : "${var.name}-private-secondary1-${v}"]
  )
  public_subnet_names = [for k, v in local.azs : "${var.name}-public-${v}"]

  enable_nat_gateway = true
  single_nat_gateway = true

  enable_ipv6            = false
  create_egress_only_igw = false

  public_subnet_tags = merge(var.public_subnet_tags, {
    "kubernetes.io/role/elb" = 1
  })

  private_subnet_tags = merge(var.private_subnet_tags, {
    "kubernetes.io/role/internal-elb" = 1
  })
}

#---------------------------------------------------------------
# VPC Endpoints
#---------------------------------------------------------------
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = module.vpc.vpc_id
  service_name = "com.amazonaws.${var.region}.s3"
  route_table_ids = concat(
    module.vpc.private_route_table_ids,
    module.vpc.public_route_table_ids
  )

  tags = merge(local.tags, {
    Name = "${var.name}-s3-gateway-endpoint"
  })
}