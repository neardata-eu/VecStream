data "aws_eks_cluster_auth" "this" {
  name = module.eks.cluster_name
}

data "aws_partition" "current" {}
data "aws_iam_session_context" "current" {
  arn = data.aws_caller_identity.current.arn
}

data "aws_caller_identity" "current" {}

locals {
  name       = var.name
  region     = var.region
  partition  = data.aws_partition.current.partition
  account_id = data.aws_caller_identity.current.account_id
  tags = merge(var.tags, {
    Blueprint    = local.name
    GithubRepo   = "github.com/awslabs/data-on-eks"
    DeploymentId = var.deployment_id
  })

  eks_core_addons = {
    coredns    = {}
    kube-proxy = {}
    eks-pod-identity-agent = {
      before_compute = true
    }
    vpc-cni = {
      before_compute              = true
      preserve                    = true
      resolve_conflicts_on_create = "OVERWRITE"
      configuration_values = jsonencode({
        env = {
          ENABLE_PREFIX_DELEGATION = "true"
          WARM_PREFIX_TARGET       = "1"
        }
      })
    }
  }

  default_node_groups = {
    core_node_group = {
      name        = "core-node-group"
      partition   = local.partition
      account_id  = local.account_id
      description = "EKS Core node group for hosting system add-ons"
      subnet_ids = compact([for subnet_id, cidr_block in zipmap(module.vpc.private_subnets, module.vpc.private_subnets_cidr_blocks) :
        substr(cidr_block, 0, 4) == "100." ? subnet_id : null]
      )
      ami_type     = "AL2023_x86_64_STANDARD"
      min_size     = 2
      max_size     = 4
      desired_size = 2

      instance_types = ["m6a.xlarge"]

      iam_role_additional_policies = {
        AmazonSSMManagedInstanceCore = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
      }

      ebs_optimized = true

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size = 100
            volume_type = "gp3"
          }
        }
      }

      labels = {
        WorkerType    = "ON_DEMAND"
        NodeGroupType = "core"
      }

      tags = merge(local.tags, {
        Name = "core-node-grp"
      })
    }

    kafka_node_group = {
      name        = "kafka-node-group"
      partition   = local.partition
      account_id  = local.account_id
      description = "Fixed size EKS node group for Kafka brokers"
      subnet_ids = [
        # Find the index of the requested AZ in local.azs, then get the corresponding secondary subnet.
        # The secondary subnets are appended after the primary private subnets, so we add length(local.azs).
        module.vpc.private_subnets[index(local.azs, var.kafka_az) + length(local.azs)]
      ]

      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = ["m6i.4xlarge"]

      min_size     = 3
      max_size     = 3
      desired_size = 3

      iam_role_additional_policies = {
        AmazonSSMManagedInstanceCore = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
      }

      ebs_optimized = true

      labels = {
        WorkerType    = "ON_DEMAND"
        NodeGroupType = "kafka"
      }

      tags = merge(local.tags, {
        Name = "kafka-node-grp"
      })
    }
  }
}

provider "aws" {
  region = local.region
  default_tags {
    tags = local.tags
  }
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  token                  = data.aws_eks_cluster_auth.this.token
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    token                  = data.aws_eks_cluster_auth.this.token
  }
}

provider "kubectl" {
  apply_retry_count      = 30
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  token                  = data.aws_eks_cluster_auth.this.token
  load_config_file       = false
}