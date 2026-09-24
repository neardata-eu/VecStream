#---------------------------------------------------------------
# EKS Cluster
#---------------------------------------------------------------

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name               = local.name
  kubernetes_version = var.eks_cluster_version

  endpoint_public_access = var.cluster_endpoint_public_access

  authentication_mode                      = "API_AND_CONFIG_MAP"
  enable_cluster_creator_admin_permissions = true

  addons = local.eks_core_addons

  vpc_id = module.vpc.vpc_id
  subnet_ids = compact([for subnet_id, cidr_block in zipmap(module.vpc.private_subnets, module.vpc.private_subnets_cidr_blocks) :
    substr(cidr_block, 0, 4) == "100." ? subnet_id : null]
  )

  kms_key_administrators = distinct(concat([
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"],
    var.kms_key_admin_roles,
    [data.aws_iam_session_context.current.issuer_arn]
  ))

  security_group_additional_rules = {
    ingress_nodes_ephemeral_ports_tcp = {
      description                = "Nodes on ephemeral ports"
      protocol                   = "tcp"
      from_port                  = 1025
      to_port                    = 65535
      type                       = "ingress"
      source_node_security_group = true
    }
  }

  node_security_group_additional_rules = {
    ingress_self_all = {
      description = "Node to node all ports/protocols"
      protocol    = "-1"
      from_port   = 0
      to_port     = 0
      type        = "ingress"
      self        = true
    }
    ingress_cluster_to_node_all_traffic = {
      description                   = "Cluster API to Nodegroup all traffic"
      protocol                      = "-1"
      from_port                     = 0
      to_port                       = 0
      type                          = "ingress"
      source_cluster_security_group = true
    }
    ingress_nodeport_from_vpc = {
      description = "Allow NodePort traffic from VPC for external Kafka access"
      protocol    = "tcp"
      from_port   = 30000
      to_port     = 32767
      type        = "ingress"
      cidr_blocks = [var.vpc_cidr]
    }
  }

  eks_managed_node_groups = local.default_node_groups
}

#---------------------------------------------------------------
# EBS CSI Driver Addon with Pod Identity
#---------------------------------------------------------------

resource "aws_iam_role" "ebs_csi_pod_identity_role" {
  name = "${var.name}-ebs-csi-pod-identity-role"

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

resource "aws_iam_role_policy_attachment" "ebs_csi_pod_identity_policy" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
  role       = aws_iam_role.ebs_csi_pod_identity_role.name
}

resource "aws_eks_addon" "aws_ebs_csi_driver" {
  cluster_name = module.eks.cluster_name
  addon_name   = "aws-ebs-csi-driver"

  pod_identity_association {
    role_arn        = aws_iam_role.ebs_csi_pod_identity_role.arn
    service_account = "ebs-csi-controller-sa"
  }
}

#---------------------------------------------------------------
# gp3 Encrypted Storage Class
#---------------------------------------------------------------

resource "kubernetes_storage_class" "ebs_csi_encrypted_gp3_storage_class" {
  metadata {
    name = "gp3"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }

  storage_provisioner    = "ebs.csi.aws.com"
  reclaim_policy         = "Delete"
  allow_volume_expansion = true
  volume_binding_mode    = "WaitForFirstConsumer"
  parameters = {
    fsType             = "xfs"
    encrypted          = "false"
    type               = "gp3"
    tagSpecification_1 = "DeploymentId=${var.deployment_id}"
  }
}

#---------------------------------------------------------------
# Metrics Server Addon
#---------------------------------------------------------------

resource "aws_eks_addon" "metrics_server" {
  cluster_name = module.eks.cluster_name
  addon_name   = "metrics-server"
}