################################################################################
# Cluster
################################################################################

output "cluster_arn" {
  description = "The Amazon Resource Name (ARN) of the cluster"
  value       = module.eks.cluster_arn
}

output "cluster_name" {
  description = "The name of the EKS cluster"
  value       = module.eks.cluster_name
}

output "configure_kubectl" {
  description = "Configure kubectl: make sure you're logged in with the correct AWS profile and run the following command to update your kubeconfig"
  value       = "aws eks --region ${local.region} update-kubeconfig --name ${module.eks.cluster_name}"
}

output "region" {
  description = "AWS region"
  value       = local.region
}

output "deployment_id" {
  description = "Deployment ID unique to this stack"
  value       = var.deployment_id
}

################################################################################
# Grafana
################################################################################

output "grafana_password" {
  description = "Admin password for Grafana (if monitoring is enabled)"
  value       = var.enable_monitoring ? kubernetes_secret.grafana_admin[0].data["admin-password"] : ""
  sensitive   = true
}

output "custom_kafka_image_url" {
  description = "ECR Repository URL for the custom Kafka image (if tiered storage is enabled)"
  value       = var.enable_tiered_storage ? aws_ecr_repository.kafka_custom[0].repository_url : ""
}