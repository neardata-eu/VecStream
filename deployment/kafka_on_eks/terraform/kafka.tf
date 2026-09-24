#---------------------------------------------------------------
# Strimzi Kafka Operator (Helm release via Terraform)
#---------------------------------------------------------------

resource "helm_release" "strimzi_kafka_operator" {
  name             = "strimzi-kafka-operator"
  repository       = "https://strimzi.io/charts/"
  chart            = "strimzi-kafka-operator"
  version          = "0.47.0"
  namespace        = "strimzi-system"
  create_namespace = true

  values = [
    templatefile("${path.module}/helm-values/strimzi-kafka-operator.yaml", {})
  ]

  depends_on = [aws_eks_addon.aws_ebs_csi_driver]
}

#---------------------------------------------------------------
# Kafka Namespace
#---------------------------------------------------------------

resource "kubectl_manifest" "kafka_namespace" {
  yaml_body = templatefile("${path.module}/manifests/kafka/namespace.yaml", {})

  depends_on = [helm_release.strimzi_kafka_operator]
}

#---------------------------------------------------------------
# Kafka Manifests (cluster, node pools, configmap, rebalance)
#---------------------------------------------------------------

resource "kubectl_manifest" "kafka_manifests" {
  for_each = {
    for f in fileset("${path.module}/manifests/kafka", "*.yaml") : f => f
    if f != "namespace.yaml"
  }

  yaml_body = templatefile("${path.module}/manifests/kafka/${each.value}", {
    cluster_name               = var.name
    enable_monitoring          = var.enable_monitoring
    enable_tiered_storage      = var.enable_tiered_storage
    tiered_storage_bucket_name = var.enable_tiered_storage ? aws_s3_bucket.tiered_storage[0].bucket : ""
    custom_image_url           = var.enable_tiered_storage ? "${aws_ecr_repository.kafka_custom[0].repository_url}:latest" : ""
    region                     = var.region
  })

  depends_on = [
    helm_release.strimzi_kafka_operator,
    kubectl_manifest.kafka_namespace
  ]
}