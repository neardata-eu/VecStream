#---------------------------------------------------------------
# Grafana Admin Password
#---------------------------------------------------------------

resource "random_password" "grafana" {
  count   = var.enable_monitoring ? 1 : 0
  length  = 16
  special = true
}

#---------------------------------------------------------------
# Namespace for kube-prometheus-stack
#---------------------------------------------------------------

resource "kubernetes_namespace" "monitoring" {
  count = var.enable_monitoring ? 1 : 0
  metadata {
    name = "monitoring"
  }

  timeouts {
    delete = "5m"
  }
}

#---------------------------------------------------------------
# Kubernetes Secret for Grafana Admin
#---------------------------------------------------------------

resource "kubernetes_secret" "grafana_admin" {
  count = var.enable_monitoring ? 1 : 0
  metadata {
    name      = "grafana-admin-secret"
    namespace = kubernetes_namespace.monitoring[0].metadata[0].name
  }

  data = {
    admin-user     = "admin"
    admin-password = random_password.grafana[0].result
  }
}

#---------------------------------------------------------------
# Kube Prometheus Stack
#---------------------------------------------------------------

resource "helm_release" "kube_prometheus_stack" {
  count            = var.enable_monitoring ? 1 : 0
  name             = "kube-prometheus-stack"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = "82.13.3"
  namespace        = kubernetes_namespace.monitoring[0].metadata[0].name
  create_namespace = false

  values = [
    templatefile("${path.module}/helm-values/kube-prometheus.yaml", {
      grafana_password = random_password.grafana[0].result
    })
  ]

  depends_on = [aws_eks_addon.aws_ebs_csi_driver]
}