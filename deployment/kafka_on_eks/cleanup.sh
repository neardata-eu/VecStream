#!/bin/bash

set -e

TERRAFORM_DIR="terraform"
TFVARS_FILE="$TERRAFORM_DIR/terraform.tfvars"

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

print_status "This will destroy ALL resources for the Kafka on EKS deployment."
read -p "Are you sure? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

cd "$TERRAFORM_DIR"

# Get deployment info from terraform outputs, fall back to tfvars
CLUSTER_NAME=$(terraform output -raw cluster_name 2>/dev/null || echo "")
REGION=$(terraform output -raw region 2>/dev/null || echo "")
DEPLOYMENT_ID=$(terraform output -raw deployment_id 2>/dev/null || echo "")

if [ -z "$CLUSTER_NAME" ] && [ -f "terraform.tfvars" ]; then
    CLUSTER_NAME=$(grep -E '^[[:space:]]*name[[:space:]]*=' terraform.tfvars | sed -E 's/.*=[[:space:]]*"([^"]*)".*/\1/')
fi
if [ -z "$REGION" ] && [ -f "terraform.tfvars" ]; then
    REGION=$(grep -E '^[[:space:]]*region[[:space:]]*=' terraform.tfvars | sed -E 's/.*=[[:space:]]*"([^"]*)".*/\1/')
fi
if [ -z "$DEPLOYMENT_ID" ] && [ -f "terraform.tfvars" ]; then
    DEPLOYMENT_ID=$(grep -E '^[[:space:]]*deployment_id[[:space:]]*=' terraform.tfvars | sed -E 's/.*=[[:space:]]*"([^"]*)".*/\1/')
fi
if [ -z "$REGION" ]; then
    REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
fi

echo "Destroying Kafka on EKS: ${CLUSTER_NAME:-unknown} in region $REGION"

# Delete Kafka resources first to release EBS volumes
if [ -n "$CLUSTER_NAME" ]; then
    aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$REGION" --kubeconfig /tmp/kafka-cleanup-kubeconfig.yaml 2>/dev/null || true
    export KUBECONFIG=/tmp/kafka-cleanup-kubeconfig.yaml

    print_status "Deleting Kafka resources..."
    kubectl delete kafka -n kafka --all --wait=false 2>/dev/null || true
    kubectl delete kafkanodepool -n kafka --all --wait=false 2>/dev/null || true

    helm uninstall strimzi-kafka-operator -n strimzi-system --wait 2>/dev/null || true

    print_status "Deleting namespaces..."
    kubectl delete namespace monitoring --wait=true --timeout=120s 2>/dev/null || true
    kubectl delete namespace kafka --wait=true --timeout=120s 2>/dev/null || true
    kubectl delete namespace strimzi-system --wait=true --timeout=120s 2>/dev/null || true

    rm -f /tmp/kafka-cleanup-kubeconfig.yaml
    unset KUBECONFIG
fi

# Remove pre-deleted k8s resources from Terraform state to prevent timeouts
print_status "Removing pre-deleted k8s resources from Terraform state..."
terraform state rm kubernetes_namespace.monitoring 2>/dev/null || true
terraform state rm helm_release.kube_prometheus_stack 2>/dev/null || true
terraform state rm helm_release.strimzi_kafka_operator 2>/dev/null || true
terraform state rm 'kubectl_manifest.kafka_namespace' 2>/dev/null || true
for key in $(terraform state list | grep '^kubectl_manifest\.kafka_manifests\[' 2>/dev/null); do
    terraform state rm "$key" 2>/dev/null || true
done
for key in $(terraform state list | grep '^kubectl_manifest\.karpenter_resources\[' 2>/dev/null); do
    terraform state rm "$key" 2>/dev/null || true
done
terraform state rm kubernetes_secret.grafana_admin 2>/dev/null || true

# Destroy all Terraform resources
print_status "Running terraform destroy..."
terraform destroy -auto-approve -var-file=terraform.tfvars

# Clean up orphaned EBS volumes
if [ -n "$DEPLOYMENT_ID" ]; then
    print_status "Cleaning up orphaned EBS volumes with DeploymentId: $DEPLOYMENT_ID"
    VOLUME_IDS=$(aws ec2 describe-volumes --region "$REGION" --filters "Name=tag:DeploymentId,Values=$DEPLOYMENT_ID" --query "Volumes[].VolumeId" --output text 2>/dev/null || true)

    if [ -n "$VOLUME_IDS" ]; then
        for volume_id in $VOLUME_IDS; do
            echo "Deleting EBS volume: $volume_id"
            aws ec2 delete-volume --region "$REGION" --volume-id "$volume_id" 2>/dev/null || true
        done
    else
        print_status "No orphaned EBS volumes found."
    fi
fi

cd ..
print_status "Cleanup complete."