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
print_warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# --- Prerequisites ---
print_status "Checking prerequisites..."
command -v terraform >/dev/null 2>&1 || { print_error "terraform is required but not installed."; exit 1; }
command -v kubectl >/dev/null 2>&1 || { print_error "kubectl is required but not installed."; exit 1; }
command -v aws >/dev/null 2>&1 || { print_error "aws cli is required but not installed."; exit 1; }
ENABLE_TIERED_STORAGE_CHECK=$(grep -E '^[[:space:]]*enable_tiered_storage[[:space:]]*=' "$TFVARS_FILE" | sed -E 's/.*=[[:space:]]*(true|false).*/\1/' || echo "false")
if [ "$ENABLE_TIERED_STORAGE_CHECK" = "true" ]; then
    command -v docker >/dev/null 2>&1 || { print_error "docker is required for tiered storage but not installed."; exit 1; }
fi
aws sts get-caller-identity >/dev/null 2>&1 || { print_error "AWS credentials not configured."; exit 1; }
print_status "Prerequisites check passed"

# --- Auto-generate deployment_id ---
if grep -qE '^[[:space:]]*deployment_id[[:space:]]*=[[:space:]]*"DO-NOT-EDIT-AUTO-GENERATED"' "$TFVARS_FILE"; then
    print_status "Default deployment_id found. Generating a new random one."
    RANDOM_ID=$(openssl rand -base64 32 | tr -dc 'A-Za-z0-9' | head -c 8)
    sed -i.bak -E "s/^([[:space:]]*deployment_id[[:space:]]*=[[:space:]]*)\"DO-NOT-EDIT-AUTO-GENERATED\"/\1\"$RANDOM_ID\"/" "$TFVARS_FILE" && rm -f "$TFVARS_FILE.bak"
    print_status "Updated deployment_id to $RANDOM_ID"
fi

# --- Terraform Init ---
print_status "Initializing Terraform..."
cd "$TERRAFORM_DIR"
terraform init -upgrade

# --- Apply VPC first ---
print_status "Applying module.vpc..."
terraform apply -auto-approve -var-file=terraform.tfvars -target=module.vpc

# --- Apply EKS next ---
print_status "Applying module.eks..."
terraform apply -auto-approve -var-file=terraform.tfvars -target=module.eks -target=aws_iam_role.ebs_csi_pod_identity_role -target=aws_iam_role_policy_attachment.ebs_csi_pod_identity_policy -target=aws_eks_addon.aws_ebs_csi_driver -target=aws_eks_addon.metrics_server -target=random_password.grafana

# --- Check Tiered Storage and build custom Docker image ---
print_status "Tiered storage is enabled. Setting up ECR and Docker image..."
terraform apply -target="aws_ecr_repository.kafka_custom" -auto-approve -var-file=terraform.tfvars

ECR_URL=$(terraform output -raw custom_kafka_image_url 2>/dev/null || echo "")
REGION=$(terraform output -raw region 2>/dev/null || echo "")
REGION=${REGION:-us-east-1}
if [ -n "$ECR_URL" ]; then
    aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ECR_URL"
    print_status "Building custom Kafka Docker image with Aiven Tiered Storage plugin..."
    docker build -t "${ECR_URL}:latest" -f ../docker/Dockerfile ../docker
    print_status "Pushing image to ECR..."
    docker push "${ECR_URL}:latest"
fi

# --- Apply remaining resources ---
print_status "Applying remaining resources (Kafka)..."
terraform apply -auto-approve -var-file=terraform.tfvars

# --- Setup kubeconfig ---
cd ..
print_status "Setting up kubeconfig..."
CLUSTER_NAME=$(cd "$TERRAFORM_DIR" && terraform output -raw cluster_name)
REGION=$(cd "$TERRAFORM_DIR" && terraform output -raw region)
REGION=${REGION:-us-east-1}
aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$REGION" --kubeconfig kubeconfig.yaml
export KUBECONFIG=kubeconfig.yaml

# --- Get passwords ---
ARGOCD_PASSWORD=""
GRAFANA_PASSWORD=$(cd "$TERRAFORM_DIR" && terraform output -raw grafana_password)

echo ""
echo "========================================="
echo "Deployment Complete"
echo "========================================="
echo "Cluster: $CLUSTER_NAME ($REGION)"
echo ""

if [ -n "$GRAFANA_PASSWORD" ] && [ "$GRAFANA_PASSWORD" != "null" ]; then
  echo "Monitoring Stack is ENABLED"
  echo "Grafana credentials:"
  echo "  Username: admin"
  echo "  Password: $GRAFANA_PASSWORD"
  echo ""
  echo "  To access Grafana:"
  echo "     kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana 3000:80"
  echo "     Open http://localhost:3000"
  echo ""
  echo "  To apply monitoring manifests (PodMonitors + Grafana dashboards):"
  echo "     kubectl apply -f monitoring-manifests/"
  echo ""
else
  echo "Monitoring Stack is DISABLED (Pure Benchmark Mode)"
  echo ""
fi

echo "Next steps:"
echo "  1. source set-env.sh"
echo "  2. kubectl get pods -n strimzi-system    # Check Strimzi operator"
echo "  3. kubectl get pods -n kafka             # Check Kafka pods"
echo "  4. kubectl get kafka -n kafka            # Check Kafka cluster status"
echo ""
echo "  To destroy everything:"
echo "     ./cleanup.sh"
echo "========================================"