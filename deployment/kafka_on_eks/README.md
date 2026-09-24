# Kafka on EKS

A simplified, self-contained Terraform deployment for running Apache Kafka on Amazon EKS using the Strimzi operator in KRaft mode (ZooKeeper-free).

## Architecture

| Layer | Components | Purpose |
|-------|-----------|---------|
| AWS Infrastructure | VPC, EKS v1.34, EBS gp3 | Network isolation, managed Kubernetes, persistent storage |
| Platform | Prometheus + Grafana (Optional) | Metrics collection and visualization |
| Kafka Core | 3 Brokers, 3 Controllers (KRaft), Strimzi Operator v0.47.0 | Message storage, metadata management, Kubernetes-native orchestration |
| Kafka Add-ons | Kafka Exporter (Optional) | Metrics export |

## Prerequisites

- AWS CLI configured with appropriate credentials
- Terraform >= 1.8.0
- kubectl
- Sufficient AWS quotas for EKS, EC2, and EBS

## Folder Structure

```
kafka_on_eks/
├── terraform/
│   ├── versions.tf              # Provider requirements
│   ├── main.tf                  # Providers, locals, data sources
│   ├── variables.tf             # Input variables
│   ├── vpc.tf                  # VPC + subnets + endpoints
│   ├── eks.tf                  # EKS cluster + EBS CSI + metrics server + gp3 StorageClass
│   ├── karpenter.tf            # Karpenter module + Helm release + NodePools + EC2NodeClasses
│   ├── kafka.tf                # Strimzi operator Helm release + Kafka CRD manifests
│   ├── monitoring.tf           # kube-prometheus-stack Helm release + Grafana secret
│   ├── outputs.tf              # Terraform outputs
│   ├── terraform.tfvars        # Configuration values (region, name)
│   ├── helm-values/
│   │   ├── strimzi-kafka-operator.yaml
│   │   └── kube-prometheus.yaml
│   └── manifests/
│       ├── kafka/
│       │   ├── namespace.yaml
│       │   ├── kafka-cluster.yaml
│       │   ├── node-pool-broker.yaml
│       │   ├── node-pool-controller.yaml
│       │   └── cm.yaml
│       └── karpenter/
│           ├── ec2nodeclass.yaml
│           ├── nodepool-general-purpose.yaml
│           └── nodepool-memory-optimized-graviton.yaml
├── monitoring-manifests/        # Optional: PodMonitors + Grafana dashboards
├── examples/                    # Optional: Kafka topics + producer/consumer demos
├── deploy.sh                   # One-command deployment script
├── cleanup.sh                  # One-command destroy script
├── helper.sh                    # Kafka management helper commands
├── set-env.sh                  # Set KUBECONFIG and AWS_REGION
└── README.md
```

## Configuration

Edit `terraform/terraform.tfvars` to customize:

```hcl
name           = "kafka-on-eks"
region         = "us-east-1"
deployment_id  = "DO-NOT-EDIT-AUTO-GENERATED"  # Auto-generated on first deploy
```

### Key Defaults

- **Cluster name**: `kafka-on-eks`
- **Kafka version**: 3.9.0
- **Strimzi version**: 0.47.0
- **Kafka instance type**: m6i.4xlarge, fixed size (3 nodes, single AZ)
- **Broker storage**: 1000Gi gp3 per broker
- **Kafka replication factor**: 3
- **EKS version**: 1.34
- **Core node type**: m6a.xlarge (2-4 nodes)
- **Monitoring**: Disabled by default (`enable_monitoring = false`)

## Deployment

```bash
./deploy.sh
```

This script will:
1. Auto-generate a unique `deployment_id` in `terraform.tfvars`
2. Create VPC with public/private subnets across 3 AZs
3. Create EKS cluster with a core managed node group
4. Install EBS CSI driver and metrics server add-ons
5. Deploy Strimzi Kafka Operator via Helm
6. Create the Kafka cluster (3 brokers + 3 controllers in KRaft mode)
7. (Optional) Deploy kube-prometheus-stack (Prometheus + Grafana) and Kafka Exporter if enabled

**Deployment time**: ~25-30 minutes

## Verify Deployment

```bash
source set-env.sh

# Check Strimzi operator
kubectl get pods -n strimzi-system

# Check Kafka cluster
kubectl get kafka -n kafka
kubectl get pods -n kafka

# Check Kafka node pools
kubectl get kafkanodepool -n kafka

# Print Kafka boostrap servers for client connectivity
./helper.sh get-external-bootstrap-servers
```

Expected pod status: all pods Running.

## Testing

### Create a Kafka CLI Pod

```bash
./helper.sh create-kafka-cli-pod
```

### Create Topics

```bash
kubectl apply -f examples/kafka-topics.yaml
kubectl get kafkatopic -n kafka
```

### Deploy Producer and Consumer

```bash
kubectl apply -f examples/kafka-producers-consumers.yaml
```

This deploys a producer sending messages to `my-topic`, a Kafka Streams app that reverses them, and a consumer reading from `my-topic-reversed`.

### Verify Message Flow

```bash
./helper.sh verify-kafka-producer
./helper.sh verify-kafka-consumer
```

### List Topics via CLI

```bash
./helper.sh list-topics-via-cli
```

## Monitoring (Optional)

Monitoring is disabled by default to provide a pure benchmark environment. To enable it, set `enable_monitoring = true` in `terraform/terraform.tfvars` before deploying.

## Tiered Storage (Optional)

You can enable Tiered Storage backed by Amazon S3 by modifying `terraform/terraform.tfvars`:

```hcl
enable_tiered_storage = true
# tiered_storage_bucket_name = "my-custom-bucket-name" # Optional: auto-generated if omitted
```

When enabled, the deployment script will automatically:
1. Create an S3 Bucket and attach an IAM policy to the EKS worker nodes.
2. Create an Amazon ECR Repository.
3. Build a custom Strimzi Kafka Docker image containing the [Aiven Tiered Storage Plugin](https://github.com/Aiven-Open/tiered-storage-for-apache-kafka).
4. Push the image to ECR and deploy Kafka with remote storage properties enabled.

To use tiered storage for a specific topic, ensure `remote.storage.enable: true` is added to the topic's configuration.

### Apply PodMonitors (enables Prometheus scraping of Kafka metrics)

```bash
kubectl apply -f monitoring-manifests/
```

### Access Grafana

```bash
kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana 3000:80
```

Open http://localhost:3000. Username: `admin`. Password is shown at the end of `deploy.sh` output or can be retrieved with:

```bash
cd terraform && terraform output -raw grafana_password
```

### Import Strimzi Dashboards

The monitoring manifests include three Grafana dashboard ConfigMaps that are automatically imported:
- Strimzi Kafka Dashboard (broker metrics, throughput, partition status)
- Strimzi Exporter Dashboard (consumer lag, topic metrics)
- Strimzi Operators Dashboard (reconciliation activity)

## Helper Commands

```bash
./helper.sh get-kafka-pods           # List all Kafka pods
./helper.sh describe-kafka-cluster   # Describe Kafka cluster resource
./helper.sh get-kafka-brokers       # Get Kafka broker pods
./helper.sh get-kafka-controllers   # Get Kafka controller pods
./helper.sh get-kafka-nodes         # Get r8g nodes
./helper.sh debug-kafka-connectivity # Debug connectivity issues
./helper.sh list-topics-via-cli     # List topics via CLI
./helper.sh get-strimzi-operator    # Get Strimzi operator pods
```

## Node Configuration

Kafka pods run on **r8g** instances (Graviton4 memory-optimized, on-demand):

- **r8g.xlarge**: 4 vCPU, 32 GiB
- **r8g.2xlarge**: 8 vCPU, 64 GiB
- **r8g.4xlarge**: 16 vCPU, 128 GiB

The Kafka CRD specifies `nodeAffinity` for `karpenter.k8s.aws/instance-family: r8g` and `karpenter.sh/capacity-type: on-demand`, so Karpenter provisions the right node type when Kafka pods need scheduling.

A general-purpose NodePool handles system workloads (Prometheus, Strimzi operator, etc.) using m5/m6/m7 instance families with spot + on-demand capacity.

## Cleanup

```bash
./cleanup.sh
```

This will:
1. Delete Kafka and Karpenter resources
2. Destroy all Terraform-managed infrastructure
3. Clean up orphaned EBS volumes by deployment ID tag

**Warning**: This is irreversible and will delete all data.

## Differences from the Original data-on-eks Repo

This is a simplified, self-contained version of the [data-on-eks Kafka stack](https://awslabs.github.io/data-on-eks/docs/datastacks/streaming/kafka-on-eks/infra) with these key changes:

- **No ArgoCD**: Strimzi operator and kube-prometheus-stack are installed directly via Terraform Helm releases
- **No overlay mechanism**: All Terraform files are in a flat `terraform/` directory, no `_local/` copy step
- **Simplified Karpenter**: Only 2 NodePools (general-purpose + r8g memory-optimized) instead of 8+
- **No S3 buckets, Spark teams, or EMR**: Removed infrastructure not needed for Kafka
- **Optional Monitoring**: The entire Prometheus/Grafana stack and Kafka Exporter can be disabled via a toggle for a pure benchmarking environment.
- **Region**: Defaults to `us-east-1` instead of `us-west-2`
- **Cluster name**: `kafka-on-eks` instead of `data-on-eks`
- **Monitoring manifests**: Applied separately via `kubectl apply` instead of through ArgoCD