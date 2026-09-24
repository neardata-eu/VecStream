# VecStream ingestion benchmark

Measures end-to-end ingestion latency of the [VecStream](https://github.com/gfinol/VecStream) Kafka-backed stream search system. The benchmark is a thin wrapper around the `vecstream.ingestion` library: it computes (or reuses) centroids, builds an IVF routing, and forwards each batch to a `VecStreamIngestionClient` that owns the Kafka producer and topic lifecycle.

This suite reproduces paper §5.3 (figures 6 and 7): the p95 write latency of VecStream at 16 and 250 partitions, 1-1000 req/s and batch sizes 1-500.

## Structure

```
ingestion/
├── benchmark.py              CLI entry point (argparse, main())
├── systems/
│   ├── base.py               VectorSystem abstract class
│   ├── vecstream.py          Thin wrapper over vecstream.ingestion
│   └── __init__.py           get_system() factory
└── README.md
```

Deployment tooling lives in the repo-root `deployment/` folder: `deployment/benchmark_clients/` provisions the EC2 benchmark client and `deployment/kafka_on_eks/` deploys Kafka on EKS.

## How it works

The runner loads the `.npy` or `.fbin` dataset, slices it into batches of `batch_size` vectors, and sends each batch to the system. Vector IDs are sequential integers as strings (`"0"`, `"1"`, ...) offset by batch position. When `--reuse_batch` is set, the same `batch_size` vectors are reused for every iteration instead of slicing sequentially through the dataset.

Between batches, the runner sleeps to match the target throughput: `sleep(1/throughput - elapsed)`. If throughput is 0, no sleep occurs and batches fire as fast as possible.

When `--concurrency` is greater than 1, the runner uses a `ThreadPoolExecutor` to submit batches concurrently up to the specified limit.

The `VectorSystem.put_vectors` method is the only thing measured for latency. The runner wraps each call and records wall-clock start/end timestamps independently, so there is no drift from the system-side timing.

### VecStream flow

The `VecStreamSystem` constructor performs three steps, all delegated to `vecstream.ingestion`:

1. **Centroids**: load a precomputed `.npy` file (`--vecstream_centroids`) or fit KMeans on a sample of the dataset (`--vecstream_sample_file`).
2. **Routing**: wrap the centroids in an `IVFRouting` (`vecstream.ingestion.build_ivf_routing`). The routing's `n_partitions` (= number of centroids) sizes the Kafka topic.
3. **Client**: construct a `VecStreamIngestionClient`, which owns the Kafka producer, deletes and recreates the topic on `__init__`, and deletes it again on `close()`.

`put_vectors` and `close` are pure delegations to the client.

## Quick start

With precomputed centroids:

```bash
python -m benchmarks.ingestion.benchmark \
  --vector_dataset datasets/deep100m.fbin \
  --system vecstream \
  --vecstream_bootstrap_servers b-1.kafka-on-eks.example.amazonaws.com:9092 \
  --vecstream_centroids datasets/sample_centroids_250.npy \
  --vecstream_partitions 250 \
  --batch_size 100 \
  --num_batches 500
```

Computing centroids from a sample file:

```bash
python -m benchmarks.ingestion.benchmark \
  --vector_dataset datasets/deep100m.fbin \
  --system vecstream \
  --vecstream_bootstrap_servers b-1.kafka-on-eks.example.amazonaws.com:9092 \
  --vecstream_sample_file datasets/sample.fbin \
  --vecstream_partitions 250 \
  --batch_size 100 \
  --num_batches 500
```

## CLI arguments

Generic flags (shared with the rest of the suite):

| Argument | Default | Description |
|---|---|---|
| `--vector_dataset` | (required) | Path to a `.npy` or `.fbin` file containing a 2D float32 array of vectors |
| `--system` | `vecstream` | Target system. The only supported value is `vecstream` |
| `--throughput` | `1` | Target ingestion rate in vectors/second. The runner sleeps between batches to meet this rate |
| `--batch_size` | `1` | Number of vectors sent per batch |
| `--num_batches` | `1000` | Total number of batches to send |
| `--concurrency` | `32` | Max concurrent in-flight requests (uses ThreadPoolExecutor when > 1) |
| `--reuse_batch` | `false` | Reuse the same batch of vectors for all iterations instead of sequential slicing |
| `--output_file` | (auto) | Path to write results JSON. Auto-generated if omitted |

VecStream-specific arguments:

| Argument | Default | Description |
|---|---|---|
| `--vecstream_bootstrap_servers` | `localhost:9092` | Comma-separated Kafka bootstrap servers |
| `--vecstream_topic` | `bench-ingestion` | Kafka topic name. Deleted and recreated on every run |
| `--vecstream_partitions` | `1` | Target partition count. Recorded in the JSON config and used as the cluster count when centroids are computed from a sample |
| `--vecstream_block_size` | `7500000` | Kafka segment size in bytes (7.5 MB). Drives the async indexer seal threshold |
| `--vecstream_metric` | `euclidean` | Distance metric: `euclidean` or `cosine` |
| `--vecstream_dimension` | `0` | Vector dimensionality. `0` infers the dimension from the loaded dataset |
| `--vecstream_centroids` | (none) | Path to `.npy` file with precomputed centroid vectors. Mutually exclusive with `--vecstream_sample_file` (centroids win) |
| `--vecstream_sample_file` | (none) | Path to vector file (`.npy`/`.fbin`) used to fit KMeans when `--vecstream_centroids` is not provided |
| `--vecstream_num_sample` | `100000` | Number of vectors to read from `sample_file` for centroid computation |
| `--vecstream_n_clusters` | `--vecstream_partitions` | Number of clusters (centroids) to compute. Defaults to `--vecstream_partitions` when omitted |
| `--vecstream_remote_storage_disabled` | `false` | Disable VecStream remote storage in Kafka (`remote.storage.enable=false` on the topic) |
| `--vecstream_kafka_acks` | `1` | Kafka producer acks setting |
| `--vecstream_kafka_local_retention_bytes` | `1` | Kafka topic local retention bytes |
| `--vecstream_batch_to_same_partition` | `false` | Send all vectors in the same batch to the same Kafka partition (debug knob) |
| `--vecstream_kafka_compression_type` | (none) | Kafka producer compression type (e.g. `lz4`, `zstd`, `snappy`) |

## Output format

The benchmark writes a JSON file named `ingestion_vecstream_tp{throughput}_bs{batch_size}_{timestamp}.json` (unless `--output_file` is set). The structure:

```json
{
  "configuration": {
    "system": "vecstream",
    "throughput": 10,
    "batch_size": 100,
    "num_batches": 500,
    "concurrency": 32,
    "vector_dataset": "datasets/deep100m.fbin",
    "vector_dimension": 96,
    "total_vectors": 10000000,
    "reuse_batch": false,
    "vecstream_bootstrap_servers": "b-1.kafka-on-eks.example.amazonaws.com:9092",
    "vecstream_topic": "bench-ingestion",
    "vecstream_partitions": 250,
    "vecstream_block_size": 7500000,
    "vecstream_metric": "euclidean",
    "vecstream_dimension": 96,
    "vecstream_sample_file": "datasets/sample.fbin",
    "vecstream_num_sample": 100000,
    "vecstream_n_clusters": 250,
    "vecstream_remote_storage_disabled": false,
    "vecstream_kafka_acks": "1",
    "vecstream_kafka_local_retention_bytes": 1,
    "vecstream_batch_to_same_partition": false,
    "vecstream_kafka_compression_type": null
  },
  "results": [
    {
      "batch_index": 0,
      "start_time": "2026-09-24T10:00:00.123456+00:00",
      "end_time": "2026-09-24T10:00:00.456789+00:00",
      "latency_seconds": 0.333333,
      "insert_count": 100,
      "partition_serialization_times": [[0, 0.45, 0.45], [1, 0.45, 0.45]],
      "flush_time_seconds": 0.45
    }
  ]
}
```

Each result contains:

- `batch_index`: 0-based batch number.
- `start_time`: ISO 8601 timestamp when the put_vectors call started.
- `end_time`: ISO 8601 timestamp when the call returned.
- `latency_seconds`: `end_time - start_time` in seconds, rounded to microseconds.
- `insert_count`: number of (id, vector) pairs sent.
- `partition_serialization_times`: list of `(kafka_partition, end_serialize_time, end_key_time)` tuples, one per partition that received messages in this batch.
- `flush_time_seconds`: wall-clock at the moment `producer.flush()` was called.

The plotting notebook (`plots/ingestion.ipynb`) reads `configuration.system`, `configuration.vecstream_partitions`, `configuration.throughput`, `configuration.batch_size`, `configuration.vector_dataset`, plus the per-result `latency_seconds` / `start_time` / `end_time` fields.

## Deployment

### Prerequisites

The prerequisite list lives in [deployment/benchmark_clients/README.md](../../deployment/benchmark_clients/README.md). Required tools:

- Terraform >= 1.3
- Ansible >= 2.14 with the `amazon.aws` collection: `ansible-galaxy collection install amazon.aws`
- AWS CLI >= 2.0 configured with appropriate credentials
- kubectl >= 1.27
- helm >= 3.14
- Python >= 3.13 on the local machine
- Access to your vector dataset files

### Deploy the benchmark client with Terraform

All Terraform commands are run from `deployment/benchmark_clients/terraform/`. The suite is selected by the active terraform workspace; this suite uses the `ingestion` workspace (applying in `default` fails by design).

```bash
cd deployment/benchmark_clients/terraform
terraform init
terraform workspace new ingestion     # or: terraform workspace select ingestion
```

Deploy the client EC2 (the ingestion benchmark itself does not require any extra AWS infrastructure; the Kafka cluster lives in EKS):

```bash
terraform apply
```

After `terraform apply`, the SSH private key is saved to `bench-key-ingestion.pem` in the Terraform directory (one key file per workspace). Use the `ssh_command` output to connect:

```bash
terraform output ssh_command
# ssh -i bench-key-ingestion.pem ubuntu@<client-ip>
```

Other useful outputs:

```bash
terraform output client_public_ip
terraform output client_instance_id
```

### Configure with Ansible

All Ansible commands are run from `deployment/benchmark_clients/ansible/`, selecting this suite with `-i inventory/aws_ec2_ingestion.yml`.

```bash
cd deployment/benchmark_clients/ansible
```

Set up the benchmark client (syncs `benchmarks/`, `vecstream/` and `pyproject.toml` to the instance, plus this suite's local dataset samples):

```bash
ansible-playbook -i inventory/aws_ec2_ingestion.yml playbooks/client.yml
```

To sync code changes to an already-provisioned client without re-running the full setup:

```bash
ansible-playbook -i inventory/aws_ec2_ingestion.yml playbooks/resync-code.yml
```

### Deploy Kafka on EKS

Follow instructions in [`deployment/kafka_on_eks`](../../deployment/kafka_on_eks/README.md). This deploys a Kafka cluster on EKS with the Strimzi operator. Use `./helper.sh get-external-bootstrap-servers` to get the bootstrap servers for the `--vecstream_bootstrap_servers` argument.

### Run the benchmark on the client

SSH into the client instance and run the benchmark:

```bash
ssh -i bench-key-ingestion.pem ubuntu@<client-ip>

# VecStream benchmark (precomputed centroids)
python -m benchmarks.ingestion.benchmark \
  --vector_dataset datasets/vectors.fbin \
  --system vecstream \
  --vecstream_bootstrap_servers localhost:9092 \
  --vecstream_centroids datasets/centroids.npy \
  --vecstream_partitions 250 \
  --batch_size 100 \
  --num_batches 500

# VecStream benchmark (compute centroids from a sample file)
python -m benchmarks.ingestion.benchmark \
  --vector_dataset datasets/vectors.fbin \
  --system vecstream \
  --vecstream_bootstrap_servers localhost:9092 \
  --vecstream_sample_file datasets/samples/sample_100k.fbin \
  --vecstream_partitions 250 \
  --batch_size 100 \
  --num_batches 500
```

### Teardown

```bash
cd deployment/benchmark_clients/terraform
terraform workspace select ingestion
terraform destroy
```

### Terraform variables

Variables of the unified client stack (full list in `deployment/benchmark_clients/terraform/variables.tf`):

| Variable | Default | Description |
|---|---|---|
| `region` | `us-east-1` | AWS region |
| `instance_type` | `m6i.large` | EC2 instance type for the client |
| `availability_zone` | `us-east-1a` | Availability zone for client EC2 |
| `vpc_id` | (empty) | VPC ID for the client (uses default VPC if empty) |
| `subnet_id` | (empty) | Subnet ID for the client (uses default subnet matching availability_zone if empty) |
| `enable_client` | `true` | Deploy the benchmark client EC2 instance |
| `enable_s3` | `false` | Deploy an S3 bucket for legacy S3-based benchmarks |
| `s3_bucket_name` | `ingestion-vector-store` | S3 bucket name (used when `enable_s3=true`) |
| `express_bucket_name` | `ingestion-vector-express` | Base name for S3 Express One Zone bucket |
| `availability_zone_abbreviation` | `use1-az6` | Availability zone abbreviation for S3 Express One Zone bucket |
| `dataset_s3_bucket_name` | `vecstream-benchmarks-data` | S3 bucket name where the dataset is stored |
| `tags` | `{}` | Extra tags applied to all resources; the `Project: ingestion-bench` tag comes from the `ingestion` workspace, not this variable |

## Adding a new system

1. Create `benchmarks/ingestion/systems/<name>.py` with a class that extends `VectorSystem`. The constructor should wire the system's own producer/client; `put_vectors` and `close` should defer to the underlying library where possible.
2. Register it in `benchmarks/ingestion/systems/__init__.py`:

```python
from benchmarks.ingestion.systems.base import VectorSystem
from benchmarks.ingestion.systems.<name> import <Name>System

_REGISTRY: dict[str, type[VectorSystem]] = {
    "vecstream": VecStreamSystem,
    "<name>": <Name>System,
}
```

3. Add system-specific CLI args in `benchmark.py`'s `parse_args()` and wire them in `run_benchmark()` where `system_kwargs` and `configuration` are built.
4. If the system needs infrastructure, add a Terraform module under `deployment/benchmark_clients/terraform/modules/` and a toggle variable in `variables.tf`. Add an Ansible playbook under `deployment/benchmark_clients/ansible/playbooks/` for system-specific provisioning.
