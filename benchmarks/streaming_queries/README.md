# Vector streaming queries benchmark

Measures VecStream's query latency and recall as new vectors are ingested in
fixed-size epochs. After every epoch the same query set is re-run against
the cumulative index.

The benchmark drives VecStream's production pipeline end-to-end:

1. **Ingest.** A single long-lived `VecStreamIngestionClient` writes each
   epoch's vectors into a single Kafka topic. Tiered storage seals segments
   to S3 (default `segment.bytes = 7.5 MB`).
2. **Index asynchronously.** The deployed `async_index_creation` Lambda
   tails each sealed segment, trains a FAISS IVF index on it, uploads the
   `.ann` file under `{prefix}/partition_N/`, and updates the registry at
   `{prefix}.index_list.json`. We poll that registry until it has stabilized
   (`wait_for_indexes`) before firing the query set.
3. **Query.** A fresh `VecStreamQuerySystem` (the same class the
   `static_queries` benchmark uses) runs the queries against the
   cumulative index. Per-query wall-clock timeout with bounded retries is
   preserved from the previous implementation.

## Structure

```
streaming_queries/
├── vecstream.py          Streaming benchmark CLI (ingest -> wait -> query per epoch)
├── faiss_exhaustive_search.py  Cumulative per-epoch ground truth (FAISS flat search)
└── systems/
    └── __init__.py       Thin `get_system("vecstream")` factory; re-exports the shared
                          `VectorSystem` ABC and `VecStreamQuerySystem` from
                          benchmarks.static_queries.systems.
```

Deployment artifacts (unified stack, `streaming-queries` workspace):

```
deployment/benchmark_clients/
├── terraform/    EC2 client only (no baseline provisioning)
└── ansible/      Provisioning (client.yml, resync-code.yml)
```

## Quick start

The benchmark assumes the VecStream Lambda fleet is already deployed (see
the top-level [README](../../README.md#deploy-the-lambda-fleet)) and that
the `async_index_lambda` is wired up to the same Kafka brokers and the same
S3 tiered storage bucket used by the topic. The async indexer must be
configured with an `INDEX_PREFIX` matching the `--vecstream_prefix` you pass
to this CLI (so its `{prefix}.index_list.json` ends up where
`VecStreamQuerySystem` looks). Leaving `INDEX_PREFIX` empty falls back to
`{topic}/indexes`, in which case `--vecstream_prefix` must match.

```bash
# 1. Pre-train the centroids once (kept fixed across all epochs).
uv run python -c "
import numpy as np
from vecstream.ingestion import compute_centroids
centroids = compute_centroids('datasets/sample_msturing.fbin', num_sample=100_000, n_clusters=250)
np.save('datasets/centroids_250.npy', centroids)
"

# 2. Run the benchmark.
uv run python -m benchmarks.streaming_queries.vecstream \
  --query_dataset datasets/queries_msturing.fbin \
  --vector_dataset datasets/msturing_30m.fbin \
  --num-epochs 10 \
  --epoch-size 3000000 \
  --batch-size 100 \
  --top_ks 1,10,100 \
  --vecstream_bucket vecstream-vectors \
  --vecstream_prefix msturing/ \
  --vecstream_dataset_name msturing \
  --vecstream_centroids datasets/centroids_250.npy \
  --vecstream_bootstrap_servers b-1.kafka-on-eks.example.amazonaws.com:9092
```

One JSON per (epoch, `top_k`):
`query_vecstream_<dataset>_topk<K>_epoch<E>_<ts>.json`.

The per-epoch output schema matches what `plots/streaming_queries.ipynb`
and `plots/streaming_recall.ipynb` consume (see `Output format` below).

## CLI arguments

### `vecstream.py`

Required unless noted; defaults in parentheses.

| Argument | Default | Description |
|---|---|---|
| `--query_dataset` | (required) | `.npy` or `.fbin` with query vectors |
| `--vector_dataset` | (required) | `.npy` or `.fbin` with the vectors to ingest, distinct from `--query_dataset` |
| `--num-epochs` | (required) | Number of epochs to run (0..N-1) |
| `--epoch-size` | (required) | Vectors ingested per epoch; must be a multiple of `--batch-size` |
| `--batch-size` | `100` | Vectors per `put_vectors()` Kafka batch |
| `--top_ks` | `1,10,100` | Comma-separated `top_k` values (one JSON per value) |
| `--max_queries` | `1000` | Queries to run per checkpoint |
| `--output_dir` | `.` | Directory for the per-epoch output JSON files |
| `--dataset_name` | (stem of `--vector_dataset`) | Override the dataset name baked into filenames and metadata |
| `--vecstream_bucket` | (required) | S3 bucket holding the per-partition indexes and `{prefix}.index_list.json` |
| `--vecstream_prefix` | (required) | Base S3 prefix; the async indexer writes here |
| `--vecstream_dataset_name` | (stem of `--vector_dataset`) | Used to derive the default Kafka topic name when `--vecstream_topic` is absent |
| `--vecstream_topic` | (auto) | Override the Kafka topic name. One topic for the whole run; recreates on `__init__`, deletes on `close()` |
| `--vecstream_centroids` | (none) | `.npy` with KMeans centroids (shape `[num_partitions, dim]`); mutually exclusive with `--vecstream_sample_file` |
| `--vecstream_sample_file` | (none) | `.npy` or `.fbin` to fit KMeans on (used when `--vecstream_centroids` is absent) |
| `--vecstream_n_clusters` | `250` | Centroids to fit when using `--vecstream_sample_file` |
| `--vecstream_num_sample` | `100_000` | Rows to load from `--vecstream_sample_file` for the fit |
| `--vecstream_num_partitions` | (auto) | Optional override; must match `centroids.shape[0]` |
| `--vecstream_metric` | `euclidean` | `euclidean` or `cosine` |
| `--vecstream_bootstrap_servers` | `localhost:9092` | Kafka bootstrap servers (comma-separated) |
| `--vecstream_group_id` | `vecstream_group` | Kafka consumer group id |
| `--vecstream_block_size` | `7_500_000` | `segment.bytes` (7.5 MB default; the seal threshold the async indexer consumes) |
| `--vecstream_kafka_acks` | `1` | Producer `acks` (use `all` for acks=-1) |
| `--vecstream_kafka_local_retention_bytes` | `1` | `local.retention.bytes` (1 byte: tier out ASAP) |
| `--vecstream_kafka_compression_type` | (none) | Optional `compression.type` (`gzip`/`snappy`/`lz4`/`zstd`) |
| `--vecstream_remote_storage_disabled` | (false) | Set `remote.storage.enable=false` on the topic |
| `--vecstream_num_partitions_to_search` | `16` | Partitions each query fans out to |
| `--vecstream_reduce_branching_factor` | `16` | Reduce fan-out branching factor |
| `--vecstream_map_invocations_per_lambda` | `16` | Map invocations per Lambda worker |
| `--vecstream_use_cache` | (false) | Route queries through the L1/L2 cache Lambdas |
| `--vecstream_no_warmup` | (false) | Skip `warmup_lambdas()` in `VecStreamQuerySystem.__init__` |
| `--vecstream_region` | (env default) | AWS region for the boto3 S3 client |
| `--vecstream_wait_stability_polls` | `3` | Consecutive stable polls before `wait_for_indexes()` returns |
| `--vecstream_wait_poll_interval` | `10.0` | Seconds between `wait_for_indexes()` polls |
| `--vecstream_wait_timeout` | `3600.0` | Max seconds to wait per epoch before raising `TimeoutError` |
| `--ingest_max_retries` | `3` | Retries per failed `put_vectors()` batch |
| `--ingest_retry_sleep_seconds` | `2.0` | Sleep between `put_vectors()` retries |
| `--query_timeout_seconds` | `10.0` | Per-query wall-clock timeout |
| `--max_retries` | `3` | Max retries on a query timeout (so up to 4 total attempts) |

### `faiss_exhaustive_search.py`

Used for ground truth (recall computation). Unchanged from the previous
release.

| Argument | Default | Description |
|---|---|---|
| `--dataset` | (required) | `.npy` or `.fbin` of the full dataset |
| `--queries` | (required) | `.npy` or `.fbin` of the query set |
| `--epoch-size` | (required) | Vectors per epoch (cumulative: epoch `e` = `(e+1)*epoch_size` rows) |
| `--topk` | (required) | Number of nearest neighbors |
| `--metric` | `euclidean` | `euclidean` or `cosine` |
| `--max-queries` | `1000` | Maximum queries to run |
| `--output` | (auto) | Output JSON path |

## Output format

The per-`(epoch, top_k)` JSON schema (consumed by the notebooks in
`plots/`):

```json
{
  "configuration": {
    "system": "vecstream",
    "query_dataset": "datasets/queries_msturing.fbin",
    "vector_dataset": "datasets/msturing_30m.fbin",
    "dataset_name": "msturing",
    "num_queries": 1000,
    "vector_dimension": 100,
    "num_epochs": 10,
    "epoch_size": 3000000,
    "batch_size": 100,
    "query_timeout_seconds": 10.0,
    "max_retries": 3,
    "ingest_max_retries": 3,
    "ingest_retry_sleep_seconds": 2.0,
    "vecstream": {
      "bucket": "vecstream-vectors",
      "base_prefix": "msturing/",
      "num_partitions": 250,
      "num_stream_partitions": 250,
      "centroids_file": "datasets/centroids_250.npy",
      "bootstrap_servers": "b-1.kafka-on-eks.example.amazonaws.com:9092",
      "group_id": "vecstream_group",
      "metric": "euclidean",
      "use_cache": false,
      "warmup": true,
      "region": null,
      "num_partitions_to_search": 16,
      "reduce_branching_factor": 16,
      "map_invocations_per_lambda": 16,
      "topic": "msturing_250",
      "block_size": 7500000,
      "kafka_acks": "1",
      "kafka_local_retention_bytes": 1,
      "kafka_compression_type": null,
      "remote_storage_enabled": true,
      "wait_stability_polls": 3,
      "wait_poll_interval": 10.0,
      "wait_timeout": 3600.0
    },
    "top_k": 10,
    "epoch_index": 0,
    "epoch_prefix": "msturing/",
    "epoch_topic": "msturing_250",
    "epoch_start_offset": 0,
    "epoch_end_offset": 3000000,
    "epoch_ingest_seconds": 142.51,
    "epoch_registry_total_indexes": 134,
    "epoch_registry_stable_for": 3,
    "epoch_registry_key": "msturing.index_list.json"
  },
  "results": [
    {
      "query_index": 0,
      "start_time": "2026-09-24T10:00:00.123456+00:00",
      "end_time": "2026-09-24T10:00:00.456789+00:00",
      "latency_seconds": 0.333333,
      "query_vector": [0.1, 0.2, "..."],
      "results": [{"id": "2018052", "distance": 0.7068}],
      "distance_metric": "euclidean",
      "system_data": {
        "vecstream_timestamps": {...},
        "metric": "euclidean",
        "num_partitions_to_search": 16,
        "use_cache": false
      },
      "attempts": 1,
      "error": null
    },
    "..."
  ]
}
```

## Ground truth for recall

`faiss_exhaustive_search.py` produces one ground-truth JSON with a
`{configuration, results: {epoch_0: [...], epoch_1: [...]}}` shape that
`plots/streaming_recall.ipynb` joins on `(query_dataset_stem, topk, epoch)`.

```bash
uv run python -m benchmarks.streaming_queries.faiss_exhaustive_search \
  --dataset datasets/msturing_30m.fbin \
  --queries datasets/queries_msturing.fbin \
  --epoch-size 3000000 \
  --topk 10 \
  --metric euclidean
```

## Deployment

The benchmark client EC2 is provisioned by the unified stack in
`deployment/benchmark_clients/` (the `streaming-queries` workspace). 

### Prerequisites

- Terraform >= 1.3
- Ansible >= 2.14 with the `amazon.aws` collection
- AWS CLI >= 2.0
- Existing EKS cluster running Kafka and the VecStream Lambda fleet
  (Kafka deployed via `deployment/kafka_on_eks/`, the Lambda fleet via the
  root `deployment/` Terraform)

### Deploy the benchmark client

```bash
cd deployment/benchmark_clients/terraform
terraform init
terraform workspace select streaming-queries   # first time: terraform workspace new streaming-queries
terraform apply
```

Useful outputs:

```bash
terraform output client_public_ip
terraform output ssh_command
```

Provision the client (syncs the codebase and runs `uv sync`):

```bash
cd deployment/benchmark_clients/ansible
ansible-playbook -i inventory/aws_ec2_streaming_queries.yml playbooks/client.yml
```

### Terraform variables

| Variable | Default | Description |
|---|---|---|
| `region` | `us-east-1` | AWS region |
| `instance_type` | `m6i.large` | EC2 instance type for the benchmark client |
| `availability_zone` | `us-east-1a` | AZ for the client EC2 instance |
| `vpc_id` | (default VPC) | VPC id for the EC2 |
| `subnet_id` | (default subnet in AZ) | Subnet id for the EC2 |
| `enable_client` | `true` | Deploy the benchmark client EC2 instance |
| `dataset_s3_bucket_name` | `vecstream-benchmarks-data` | Bucket the client syncs source datasets from |
| `tags` | `{"Project": "streaming-queries-bench"}` | Tags applied to all resources |

Note: the old per-suite `terraform.tfstate` files were deleted along with the
deployment directories, so the merged stack starts from empty state in each
workspace. If a previous benchmark client (or its S3 Vectors resources) is
still deployed in your account, it is no longer managed by Terraform; clean
it up manually via the AWS CLI.
