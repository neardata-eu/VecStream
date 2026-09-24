# VecStream static queries benchmark

Two-phase benchmark for paper §5.4 (figure 9): ingest Deep-100M via the real
VecStream pipeline, then run 1000 queries with top-k in {1, 10, 100} and
measure recall against FAISS ground truth plus warm p95 latency.

The pipeline uses only the real VecStream path:

1. `ingest.py` produces vectors through `VecStreamIngestionClient` to Kafka,
   lets the deployed async indexer Lambda train per-segment FAISS IVF indexes
   and update the S3 registry, then blocks until `wait_for_indexes` reports a
   stable registry.
2. `benchmark.py` issues queries sequentially through `VecStreamClient`, which
   fans out to the L1/L2 cache Lambdas and the Kafka stream-search Lambdas.

`faiss_exhaustive_search.py` produces the ground truth used by
`plots/static_queries.ipynb` for recall.

## Structure

```
static_queries/
├── ingest.py                  Phase 1: real VecStream ingestion + wait for async indexing
├── benchmark.py               Phase 2: query latency benchmark (VecStream only)
├── data_loader.py             Canonical vector loader (.npy / .fbin)
├── faiss_exhaustive_search.py FAISS flat-index ground truth
└── systems/
    ├── base.py                VectorSystem ABC
    ├── vecstream.py           VecStreamQuerySystem (queries via VecStreamClient)
    └── __init__.py            get_system() factory (registry = {"vecstream"})
```

The benchmark client EC2 is provisioned by the unified stack in
`deployment/benchmark_clients/` (the `static-queries` workspace), see
[Deployment](#deployment) below.

## Quick start

### One-time: deploy the VecStream infrastructure

The benchmark consumes the Lambda fleet deployed by `deployment/` (root) and
the Kafka cluster deployed by `deployment/kafka_on_eks/`. Run those first,
keep `enable_tiered_storage = true` on the Kafka deployment so the async
indexer Lambda can pick up cold segments, and capture the bootstrap servers,
tiered-storage bucket name, and Lambda URLs.

### Phase 1: ingest

`ingest.py` is the real VecStream ingest entry point. Either precompute the
centroids from a sample or pass a `.npy` file, then let the runner stream the
dataset to Kafka and block until the async indexer has caught up.

```bash
python -m benchmarks.static_queries.ingest \
  --dataset datasets/deep100m.fbin \
  --batch-size 100 \
  --sample-file datasets/sample.fbin \
  --n-clusters 250 \
  --bootstrap-servers b-1.kafka-on-eks.example.amazonaws.com:9092 \
  --topic vecstream_deep100m \
  --bucket vecstream-vectors \
  --prefix deep100m/ \
  --metric euclidean \
  --output-file results/ingest_deep100m.json
```

If you already have a centroids file:

```bash
python -m benchmarks.static_queries.ingest \
  --dataset datasets/deep100m.fbin \
  --batch-size 100 \
  --centroids datasets/centroids_250.npy \
  --bootstrap-servers b-1.kafka-on-eks.example.amazonaws.com:9092 \
  --topic vecstream_deep100m \
  --bucket vecstream-vectors \
  --prefix deep100m/ \
  --metric euclidean
```

`centroids.shape[0]` must equal the Kafka topic partition count, which is set
by the `IVFRouting` built from the centroids. The number of partitions printed
by the runner is the topic size the producer will create.

### Phase 2: queries

Run the query benchmark. Pass the same centroids file used for ingest and the
same `--bucket`/`--prefix` so `VecStreamClient.load_index_list()` finds the
registry the async indexer just finished populating.

```bash
python -m benchmarks.static_queries.benchmark \
  --query_dataset datasets/queries.fbin \
  --top_k 10 \
  --vecstream_bucket vecstream-vectors \
  --vecstream_prefix deep100m/ \
  --vecstream_centroids datasets/centroids_250.npy \
  --vecstream_num_partitions 250 \
  --vecstream_bootstrap_servers b-1.kafka-on-eks.example.amazonaws.com:9092 \
  --vecstream_topic vecstream_deep100m
```

To capture the paper's warm L1 numbers, leave warmup enabled (default). Pass
`--vecstream_no_warmup` to measure cold-start latency. `--vecstream_use_cache`
routes through the L1/L2 cache Lambdas (CACHED_QUERY); without it the queries
go directly through the search Lambdas (FAISS_QUERY).

### Ground truth

```bash
python -m benchmarks.static_queries.faiss_exhaustive_search \
  --dataset datasets/deep100m.fbin \
  --queries datasets/queries.fbin \
  --topk 10 \
  --metric l2
```

The output `{queries_stem}_results.json` is consumed by `plots/static_queries.ipynb`
together with the benchmark JSON to compute recall.

## CLI arguments

### `ingest.py`

| Argument | Default | Description |
|---|---|---|
| `--dataset` / `--vector_dataset` | (required) | Path to `.npy` or `.fbin` vector file. |
| `--batch-size` / `--batch_size` | `100` | Vectors per batch. |
| `--centroids` | (none) | Path to a `.npy` file with precomputed centroid vectors. Mutually exclusive with `--sample-file`. |
| `--sample-file` / `--sample_file` | (none) | Path to a `.npy` or `.fbin` sample used to fit KMeans. Required when `--centroids` is not set. |
| `--n-clusters` / `--n_clusters` | `--num-partitions` | Number of KMeans clusters (and Kafka topic partitions) when computing centroids. |
| `--num-sample` / `--num_sample` | `100000` | Sample size for centroid fitting. |
| `--bootstrap-servers` / `--bootstrap_servers` | `localhost:9092` | Comma-separated Kafka bootstrap servers. |
| `--topic` | `vecstream_topic` | Kafka topic name. |
| `--bucket` | (required) | S3 bucket the async indexer writes per-partition FAISS indexes and the `{prefix}.index_list.json` registry to. |
| `--prefix` | (required) | S3 prefix used by the async indexer. The registry key is `{prefix.rstrip('/')}.index_list.json`. |
| `--dimension` | `0` | Vector dimensionality; `0` to infer from `--dataset`. |
| `--metric` | `euclidean` | `euclidean` or `cosine`. |
| `--num-partitions` / `--num_partitions` | (derived) | Kafka topic partition count. Defaults to `--n-clusters` when computing centroids, else to `centroids.shape[0]`. |
| `--block-size` / `--block_size` | `7500000` | Kafka `segment.bytes` (sealing threshold for the async indexer). |
| `--remote-storage-disabled` / `--remote_storage_disabled` | `False` | Disable tiered storage on the topic. |
| `--kafka-acks` / `--kafka_acks` | `1` | Kafka producer `acks`. |
| `--kafka-local-retention-bytes` / `--kafka_local_retention_bytes` | `1` | Kafka `local.retention.bytes`. |
| `--kafka-compression-type` / `--kafka_compression_type` | `None` | Kafka producer `compression.type` (`gzip`, `snappy`, `lz4`, `zstd`). |
| `--batch-to-same-partition` / `--batch_to_same_partition` | `False` | Send each batch to one random partition (debug knob). |
| `--num-batches` / `--num_batches` | (all) | Number of batches to ingest. |
| `--max-vectors` / `--max_vectors` | (all) | Max vectors to ingest. |
| `--start-batch` / `--start_batch` | `0` | Batch index to start ingestion from. |
| `--throughput` | `0` | Target batches per second for pacing. `0` disables pacing. |
| `--max-retries` / `--max_retries` | `3` | Max retries per failed batch during ingest. |
| `--wait-stability-polls` / `--wait_stability_polls` | `3` | Consecutive identical registry totals before `wait_for_indexes` returns. |
| `--wait-poll-interval` / `--wait_poll_interval` | `10` | Seconds between registry polls. |
| `--wait-timeout` / `--wait_timeout` | `3600` | Max seconds to wait for the async indexer to stabilize. |
| `--skip-wait` / `--skip_wait` | `False` | Skip `wait_for_indexes` (use when the registry is already populated). |
| `--region` | (default chain) | AWS region for the boto3 S3 client used by `wait_for_indexes`. |
| `--output-file` / `--output_file` | (auto) | Output JSON path. |

The runner constructs `VecStreamIngestionClient` with `delete_topic_on_close=False`
so the topic persists for the query phase; only the producer is flushed on
`close()`.

### `benchmark.py`

| Argument | Default | Description |
|---|---|---|
| `--query_dataset` | (required) | Path to `.npy` or `.fbin` query vectors. |
| `--system` | `vecstream` | Target system (only `vecstream` accepted). |
| `--top_k` | `10` | Nearest neighbors per query. |
| `--max_queries` | (all) | Max queries to run. |
| `--output_file` | (auto) | Output JSON path. |
| `--vecstream_bucket` | (required) | S3 bucket with per-partition FAISS indexes. |
| `--vecstream_prefix` | (required) | S3 key prefix; indexes live under `{prefix}/partition_{N}/`. |
| `--vecstream_centroids` | (required) | Path to the `.npy` centroids file used at ingest. |
| `--vecstream_num_partitions` | (centroids.shape[0]) | Number of partitions. Must match the Kafka topic and `centroids.shape[0]`. |
| `--vecstream_bootstrap_servers` | `localhost:9092` | Kafka bootstrap address. |
| `--vecstream_topic` | `vecstream_topic` | Kafka topic for stream search. |
| `--vecstream_group_id` | `vecstream_group` | Kafka consumer group id. |
| `--vecstream_metric` | `euclidean` | Distance metric (`euclidean` or `cosine`). |
| `--vecstream_num_partitions_to_search` | `16` | Partitions each query fans out to. |
| `--vecstream_reduce_branching_factor` | `16` | Reduce fanout branching factor. |
| `--vecstream_map_invocations_per_lambda` | `16` | Map invocations per Lambda worker. |
| `--vecstream_use_cache` | `False` | Route through L1/L2 cache Lambdas (`CACHED_QUERY`). |
| `--vecstream_no_warmup` | `False` | Skip `load_l1_cache()` and `warmup_lambdas()` in `__init__`. |
| `--vecstream_region` | (env default) | AWS region for the boto3 S3 client. |

## Output format

### Ingestion results

Auto-generated name: `ingest_vecstream_{dataset}_bs{batch-size}[_tp{throughput}]_{timestamp}.json`.

```json
{
  "configuration": {
    "system": "vecstream",
    "dataset": "datasets/deep100m.fbin",
    "batch_size": 100,
    "throughput": 0,
    "max_retries": 3,
    "vector_dimension": 96,
    "total_dataset_vectors": 100000000,
    "start_batch": 0,
    "num_batches": null,
    "max_vectors": null,
    "vecstream_bootstrap_servers": "b-1...:9092",
    "vecstream_topic": "vecstream_deep100m",
    "vecstream_bucket": "vecstream-vectors",
    "vecstream_prefix": "deep100m/",
    "vecstream_metric": "euclidean",
    "vecstream_num_partitions": 250,
    "vecstream_block_size": 7500000,
    "vecstream_remote_storage_disabled": false,
    "vecstream_kafka_acks": "1",
    "vecstream_kafka_local_retention_bytes": 1,
    "vecstream_kafka_compression_type": null,
    "vecstream_batch_to_same_partition": false,
    "vecstream_centroids": "datasets/centroids_250.npy",
    "vecstream_sample_file": null,
    "vecstream_num_sample": 100000,
    "vecstream_n_clusters": 250,
    "vecstream_wait_stability_polls": 3,
    "vecstream_wait_poll_interval": 10,
    "vecstream_wait_timeout": 3600,
    "vecstream_skip_wait": false,
    "vecstream_region": null,
    "vecstream_registry_key": "deep100m.index_list.json",
    "vecstream_total_indexes": 1342,
    "vecstream_stable_for": 3
  },
  "results": [
    {
      "batch_index": 0,
      "start_time": "2026-09-24T10:00:00.123456+00:00",
      "end_time": "2026-09-24T10:00:00.456789+00:00",
      "latency_seconds": 0.333333,
      "insert_count": 100,
      "partition_serialization_times": [[0, 0.4, 0.41], [1, 0.41, 0.42]],
      "flush_time_seconds": 0.43
    }
  ],
  "registry": {
    "partition_0": ["index_0.ann", "index_750000.ann", ...],
    "partition_1": [...]
  }
}
```

Per-batch entries include `latency_seconds`, `insert_count`,
`partition_serialization_times`, `flush_time_seconds`, and (for recovered
batches) `retries_needed`.

### Benchmark results

Auto-generated name: `query_vecstream_{dataset}_topk{top_k}_{timestamp}.json`.
The schema is unchanged from the previous multi-system version so
`plots/static_queries.ipynb` keeps working:

```json
{
  "configuration": {
    "system": "vecstream",
    "query_dataset": "datasets/queries.fbin",
    "top_k": 10,
    "num_queries": 1000,
    "vector_dimension": 96,
    "vecstream": {
      "bucket": "vecstream-vectors",
      "prefix": "deep100m/",
      "num_partitions": 250,
      "centroids_file": "datasets/centroids_250.npy",
      "bootstrap_servers": "b-1...:9092",
      "topic": "vecstream_deep100m",
      "group_id": "vecstream_group",
      "metric": "euclidean",
      "use_cache": false,
      "warmup": true,
      "region": null,
      "num_partitions_to_search": 16,
      "reduce_branching_factor": 16,
      "map_invocations_per_lambda": 16
    }
  },
  "results": [
    {
      "query_index": 0,
      "start_time": "2026-09-24T11:00:00.123456+00:00",
      "end_time": "2026-09-24T11:00:00.173579+00:00",
      "latency_seconds": 0.050123,
      "query_vector": [0.1, 0.2, ...],
      "results": [{"id": "42", "distance": 0.15}, {"id": "7", "distance": 0.23}],
      "distance_metric": "euclidean",
      "system_data": {
        "vecstream_timestamps": {"start_search": ..., "end_search": ...},
        "metric": "euclidean",
        "num_partitions_to_search": 16,
        "use_cache": false
      }
    }
  ]
}
```

The notebook reads `configuration.system`, `configuration.top_k`,
`results[].query_index`, `results[].results[].id`, and
`results[].latency_seconds`.
## Deployment

### Prerequisites

Required tools: Terraform >= 1.3, AWS CLI >= 2.0, Ansible >= 2.14 with the
`amazon.aws` collection (>= 7.0), Python >= 3.13, and SSH.

### Deploy the benchmark client

All Terraform commands are run from `deployment/benchmark_clients/terraform/`.

```bash
cd deployment/benchmark_clients/terraform
terraform init
terraform workspace select static-queries   # first time: terraform workspace new static-queries
terraform apply
```

This creates one `bench-client` EC2 instance with `bench-client-dataset-access`
S3 permissions on the configured dataset bucket (default
`vecstream-benchmarks-data`). The VecStream Lambda fleet, Kafka cluster and
S3 tiered-storage bucket are deployed separately, see the top-level
`README.md`.

Override the dataset bucket:

```bash
terraform apply -var='dataset_s3_bucket_name=my-bench-data'
```

After `terraform apply`, the SSH private key is saved to
`bench-key-static-queries.pem` in the Terraform directory. Use the
`ssh_command` output to connect:

```bash
terraform output ssh_command
# ssh -i bench-key-static-queries.pem ubuntu@<client-ip>
```

### Configure the client with Ansible

```bash
cd deployment/benchmark_clients/ansible
ansible-playbook -i inventory/aws_ec2_static_queries.yml playbooks/client.yml
```

The playbook installs Python 3.13 via `uv`, syncs `benchmarks/`,
`vecstream/`, and `pyproject.toml` to `/opt/vecstream/`, runs `uv sync`, and
downloads the dataset from `s3://vecstream-benchmarks-data/`. Use
`playbooks/resync-code.yml` to push code changes without re-running the full
setup.

### Run benchmarks on the client

```bash
ssh -i bench-key-static-queries.pem ubuntu@<client-ip>
cd /opt/vecstream

python -m benchmarks.static_queries.ingest \
  --dataset datasets/deep100m.fbin \
  --centroids datasets/centroids_250.npy \
  --bootstrap-servers b-1.kafka-on-eks.example.amazonaws.com:9092 \
  --topic vecstream_deep100m \
  --bucket vecstream-vectors \
  --prefix deep100m/ \
  --metric euclidean

python -m benchmarks.static_queries.benchmark \
  --query_dataset datasets/queries.fbin \
  --top_k 10 \
  --vecstream_bucket vecstream-vectors \
  --vecstream_prefix deep100m/ \
  --vecstream_centroids datasets/centroids_250.npy \
  --vecstream_num_partitions 250 \
  --vecstream_bootstrap_servers b-1.kafka-on-eks.example.amazonaws.com:9092 \
  --vecstream_topic vecstream_deep100m
```

### Teardown

```bash
cd deployment/benchmark_clients/terraform
terraform workspace select static-queries
terraform destroy
```

### Terraform variables

| Variable | Default | Description |
|---|---|---|
| `region` | `us-east-1` | AWS region. |
| `instance_type` | `m6i.large` | EC2 instance type for the client. |
| `availability_zone` | `us-east-1a` | Availability zone for the client EC2. |
| `vpc_id` | (default VPC) | VPC ID for the client. |
| `subnet_id` | (default subnet) | Subnet ID for the client. |
| `enable_client` | `true` | Deploy the benchmark client EC2. |
| `dataset_s3_bucket_name` | `vecstream-benchmarks-data` | S3 bucket where the dataset lives. |
| `tags` | `{Project: static-queries-bench}` | Tags applied to all resources. |
