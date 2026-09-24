# VecStream

VecStream is a serverless vector database built on top of AWS Lambda, S3 and Kafka. It supports continuous ingestion and interactive queries. 

## Repository layout

```
VecStream/
├── vecstream/              Core library: Lambda handlers, routing, serialization, query client
├── benchmarks/             Benchmark suites (ingestion, static_queries, streaming_queries, tree_invocation, cache_retention)
├── deployment/             Terraform for the Lambda fleet
├── datasets/               Vector datasets (.npy, .fbin), gitignored
├── results/                Benchmark output, gitignored
└── pyproject.toml          Project metadata and dependencies
```

## Requirements

| Tool | Version | Used for |
|---|---|---|
| Python | 3.13+ | library and benchmarks |
| uv | any recent release | dependency management |
| AWS CLI | v2 | authentication, layer publishing |
| Terraform | 1.8+ | Lambda fleet, Kafka on EKS, benchmark clients |
| kubectl | 1.27+ | Kafka on EKS |
| Helm | 3.14+ | Kafka on EKS (Strimzi operator) |
| Ansible | 2.14+ with the `amazon.aws` collection | benchmark client provisioning |
| jq | any | wiring Lambda URLs into the client |

You need an AWS account with credentials available to boto3 and the AWS CLI (environment variables, shared credentials file or SSO). Everything defaults to the us-east-1 region.

Costs: the full stack is an EKS cluster with 3 Kafka brokers (m6i.4xlarge with a 1 TB gp3 volume each) plus Karpenter-provisioned r8g nodes, roughly 1026 Lambda functions and S3 storage. AWS charges accrue while the stack is deployed. The teardown commands are in [Teardown](#teardown); run them when you are done. 

## Installation

```bash
git clone https://github.com/gfinol/VecStream.git
cd VecStream
uv sync
```

Put your vector datasets under `datasets/` (gitignored). 

## Deploy Kafka on EKS

Kafka runs on EKS with the Strimzi operator. The deployment lives in `benchmarks/ingestion/deployment/vecstream/kafka_on_eks/`.

```bash
cd benchmarks/ingestion/deployment/vecstream/kafka_on_eks

# Recommended for the full pipeline: enable tiered storage so sealed segments
# are written to S3 and trigger the async indexer. Edit terraform/terraform.tfvars:
#   enable_tiered_storage = true
#   tiered_storage_bucket_name = "my-tiered-bucket"   # optional, auto-generated if omitted

./deploy.sh                                     # 25-30 minutes
source set-env.sh
kubectl get pods -n kafka                       # wait until all pods are Running
./helper.sh get-external-bootstrap-servers
```

Save the bootstrap servers and the tiered storage bucket name: both are inputs to the Lambda deployment in the next step. `deploy.sh` auto-generates a `deployment_id` in `terraform.tfvars` on first run; do not edit it manually. The full reference, including monitoring and helper commands, is in [benchmarks/ingestion/deployment/vecstream/kafka_on_eks/README.md](benchmarks/ingestion/deployment/vecstream/kafka_on_eks/README.md).

## Deploy the Lambda fleet

The Terraform stack in `deployment/` creates the query fleet. Build and publish the artifacts first:

```bash
cd deployment
bash package_lambda.sh     # code.zip built from vecstream/
bash package_layer.sh      # vecstream-layer.zip: numpy, faiss-cpu, aiohttp, confluent-kafka, mmh3
bash publish-layer.sh      # runs aws lambda publish-layer-version, prints the new layer version
```

Pin the new layer version in the `lambda_layers` default in `deployment/variables.tf`, then apply:

```bash
terraform init
terraform apply \
  -var 'async_index_kafka_bootstrap_servers=<bootstrap-servers>' \
  -var 'async_index_tiered_storage_bucket_name=<tiered-bucket>'
```

The two `async_index_*` variables wire the indexer: the bootstrap servers enable Kafka offset commits, the bucket name enables the S3 ObjectCreated notification that triggers indexing. 

The default deployment creates 500 L1 cache, 500 L2 cache and 25 Kafka search Lambdas (1769 MB memory, 60 s timeout, x86_64) plus one async index Lambda (4096 MB, 300 s). L1 and L2 share the `vecstream.map_lambda.event_handler` handler; the Kafka Lambdas run `vecstream.kafka_lambda.event_handler`. Lambdas run outside any VPC by default; set `vpc_id` to opt into VPC mode with S3 and S3 Express Gateway endpoints. Scale the fleet with `lambda_count_l1`, `lambda_count_l2` and `lambda_count_kafka`.

Wire the function URLs into the client:

```bash
cd deployment
terraform output -json \
  | jq '{l1: .l1_lambda_function_urls.value, l2: .l2_lambda_function_urls.value, kafka: .kafka_lambda_function_urls.value}'
```

Copy the three lists into `vecstream/urls.py`: `l1` into `l1_cache_lambda_urls`, `l2` into `l2_cache_lambda_urls`, `kafka` into `kafka_search_lambda_urls`. If you change the fleet later, regenerate the lists.

Smoke test with the WARMUP query type:

```bash
L1_URL=$(terraform output -json l1_lambda_function_urls | jq -r '.[0]')
curl -sS -X POST "$L1_URL" -H 'content-type: application/json' -d '{"query_type": "WARMUP"}'
```

Repeat with `l2_lambda_function_urls` and `kafka_lambda_function_urls`. Expected responses: `{"message": "Lambda warmup complete."}` for L1/L2 and `{"message": "Warmup completed successfully."}` for Kafka, all with HTTP 200. More deployment details, including the VPC and lambda count customizations, are in [deployment/README.md](deployment/README.md).

## Ingesting vectors

The library hides the Kafka producer and the routing. Compute centroids once from a sample, build the routing, then put batches:

```python
import numpy as np
from vecstream.ingestion import (
    VecStreamIngestionClient, build_ivf_routing, compute_centroids, load_vectors,
)

centroids = compute_centroids("datasets/sample.fbin", num_sample=100_000, n_clusters=250)
routing = build_ivf_routing(centroids, metric="euclidean")

client = VecStreamIngestionClient(
    bootstrap_servers="b-1.kafka-on-eks.example.amazonaws.com:9092",
    topic_name="vecstream_deep100m",
    routing=routing,
    dimension=96,
    metric="euclidean",
)

vectors = load_vectors("datasets/deep10M.fbin").astype(np.float32)
ids = [str(i) for i in range(len(vectors))]
for start in range(0, len(vectors), 100):
    timing = client.put_vectors(vectors[start:start + 100], ids[start:start + 100])
    print(timing["latency_seconds"])

client.close()
```

`put_vectors` returns a timing dict with `latency_seconds`, `insert_count` and per-partition serialization times. Two lifecycle details matter: the client deletes and recreates the topic on `__init__` (with a 10 s settle sleep), and `close()` deletes it again unless the client is built with `delete_topic_on_close=False`. The default suits the ingestion benchmark, where each run starts from a clean topic; the static queries ingest phase passes `delete_topic_on_close=False` so the topic persists for the query phase. `remote_storage_enabled=True` (the default) sets `remote.storage.enable=true` on the topic so cold segments tier to S3.

## Querying vectors

The query client is coroutine-based. Build it with the same centroids used for ingestion, load the L1 cache, warm the Lambdas, then search:

```python
import asyncio
import numpy as np
from vecstream.routing import IVFRouting
from vecstream.vecstream_client import VecStreamClient

centroids = np.load("datasets/centroids_250.npy")
routing = IVFRouting(
    n_partitions=centroids.shape[0],
    d=centroids.shape[1],
    centroids=centroids,
    metric="euclidean",
)

client = VecStreamClient(
    bucket="vecstream-vectors",
    prefix="deep100m/",
    num_partitions=centroids.shape[0],
    dimension=centroids.shape[1],
    routing=routing,
    bootstrap_servers="b-1.kafka-on-eks.example.amazonaws.com:9092",
    kafka_topic="vecstream_deep100m",
)

async def main() -> None:
    query = np.random.rand(96).astype(np.float32)
    distances, ids, timestamps = await client.search(query, topK=10)

asyncio.run(main())
```

`search` fans out to `num_partitions_to_search` partitions (default 16), merges partial results through the reduce tree with `reduce_branching_factor` (default 16) and `map_invocations_per_lambda` (default 16), and returns distances, ids and per-stage timestamps.


