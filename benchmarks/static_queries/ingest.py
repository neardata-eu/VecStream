"""Real VecStream ingestion entry point for the static_queries benchmark.

Phase 1 of the two-phase workflow. Streams the dataset through
``vecstream.ingestion.VecStreamIngestionClient`` to Kafka, lets the deployed
async indexer Lambda train per-segment FAISS indexes and update the S3 index
registry, then blocks until ``wait_for_indexes`` reports a stable registry.

The query phase (``benchmark.py``) reads the same registry through
``VecStreamClient``, so the two halves stay in lock-step.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from benchmarks.static_queries.data_loader import (
    get_vector_count_and_dimension,
    vector_generator,
)
from vecstream.ingestion import (
    VecStreamIngestionClient,
    build_ivf_routing,
    compute_centroids,
    wait_for_indexes,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest a vector dataset into VecStream via Kafka and wait for the "
            "async indexer Lambda to register every sealed-segment index in S3."
        )
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help=(
            "Path to .npy or .fbin file containing vectors. Required unless "
            "--centroids is supplied together with --num-batches 0."
        ),
    )
    parser.add_argument(
        "--vector_dataset",
        dest="dataset",
        type=str,
        default=None,
        help="Alias for --dataset (matches the ingestion benchmark naming).",
    )
    parser.add_argument(
        "--batch-size",
        "--batch_size",
        dest="batch_size",
        type=int,
        default=100,
        help="Vectors per batch (default: 100).",
    )
    parser.add_argument(
        "--centroids",
        type=str,
        default=None,
        help=(
            "Path to a .npy file with precomputed centroid vectors. Mutually "
            "exclusive with --sample-file; required when --sample-file is not "
            "given."
        ),
    )
    parser.add_argument(
        "--sample-file",
        "--sample_file",
        dest="sample_file",
        type=str,
        default=None,
        help=(
            "Path to a .npy or .fbin sample used to fit KMeans when no "
            "--centroids file is supplied."
        ),
    )
    parser.add_argument(
        "--n-clusters",
        "--n_clusters",
        dest="n_clusters",
        type=int,
        default=None,
        help=(
            "Number of KMeans clusters (and Kafka topic partitions) when "
            "computing centroids from --sample-file. Defaults to "
            "--num-partitions when omitted."
        ),
    )
    parser.add_argument(
        "--num-sample",
        "--num_sample",
        dest="num_sample",
        type=int,
        default=100_000,
        help="Number of vectors to sample from --sample-file (default: 100000).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        "--bootstrap_servers",
        dest="bootstrap_servers",
        type=str,
        default="localhost:9092",
        help="Comma-separated Kafka bootstrap servers (default: localhost:9092).",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="vecstream_topic",
        help="Kafka topic name (default: vecstream_topic).",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help=(
            "S3 bucket where the async indexer stores its per-partition "
            "FAISS indexes and the {prefix}.index_list.json registry."
        ),
    )
    parser.add_argument(
        "--prefix",
        type=str,
        required=True,
        help=(
            "S3 prefix used by the async indexer. The registry key is "
            "{prefix.rstrip('/')}.index_list.json."
        ),
    )
    parser.add_argument(
        "--dimension",
        type=int,
        default=0,
        help=(
            "Vector dimensionality; 0 to infer from --dataset "
            "(default: 0)."
        ),
    )
    parser.add_argument(
        "--metric",
        type=str,
        default="euclidean",
        choices=["euclidean", "cosine"],
        help="Distance metric: euclidean or cosine (default: euclidean).",
    )
    parser.add_argument(
        "--num-partitions",
        "--num_partitions",
        dest="num_partitions",
        type=int,
        default=None,
        help=(
            "Number of Kafka topic partitions. Defaults to --n-clusters when "
            "centroids are computed, otherwise to centroids.shape[0] when a "
            "--centroids file is loaded."
        ),
    )
    parser.add_argument(
        "--block-size",
        "--block_size",
        dest="block_size",
        type=int,
        default=7_500_000,
        help=(
            "Kafka segment.bytes (sealing threshold for the async indexer) "
            "(default: 7500000, i.e. 7.5MB)."
        ),
    )
    parser.add_argument(
        "--remote-storage-disabled",
        "--remote_storage_disabled",
        dest="remote_storage_disabled",
        action="store_true",
        help="Disable tiered storage on the Kafka topic (default: enabled).",
    )
    parser.add_argument(
        "--kafka-acks",
        "--kafka_acks",
        dest="kafka_acks",
        type=str,
        default="1",
        help="Kafka producer acks setting (default: 1).",
    )
    parser.add_argument(
        "--kafka-local-retention-bytes",
        "--kafka_local_retention_bytes",
        dest="kafka_local_retention_bytes",
        type=int,
        default=1,
        help="Kafka local.retention.bytes (default: 1, i.e. minimal local).",
    )
    parser.add_argument(
        "--kafka-compression-type",
        "--kafka_compression_type",
        dest="kafka_compression_type",
        type=str,
        default=None,
        help=(
            "Kafka producer compression.type (gzip, snappy, lz4, zstd). "
            "Default: None (no compression)."
        ),
    )
    parser.add_argument(
        "--batch-to-same-partition",
        "--batch_to_same_partition",
        dest="batch_to_same_partition",
        action="store_true",
        help=(
            "Send every batch to a single random partition (debugging knob; "
            "skips the IVF routing)."
        ),
    )
    parser.add_argument(
        "--num-batches",
        "--num_batches",
        dest="num_batches",
        type=int,
        default=None,
        help=(
            "Number of batches to ingest (default: entire dataset at "
            "--batch-size)."
        ),
    )
    parser.add_argument(
        "--max-vectors",
        "--max_vectors",
        dest="max_vectors",
        type=int,
        default=None,
        help="Max vectors to ingest (default: entire dataset).",
    )
    parser.add_argument(
        "--start-batch",
        "--start_batch",
        dest="start_batch",
        type=int,
        default=0,
        help="Batch index to start ingestion from (default: 0).",
    )
    parser.add_argument(
        "--throughput",
        type=float,
        default=0.0,
        help=(
            "Optional target batches per second for pacing. The runner sleeps "
            "between batches to match this rate. 0 disables pacing (default)."
        ),
    )
    parser.add_argument(
        "--max-retries",
        "--max_retries",
        dest="max_retries",
        type=int,
        default=3,
        help="Max retries per failed batch during ingest (default: 3).",
    )
    parser.add_argument(
        "--wait-stability-polls",
        "--wait_stability_polls",
        dest="wait_stability_polls",
        type=int,
        default=3,
        help=(
            "Consecutive identical registry totals before wait_for_indexes "
            "returns (default: 3)."
        ),
    )
    parser.add_argument(
        "--wait-poll-interval",
        "--wait_poll_interval",
        dest="wait_poll_interval",
        type=float,
        default=10.0,
        help="Seconds between registry polls (default: 10).",
    )
    parser.add_argument(
        "--wait-timeout",
        "--wait_timeout",
        dest="wait_timeout",
        type=float,
        default=3600.0,
        help=(
            "Maximum seconds to wait for the async indexer to stabilize "
            "(default: 3600)."
        ),
    )
    parser.add_argument(
        "--skip-wait",
        "--skip_wait",
        dest="skip_wait",
        action="store_true",
        help=(
            "Skip wait_for_indexes. Useful when the benchmark reuses an "
            "already-indexed bucket/prefix and just wants to produce."
        ),
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help=(
            "AWS region for the boto3 S3 client used by wait_for_indexes. "
            "Default: boto3 default chain."
        ),
    )
    parser.add_argument(
        "--output-file",
        "--output_file",
        dest="output_file",
        type=str,
        default=None,
        help="Output JSON path (auto-generated if omitted).",
    )
    return parser.parse_args()


def print_configuration(config: dict) -> None:
    print("Ingestion Configuration:")
    for key, value in config.items():
        print(f"  {key}: {value}")


def build_output_filename(args: argparse.Namespace) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.dataset is not None:
        stem = Path(args.dataset).stem
    else:
        stem = Path(args.prefix).strip().strip("/").replace("/", "_") or "vecstream"
    tp = f"_tp{args.throughput}" if args.throughput > 0 else ""
    return f"ingest_vecstream_{stem}_bs{args.batch_size}{tp}_{ts}.json"


def _resolve_centroids(args: argparse.Namespace, dataset_dim: int | None) -> np.ndarray:
    if args.centroids is not None:
        centroids = np.load(args.centroids)
        if centroids.ndim != 2:
            raise ValueError(
                f"--centroids file must contain a 2D array, got shape "
                f"{centroids.shape}"
            )
        return centroids

    if args.sample_file is None:
        print(
            "Error: provide either --centroids or --sample-file",
            file=sys.stderr,
        )
        sys.exit(1)

    n_clusters = args.n_clusters
    if n_clusters is None:
        if args.num_partitions is None:
            print(
                "Error: --n-clusters (or --num-partitions) is required when "
                "computing centroids from --sample-file",
                file=sys.stderr,
            )
            sys.exit(1)
        n_clusters = args.num_partitions

    print(
        f"Computing {n_clusters} centroids from {args.sample_file} "
        f"(num_sample={args.num_sample})..."
    )
    centroids = compute_centroids(
        args.sample_file,
        num_sample=args.num_sample,
        n_clusters=n_clusters,
    )
    print(f"Centroids computed: shape={centroids.shape}")
    return centroids


def _resolve_num_partitions(args: argparse.Namespace, centroids: np.ndarray) -> int:
    if args.num_partitions is not None:
        return args.num_partitions
    return int(centroids.shape[0])


def run_ingestion(args: argparse.Namespace) -> dict:
    if args.dataset is None:
        print(
            "Error: --dataset (or --vector_dataset) is required",
            file=sys.stderr,
        )
        sys.exit(1)

    num_vectors, dataset_dimension = get_vector_count_and_dimension(args.dataset)
    centroids = _resolve_centroids(args, dataset_dimension=dataset_dimension)

    num_partitions = _resolve_num_partitions(args, centroids)
    if num_partitions <= 0:
        print(
            f"Error: --num-partitions must be > 0 (got {num_partitions})",
            file=sys.stderr,
        )
        sys.exit(1)
    if num_partitions != centroids.shape[0]:
        print(
            f"Error: --num-partitions ({num_partitions}) must equal "
            f"centroids.shape[0] ({centroids.shape[0]}); the routing uses the "
            f"centroids as the bucket space and the topic is sized to match.",
            file=sys.stderr,
        )
        sys.exit(1)

    dimension = args.dimension if args.dimension > 0 else dataset_dimension
    if centroids.shape[1] != dimension:
        print(
            f"Error: centroids dimension ({centroids.shape[1]}) does not "
            f"match dataset dimension ({dimension})",
            file=sys.stderr,
        )
        sys.exit(1)

    routing = build_ivf_routing(centroids, metric=args.metric)

    bootstrap_servers = [
        s.strip() for s in args.bootstrap_servers.split(",") if s.strip()
    ]

    client = VecStreamIngestionClient(
        bootstrap_servers=bootstrap_servers,
        topic_name=args.topic,
        routing=routing,
        block_size=args.block_size,
        dimension=dimension,
        metric=args.metric,
        remote_storage_enabled=not args.remote_storage_disabled,
        kafka_acks=args.kafka_acks,
        kafka_local_retention_bytes=args.kafka_local_retention_bytes,
        kafka_compression_type=args.kafka_compression_type,
        batch_to_same_partition=args.batch_to_same_partition,
        delete_topic_on_close=False,
    )

    configuration: dict = {
        "system": "vecstream",
        "dataset": args.dataset,
        "batch_size": args.batch_size,
        "throughput": args.throughput,
        "max_retries": args.max_retries,
        "vector_dimension": dimension,
        "total_dataset_vectors": num_vectors,
        "start_batch": args.start_batch,
        "num_batches": args.num_batches,
        "max_vectors": args.max_vectors,
        "vecstream_bootstrap_servers": args.bootstrap_servers,
        "vecstream_topic": args.topic,
        "vecstream_bucket": args.bucket,
        "vecstream_prefix": args.prefix,
        "vecstream_metric": args.metric,
        "vecstream_num_partitions": num_partitions,
        "vecstream_block_size": args.block_size,
        "vecstream_remote_storage_disabled": args.remote_storage_disabled,
        "vecstream_kafka_acks": args.kafka_acks,
        "vecstream_kafka_local_retention_bytes": args.kafka_local_retention_bytes,
        "vecstream_kafka_compression_type": args.kafka_compression_type,
        "vecstream_batch_to_same_partition": args.batch_to_same_partition,
        "vecstream_centroids": args.centroids,
        "vecstream_sample_file": args.sample_file,
        "vecstream_num_sample": args.num_sample,
        "vecstream_n_clusters": args.n_clusters,
        "vecstream_wait_stability_polls": args.wait_stability_polls,
        "vecstream_wait_poll_interval": args.wait_poll_interval,
        "vecstream_wait_timeout": args.wait_timeout,
        "vecstream_skip_wait": args.skip_wait,
        "vecstream_region": args.region,
    }

    results: list[dict] = []
    inter_batch_interval = 1.0 / args.throughput if args.throughput > 0 else 0.0
    generator = vector_generator(
        args.dataset,
        args.batch_size,
        max_vectors=args.max_vectors,
        start_batch=args.start_batch,
    )
    total_batches = (
        args.num_batches
        if args.num_batches is not None
        else num_vectors // args.batch_size
    )
    progress_interval = max(1, total_batches // 100)

    for batch_idx, (batch_vectors, ids) in enumerate(
        generator, start=args.start_batch
    ):
        if args.num_batches is not None and batch_idx >= args.num_batches:
            break

        start = time.time()
        success = False
        last_error: str | None = None
        result_entry: dict | None = None

        for attempt in range(1, args.max_retries + 1):
            try:
                result = client.put_vectors(batch_vectors, ids)
                end = time.time()
                success = True
                result_entry = {
                    "batch_index": batch_idx,
                    "start_time": datetime.fromtimestamp(
                        start, tz=timezone.utc
                    ).isoformat(),
                    "end_time": datetime.fromtimestamp(
                        end, tz=timezone.utc
                    ).isoformat(),
                    "latency_seconds": round(end - start, 6),
                    "insert_count": result.get("insert_count"),
                    "partition_serialization_times": result.get(
                        "partition_serialization_times"
                    ),
                    "flush_time_seconds": result.get("flush_time_seconds"),
                }
                if attempt > 1:
                    result_entry["retries_needed"] = attempt
                results.append(result_entry)
                break
            except Exception as exc:
                last_error = str(exc)
                print(
                    f"Error on batch {batch_idx} (attempt {attempt}/"
                    f"{args.max_retries}): {exc}",
                    file=sys.stderr,
                )
                if attempt < args.max_retries:
                    time.sleep(2)

        if not success:
            print(
                f"Batch {batch_idx} failed permanently after "
                f"{args.max_retries} attempts.",
                file=sys.stderr,
            )
            results.append(
                {
                    "batch_index": batch_idx,
                    "error": f"Failed after {args.max_retries} retries.",
                    "last_error": last_error,
                }
            )

        if total_batches > 0 and batch_idx % progress_interval == 0:
            print(
                f"Submitted batch {batch_idx} / {total_batches} "
                f"({int(batch_idx / total_batches * 100)}%)"
            )

        elapsed = time.time() - start
        if inter_batch_interval > 0 and elapsed < inter_batch_interval:
            time.sleep(inter_batch_interval - elapsed)

    client.close()

    registry_summary: dict | None = None
    if not args.skip_wait:
        import boto3

        s3_client = (
            boto3.client("s3", region_name=args.region)
            if args.region
            else boto3.client("s3")
        )
        print(
            f"Waiting for async indexer to stabilize at "
            f"s3://{args.bucket}/{args.prefix.rstrip('/')}.index_list.json..."
        )
        registry_summary = wait_for_indexes(
            bucket=args.bucket,
            prefix=args.prefix,
            s3_client=s3_client,
            stability_polls=args.wait_stability_polls,
            poll_interval=args.wait_poll_interval,
            timeout=args.wait_timeout,
        )
        configuration["vecstream_registry_key"] = registry_summary["registry_key"]
        configuration["vecstream_total_indexes"] = registry_summary["total_indexes"]
        configuration["vecstream_stable_for"] = registry_summary["stable_for"]

        partitions = sorted(registry_summary["registry"].keys())
        per_partition = {
            partition: len(registry_summary["registry"][partition])
            for partition in partitions
        }
        print(
            f"Async indexer registered {registry_summary['total_indexes']} "
            f"indexes across {len(partitions)} partitions "
            f"(stable for {registry_summary['stable_for']} polls)."
        )
        for partition in partitions:
            print(f"  {partition}: {per_partition[partition]} indexes")

    return {
        "configuration": configuration,
        "results": results,
        "registry": registry_summary["registry"] if registry_summary else None,
    }


def print_stats(results: list[dict]) -> None:
    latencies = [r["latency_seconds"] for r in results if "latency_seconds" in r]
    if not latencies:
        return
    latencies_ms = [lat * 1000 for lat in latencies]
    mean_latency = np.mean(latencies_ms)
    p50_latency = np.percentile(latencies_ms, 50)
    p95_latency = np.percentile(latencies_ms, 95)
    p99_latency = np.percentile(latencies_ms, 99)

    print(f"Mean latency: {mean_latency:.3f} ms")
    print(f"P50 latency: {p50_latency:.3f} ms")
    print(f"P95 latency: {p95_latency:.3f} ms")
    print(f"P99 latency: {p99_latency:.3f} ms")


def main() -> None:
    args = parse_args()
    print_configuration(vars(args))
    output = run_ingestion(args)

    if args.output_file:
        output_path = Path(args.output_file)
    else:
        output_path = Path(build_output_filename(args))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Results written to {output_path}")
    print_stats(output["results"])


if __name__ == "__main__":
    main()