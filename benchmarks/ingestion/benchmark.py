import argparse
import json
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import numpy as np

from benchmarks.ingestion.systems import get_system


def load_vectors(path: str, max_vectors: int | None = None) -> np.ndarray:
    path_obj = Path(path)
    if path_obj.suffix == ".fbin":
        with open(path, "rb") as f:
            nb, d = struct.unpack("<ii", f.read(8))
            vectors_to_load = min(nb, max_vectors) if max_vectors is not None else nb
            vectors = np.frombuffer(
                f.read(vectors_to_load * d * 4), dtype=np.float32
            ).reshape(vectors_to_load, d)
        return vectors
    if max_vectors is not None:
        return np.load(path)[:max_vectors]
    return np.load(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark VecStream (Kafka-backed stream search) ingestion latency"
    )
    parser.add_argument(
        "--throughput",
        type=int,
        default=1,
        help="Target vectors per second (default: 1)",
    )
    parser.add_argument(
        "--vector_dataset",
        type=str,
        required=True,
        help="Path to .npy or .fbin file containing vectors",
    )
    parser.add_argument(
        "--system",
        type=str,
        default="vecstream",
        choices=["vecstream"],
        help="Target system (default: vecstream)",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1,
        help="Vectors per batch (default: 1)",
    )
    parser.add_argument(
        "--num_batches",
        type=int,
        default=1000,
        help="Total batches to ingest (default: 1000)",
    )
    parser.add_argument(
        "--output_file",
        type=str,
        default=None,
        help="Output JSON path (auto-generated if omitted)",
    )
    parser.add_argument(
        "--reuse_batch",
        action="store_true",
        help="Whether to reuse the same batch of vectors for all iterations (default: False)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=32,
        help="Max concurrent in-flight requests (default: 32)",
    )
    parser.add_argument(
        "--vecstream_bootstrap_servers",
        type=str,
        default="localhost:9092",
        help="Comma-separated Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--vecstream_topic",
        type=str,
        default="bench-ingestion",
        help="Kafka topic name (default: bench-ingestion)",
    )
    parser.add_argument(
        "--vecstream_partitions",
        type=int,
        default=1,
        help="Number of Kafka partitions (also used as the cluster count when centroids are computed from a sample; default: 1)",
    )
    parser.add_argument(
        "--vecstream_block_size",
        type=int,
        default=7_500_000,
        help="Kafka segment size in bytes (default: 7_500_000, i.e. 7.5MB)",
    )
    parser.add_argument(
        "--vecstream_metric",
        type=str,
        default="euclidean",
        choices=["euclidean", "cosine"],
        help="Distance metric: euclidean or cosine (default: euclidean)",
    )
    parser.add_argument(
        "--vecstream_dimension",
        type=int,
        default=0,
        help="Vector dimensionality (0 to infer from the dataset; default: 0)",
    )
    parser.add_argument(
        "--vecstream_centroids",
        type=str,
        default=None,
        help="Path to .npy file containing precomputed centroid vectors",
    )
    parser.add_argument(
        "--vecstream_sample_file",
        type=str,
        default=None,
        help="Path to vector file (.npy/.fbin) to compute centroids from (used if --vecstream_centroids not provided)",
    )
    parser.add_argument(
        "--vecstream_num_sample",
        type=int,
        default=100_000,
        help="Number of vectors to sample from sample_file for centroid computation (default: 100_000)",
    )
    parser.add_argument(
        "--vecstream_n_clusters",
        type=int,
        default=None,
        help="Number of clusters for centroid computation (defaults to --vecstream_partitions when omitted)",
    )
    parser.add_argument(
        "--vecstream_remote_storage_disabled",
        action="store_true",
        help="Disable VecStream from using remote storage in Kafka (default: False)",
    )
    parser.add_argument(
        "--vecstream_kafka_acks",
        type=str,
        default="1",
        help="Kafka producer acks setting for VecStream (default: 1)",
    )
    parser.add_argument(
        "--vecstream_kafka_local_retention_bytes",
        type=int,
        default=1,
        help="Kafka topic local retention bytes for VecStream (default: 1, i.e. minimal local retention)",
    )
    parser.add_argument(
        "--vecstream_batch_to_same_partition",
        action="store_true",
        help="Whether to send all messages in the same batch to the same Kafka partition in VecStream (default: False)",
    )
    parser.add_argument(
        "--vecstream_kafka_compression_type",
        type=str,
        default=None,
        help="Kafka producer compression type for VecStream (default: None, i.e. no compression)",
    )
    return parser.parse_args()


def print_configuration(config: dict) -> None:
    print("Benchmark Configuration:")
    for key, value in config.items():
        print(f"  {key}: {value}")


def build_output_filename(args: argparse.Namespace) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"ingestion_{args.system}_tp{args.throughput}_bs{args.batch_size}_{ts}.json"


def _resolve_vecstream_n_clusters(args: argparse.Namespace) -> int:
    if args.vecstream_n_clusters is not None:
        return args.vecstream_n_clusters
    return args.vecstream_partitions


def run_benchmark(args: argparse.Namespace) -> dict:
    max_vectors = args.batch_size * args.num_batches
    if args.reuse_batch:
        max_vectors = args.batch_size
    vectors = load_vectors(args.vector_dataset, max_vectors=max_vectors)
    if vectors.ndim != 2:
        print(f"Error: expected 2D array, got shape {vectors.shape}", file=sys.stderr)
        sys.exit(1)

    num_vectors, dataset_dimension = vectors.shape
    total_needed = args.batch_size * args.num_batches
    if total_needed > num_vectors and not args.reuse_batch:
        print(
            f"Error: need {total_needed} vectors but dataset has {num_vectors}",
            file=sys.stderr,
        )
        sys.exit(1)

    dimension = args.vecstream_dimension if args.vecstream_dimension > 0 else dataset_dimension

    if args.vecstream_centroids is None and args.vecstream_sample_file is None:
        print(
            "Error: --vecstream_centroids or --vecstream_sample_file is required",
            file=sys.stderr,
        )
        sys.exit(1)
    system_kwargs: dict = {
        "bootstrap_servers": [
            s.strip() for s in args.vecstream_bootstrap_servers.split(",") if s.strip()
        ],
        "topic_name": args.vecstream_topic,
        "block_size": args.vecstream_block_size,
        "dimension": dimension,
        "metric": args.vecstream_metric,
        "remote_storage_enabled": not args.vecstream_remote_storage_disabled,
        "kafka_acks": args.vecstream_kafka_acks,
        "kafka_local_retention_bytes": args.vecstream_kafka_local_retention_bytes,
        "kafka_compression_type": args.vecstream_kafka_compression_type,
        "batch_to_same_partition": args.vecstream_batch_to_same_partition,
    }
    if args.vecstream_centroids is not None:
        system_kwargs["centroids_file"] = args.vecstream_centroids
    else:
        system_kwargs["sample_file"] = args.vecstream_sample_file
        system_kwargs["num_sample"] = args.vecstream_num_sample
        system_kwargs["n_clusters"] = _resolve_vecstream_n_clusters(args)
    system = get_system(args.system, **system_kwargs)

    configuration = {
        "system": args.system,
        "throughput": args.throughput,
        "batch_size": args.batch_size,
        "num_batches": args.num_batches,
        "concurrency": args.concurrency,
        "vector_dataset": args.vector_dataset,
        "vector_dimension": dimension,
        "total_vectors": num_vectors,
        "reuse_batch": args.reuse_batch,
        "vecstream_bootstrap_servers": args.vecstream_bootstrap_servers,
        "vecstream_topic": args.vecstream_topic,
        "vecstream_partitions": args.vecstream_partitions,
        "vecstream_block_size": args.vecstream_block_size,
        "vecstream_metric": args.vecstream_metric,
        "vecstream_dimension": dimension,
        "vecstream_remote_storage_disabled": args.vecstream_remote_storage_disabled,
        "vecstream_kafka_acks": args.vecstream_kafka_acks,
        "vecstream_kafka_local_retention_bytes": args.vecstream_kafka_local_retention_bytes,
        "vecstream_batch_to_same_partition": args.vecstream_batch_to_same_partition,
        "vecstream_kafka_compression_type": args.vecstream_kafka_compression_type,
    }
    if args.vecstream_centroids is not None:
        configuration["vecstream_centroids"] = args.vecstream_centroids
    else:
        configuration["vecstream_sample_file"] = args.vecstream_sample_file
        configuration["vecstream_num_sample"] = args.vecstream_num_sample
        configuration["vecstream_n_clusters"] = _resolve_vecstream_n_clusters(args)

    results: List[dict] = []
    inter_batch_interval = 1.0 / args.throughput if args.throughput > 0 else 0.0
    futures: List = []

    def timed_put(system, batch_vectors, ids):
        start = time.time()
        result = system.put_vectors(batch_vectors, ids)
        end = time.time()
        return (start, end, result)

    progress_interval = max(1, args.num_batches // 10)

    if args.concurrency > 1:
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            for batch_idx in range(args.num_batches):
                if args.reuse_batch:
                    batch_vectors = vectors[: args.batch_size]
                    ids = [str(i) for i in range(args.batch_size)]
                else:
                    start_offset = batch_idx * args.batch_size
                    end_offset = start_offset + args.batch_size
                    batch_vectors = vectors[start_offset:end_offset]
                    ids = [str(i) for i in range(start_offset, end_offset)]

                future = executor.submit(timed_put, system, batch_vectors, ids)
                futures.append((batch_idx, future))

                if batch_idx % progress_interval == 0:
                    print(f"Submitted batch {batch_idx}/{args.num_batches}")

                if inter_batch_interval > 0 and batch_idx < args.num_batches - 1:
                    time.sleep(inter_batch_interval)

            try:
                for batch_idx, future in futures:
                    batch_start, batch_end, result = future.result(10)
                    results.append(
                        {
                            "batch_index": batch_idx,
                            "start_time": datetime.fromtimestamp(
                                batch_start, tz=timezone.utc
                            ).isoformat(),
                            "end_time": datetime.fromtimestamp(
                                batch_end, tz=timezone.utc
                            ).isoformat(),
                            "latency_seconds": round(batch_end - batch_start, 6),
                            **{
                                k: v
                                for k, v in result.items()
                                if k not in ("start_time", "end_time")
                            },
                        }
                    )
            except Exception as e:
                print(f"Error during benchmark execution: {e}", file=sys.stderr)
                results.append(
                    {
                        "batch_index": batch_idx,
                        "error": str(e),
                    }
                )

    else:
        for batch_idx in range(args.num_batches):
            if args.reuse_batch:
                batch_vectors = vectors[: args.batch_size]
                ids = [str(i) for i in range(args.batch_size)]
            else:
                start_offset = batch_idx * args.batch_size
                end_offset = start_offset + args.batch_size
                batch_vectors = vectors[start_offset:end_offset]
                ids = [str(i) for i in range(start_offset, end_offset)]

            try:
                batch_start, batch_end, result = timed_put(system, batch_vectors, ids)
                results.append(
                    {
                        "batch_index": batch_idx,
                        "start_time": datetime.fromtimestamp(
                            batch_start, tz=timezone.utc
                        ).isoformat(),
                        "end_time": datetime.fromtimestamp(
                            batch_end, tz=timezone.utc
                        ).isoformat(),
                        "latency_seconds": round(batch_end - batch_start, 6),
                        **{
                            k: v
                            for k, v in result.items()
                            if k not in ("start_time", "end_time")
                        },
                    }
                )

                if batch_idx % progress_interval == 0:
                    print(f"Submitted batch {batch_idx}/{args.num_batches}")

                if inter_batch_interval > 0 and batch_idx < args.num_batches - 1:
                    time.sleep(inter_batch_interval)
            except Exception as e:
                print(f"Error during benchmark execution: {e}", file=sys.stderr)
                results.append(
                    {
                        "batch_index": batch_idx,
                        "error": str(e),
                    }
                )

            if batch_idx % progress_interval == 0:
                print(f"Completed batch {batch_idx}/{args.num_batches}")

            if inter_batch_interval > 0 and batch_idx < args.num_batches - 1:
                time.sleep(inter_batch_interval)

    system.close()

    return {
        "configuration": configuration,
        "results": results,
    }


def print_stats(results: List[dict]) -> None:
    """Print summary statistics about the benchmark results. Mean, p50, p95, p99 latencies."""
    latencies = [r["latency_seconds"] for r in results]
    latencies_ms = [lat * 1000 for lat in latencies]
    mean_latency = np.mean(latencies_ms)
    p50_latency = np.percentile(latencies_ms, 50)
    p95_latency = np.percentile(latencies_ms, 95)
    p99_latency = np.percentile(latencies_ms, 99)

    print(f"Mean latency: {mean_latency:.3f} ms")
    print(f"P50 latency: {p50_latency:.3f} ms")
    print(f"P95 latency: {p95_latency:.3f} ms")
    print(f"P99 latency: {p99_latency:.3f} ms")


def main():
    args = parse_args()
    print_configuration(vars(args))
    output = run_benchmark(args)

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
