"""VecStream-only query latency benchmark.

Phase 2 of the two-phase workflow. Loads the per-partition FAISS indexes
registered by the async indexer (the ones ingested by ``ingest.py``) and
issues queries sequentially through ``VecStreamClient``, measuring per-query
latency against the ground truth produced by ``faiss_exhaustive_search.py``.

The output JSON schema is preserved for ``plots/static_queries.ipynb``:
  ``configuration.system == "vecstream"``,
  ``configuration.top_k``,
  per-result ``query_index``, ``start_time``, ``end_time``,
  ``latency_seconds``, ``results`` with ``{id, distance}`` items,
  ``distance_metric``, ``system_data``.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from tqdm import tqdm

from benchmarks.static_queries.data_loader import load_vectors
from benchmarks.static_queries.systems import get_system
from benchmarks.static_queries.systems.vecstream import load_centroids


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark VecStream query latency against a fixed dataset. "
            "Run ingest.py first to populate S3 with FAISS indexes."
        )
    )
    parser.add_argument(
        "--query_dataset",
        type=str,
        required=True,
        help="Path to .npy or .fbin file containing query vectors.",
    )
    parser.add_argument(
        "--system",
        type=str,
        default="vecstream",
        choices=["vecstream"],
        help="Target system (default: vecstream).",
    )
    parser.add_argument(
        "--top_k",
        type=int,
        default=10,
        help="Number of nearest neighbors to retrieve per query (default: 10).",
    )
    parser.add_argument(
        "--max_queries",
        type=int,
        default=None,
        help="Max queries to run (default: entire dataset).",
    )
    parser.add_argument(
        "--output_file",
        type=str,
        default=None,
        help="Output JSON path (auto-generated if omitted).",
    )
    parser.add_argument(
        "--vecstream_bucket",
        type=str,
        default=None,
        help=(
            "S3 bucket with the per-partition FAISS indexes written by the "
            "async indexer Lambda (required)."
        ),
    )
    parser.add_argument(
        "--vecstream_prefix",
        type=str,
        default=None,
        help=(
            "S3 key prefix for VecStream indexes (required). The async "
            "indexer writes to {prefix}/partition_{N}/ and updates "
            "{prefix}.index_list.json."
        ),
    )
    parser.add_argument(
        "--vecstream_num_partitions",
        type=int,
        default=None,
        help=(
            "Number of partitions (must match the Kafka topic and "
            "centroids.shape[0]). Defaults to centroids.shape[0] when a "
            "--vecstream_centroids file is supplied."
        ),
    )
    parser.add_argument(
        "--vecstream_centroids",
        type=str,
        default=None,
        help=(
            "Path to the .npy centroids file used by ingest.py (required)."
        ),
    )
    parser.add_argument(
        "--vecstream_bootstrap_servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers for the VecStream stream search (default: localhost:9092).",
    )
    parser.add_argument(
        "--vecstream_topic",
        type=str,
        default="vecstream_topic",
        help="Kafka topic for VecStream stream search (default: vecstream_topic).",
    )
    parser.add_argument(
        "--vecstream_group_id",
        type=str,
        default="vecstream_group",
        help="Kafka consumer group id (default: vecstream_group).",
    )
    parser.add_argument(
        "--vecstream_metric",
        type=str,
        default="euclidean",
        choices=["euclidean", "cosine"],
        help="Distance metric (default: euclidean).",
    )
    parser.add_argument(
        "--vecstream_num_partitions_to_search",
        type=int,
        default=16,
        help="Partitions each query fans out to (default: 16).",
    )
    parser.add_argument(
        "--vecstream_reduce_branching_factor",
        type=int,
        default=16,
        help="Reduce fanout branching factor (default: 16).",
    )
    parser.add_argument(
        "--vecstream_map_invocations_per_lambda",
        type=int,
        default=16,
        help="Map invocations per Lambda worker (default: 16).",
    )
    parser.add_argument(
        "--vecstream_use_cache",
        action="store_true",
        default=False,
        help="Route VecStream queries through the L1/L2 cache Lambdas (default: False).",
    )
    parser.add_argument(
        "--vecstream_no_warmup",
        action="store_true",
        default=False,
        help="Skip load_l1_cache() and warmup_lambdas() in __init__ (default: warmup is enabled).",
    )
    parser.add_argument(
        "--vecstream_region",
        type=str,
        default=None,
        help="AWS region for the VecStream boto3 S3 client (default: use default chain).",
    )
    return parser.parse_args()


def print_configuration(config: dict) -> None:
    print("Benchmark Configuration:")
    for key, value in config.items():
        print(f"  {key}: {value}")


def build_output_filename(args: argparse.Namespace) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stem = Path(args.query_dataset).stem
    return f"query_{args.system}_{stem}_topk{args.top_k}_{ts}.json"


def _validate_vecstream_args(args: argparse.Namespace) -> None:
    missing = [
        name
        for name in ("vecstream_bucket", "vecstream_prefix", "vecstream_centroids")
        if getattr(args, name) is None
    ]
    if missing:
        print(
            f"Error: {', '.join('--' + m for m in missing)} required",
            file=sys.stderr,
        )
        sys.exit(1)


def run_benchmark(args: argparse.Namespace, output_path: Path) -> list[float]:
    if args.system == "vecstream":
        _validate_vecstream_args(args)

    queries = load_vectors(args.query_dataset, max_vectors=args.max_queries)
    if queries.ndim != 2:
        print(
            f"Error: expected 2D array, got shape {queries.shape}",
            file=sys.stderr,
        )
        sys.exit(1)

    num_queries, dimension = queries.shape

    centroids = load_centroids(args.vecstream_centroids)
    if centroids.shape[1] != dimension:
        print(
            f"Error: query dataset dimension ({dimension}) does not match "
            f"centroids dimension ({centroids.shape[1]})",
            file=sys.stderr,
        )
        sys.exit(1)

    num_partitions = (
        args.vecstream_num_partitions
        if args.vecstream_num_partitions is not None
        else centroids.shape[0]
    )
    if num_partitions != centroids.shape[0]:
        print(
            f"Error: --vecstream_num_partitions ({num_partitions}) must equal "
            f"centroids.shape[0] ({centroids.shape[0]})",
            file=sys.stderr,
        )
        sys.exit(1)

    system = get_system(
        args.system,
        bucket=args.vecstream_bucket,
        prefix=args.vecstream_prefix,
        centroids=centroids,
        num_partitions=num_partitions,
        dimension=dimension,
        metric=args.vecstream_metric,
        bootstrap_servers=args.vecstream_bootstrap_servers,
        kafka_topic=args.vecstream_topic,
        kafka_group_id=args.vecstream_group_id,
        reduce_branching_factor=args.vecstream_reduce_branching_factor,
        map_invocations_per_lambda=args.vecstream_map_invocations_per_lambda,
        num_partitions_to_search=args.vecstream_num_partitions_to_search,
        use_cache=args.vecstream_use_cache,
        warmup=not args.vecstream_no_warmup,
        region=args.vecstream_region,
    )

    configuration = {
        "system": args.system,
        "query_dataset": args.query_dataset,
        "top_k": args.top_k,
        "num_queries": num_queries,
        "vector_dimension": dimension,
        "vecstream": {
            "bucket": args.vecstream_bucket,
            "prefix": args.vecstream_prefix,
            "num_partitions": num_partitions,
            "centroids_file": args.vecstream_centroids,
            "bootstrap_servers": args.vecstream_bootstrap_servers,
            "topic": args.vecstream_topic,
            "group_id": args.vecstream_group_id,
            "metric": args.vecstream_metric,
            "use_cache": args.vecstream_use_cache,
            "warmup": not args.vecstream_no_warmup,
            "region": args.vecstream_region,
            "num_partitions_to_search": args.vecstream_num_partitions_to_search,
            "reduce_branching_factor": args.vecstream_reduce_branching_factor,
            "map_invocations_per_lambda": args.vecstream_map_invocations_per_lambda,
        },
    }

    latencies: list[float] = []

    def timed_query(query_vector: np.ndarray, query_idx: int, top_k: int):
        start = time.time()
        result = system.query_vectors(query_vector.reshape(1, -1), top_k)
        end = time.time()
        return (query_idx, start, end, result)

    with open(output_path, "w") as f:
        f.write('{\n  "configuration": ')
        f.write(json.dumps(configuration, indent=2))
        f.write(',\n  "results": [\n')

        for i in tqdm(range(num_queries), desc="Queries"):
            query_vector = queries[i]
            _, query_start, query_end, result = timed_query(
                query_vector, i, args.top_k
            )
            result_dict = {
                "query_index": i,
                "start_time": datetime.fromtimestamp(
                    query_start, tz=timezone.utc
                ).isoformat(),
                "end_time": datetime.fromtimestamp(
                    query_end, tz=timezone.utc
                ).isoformat(),
                "latency_seconds": round(query_end - query_start, 6),
                "query_vector": result.get("query_vector", []),
                "results": result.get("results", []),
                "distance_metric": result.get("distance_metric", "unknown"),
                "system_data": result.get("system_data"),
            }
            latencies.append(result_dict["latency_seconds"])

            result_json = json.dumps(result_dict, indent=2)
            lines = result_json.split("\n")
            indented = "    " + "\n    ".join(lines)
            if i > 0:
                f.write(",\n")
            f.write(indented)

        f.write("\n  ]\n}\n")

    system.close()
    return latencies


def print_stats(latencies: list[float]) -> None:
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

    if args.output_file:
        output_path = Path(args.output_file)
    else:
        output_path = Path(build_output_filename(args))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    latencies = run_benchmark(args, output_path)

    print(f"Results written to {output_path}")
    print_stats(latencies)


if __name__ == "__main__":
    main()