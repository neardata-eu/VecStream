"""Streaming query benchmark for VecStream against the real production pipeline.

This CLI replaces the legacy simulated per-epoch flow (manual per-epoch IVF
index builds, manual S3 uploads, manual Kafka re-streams) with the real
ingestion pipeline described in paper section 5.5:

    1. Construct one long-lived :class:`VecStreamIngestionClient` for the
       whole run. Its ``__init__`` recreates the Kafka topic exactly once
       (which is what a fresh run wants) and we keep it open across every
       epoch; ``close()`` with ``delete_topic_on_close=True`` cleans the
       topic up at the end.
    2. For each ``epoch`` in ``[0, num_epochs)``:
         a. Ingest that epoch's vectors into Kafka via ``put_vectors``,
            batched through ``benchmarks.static_queries.data_loader.vector_generator``.
         b. Call :func:`vecstream.ingestion.wait_for_indexes` so the deployed
            ``async_index_creation`` Lambda seals each Kafka segment, trains
            a FAISS IVF index on it, uploads the ``.ann`` and merges it into
            ``{prefix}.index_list.json``. We block until the registry's
            registered index count has stabilized for a configurable number
            of consecutive polls before running the query set.
         c. Run the query set through :class:`VecStreamQuerySystem`
            (the same class the static_queries suite uses, see
            ``benchmarks.static_queries.systems``). The system probes
            ``num_partitions_to_search`` partitions with the configured
            routing; results land in the per-epoch output JSON.
    3. ``client.close()`` flushes the producer and deletes the topic.

The query-side timeout/retry logic from the previous ``vecstream.py``
implementation is preserved unchanged; this module adds the ingestion phase
on top of it rather than redesigning the query side.

Output JSON schema (consumed by ``plots/streaming_queries.ipynb`` and
``plots/streaming_recall.ipynb``):

    configuration:
        system: "vecstream"
        query_dataset: <str>
        dataset_name: <str>
        num_queries: <int>
        vector_dimension: <int>
        num_epochs: <int>
        epoch_size: <int>
        batch_size: <int>
        query_timeout_seconds: <float>
        max_retries: <int>
        vecstream:
            bucket, prefix, num_partitions, num_stream_partitions,
            centroids_file, bootstrap_servers, group_id, metric, use_cache,
            warmup, region, num_partitions_to_search,
            reduce_branching_factor, map_invocations_per_lambda,
            topic, vector_dataset, block_size, kafka_acks,
            kafka_local_retention_bytes, kafka_compression_type,
            wait_stability_polls, wait_poll_interval, wait_timeout
        top_k: <int>
        epoch_index: <int>
        epoch_prefix: <str>            # informational; real pipeline uses base prefix
        epoch_topic: <str>             # informational; same topic for every epoch
        epoch_start_offset: <int>
        epoch_end_offset: <int>
        epoch_ingest_seconds: <float>
        epoch_registry_total_indexes: <int>    # final size of {prefix}.index_list.json
        epoch_registry_stable_for: <int>
    results: [
        {
            query_index, start_time, end_time, latency_seconds, query_vector,
            results: [{id, distance}], distance_metric, system_data,
            attempts, error,
        },
        ...
    ]
"""

import argparse
import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from tqdm import tqdm

from benchmarks.static_queries.data_loader import load_vectors, vector_generator
from benchmarks.streaming_queries.systems import VecStreamQuerySystem
from vecstream.ingestion import (
    VecStreamIngestionClient,
    build_ivf_routing,
    compute_centroids,
    wait_for_indexes,
)


def parse_args() -> argparse.Namespace:
    """Parse and return command-line arguments for the streaming benchmark."""
    parser = argparse.ArgumentParser(
        description=(
            "Streaming query benchmark for VecStream. Drives the real "
            "production pipeline: a single long-lived "
            "VecStreamIngestionClient writes per-epoch vectors into Kafka "
            "(tiered storage seals segments to S3), the deployed "
            "async_index_creation Lambda builds per-segment FAISS IVF "
            "indexes and updates {prefix}.index_list.json, "
            "wait_for_indexes() blocks until the registry has caught up, "
            "then VecStreamQuerySystem runs the same query set against the "
            "cumulative index. Repeats for --num-epochs rounds."
        )
    )

    parser.add_argument(
        "--query_dataset",
        type=str,
        required=True,
        help="Path to .npy or .fbin file containing query vectors",
    )
    parser.add_argument(
        "--num-epochs",
        type=int,
        required=True,
        help="Number of epochs to run (epochs 0..N-1). Each epoch ingests "
        "--epoch-size new vectors and re-runs the query set against the "
        "cumulative index.",
    )
    parser.add_argument(
        "--epoch-size",
        type=int,
        required=True,
        help="Vectors ingested per epoch (must be a multiple of --batch-size). "
        "The total dataset must hold at least num-epochs * epoch-size vectors.",
    )
    parser.add_argument(
        "--vector_dataset",
        type=str,
        required=True,
        help="Path to .npy or .fbin file containing the vectors to ingest "
        "(used by the per-epoch Kafka writes). Distinct from --query_dataset.",
    )
    parser.add_argument(
        "--top_ks",
        type=str,
        default="1,10,100",
        help="Comma-separated top_k values (default: 1,10,100)",
    )
    parser.add_argument(
        "--max_queries",
        type=int,
        default=1000,
        help="Number of queries to run per checkpoint (default: 1000)",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=100,
        help="Vectors per put_vectors() batch during ingestion (default: 100). "
        "Must divide --epoch-size.",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=".",
        help="Directory for output JSON files (default: current directory)",
    )
    parser.add_argument(
        "--dataset_name",
        type=str,
        default=None,
        help="Override dataset name for metadata and filenames. Defaults "
        "to the stem of --vector_dataset.",
    )

    parser.add_argument(
        "--vecstream_bucket",
        type=str,
        required=True,
        help="S3 bucket containing the per-partition FAISS indexes and "
        "{prefix}.index_list.json (must match the async indexer's "
        "INDEX_STORAGE_BUCKET / TIERED_STORAGE_BUCKET).",
    )
    parser.add_argument(
        "--vecstream_prefix",
        type=str,
        required=True,
        help="Base S3 key prefix shared across all epochs (the async indexer "
        "writes indexes to {prefix}/partition_N/index_K.ann and updates "
        "{prefix}.index_list.json). Must include a trailing '/' when "
        "non-empty.",
    )
    parser.add_argument(
        "--vecstream_dataset_name",
        type=str,
        default=None,
        help="Dataset name used to derive the default Kafka topic name "
        "(default: stem of --vector_dataset).",
    )
    parser.add_argument(
        "--vecstream_topic",
        type=str,
        default=None,
        help="Override the Kafka topic name (default: "
        "{vecstream_dataset_name}_{num_partitions}). One topic for the whole run.",
    )
    parser.add_argument(
        "--vecstream_centroids",
        type=str,
        default=None,
        help="Path to .npy file with KMeans centroids (also defines "
        "num_partitions via centroids.shape[0]). Mutually exclusive with "
        "--vecstream_sample_file; required unless --vecstream_sample_file "
        "is given.",
    )
    parser.add_argument(
        "--vecstream_sample_file",
        type=str,
        default=None,
        help="Path to a .npy or .fbin file used to compute centroids when "
        "--vecstream_centroids is not provided. Combined with "
        "--vecstream_n_clusters and the library's "
        "vecstream.ingestion.compute_centroids.",
    )
    parser.add_argument(
        "--vecstream_n_clusters",
        type=int,
        default=250,
        help="Number of KMeans centroids to fit when --vecstream_centroids "
        "is not provided (default: 250).",
    )
    parser.add_argument(
        "--vecstream_num_sample",
        type=int,
        default=100_000,
        help="Maximum rows to load from --vecstream_sample_file for "
        "centroid fitting (default: 100_000).",
    )
    parser.add_argument(
        "--vecstream_num_partitions",
        type=int,
        default=None,
        help="Override num_partitions (must match centroids.shape[0]). "
        "Defaults to centroids.shape[0]. Use this only when passing a "
        "centroids file whose shape you want the runner to assert explicitly.",
    )
    parser.add_argument(
        "--vecstream_metric",
        type=str,
        choices=["euclidean", "cosine"],
        default="euclidean",
        help="Distance metric for VecStream (default: euclidean)",
    )
    parser.add_argument(
        "--vecstream_bootstrap_servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers for VecStream (default: localhost:9092)",
    )
    parser.add_argument(
        "--vecstream_group_id",
        type=str,
        default="vecstream_group",
        help="Kafka consumer group id for VecStream (default: vecstream_group)",
    )
    parser.add_argument(
        "--vecstream_block_size",
        type=int,
        default=7_500_000,
        help="Kafka segment.bytes (the seal threshold the async indexer "
        "consumes). Default: 7_500_000 (7.5 MB).",
    )
    parser.add_argument(
        "--vecstream_kafka_acks",
        type=str,
        default="1",
        help="Kafka producer acks passed to the ingestion client "
        "(default: 1). Use 'all' for acks=-1.",
    )
    parser.add_argument(
        "--vecstream_kafka_local_retention_bytes",
        type=int,
        default=1,
        help="Kafka topic local.retention.bytes; default 1 (tier cold "
        "segments to S3 as soon as possible).",
    )
    parser.add_argument(
        "--vecstream_kafka_compression_type",
        type=str,
        default=None,
        help="Optional Kafka producer compression.type "
        "(gzip/snappy/lz4/zstd). Default: no compression.",
    )
    parser.add_argument(
        "--vecstream_remote_storage_disabled",
        action="store_true",
        default=False,
        help="Set remote.storage.enable=false on the topic (default: enabled). "
        "Disable only if running without S3 tiered storage.",
    )
    parser.add_argument(
        "--vecstream_num_partitions_to_search",
        type=int,
        default=16,
        help="Number of partitions each query fans out to (default: 16)",
    )
    parser.add_argument(
        "--vecstream_reduce_branching_factor",
        type=int,
        default=16,
        help="Reduce fanout branching factor (default: 16)",
    )
    parser.add_argument(
        "--vecstream_map_invocations_per_lambda",
        type=int,
        default=16,
        help="Map invocations per Lambda worker (default: 16)",
    )
    parser.add_argument(
        "--vecstream_use_cache",
        action="store_true",
        default=False,
        help="Route VecStream queries through the L1/L2 cache Lambdas "
        "(default: False). Skips the cache by default because the "
        "asynchronous indexer maintains {prefix}.index_list.json "
        "directly, and the streaming run wants each query to fan out "
        "without an extra L1 round trip.",
    )
    parser.add_argument(
        "--vecstream_no_warmup",
        action="store_true",
        default=False,
        help="Skip warmup_lambdas() in VecStreamQuerySystem.__init__ "
        "(default: warmup enabled).",
    )
    parser.add_argument(
        "--vecstream_region",
        type=str,
        default=None,
        help="AWS region for the VecStream boto3 S3 client (default: use "
        "the default chain).",
    )
    parser.add_argument(
        "--vecstream_wait_stability_polls",
        type=int,
        default=3,
        help="Number of consecutive polls whose total registered index "
        "count must match before wait_for_indexes() returns. Default: 3.",
    )
    parser.add_argument(
        "--vecstream_wait_poll_interval",
        type=float,
        default=10.0,
        help="Seconds between registry polls inside wait_for_indexes() "
        "(default: 10.0).",
    )
    parser.add_argument(
        "--vecstream_wait_timeout",
        type=float,
        default=3600.0,
        help="Maximum seconds to wait inside wait_for_indexes() before "
        "raising TimeoutError (default: 3600.0 = 1 hour per epoch).",
    )
    parser.add_argument(
        "--ingest_max_retries",
        type=int,
        default=3,
        help="Retries per put_vectors() failure inside an epoch's ingest "
        "loop (default: 3).",
    )
    parser.add_argument(
        "--ingest_retry_sleep_seconds",
        type=float,
        default=2.0,
        help="Seconds to sleep between put_vectors() retries within a "
        "single batch (default: 2.0).",
    )
    parser.add_argument(
        "--query_timeout_seconds",
        type=float,
        default=10.0,
        help="Per-query wall-clock timeout in seconds (default: 10.0)",
    )
    parser.add_argument(
        "--max_retries",
        type=int,
        default=3,
        help="Max number of retries on a query timeout (default: 3, so up "
        "to 4 total attempts).",
    )
    return parser.parse_args()


def print_configuration(config: dict) -> None:
    print("Benchmark Configuration:")
    for key, value in config.items():
        print(f"  {key}: {value}")


def build_output_filename(dataset_stem: str, top_k: int, epoch: int, ts: str) -> str:
    return f"query_vecstream_{dataset_stem}_topk{top_k}_epoch{epoch}_{ts}.json"


def print_stats(latencies: list[float], label: str) -> None:
    if not latencies:
        return
    latencies_ms = [lat * 1000 for lat in latencies]
    print(
        f"  {label}: mean={np.mean(latencies_ms):.3f} ms "
        f"p50={np.percentile(latencies_ms, 50):.3f} ms "
        f"p95={np.percentile(latencies_ms, 95):.3f} ms "
        f"p99={np.percentile(latencies_ms, 99):.3f} ms"
    )


def _run_query_with_timeout(
    system,
    query_vector: np.ndarray,
    query_index: int,
    top_k: int,
    timeout_s: float,
    max_retries: int,
) -> tuple[dict, int]:
    """Run one query with a per-attempt wall-clock timeout and bounded retries.

    The timeout is applied at the benchmark layer: each attempt runs
    ``system.query_vectors(...)`` in a fresh daemon thread, and the main
    thread waits on it with ``thread.join(timeout=timeout_s)``. The system's
    own ``query_vectors`` is unchanged.

    On timeout, the daemon thread is left running in the background (Python
    has no way to cancel a thread). The next attempt starts in a new thread,
    so attempts run concurrently. This is required for correct latency
    measurement: the retry's latency must not include the time the previous
    attempt spent running.

    Latency is measured from the start of the **successful** attempt only;
    failed attempts are not reflected in ``latency_seconds``.

    Returns
    -------
    (result_dict, attempts_used)
        ``result_dict`` carries the system response on success, or an empty
        ``results`` list plus an ``error`` field if all attempts timed out.
        ``attempts_used`` is 1 on first-attempt success, up to
        ``max_retries + 1`` if every attempt timed out.
    """
    total_attempts = max_retries + 1
    last_error: str | None = None
    for attempt in range(1, total_attempts + 1):
        start = time.time()
        result_holder: list[dict] = []
        error_holder: list[BaseException] = []

        def _runner() -> None:
            try:
                result_holder.append(
                    system.query_vectors(query_vector.reshape(1, -1), top_k)
                )
            except BaseException as e:
                error_holder.append(e)

        thread = threading.Thread(target=_runner, daemon=True)
        thread.start()
        thread.join(timeout=timeout_s)

        if thread.is_alive():
            last_error = f"timeout after {timeout_s}s"
            tqdm.write(
                f"  WARNING: query {query_index} attempt {attempt}/{total_attempts} "
                f"timed out after {timeout_s:.2f}s"
            )
            continue

        if error_holder:
            raise error_holder[0]

        end = time.time()
        system_result = result_holder[0]
        system_result["start_time"] = start
        system_result["end_time"] = end
        system_result["latency_seconds"] = round(end - start, 6)
        system_result["attempts"] = attempt
        system_result.pop("error", None)
        system.cleanup()
        return system_result, attempt

    return (
        {
            "start_time": None,
            "end_time": None,
            "latency_seconds": None,
            "query_vector": query_vector.tolist(),
            "results": [],
            "distance_metric": "unknown",
            "system_data": None,
            "attempts": total_attempts,
            "error": last_error or "all retries failed",
        },
        total_attempts,
    )


def run_queries_for_epoch(
    system,
    queries: np.ndarray,
    top_k: int,
    timeout_s: float,
    max_retries: int,
) -> list[dict]:
    """Run the query set sequentially against the given system; return per-query result dicts.

    Each query has a wall-clock timeout of ``timeout_s`` seconds. On timeout,
    the query is retried up to ``max_retries`` times. Only the successful
    attempt's latency is recorded in the result entry.
    """
    results: list[dict] = []
    timed_out = 0
    for i in tqdm(range(queries.shape[0]), desc=f"top_k={top_k}", leave=False):
        query_vector = queries[i]
        result, attempts = _run_query_with_timeout(
            system,
            query_vector,
            query_index=i,
            top_k=top_k,
            timeout_s=timeout_s,
            max_retries=max_retries,
        )

        start_iso = (
            datetime.fromtimestamp(result["start_time"], tz=timezone.utc).isoformat()
            if result.get("start_time") is not None
            else None
        )
        end_iso = (
            datetime.fromtimestamp(result["end_time"], tz=timezone.utc).isoformat()
            if result.get("end_time") is not None
            else None
        )
        results.append(
            {
                "query_index": i,
                "start_time": start_iso,
                "end_time": end_iso,
                "latency_seconds": result.get("latency_seconds"),
                "query_vector": result.get("query_vector", []),
                "results": result.get("results", []),
                "distance_metric": result.get("distance_metric", "unknown"),
                "system_data": result.get("system_data"),
                "attempts": attempts,
                "error": result.get("error"),
            }
        )
        if result.get("error"):
            timed_out += 1
    if timed_out:
        print(
            f"  top_k={top_k}: {timed_out}/{len(results)} queries exhausted all retries"
        )
    return results


def write_results(
    output_dir: Path,
    dataset_stem: str,
    top_k: int,
    epoch: int,
    configuration: dict,
    results: list[dict],
) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = output_dir / build_output_filename(dataset_stem, top_k, epoch, ts)
    with open(output_path, "w") as f:
        f.write('{\n  "configuration": ')
        f.write(json.dumps(configuration, indent=2))
        f.write(',\n  "results": [\n')
        for i, result_dict in enumerate(results):
            result_json = json.dumps(result_dict, indent=2)
            lines = result_json.split("\n")
            indented = "    " + "\n    ".join(lines)
            if i > 0:
                f.write(",\n")
            f.write(indented)
        f.write("\n  ]\n}\n")
    return output_path


def _load_centroids(
    centroids_file: str | None,
    sample_file: str | None,
    num_sample: int,
    n_clusters: int,
) -> np.ndarray:
    """Load centroids from a .npy file or fit KMeans via vecstream.ingestion.

    Exactly one of ``centroids_file`` or ``sample_file`` must be provided.
    """
    if centroids_file is not None and sample_file is not None:
        raise ValueError(
            "Pass either --vecstream_centroids or --vecstream_sample_file, "
            "not both."
        )
    if centroids_file is not None:
        return np.load(centroids_file).astype(np.float32)
    if sample_file is None:
        raise ValueError(
            "Either --vecstream_centroids or --vecstream_sample_file is required"
        )
    print(
        f"Computing {n_clusters} centroids from {sample_file} "
        f"(num_sample={num_sample})..."
    )
    centroids = compute_centroids(
        sample_file,
        num_sample=num_sample,
        n_clusters=n_clusters,
    ).astype(np.float32)
    print(f"Centroids computed: shape={centroids.shape}")
    return centroids


def _ingest_epoch(
    client: VecStreamIngestionClient,
    vector_dataset: str,
    start_offset: int,
    end_offset: int,
    batch_size: int,
    max_retries: int,
    retry_sleep_s: float,
) -> float:
    """Ingest vectors ``[start_offset:end_offset]`` via ``put_vectors``, with bounded retries.

    Streams the slice via ``vector_generator`` so the full dataset is never
    loaded into RAM. Returns the elapsed seconds (caller records it for the
    per-epoch output JSON).
    """
    if end_offset <= start_offset:
        return 0.0
    if batch_size <= 0:
        raise ValueError(f"batch_size must be > 0, got {batch_size}")
    if start_offset % batch_size != 0:
        raise ValueError(
            f"start_offset ({start_offset}) must be a multiple of "
            f"batch_size ({batch_size}) so vector_generator can align"
        )

    start_batch = start_offset // batch_size
    elapsed = 0.0
    submitted_batches = 0
    failed_batches = 0
    total_to_ingest = end_offset - start_offset

    overall_start = time.time()
    for batch_vectors, ids in vector_generator(
        vector_dataset,
        batch_size,
        max_vectors=end_offset,
        start_batch=start_batch,
    ):
        submitted_batches += 1
        for attempt in range(1, max_retries + 1):
            try:
                client.put_vectors(batch_vectors, ids)
                break
            except Exception as e:
                tqdm.write(
                    f"  WARNING: put_vectors attempt {attempt}/{max_retries} "
                    f"failed for batch starting at id={ids[0]}: {e}"
                )
                if attempt < max_retries:
                    time.sleep(retry_sleep_s)
                else:
                    failed_batches += 1
                    tqdm.write(
                        f"  ERROR: batch starting at id={ids[0]} failed "
                        f"after {max_retries} attempts; continuing epoch."
                    )
        done = start_offset + submitted_batches * batch_size
        if submitted_batches % max(1, total_to_ingest // (batch_size * 10)) == 0:
            tqdm.write(
                f"  Ingested ~{min(done, end_offset)}/{end_offset} vectors "
                f"of epoch ({submitted_batches} batches submitted, "
                f"{failed_batches} failed permanently)"
            )
    elapsed = time.time() - overall_start
    if failed_batches:
        tqdm.write(
            f"  Epoch ingest complete in {elapsed:.2f}s with "
            f"{failed_batches}/{submitted_batches} batches failed "
            f"permanently (queries still run on partial index)."
        )
    return elapsed


def run_benchmark(args: argparse.Namespace) -> None:
    """Run the VecStream streaming benchmark across N epochs.

    High-level phases:

    1. Validate CLI, load centroids (file or KMeans fit), build the
       ``IVFRouting``, construct a single :class:`VecStreamIngestionClient`
       for the entire run with ``delete_topic_on_close=True`` so close()
       leaves a clean Kafka cluster.
    2. For each epoch:
         * Stream that epoch's vectors into Kafka via ``put_vectors``
           (``_ingest_epoch``).
         * Block on :func:`vecstream.ingestion.wait_for_indexes` until the
           async indexer has merged new entries into
           ``{prefix}.index_list.json`` for ``stability_polls`` consecutive
           polls.
         * Build a fresh ``VecStreamQuerySystem`` against the cumulative
           prefix and run the query set, writing one JSON per top_k.
    3. ``client.close()`` deletes the topic.
    """
    try:
        top_ks = [int(k.strip()) for k in args.top_ks.split(",") if k.strip()]
    except ValueError:
        print(f"Error: invalid --top_ks value '{args.top_ks}'", file=sys.stderr)
        sys.exit(1)
    if not top_ks:
        print("Error: --top_ks must contain at least one value", file=sys.stderr)
        sys.exit(1)
    if args.num_epochs <= 0:
        print(
            f"Error: --num-epochs ({args.num_epochs}) must be > 0",
            file=sys.stderr,
        )
        sys.exit(1)
    if args.epoch_size <= 0:
        print(
            f"Error: --epoch-size ({args.epoch_size}) must be > 0",
            file=sys.stderr,
        )
        sys.exit(1)
    if args.batch_size <= 0:
        print(
            f"Error: --batch_size ({args.batch_size}) must be > 0",
            file=sys.stderr,
        )
        sys.exit(1)
    if args.epoch_size % args.batch_size != 0:
        print(
            f"Error: --epoch-size ({args.epoch_size}) must be a multiple "
            f"of --batch_size ({args.batch_size}).",
            file=sys.stderr,
        )
        sys.exit(1)
    if args.query_timeout_seconds <= 0:
        print(
            f"Error: --query_timeout_seconds ({args.query_timeout_seconds}) "
            f"must be > 0",
            file=sys.stderr,
        )
        sys.exit(1)
    if args.max_retries < 0:
        print(
            f"Error: --max_retries ({args.max_retries}) must be >= 0",
            file=sys.stderr,
        )
        sys.exit(1)

    queries = load_vectors(args.query_dataset, max_vectors=args.max_queries)
    if queries.ndim != 2:
        print(
            f"Error: expected 2D query array, got shape {queries.shape}",
            file=sys.stderr,
        )
        sys.exit(1)
    num_queries, dimension = queries.shape

    centroids = _load_centroids(
        args.vecstream_centroids,
        args.vecstream_sample_file,
        args.vecstream_num_sample,
        args.vecstream_n_clusters,
    )
    if centroids.ndim != 2:
        print(
            f"Error: centroids must be 2D, got shape {centroids.shape}",
            file=sys.stderr,
        )
        sys.exit(1)
    num_stream_partitions = centroids.shape[0]
    if args.vecstream_num_partitions is not None:
        if args.vecstream_num_partitions != num_stream_partitions:
            print(
                f"Error: --vecstream_num_partitions "
                f"({args.vecstream_num_partitions}) does not match "
                f"centroids.shape[0] ({num_stream_partitions}).",
                file=sys.stderr,
            )
            sys.exit(1)
    if centroids.shape[1] != dimension:
        print(
            f"Error: query dataset dimension ({dimension}) does not match "
            f"centroids dimension ({centroids.shape[1]}).",
            file=sys.stderr,
        )
        sys.exit(1)

    # Resolve dataset name early; it drives filenames and the default topic.
    dataset_name = (
        args.dataset_name
        or args.vecstream_dataset_name
        or Path(args.vector_dataset).stem
    )
    topic_name = (
        args.vecstream_topic
        or f"{args.vecstream_dataset_name or dataset_name}_{num_stream_partitions}"
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Query dataset:    {args.query_dataset} ({num_queries} queries, "
          f"dim={dimension})")
    print(f"Vector dataset:   {args.vector_dataset}")
    print(f"Dataset name:     {dataset_name}")
    print(f"Num epochs:       {args.num_epochs}")
    print(f"Epoch size:       {args.epoch_size}")
    print(f"Batch size:       {args.batch_size}")
    print(f"Top ks:           {top_ks}")
    print(f"Num stream partitions (from centroids): {num_stream_partitions}")
    print(f"S3 bucket:        {args.vecstream_bucket}")
    print(f"S3 base prefix:   {args.vecstream_prefix}")
    print(f"Kafka topic:      {topic_name}")
    print(f"Output dir:       {output_dir.resolve()}")
    print()

    routing = build_ivf_routing(centroids, metric=args.vecstream_metric)

    # ONE long-lived client across the whole run. __init__ recreates the
    # topic exactly once; close() (with delete_topic_on_close=True) cleans
    # it up at the very end. Constructing a fresh client per epoch would
    # delete+recreate the topic and wipe previous epochs' data, so this is
    # deliberate.
    client = VecStreamIngestionClient(
        bootstrap_servers=args.vecstream_bootstrap_servers,
        topic_name=topic_name,
        routing=routing,
        block_size=args.vecstream_block_size,
        dimension=dimension,
        metric=args.vecstream_metric,
        remote_storage_enabled=not args.vecstream_remote_storage_disabled,
        kafka_acks=args.vecstream_kafka_acks,
        kafka_local_retention_bytes=args.vecstream_kafka_local_retention_bytes,
        kafka_compression_type=args.vecstream_kafka_compression_type,
        batch_to_same_partition=False,
        delete_topic_on_close=True,
    )

    base_configuration = {
        "system": "vecstream",
        "query_dataset": args.query_dataset,
        "vector_dataset": args.vector_dataset,
        "dataset_name": dataset_name,
        "num_queries": num_queries,
        "vector_dimension": dimension,
        "num_epochs": args.num_epochs,
        "epoch_size": args.epoch_size,
        "batch_size": args.batch_size,
        "query_timeout_seconds": args.query_timeout_seconds,
        "max_retries": args.max_retries,
        "ingest_max_retries": args.ingest_max_retries,
        "ingest_retry_sleep_seconds": args.ingest_retry_sleep_seconds,
        "vecstream": {
            "bucket": args.vecstream_bucket,
            "base_prefix": args.vecstream_prefix,
            "num_partitions": num_stream_partitions,
            "num_stream_partitions": num_stream_partitions,
            "centroids_file": args.vecstream_centroids,
            "bootstrap_servers": args.vecstream_bootstrap_servers,
            "group_id": args.vecstream_group_id,
            "metric": args.vecstream_metric,
            "use_cache": args.vecstream_use_cache,
            "warmup": not args.vecstream_no_warmup,
            "region": args.vecstream_region,
            "num_partitions_to_search": args.vecstream_num_partitions_to_search,
            "reduce_branching_factor": args.vecstream_reduce_branching_factor,
            "map_invocations_per_lambda": args.vecstream_map_invocations_per_lambda,
            "topic": topic_name,
            "block_size": args.vecstream_block_size,
            "kafka_acks": args.vecstream_kafka_acks,
            "kafka_local_retention_bytes": args.vecstream_kafka_local_retention_bytes,
            "kafka_compression_type": args.vecstream_kafka_compression_type,
            "remote_storage_enabled": not args.vecstream_remote_storage_disabled,
            "wait_stability_polls": args.vecstream_wait_stability_polls,
            "wait_poll_interval": args.vecstream_wait_poll_interval,
            "wait_timeout": args.vecstream_wait_timeout,
        },
    }

    try:
        for epoch in range(args.num_epochs):
            start_offset = epoch * args.epoch_size
            end_offset = start_offset + args.epoch_size

            # In the real pipeline a single Kafka topic carries every
            # epoch's writes; the async indexer merges them all under one
            # {prefix}.index_list.json. epoch_prefix/epoch_topic stay in
            # the JSON for backward-compatibility with the plots
            # notebooks, but they all carry the same value.
            epoch_prefix = args.vecstream_prefix
            epoch_topic = topic_name

            print(
                f"\n=== Epoch {epoch}: ingesting offsets {start_offset}.."
                f"{end_offset} into topic '{epoch_topic}' "
                f"(prefix '{epoch_prefix}') ==="
            )

            ingest_start = time.time()
            ingest_seconds = _ingest_epoch(
                client,
                args.vector_dataset,
                start_offset=start_offset,
                end_offset=end_offset,
                batch_size=args.batch_size,
                max_retries=args.ingest_max_retries,
                retry_sleep_s=args.ingest_retry_sleep_seconds,
            )
            ingest_seconds = round(ingest_seconds, 6)
            print(
                f"Epoch {epoch} ingest done in {ingest_seconds:.2f}s. "
                f"Waiting for async indexer to catch up..."
            )

            registry_state = wait_for_indexes(
                bucket=args.vecstream_bucket,
                prefix=args.vecstream_prefix,
                stability_polls=args.vecstream_wait_stability_polls,
                poll_interval=args.vecstream_wait_poll_interval,
                timeout=args.vecstream_wait_timeout,
            )
            print(
                f"Async indexer caught up: registry "
                f"s3://{args.vecstream_bucket}/"
                f"{registry_state['registry_key']} now lists "
                f"{registry_state['total_indexes']} indexes "
                f"(stable for {registry_state['stable_for']} polls)."
            )

            system = VecStreamQuerySystem(
                bucket=args.vecstream_bucket,
                prefix=epoch_prefix,
                centroids=centroids,
                num_partitions=num_stream_partitions,
                dimension=dimension,
                metric=args.vecstream_metric,
                bootstrap_servers=args.vecstream_bootstrap_servers,
                kafka_topic=epoch_topic,
                kafka_group_id=args.vecstream_group_id,
                reduce_branching_factor=args.vecstream_reduce_branching_factor,
                map_invocations_per_lambda=args.vecstream_map_invocations_per_lambda,
                num_partitions_to_search=args.vecstream_num_partitions_to_search,
                use_cache=args.vecstream_use_cache,
                warmup=not args.vecstream_no_warmup,
                region=args.vecstream_region,
            )
            try:
                for top_k in top_ks:
                    configuration = {
                        **base_configuration,
                        "top_k": top_k,
                        "epoch_index": epoch,
                        "epoch_prefix": epoch_prefix,
                        "epoch_topic": epoch_topic,
                        "epoch_start_offset": start_offset,
                        "epoch_end_offset": end_offset,
                        "epoch_ingest_seconds": ingest_seconds,
                        "epoch_registry_total_indexes": registry_state[
                            "total_indexes"
                        ],
                        "epoch_registry_stable_for": registry_state["stable_for"],
                        "epoch_registry_key": registry_state["registry_key"],
                    }
                    results = run_queries_for_epoch(
                        system,
                        queries,
                        top_k,
                        timeout_s=args.query_timeout_seconds,
                        max_retries=args.max_retries,
                    )
                    output_path = write_results(
                        output_dir,
                        dataset_name,
                        top_k,
                        epoch,
                        configuration,
                        results,
                    )
                    latencies = [
                        r["latency_seconds"]
                        for r in results
                        if r["latency_seconds"] is not None
                    ]
                    timed_out = sum(
                        1 for r in results if r["latency_seconds"] is None
                    )
                    print(
                        f"  top_k={top_k}: wrote {len(results)} results to "
                        f"{output_path.name} ({timed_out} timed out)"
                    )
                    print_stats(latencies, f"epoch={epoch} top_k={top_k}")
            finally:
                system.close()
    finally:
        client.close()

    print("\nBenchmark complete.")


def main() -> None:
    args = parse_args()
    print_configuration(vars(args))
    run_benchmark(args)


if __name__ == "__main__":
    main()
