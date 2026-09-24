import argparse
import json
import math
import sys
from pathlib import Path

import faiss
import numpy as np

from benchmarks.static_queries.data_loader import load_vectors


def parse_args() -> argparse.Namespace:
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Exhaustive FAISS ground truth for the streaming-queries benchmark. "
            "For each epoch k, builds a fresh flat index from the cumulative "
            "first (k+1)*N vectors of the dataset and searches the query set."
        )
    )
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="Path to .fbin or .npy file containing the dataset vectors",
    )
    parser.add_argument(
        "--queries",
        type=str,
        required=True,
        help="Path to .fbin or .npy file containing the query vectors",
    )
    parser.add_argument(
        "--epoch-size",
        type=int,
        required=True,
        help="Number of vectors per epoch (N). At epoch k the flat index "
        "contains the cumulative first (k+1)*N vectors of the dataset.",
    )
    parser.add_argument(
        "--topk",
        type=int,
        required=True,
        help="Number of nearest neighbors to retrieve",
    )
    parser.add_argument(
        "--metric",
        type=str,
        choices=["euclidean", "cosine"],
        default="euclidean",
        help="Distance metric (default: euclidean). 'cosine' uses IndexFlatIP "
        "+ METRIC_INNER_PRODUCT and assumes vectors are pre-normalized.",
    )
    parser.add_argument(
        "--max-queries",
        type=int,
        default=1000,
        help="Maximum number of queries to use (default: 1000)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON path (default: <queries_stem>_<topk>_epochs<E>_results.json)",
    )
    return parser.parse_args()


def build_flat_index(dim: int, metric: str) -> faiss.Index:
    """Return a fresh flat index for the given metric.

    'cosine' uses IndexFlatIP + METRIC_INNER_PRODUCT and assumes the input
    vectors are pre-normalized to unit length (same contract as
    prepare_vecstream_data.py). 'euclidean' uses IndexFlatL2.
    """
    if metric == "cosine":
        return faiss.IndexFlatIP(dim)
    return faiss.IndexFlatL2(dim)


def default_output_path(queries_path: str, topk: int, num_epochs: int) -> Path:
    """Return the default output JSON path: <stem>_<topk>_epochs<E>_results.json."""
    p = Path(queries_path)
    return p.with_name(f"{p.stem}_{topk}_epochs{num_epochs}_results.json")


def main() -> None:
    """Load dataset and queries, compute per-epoch ground truth, write JSON."""
    args = parse_args()

    dataset = load_vectors(args.dataset)
    if dataset.ndim != 2:
        print(
            f"Error: expected 2D dataset array, got shape {dataset.shape}",
            file=sys.stderr,
        )
        sys.exit(1)
    total, dim = dataset.shape
    if args.epoch_size <= 0:
        print(
            f"Error: --epoch-size ({args.epoch_size}) must be > 0",
            file=sys.stderr,
        )
        sys.exit(1)
    num_epochs = math.ceil(total / args.epoch_size)
    print(f"Dataset: {total} vectors, dim={dim}, num_epochs={num_epochs}")

    queries = load_vectors(args.queries, max_vectors=args.max_queries)
    if queries.ndim != 2:
        print(
            f"Error: expected 2D queries array, got shape {queries.shape}",
            file=sys.stderr,
        )
        sys.exit(1)
    if queries.shape[1] != dim:
        print(
            f"Error: dimension mismatch: dataset={dim}, queries={queries.shape[1]}",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Queries: {queries.shape[0]} vectors (max {args.max_queries})")

    results: dict[str, list[dict]] = {}
    for k in range(num_epochs):
        end = min((k + 1) * args.epoch_size, total)
        epoch_vectors = dataset[:end]
        print(
            f"=== Epoch {k}: indexing {end} cumulative vectors, "
            f"searching {queries.shape[0]} queries (top {args.topk}) ==="
        )
        index = build_flat_index(dim, args.metric)
        index.add(epoch_vectors)
        distances, ids = index.search(queries, args.topk)
        results[f"epoch_{k}"] = [
            {"ids": ids[i].tolist(), "distances": distances[i].tolist()}
            for i in range(queries.shape[0])
        ]

    output_path = (
        Path(args.output)
        if args.output
        else default_output_path(args.queries, args.topk, num_epochs)
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "configuration": {
            "dataset": args.dataset,
            "queries": args.queries,
            "epoch_size": args.epoch_size,
            "topk": args.topk,
            "metric": args.metric,
            "max_queries": args.max_queries,
            "num_epochs": num_epochs,
            "dataset_vectors": total,
            "dataset_dimension": dim,
            "query_vectors_used": queries.shape[0],
            "query_dimension": queries.shape[1],
        },
        "results": results,
    }
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)
    print(
        f"Saved ground truth for {num_epochs} epoch(s), {queries.shape[0]} "
        f"queries each, to {output_path}"
    )


if __name__ == "__main__":
    main()
