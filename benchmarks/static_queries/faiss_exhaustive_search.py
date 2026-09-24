import argparse
import json
import sys
from pathlib import Path

import faiss
import numpy as np

from benchmarks.static_queries.data_loader import load_vectors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exhaustive similarity search with a FAISS flat index."
    )
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="Path to .fbin file containing the dataset vectors",
    )
    parser.add_argument(
        "--queries",
        type=str,
        required=True,
        help="Path to .fbin file containing the query vectors",
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
        choices=["l2", "ip"],
        default="l2",
        help="Distance metric: l2 (Euclidean) or ip (inner product) (default: l2)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    dataset = load_vectors(args.dataset)
    print(f"Loaded dataset: {dataset.shape[0]} vectors of dimension {dataset.shape[1]}")
    queries = load_vectors(args.queries)
    print(f"Loaded queries: {queries.shape[0]} vectors of dimension {queries.shape[1]}")

    if dataset.ndim != 2:
        print(f"Error: expected 2D dataset array, got shape {dataset.shape}", file=sys.stderr)
        sys.exit(1)
    if queries.ndim != 2:
        print(f"Error: expected 2D queries array, got shape {queries.shape}", file=sys.stderr)
        sys.exit(1)

    dim = dataset.shape[1]
    if queries.shape[1] != dim:
        print(
            f"Error: dimension mismatch: dataset={dim}, queries={queries.shape[1]}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Building FAISS index with metric '{args.metric}'...")
    faiss_metric = faiss.METRIC_L2 if args.metric == "l2" else faiss.METRIC_INNER_PRODUCT
    index = faiss.IndexFlat(dim, faiss_metric)
    index.add(dataset)
    print(f"Index built with {index.ntotal} vectors.")

    print(f"Performing exhaustive search for {queries.shape[0]} queries, retrieving top {args.topk} neighbors...")
    distances, ids = index.search(queries, args.topk)
    print("Search completed.")

    results = [
        {"ids": ids[i].tolist(), "distances": distances[i].tolist()}
        for i in range(len(queries))
    ]

    query_path = Path(args.queries)
    output_name = f"{query_path.stem}_{args.topk}_results.json"
    output_path = query_path.with_name(output_name)

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Saved results for {len(queries)} queries to {output_path}")


if __name__ == "__main__":
    main()
