"""Thin wrapper around :mod:`vecstream.ingestion` for the ingestion benchmark.

The benchmark used to ship a hand-rolled reimplementation of the Kafka
producer, topic lifecycle and IVF routing. That logic now lives in the
``vecstream`` core library; this module just configures the public
``VecStreamIngestionClient`` for a benchmark run and forwards calls to it.
"""

from pathlib import Path

import numpy as np

from benchmarks.ingestion.systems.base import VectorSystem
from vecstream.ingestion import (
    VecStreamIngestionClient,
    build_ivf_routing,
    compute_centroids,
    load_vectors,
)


class VecStreamSystem(VectorSystem):
    """Benchmark-side facade for the VecStream Kafka ingestion client.

    Constructor responsibilities:
      * Load centroids from a precomputed ``.npy`` file, or fit KMeans on a
        sample of the dataset via the library's :func:`compute_centroids`.
      * Wrap the centroids in an ``IVFRouting`` via
        :func:`vecstream.ingestion.build_ivf_routing`.
      * Construct a :class:`VecStreamIngestionClient`, which owns the Kafka
        producer, topic lifecycle, and segment/retention configuration.

    ``put_vectors`` and ``close`` are pure delegations.
    """

    def __init__(
        self,
        bootstrap_servers: str | list[str],
        topic_name: str,
        dimension: int = 0,
        metric: str = "euclidean",
        block_size: int = 7_500_000,
        remote_storage_enabled: bool = True,
        kafka_acks: int | str = 1,
        kafka_local_retention_bytes: int = 1,
        kafka_compression_type: str | None = None,
        batch_to_same_partition: bool = False,
        vector_centroids: np.ndarray | None = None,
        centroids_file: str | None = None,
        sample_file: str | None = None,
        num_sample: int = 100_000,
        n_clusters: int = 1000,
    ) -> None:
        super().__init__()

        if vector_centroids is None and centroids_file is not None:
            vector_centroids = np.load(centroids_file)
        if vector_centroids is None and sample_file is not None:
            print(
                f"Computing {n_clusters} centroids from {sample_file} "
                f"(num_sample={num_sample})..."
            )
            vector_centroids = compute_centroids(
                sample_file,
                num_sample=num_sample,
                n_clusters=n_clusters,
            )
            print(f"Centroids computed: shape={vector_centroids.shape}")
        if vector_centroids is None:
            raise ValueError(
                "Either vector_centroids, centroids_file, or sample_file must be provided"
            )

        routing = build_ivf_routing(vector_centroids, metric=metric)

        self.client = VecStreamIngestionClient(
            bootstrap_servers=bootstrap_servers,
            topic_name=topic_name,
            routing=routing,
            block_size=block_size,
            dimension=dimension,
            metric=metric,
            remote_storage_enabled=remote_storage_enabled,
            kafka_acks=kafka_acks,
            kafka_local_retention_bytes=kafka_local_retention_bytes,
            kafka_compression_type=kafka_compression_type,
            batch_to_same_partition=batch_to_same_partition,
        )

    def put_vectors(self, vectors: np.ndarray, ids: list[str]) -> dict:
        return self.client.put_vectors(vectors, ids)

    def close(self) -> None:
        self.client.close()


__all__ = ["VecStreamSystem"]
