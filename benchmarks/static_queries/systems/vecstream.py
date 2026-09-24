import asyncio
import threading
import time
from pathlib import Path

import boto3
import numpy as np

from benchmarks.static_queries.systems.base import VectorSystem
from vecstream.invoke_http import get_session
from vecstream.routing import IVFRouting
from vecstream.vecstream_client import VecStreamClient


class VecStreamQuerySystem(VectorSystem):
    """FAISS + Kafka search via the async VecStreamClient.

    The VecStreamClient exposes a coroutine-based API (load_l1_cache,
    warmup_lambdas, search). The static_queries benchmark is sync and runs
    one query_vectors call at a time, so this class owns a persistent
    asyncio event loop in a daemon thread and bridges to it with
    run_coroutine_threadsafe. This keeps the aiohttp.ClientSession warm
    across queries and avoids paying session creation/teardown per call.

    Parameters
    ----------
    bucket : str
        S3 bucket containing the per-partition FAISS indexes.
    prefix : str
        S3 key prefix; object keys are expected to live under
        ``{prefix}/partition_{id}/...`` so that the client's partition
        parser (``key.split('/')[-2].split('_')[-1]``) can recover the id.
    centroids : np.ndarray
        ``(num_partitions, dimension)`` array of KMeans centroids used by
        the IVF-based routing algorithm.
    num_partitions : int
        Number of partitions. Must match both the Kafka topic partition
        count and ``centroids.shape[0]``.
    dimension : int
        Vector dimensionality. Must match ``centroids.shape[1]``.
    metric : str
        ``"euclidean"`` or ``"cosine"``. Forwarded to the routing and the
        Kafka search lambdas.
    bootstrap_servers : str
        Kafka bootstrap address used by the stream-search path.
    kafka_topic : str
        Kafka topic carrying the live stream of vectors.
    kafka_group_id : str
        Kafka consumer group used for offset tracking.
    reduce_branching_factor : int
        Branching factor for the reduce fanout (default 16).
    map_invocations_per_lambda : int
        Map invocations per Lambda worker (default 16).
    num_partitions_to_search : int
        Number of partitions each query fans out to (default 16).
    use_cache : bool
        If True, route through the L1/L2 cache Lambdas (CACHED_QUERY).
        If False, force a non-cached path.
    warmup : bool
        If True (default), run load_l1_cache() and warmup_lambdas() in
        __init__ so the first timed query is hot.
    region : str, optional
        AWS region for the boto3 S3 client (overrides the default chain).
    """

    def __init__(
        self,
        bucket: str,
        prefix: str,
        centroids: np.ndarray,
        num_partitions: int,
        dimension: int,
        metric: str = "euclidean",
        bootstrap_servers: str = "localhost:9092",
        kafka_topic: str = "vecstream_topic",
        kafka_group_id: str = "vecstream_group",
        reduce_branching_factor: int = 16,
        map_invocations_per_lambda: int = 16,
        num_partitions_to_search: int = 16,
        use_cache: bool = True,
        warmup: bool = True,
        region: str | None = None,
    ):
        self.bucket = bucket
        self.prefix = prefix
        self.num_partitions = num_partitions
        self.dimension = dimension
        self.metric = metric
        self.use_cache = use_cache
        self.reduce_branching_factor = reduce_branching_factor
        self.map_invocations_per_lambda = map_invocations_per_lambda
        self.num_partitions_to_search = num_partitions_to_search

        if centroids.ndim != 2:
            raise ValueError(
                f"centroids must be 2D, got shape {centroids.shape}"
            )
        if centroids.shape[0] != num_partitions:
            raise ValueError(
                f"centroids.shape[0] ({centroids.shape[0]}) must equal "
                f"num_partitions ({num_partitions})"
            )
        if centroids.shape[1] != dimension:
            raise ValueError(
                f"centroids.shape[1] ({centroids.shape[1]}) must equal "
                f"dimension ({dimension})"
            )

        routing = IVFRouting(
            n_partitions=num_partitions,
            d=dimension,
            centroids=centroids,
            metric=metric,
        )

        self.client = VecStreamClient(
            bucket=bucket,
            prefix=prefix,
            num_partitions=num_partitions,
            dimension=dimension,
            routing=routing,
            bootstrap_servers=bootstrap_servers,
            kafka_topic=kafka_topic,
            kafka_group_id=kafka_group_id,
            metric=metric,
        )

        if region is not None:
            self.client.s3_client = boto3.client("s3", region_name=region)

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop, args=(self._loop,), daemon=True
        )
        self._thread.start()

        if warmup:
            self._submit(self.client.warmup_lambdas()).result()

            if self.use_cache:
                self.client.create_index_list()
                self._submit(self.client.load_l1_cache()).result()

                async def _start_keepalive() -> None:
                    self.client.start_keepalive()

                self._submit(_start_keepalive()).result()

    @staticmethod
    def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def _submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    async def _do_search(
        self, query_vector: np.ndarray, top_k: int
    ) -> tuple[np.ndarray, np.ndarray, dict]:
        return await self.client.search(
            query_vector,
            top_k,
            use_cache=self.use_cache,
            reduce_branching_factor=self.reduce_branching_factor,
            map_invocations_per_lambda=self.map_invocations_per_lambda,
            num_partitions_to_search=self.num_partitions_to_search,
        )

    def put_vectors(self, vectors: np.ndarray, ids: list[str]) -> dict:
        """Ingestion is intentionally out of scope for the static_queries benchmark.

        VecStream ingestion is handled by ingest.py
        """
        raise NotImplementedError(
            "VecStreamQuerySystem does not support put_vectors; "
            "use ingest.py for ingestion"
        )

    def query_vectors(self, vectors: np.ndarray, top_k: int) -> dict:
        query_vector = np.asarray(vectors[0], dtype=np.float32)
        if query_vector.ndim != 1 or query_vector.size != self.dimension:
            raise ValueError(
                f"query vector must be 1D of size {self.dimension}, "
                f"got shape {query_vector.shape}"
            )

        start = time.time()
        Ds, Is, timestamps = self._submit(
            self._do_search(query_vector, top_k)
        ).result()
        end = time.time()

        if Ds is None or Is is None or Ds.size == 0 or Is.size == 0:
            results: list[dict] = []
        else:
            results = [
                {"id": str(int(i)), "distance": float(d)}
                for d, i in zip(Ds[0], Is[0])
            ]

        return {
            "start_time": start,
            "end_time": end,
            "latency_seconds": end - start,
            "distance_metric": self.metric,
            "query_vector": query_vector.tolist(),
            "results": results,
            "system_data": {
                "vecstream_timestamps": timestamps,
                "metric": self.metric,
                "num_partitions_to_search": self.num_partitions_to_search,
                "use_cache": self.use_cache,
            },
        }

    def close(self) -> None:
        try:
            self.client.stop_keepalive()
        except Exception as e:
            print(f"[VecStreamQuerySystem] stop_keepalive raised: {e}", flush=True)

        async def _close_thread_session() -> None:
            sess = get_session()
            if not sess.closed:
                await sess.close()

        try:
            self._submit(_close_thread_session()).result(timeout=5)
        except Exception as e:
            print(f"[VecStreamQuerySystem] session close raised: {e}", flush=True)
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=10)
        finally:
            self._loop.close()


def load_centroids(path: str) -> np.ndarray:
    """Load a ``.npy`` centroids file produced by compute_centroids.py."""
    return np.load(Path(path))
