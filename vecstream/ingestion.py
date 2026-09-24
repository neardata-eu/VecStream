"""Ingestion client for VecStream's Kafka-backed stream search.

Implements the paper §4 claim that "a client library hides ingestion complexity,
transparently managing Kafka producers and routing." Applications pass numpy
vectors and string ids; the client handles:

  * Wire-protocol framing (8-byte little-endian uint64 count + two ``np.save``
    arrays), byte-compatible with ``vecstream.stream_search.deserialize_message``
    and the async indexer on the consumer side.
  * Partition routing via a pluggable ``vecstream.routing.Routing`` subclass.
    The default ``IVFRouting`` returns a single partition per vector; an MHT
    subclass may override ``get_ingest_partitions_batch`` to fan out replicas.
  * Topic lifecycle (delete + create with the same VecStream segment/
    retention configuration, 10 s settle sleep) and producer config.

The Routing ABC contract (see ``vecstream.routing``):
  * ``get_ingest_partitions_batch(vectors)`` returns a list-of-lists: one
    inner list per vector, holding every replica partition the vector should
    be written to.
  * ``n_partitions`` is the Kafka topic partition count (used both to size
    the topic and to apply the ``bucket % n_partitions`` modulo when the
    routing lives in a larger bucket space than the topic).

Public surface:
  * ``wire_protocol(ids, vectors)`` — frame vectors for Kafka transport.
  * ``load_vectors(path, max_vectors=None)`` — read .npy or .fbin.
  * ``compute_centroids(sample_file, ...)`` — KMeans centroids for routing.
  * ``build_ivf_routing(centroids, metric="euclidean")`` — convenience
    wrapper around ``IVFRouting``.
  * ``VecStreamIngestionClient`` — full producer + topic manager.
  * ``wait_for_indexes(bucket, prefix, ...)`` — poll the S3 index registry
    until the async indexer has caught up with the producer.
"""

import io
import json
import random
import struct
import time
from pathlib import Path

import boto3
import numpy as np
from botocore.exceptions import ClientError
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from sklearn.cluster import KMeans

from vecstream.routing import IVFRouting, Routing


def load_vectors(path: str, max_vectors: int | None = None) -> np.ndarray:
    """Load vectors from a ``.npy`` or `.fbin` (DiskANN-style) file.

    Args:
        path: Path to the dataset.
        max_vectors: If given, truncate to this many rows (cheaper than
            loading the full file when only a sample is needed for centroid
            fitting).

    Returns:
        A 2-D ``float32`` ``np.ndarray`` of shape ``(n, d)`` for ``.fbin``,
        or whatever dtype/ndim the ``.npy`` file holds otherwise.
    """
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


def wire_protocol(ids: np.ndarray, vectors: np.ndarray) -> list[bytes]:
    """Serialize an (ids, vectors) batch into Kafka-ready message frames.

    Wire format (must stay byte-compatible with
    ``vecstream.stream_search.deserialize_message`` and the async indexer):
      * 8-byte little-endian ``uint64`` count header (length of the chunk).
      * ``np.save(ids_chunk)`` — full ``.npy`` header + int64 ids.
      * ``np.save(vecs_chunk)`` — full ``.npy`` header + float32 vectors.

    Frames are capped at ~1 MB so that a single Kafka message never exceeds
    ``message.max.bytes`` defaults. Chunks are sliced along the first axis;
    ids and vectors stay aligned.

    Args:
        ids: 1-D array of vector ids (typically ``np.int64``).
        vectors: 2-D array of shape ``(len(ids), d)``.

    Returns:
        A list of message ``bytes``; one per chunk.
    """
    num_vectors = len(ids)
    if vectors.ndim != 2 or vectors.shape[0] != num_vectors:
        raise ValueError(
            f"wire_protocol: ids (len={num_vectors}) and vectors "
            f"(shape={vectors.shape}) must agree on the leading axis"
        )
    vector_dim = vectors.shape[1]

    max_size_msg = 1000 * 1000
    size_per_vector = 4 * vector_dim
    size_per_id = 8
    size_per_entry = size_per_id + size_per_vector
    max_vectors_per_message = max(1, max_size_msg // size_per_entry)

    messages: list[bytes] = []
    for start in range(0, num_vectors, max_vectors_per_message):
        end = min(start + max_vectors_per_message, num_vectors)
        ids_chunk = ids[start:end]
        vecs_chunk = vectors[start:end]

        memfile = io.BytesIO()
        length = len(ids_chunk)
        memfile.write(struct.pack("<Q", length))
        np.save(memfile, ids_chunk)
        np.save(memfile, vecs_chunk)
        messages.append(memfile.getvalue())

    return messages


def compute_centroids(
    sample_file: str,
    num_sample: int = 10_000,
    n_clusters: int = 1000,
    seed: int = 42,
) -> np.ndarray:
    """Fit KMeans on a sample of the dataset and return the cluster centers.

    Args:
        sample_file: Path to a ``.npy`` or ``.fbin`` vector file.
        num_sample: Maximum rows to load for fitting.
        n_clusters: Number of centroids (also the routing bucket space).
        seed: RNG seed for ``sklearn.cluster.KMeans``.

    Returns:
        ``np.ndarray`` of shape ``(n_clusters, d)`` (float64 — the KMeans
        cluster centers native dtype).
    """
    vectors = load_vectors(sample_file, max_vectors=num_sample)
    kmeans = KMeans(n_clusters=n_clusters, random_state=seed, n_init="auto")
    kmeans.fit(vectors)
    return kmeans.cluster_centers_


def build_ivf_routing(
    centroids: np.ndarray,
    metric: str = "euclidean",
) -> IVFRouting:
    """Wrap ``centroids`` in an ``IVFRouting`` configured for ingest.

    The routing's ``n_partitions`` is the centroid count — the routing space.
    Kafka topic partitions are a separate count and the client applies the
    ``bucket % n_partitions`` modulo at produce time (see
    ``VecStreamIngestionClient.put_vectors``).

    Args:
        centroids: ``(k, d)`` cluster centers.
        metric: ``"euclidean"`` or ``"cosine"``.

    Returns:
        A configured ``IVFRouting`` instance.
    """
    if centroids.ndim != 2:
        raise ValueError(f"centroids must be 2-D, got shape {centroids.shape}")
    n_partitions = centroids.shape[0]
    d = centroids.shape[1]
    return IVFRouting(n_partitions=n_partitions, d=d, centroids=centroids, metric=metric)


class VecStreamIngestionClient:
    """Kafka producer + topic manager for VecStream ingestion.

    Args:
        bootstrap_servers: A single bootstrap server string or a list of
            servers (joined with commas before being handed to librdkafka).
        topic_name: Target Kafka topic.
        routing: A ``vecstream.routing.Routing`` instance. Its
            ``n_partitions`` defines the Kafka topic partition count and
            its ``get_ingest_partitions_batch(vectors)`` returns, for each
            vector, the list of replica partitions to write to.
        block_size: ``segment.bytes`` for the Kafka topic (also the seal
            threshold consumed by the async indexer). Default: 7.5 MB.
        dimension: Vector dimensionality; stored for API symmetry with the
            benchmark and future validation.
        metric: ``"euclidean"`` or ``"cosine"``; stored on the client (routing
            subclasses consult this for distance calculations).
        remote_storage_enabled: When ``True``, sets
            ``remote.storage.enable=true`` on the topic so VecStream can
            tier cold segments to S3.
        kafka_acks: ``acks`` for the producer (``int`` or ``"all"``).
        kafka_local_retention_bytes: ``local.retention.bytes`` topic config.
        kafka_compression_type: Optional producer ``compression.type``
            (``"gzip"``, ``"snappy"``, ``"lz4"``, ``"zstd"`` or ``None``).
        batch_to_same_partition: If ``True``, every batch is written to one
            random Kafka partition (debugging knob from the original
            benchmark; useful for collapsing producer fan-out to a single
            partition for load testing).
        delete_topic_on_close: If ``True`` (default), ``close()`` deletes the
            Kafka topic to leave a clean cluster (matches the legacy
            ingestion-benchmark behavior). Set to ``False`` when the topic
            must outlive the client — e.g. a benchmark that ingests, then
            waits for the async indexer with ``wait_for_indexes(...)`` and
            finally runs queries against the same topic.

    Topic lifecycle on ``__init__``:
      1. Delete the topic (best effort — logged on failure).
      2. Create it with the VecStream segment/retention configuration.
      3. Sleep 10 s so the cluster settles before the first produce.

    Notes:
        The Routing instance is the integration point for LSH / MHT /
        hyperplane-based subclasses added separately. Any subclass that
        overrides ``get_ingest_partitions_batch`` (e.g. MHT returning one
        partition per table) plugs in here with no further client changes.
    """

    def __init__(
        self,
        bootstrap_servers: str | list[str],
        topic_name: str,
        routing: Routing,
        block_size: int = 7_500_000,
        dimension: int = 0,
        metric: str = "euclidean",
        remote_storage_enabled: bool = True,
        kafka_acks: int | str = 1,
        kafka_local_retention_bytes: int = 1,
        kafka_compression_type: str | None = None,
        batch_to_same_partition: bool = False,
        delete_topic_on_close: bool = True,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.bootstrap_servers_str = (
            ",".join(bootstrap_servers)
            if isinstance(bootstrap_servers, list)
            else bootstrap_servers
        )
        self.topic_name = topic_name
        self.routing = routing
        self.block_size = block_size
        self.dimension = dimension
        self.metric = metric
        self.remote_storage_enabled = remote_storage_enabled
        self.kafka_local_retention_bytes = kafka_local_retention_bytes
        self.batch_to_same_partition = batch_to_same_partition
        self.delete_topic_on_close = delete_topic_on_close

        if isinstance(kafka_acks, str) and kafka_acks.lower() != "all":
            try:
                self.kafka_acks: int | str = int(kafka_acks)
            except ValueError:
                self.kafka_acks = kafka_acks
        else:
            self.kafka_acks = kafka_acks

        self.kafka_compression_type = kafka_compression_type

        producer_config: dict[str, object] = {
            "bootstrap.servers": self.bootstrap_servers_str,
            "acks": str(self.kafka_acks)
            if isinstance(self.kafka_acks, int)
            else self.kafka_acks,
            "queue.buffering.max.kbytes": 327680,
        }
        if self.kafka_compression_type:
            producer_config["compression.type"] = self.kafka_compression_type

        self.producer = Producer(producer_config)
        self.delete_topic(self.topic_name)
        self._create_topic()

    def _create_topic(self) -> None:
        """Create the Kafka topic with VecStream's segment/retention config.

        Sleeps 10 s after creation to let the cluster settle before the
        first produce. Failures are logged but not raised so a stale topic
        doesn't crash the client (matches the original benchmark's behavior).
        """
        n_partitions = self.routing.n_partitions
        print(
            f"Creating topic {self.topic_name} with {n_partitions} partitions "
            f"and block size {self.block_size} bytes."
        )
        print(f"Remote storage enabled: {self.remote_storage_enabled}")
        print(f"Kafka acks: {self.kafka_acks}")
        print(f"Kafka local retention bytes: {self.kafka_local_retention_bytes}")
        print(f"Kafka batch to same partition: {self.batch_to_same_partition}")

        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers_str})
        topic = NewTopic(
            self.topic_name,
            num_partitions=n_partitions,
            replication_factor=1,
            config={
                "remote.storage.enable": str(self.remote_storage_enabled).lower(),
                "segment.bytes": str(self.block_size),
                "local.retention.bytes": str(self.kafka_local_retention_bytes),
                "local.retention.ms": "-2",
                "retention.bytes": "-1",
                "retention.ms": "-1",
                "cleanup.policy": "delete",
            },
        )
        try:
            fs = admin_client.create_topics([topic])
            for _topic_name, f in fs.items():
                f.result()
            print(f"Topic {self.topic_name} created successfully")
        except Exception as e:
            print(f"Error on creating dataset {self.topic_name}: {e}")
        finally:
            print(f"Waiting for topic {self.topic_name} to be fully created.")
            print(f"Sleeping for 10 seconds...")
            time.sleep(10)

    def delete_topic(self, topic_name: str) -> None:
        """Delete ``topic_name`` (best effort). Sleeps 5 s after deletion."""
        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers_str})
        try:
            fs = admin_client.delete_topics([topic_name])
            for _topic, f in fs.items():
                f.result()
            print(f"Topic {topic_name} deleted successfully")
            time.sleep(5)
        except Exception as e:
            print(f"Error on deleting dataset {topic_name}: {e}")

    def close(self) -> None:
        """Flush the producer and (optionally) delete the topic.

        Topic deletion is gated on ``delete_topic_on_close``: ``True`` (the
        default) preserves the legacy ingestion-benchmark behavior of a
        clean cluster after the run; ``False`` keeps the topic alive for
        downstream consumers (e.g. a benchmark that calls
        ``wait_for_indexes(...)`` after ingesting).
        """
        if self.delete_topic_on_close:
            self.delete_topic(self.topic_name)
        self.producer.flush()

    def put_vectors(self, vectors: np.ndarray, ids: list[str]) -> dict:
        """Send a batch of vectors to Kafka via the configured routing.

        Args:
            vectors: 2-D ``np.ndarray`` of shape ``(len(ids), d)``.
            ids: String ids, one per row of ``vectors``.

        Returns:
            Timing dict with keys:
              * ``latency_seconds`` — wall-clock duration of the call.
              * ``insert_count`` — number of (id, vector) pairs sent.
              * ``partition_serialization_times`` — list of tuples
                ``(kafka_partition, end_serialize_time, end_key_time)``,
                one per partition that received messages in this batch.
                Same shape as the original benchmark so existing result
                parsers keep working.
              * ``flush_time_seconds`` — wall-clock at the moment we
                called ``producer.flush()`` (i.e. before the flush completes
                and before the call returns).

        Notes:
            The Routing may return more than one partition per vector
            (replica fan-out, e.g. MHT). Each (id, vector) tuple is appended
            to the group of every partition it was assigned to, so each
            replica partition sees its own complete copy.

            When ``batch_to_same_partition`` is set the routing is bypassed
            and the entire batch goes to a single random partition; this
            fixes the prior benchmark's NameError-prone branch.
        """
        if vectors.ndim != 2 or vectors.shape[0] != len(ids):
            raise ValueError(
                f"put_vectors: ids (len={len(ids)}) and vectors "
                f"(shape={vectors.shape}) must agree on the leading axis"
            )

        n_partitions = self.routing.n_partitions

        if self.batch_to_same_partition:
            return self._put_vectors_single_partition(vectors, ids, n_partitions)

        start = time.time()
        routing_assignments = self.routing.get_ingest_partitions_batch(vectors)
        ids_array = np.array(ids)
        partition_serialization_times: list[tuple[int, float, float]] = []

        # Group (id, vector) by Kafka partition across all replica partitions.
        # When the routing returns one partition per vector, this collapses
        # to the original benchmark's "group by cluster id" behavior, with
        # the bucket -> partition modulo applied to map centroids into the
        # Kafka topic's partition count.
        groups: dict[int, list[int]] = {}
        for vec_idx, partitions in enumerate(routing_assignments):
            for routing_partition in partitions:
                kafka_partition = int(routing_partition) % n_partitions
                groups.setdefault(kafka_partition, []).append(vec_idx)

        for kafka_partition, indices in groups.items():
            batch_ids = ids_array[indices]
            batch_vectors = vectors[indices]
            key_bytes = int(kafka_partition).to_bytes(8, byteorder="big")
            time_key_bytes = time.time()
            msgs = wire_protocol(batch_ids, batch_vectors)
            time_msgs = time.time()
            partition_serialization_times.append(
                (kafka_partition, time_msgs, time_key_bytes)
            )
            for msg in msgs:
                self.producer.produce(
                    self.topic_name,
                    partition=kafka_partition,
                    key=key_bytes,
                    value=msg,
                )

        flush_time = time.time()
        self.producer.flush()
        end = time.time()

        return {
            "latency_seconds": end - start,
            "insert_count": len(ids),
            "partition_serialization_times": partition_serialization_times,
            "flush_time_seconds": flush_time,
        }

    def _put_vectors_single_partition(
        self,
        vectors: np.ndarray,
        ids: list[str],
        n_partitions: int,
    ) -> dict:
        """Debug-mode: route the entire batch to a single random partition.

        Used to compare the cost of producing vs. routing in benchmarks.
        """
        start = time.time()
        key = random.randint(0, n_partitions - 1)
        key_bytes = int(key).to_bytes(8, byteorder="big")
        time_key_bytes = time.time()
        msgs = wire_protocol(np.array(ids), vectors)
        time_msgs = time.time()
        partition_serialization_times = [(int(key), time_msgs, time_key_bytes)]
        for msg in msgs:
            self.producer.produce(
                self.topic_name,
                partition=int(key),
                key=key_bytes,
                value=msg,
            )

        flush_time = time.time()
        self.producer.flush()
        end = time.time()

        return {
            "latency_seconds": end - start,
            "insert_count": len(ids),
            "partition_serialization_times": partition_serialization_times,
            "flush_time_seconds": flush_time,
        }


def wait_for_indexes(
    bucket: str,
    prefix: str,
    s3_client: object | None = None,
    stability_polls: int = 3,
    poll_interval: float = 10.0,
    timeout: float = 3600.0,
) -> dict:
    """Block until the async indexing pipeline has caught up with the producer.

    The async indexer (deployed as ``vecstream.async_index_creation.event_handler``)
    seals each Kafka log segment, trains a FAISS index on it, uploads the
    ``.ann`` to ``{prefix}/partition_{partition}/index_{base_offset}.ann``
    and updates the registry at ``{prefix}.index_list.json`` with a
    conditional read-merge-write. This helper polls that registry and
    returns once the total number of registered index entries has stayed
    constant for ``stability_polls`` consecutive polls, mirroring the
    "ingestion is done" condition the benchmark suite waits for before
    firing query traffic.

    Paper §4: "Kafka offsets are committed only by the indexing Lambdas,
    which also maintain a metadata file in S3 describing the current state
    of the stored indexes. This file is conditionally updated after each
    segment is sealed and its index uploaded, allowing queries to discover
    the index state without S3 list operations." The registry this helper
    polls is that metadata file.

    Args:
        bucket: S3 bucket holding the index registry. Must be the same
            bucket the async indexer writes to (the indexer's
            ``INDEX_STORAGE_BUCKET``).
        prefix: S3 prefix used by the indexer (e.g. ``"deep10m/"``). The
            registry key is ``{prefix.rstrip('/')}.index_list.json``,
            matching ``VecStreamClient.get_index_list_key()`` /
            ``load_index_list()`` and
            ``async_index_creation.update_index_list()``.
        s3_client: Optional boto3 S3 client. Defaults to
            ``boto3.client("s3")`` when ``None``. Pass a custom client
            to share an existing session or override the region.
        stability_polls: Number of consecutive polls whose total index
            count must match before the helper returns. ``1`` returns as
            soon as the count stops changing on the next observation.
        poll_interval: Seconds to sleep between polls.
        timeout: Maximum total seconds to wait before raising
            ``TimeoutError``.

    Returns:
        Summary dict with the final registry snapshot:

        * ``"registry"`` — the raw ``dict[str, list[str]]`` mapping each
          ``partition_{N}`` directory to its registered index keys (same
          shape as ``VecStreamClient.load_index_list()``).
        * ``"total_indexes"`` — total number of registered index entries
          across all partitions at return time.
        * ``"stable_for"`` — number of consecutive polls whose total
          matched at return time (>= ``stability_polls``).
        * ``"registry_key"`` — the S3 key the helper polled.

    Raises:
        TimeoutError: if the registry does not stabilize within
            ``timeout`` seconds.
        RuntimeError: if the registry object is malformed (not a JSON
            object, or any value is not a list).
        botocore.exceptions.ClientError: for any S3 error other than
            ``NoSuchKey`` (network errors, throttling, credentials, ...).

    Notes:
        A missing ``index_list.json`` is treated as zero registered
        indexes — the helper keeps polling rather than returning
        immediately, so a fresh deployment does not need a warm-up
        step before ``wait_for_indexes(...)`` is called.
    """
    if stability_polls < 1:
        raise ValueError("stability_polls must be >= 1")
    if poll_interval <= 0:
        raise ValueError("poll_interval must be > 0")
    if timeout <= 0:
        raise ValueError("timeout must be > 0")

    client = s3_client if s3_client is not None else boto3.client("s3")
    registry_key = f"{prefix.rstrip('/')}.index_list.json"

    deadline = time.time() + timeout
    last_count: int | None = None
    stable_runs = 0
    registry: dict[str, list[str]] = {}

    while True:
        try:
            response = client.get_object(Bucket=bucket, Key=registry_key)
            registry = json.loads(response["Body"].read().decode("utf-8"))
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code != "NoSuchKey":
                raise
            registry = {}

        if not isinstance(registry, dict):
            raise RuntimeError(
                f"Corrupt index registry at s3://{bucket}/{registry_key}: "
                "expected a JSON object"
            )

        total = 0
        for partition, keys in registry.items():
            if not isinstance(keys, list):
                raise RuntimeError(
                    f"Corrupt index registry at s3://{bucket}/{registry_key}: "
                    f"value at {partition!r} must be a list, "
                    f"got {type(keys).__name__}"
                )
            total += len(keys)

        if total == last_count:
            stable_runs += 1
        else:
            stable_runs = 0
        last_count = total

        if stable_runs >= stability_polls:
            return {
                "registry": registry,
                "total_indexes": total,
                "stable_for": stable_runs,
                "registry_key": registry_key,
            }

        if time.time() >= deadline:
            raise TimeoutError(
                f"Index registry s3://{bucket}/{registry_key} did not "
                f"stabilize within {timeout} seconds "
                f"(last total_indexes={total})"
            )

        time.sleep(poll_interval)