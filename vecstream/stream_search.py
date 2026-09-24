import io
import struct
import time
from typing import Iterator

import numpy as np
from confluent_kafka import OFFSET_INVALID, Consumer, TopicPartition


def get_end_offsets(
    bootstrap_servers: str,
    topic: str,
    partitions: list[int],
    group_id: str = "vecstream_search",
    client_id: str = "vecstream_search",
) -> tuple[dict[int, int], dict[str, float]]:
    """Query the end offsets for the given topic partitions.

    End offset is the next offset to be written, i.e. a partition with
    end_offset=0 is empty, and one with end_offset=N has messages at
    offsets 0..N-1.

    Args:
        bootstrap_servers: Kafka bootstrap servers string.
        topic: Topic name.
        partitions: List of partition IDs to query.
        group_id: Consumer group ID (reused for performance).
        client_id: Consumer client ID.
        timings: If provided, populated with timing data for each phase.

    Returns:
        Tuple of (end_offsets dict, timestamps dict).
    """
    timestamps: dict[str, float] = {}
    start = time.time()
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "client.id": client_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })

    # print(f"Querying end offsets for topic '{topic}' partitions: {partitions}")

    result: dict[int, int] = {}
    start_watermark_queries = time.time()
    for p in partitions:
        tp = TopicPartition(topic, p)
        low, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
        # print(f"Partition {p}: low_offset={low}, end_offset={high}")
        result[p] = high
    end_watermark_queries = time.time()
    consumer.close()
    end = time.time()

    timestamps = {
        "start_consumer_create": start,
        "end_consumer_create": start_watermark_queries,
        "start_watermark_queries": start_watermark_queries,
        "end_watermark_queries": end_watermark_queries,
        "consumer_close": end,
        "total": end - start,
    }

    return result, timestamps

def get_end_offsets_with_consumer(
    topic: str,
    partitions: list[int],
    consumer: Consumer,
) -> tuple[dict[int, int], dict[str, float]]:
    """Query the end offsets for the given topic partitions.

    End offset is the next offset to be written, i.e. a partition with
    end_offset=0 is empty, and one with end_offset=N has messages at
    offsets 0..N-1.

    Args:
        bootstrap_servers: Kafka bootstrap servers string.
        topic: Topic name.
        partitions: List of partition IDs to query.
        group_id: Consumer group ID (reused for performance).
        client_id: Consumer client ID.
        timings: If provided, populated with timing data for each phase.

    Returns:
        Tuple of (end_offsets dict, timestamps dict).
    """
    timestamps: dict[str, float] = {}
    start = time.time()


    # print(f"Querying end offsets for topic '{topic}' partitions: {partitions}")

    result: dict[int, int] = {}
    start_watermark_queries = time.time()
    for p in partitions:
        tp = TopicPartition(topic, p)
        low, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
        # print(f"Partition {p}: low_offset={low}, end_offset={high}")
        result[p] = high
    end_watermark_queries = time.time()
    end = time.time()

    timestamps = {
        "start_consumer_create": start,
        "end_consumer_create": start_watermark_queries,
        "start_watermark_queries": start_watermark_queries,
        "end_watermark_queries": end_watermark_queries,
        "consumer_close": end,
        "total": end - start,
    }

    return result, timestamps


def deserialize_message(data: bytes) -> tuple[np.ndarray, np.ndarray]:
    """Deserialize a wire_protocol message into (ids, vectors).

    Wire format: 8-byte little-endian uint64 length header,
    followed by two np.save arrays (ids, vectors).

    Args:
        data: Raw bytes from a Kafka message value.

    Returns:
        Tuple of (ids array, vectors array).
    """
    buf = io.BytesIO(data)
    length = struct.unpack("<Q", buf.read(8))[0]
    ids = np.load(buf)
    vectors = np.load(buf)
    assert len(ids) == length, f"Length header {length} != actual ids count {len(ids)}"
    return ids, vectors


def _compute_distances(vectors: np.ndarray, query: np.ndarray, metric: str) -> np.ndarray:
    """Compute distances between query (1-D) and each row of vectors.

    Args:
        vectors: Shape (N, D).
        query: Shape (D,).
        metric: 'euclidean' or 'cosine'.

    Returns:
        Shape (N,) distances array.
    """
    if metric == "euclidean":
        return np.linalg.norm(vectors - query, axis=1)
    elif metric == "cosine":
        q_norm = query / np.linalg.norm(query)
        v_norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        v_norms = np.where(v_norms == 0, 1, v_norms)
        v_normalized = vectors / v_norms
        return 1.0 - v_normalized @ q_norm
    else:
        raise ValueError(f"Unsupported metric: {metric}")

def kafka_search(
    query: np.ndarray,
    k: int,
    bootstrap_servers: str,
    topic: str,
    partitions: list[int],
    # end_offsets: dict[str, int],
    group_id: str = "vecstream_search",
    client_id: str = "vecstream_search",
    metric: str = "euclidean",
    commit_group_id: str = "vecstream_indexing",
) -> tuple[np.ndarray, np.ndarray, dict[str, float]]:
    """Search for the k closest vectors to a query in Kafka stream partitions.

    Only searches the partitions listed in ``partitions``. Entries in
    ``end_offsets`` for partitions not in ``partitions`` are ignored.

    Per partition, the start offset is computed as
    ``max(committed_offset, low_watermark)`` where ``committed_offset`` is
    the offset committed by ``commit_group_id`` (the indexing consumer
    group). When no committed offset exists (``OFFSET_INVALID``) or the
    committed-offset lookup fails, the partition's low watermark is used.
    This prunes already-indexed data: once the indexing Lambda commits an
    offset for a sealed segment, subsequent searches skip past it.

    The search Consumer is created with ``group.id = commit_group_id`` so
    that ``consumer.committed(...)`` can query the indexing group's
    committed offsets. The consumer uses ``assign()`` (not ``subscribe()``)
    and keeps ``enable.auto.commit: False``, so it never triggers group
    rebalancing and never writes committed offsets of its own.

    Reads from the computed start offset without committing offsets,
    deserializes vectors, computes distances, and returns the top-k
    results sorted by distance ascending.

    Args:
        query: Query vector of shape (D,).
        k: Number of nearest neighbors to return.
        bootstrap_servers: Kafka bootstrap servers string.
        topic: Kafka topic name.
        partitions: List of partition IDs to search. Only these partitions
            are read; others are ignored even if present in end_offsets.
        end_offsets: Dict mapping partition_id -> end_offset. Use
            get_end_offsets() to obtain this. The function stops reading
            a partition once it reaches its end_offset.
        group_id: Reserved for backward compatibility; the consumer is now
            bound to ``commit_group_id`` instead so its committed offsets
            can be read directly.
        client_id: Consumer client ID.
        metric: Distance metric, 'euclidean' or 'cosine'.
        commit_group_id: Consumer group whose committed offsets define the
            start position for each partition. Defaults to
            ``"vecstream_indexing"`` to match the indexing Lambda.

    Returns:
        Tuple of (ids, distances) as np.ndarrays sorted by distance ascending.
        If fewer than k vectors are found, returns all available.
        If the stream is empty, returns empty arrays.
    """
    all_ids: list[np.ndarray] = []
    all_distances: list[np.ndarray] = []
    timestamps: dict[str, float] = {}
    # print(f"Starting Kafka search for query vector with {partitions} partitions, end_offsets: {end_offsets}")
    search_partitions = set(partitions)
    # search_end_offsets = {p: end_offsets[str(p)] for p in partitions if str(p) in end_offsets}

    # print(f"search_partitions: {search_partitions}")
    # print(f"search_end_offsets: {search_end_offsets}", flush=True)

    timestamps['t_total_start'] = time.time()

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": commit_group_id,
        "client.id": client_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        # "fetch.message.max.bytes": 10485760,
        # "queued.max.messages.kbytes": 10240,
    })
    timestamps['consumer_created'] = time.time()

    search_end_offsets, search_end_offsets_times = get_end_offsets_with_consumer(topic, partitions, consumer)
    timestamps["search_end_offsets_times"] = search_end_offsets_times  # type: ignore

    # Per partition, compute the search start offset as
    # max(committed_offset, low_watermark); see kafka_search docstring.
    timestamps['committed_lookup_start'] = time.time()
    start_offsets: dict[int, int] = {}
    for p in partitions:
        tp = TopicPartition(topic, p)
        low, _ = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
        try:
            committed_tp = consumer.committed(tp, timeout=10)
            committed_offset = (
                committed_tp.offset if committed_tp is not None else OFFSET_INVALID
            )
        except Exception as e:
            print(
                f"committed-offset lookup failed for partition {p} "
                f"(group {commit_group_id}): {e}; "
                f"falling back to low watermark {low}"
            )
            committed_offset = OFFSET_INVALID
        if committed_offset == OFFSET_INVALID or committed_offset < 0:
            start_offsets[p] = low
        else:
            start_offsets[p] = max(committed_offset, low)
    timestamps['committed_lookup_end'] = time.time()


    tps = [TopicPartition(topic, p, start_offsets[p]) for p in partitions]
    consumer.assign(tps)
    timestamps['assign_seek'] = time.time()

    n_messages = 0

    try:
        done_partitions: set[int] = {p for p in search_end_offsets if search_end_offsets[p] == 0}
        # print(f"Starting Kafka search for query vector with {len(search_partitions)} partitions, end_offsets: {search_end_offsets}")


        while len(done_partitions) < len(search_end_offsets):
            msg = consumer.poll(timeout=10.0)

            if msg is None:
                remaining = sorted(search_partitions - done_partitions)
                # print(f"No records received. Partitions still reading: {remaining}")
                continue

            if msg.error():
                # print(f"Kafka error: {msg.error()}")
                continue

            p = msg.partition()
            if p not in search_partitions or p in done_partitions:
                # print(f"Received message from partition {p} which is not in search_partitions or already done. Ignoring.")
                continue

            try:
                # print(f"Received message from partition {p} at offset {msg.offset()}. Deserializing...")
                ids, vectors = deserialize_message(msg.value()) # type: ignore
                # print(f"Deserialized {len(ids)} vectors from partition {p} at offset {msg.offset()}.")
            except Exception as e:
                print(f"Deserialization error at partition {p} offset {msg.offset()}: {e}")
                continue

            distances = _compute_distances(vectors, query, metric)

            all_ids.append(ids)
            all_distances.append(distances)
            n_messages += 1

            if msg.offset() + 1 >= search_end_offsets[p]: # type: ignore
                done_partitions.add(p)
    finally:
        consumer.close()

    timestamps['topk_start'] = time.time()

    if not all_distances:     
        timestamps['t_total_end'] = time.time()
        timestamps['n_messages'] = n_messages   
        return np.array([], dtype=np.int64), np.array([], dtype=np.float32), timestamps

    concat_ids = np.concatenate(all_ids)
    concat_distances = np.concatenate(all_distances)

    if len(concat_distances) <= k:
        sorted_idx = np.argsort(concat_distances)
    else:
        sorted_idx = np.argpartition(concat_distances, k)[:k]
        sorted_idx = sorted_idx[np.argsort(concat_distances[sorted_idx])]

    timestamps['t_total_end'] = time.time()
    timestamps['n_messages'] = n_messages

    return concat_ids[sorted_idx], concat_distances[sorted_idx], timestamps