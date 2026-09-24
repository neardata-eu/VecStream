"""Asynchronous FAISS index creation from sealed Kafka log segments.

The Lambda is invoked when Kafka tiered storage uploads a sealed segment to
the source bucket. Segment keys look like
{topic}/{partition}/{base_offset}-{end_offset}.log. It downloads the
segment, decodes it into (ids, vectors), trains a FAISS index and uploads it
to the index bucket.

Integration with the query side (VecStreamClient / map_lambda):
- Indexes are written to {index_prefix}/partition_{partition}/index_{base_offset}.ann
  so the client's partition parser (key.split('/')[-2] == 'partition_{id}')
  and build_lambda_query_routing() find them, matching the layout produced by
  benchmarks/static_queries/prepare_vecstream_data.py + upload_to_s3.py.
- Record values are decoded in BOTH producer formats: the VecStream
  wire_protocol (8-byte LE uint64 + np.save ids + np.save vectors, used by
  benchmarks/ingestion/systems/vecstream.py and stream_to_kafka.py, parsed
  with vecstream.stream_search.deserialize_message) and the legacy
  streaming-kafka orjson arrays ([id, v1, ..., vd]).
- Offsets are committed only by this Lambda, after each segment's index has
  been uploaded. The commit value is the segment end_offset, i.e. the next
  offset to read, which matches Kafka's standard commit semantic.
- After each upload the {index_prefix}.index_list.json registry is updated
  with a conditional read-merge-write (ETag compare-and-swap) instead of
  being regenerated from an S3 listing, so concurrent invocations cannot
  overwrite each other's entries. Queries discover index state with a single
  GET of the registry; no S3 list operations are used on the hot path.
- IVF parameters mirror the static_queries simulations
  (build_ivf_index): nlist clamped to the vector count, nprobe clamped to
  nlist, cosine support via L2-normalized IndexFlatIP.

Configuration (env vars):
- INDEX_STORAGE_BUCKET: destination bucket. Empty (default) writes to the
  same bucket the segment arrived in, under the index prefix.
- INDEX_PREFIX: S3 prefix for partition_N/ directories. Empty (default)
  derives {topic}/indexes from the segment key, which never overlaps the
  {topic}/{partition}/*.log tiered-storage keys.
- INDEX_TYPE: sealed-segment index family. "IVFFlat" (default, exact prior
  behavior), "HNSW", or "IVFPQ". Paper §4: "the index type and parameters
  are user-configurable (e.g., quantization or HNSW)". Unknown values log a
  warning and fall back to IVFFlat so a typo never crashes indexing.
- INDEX_NLIST / INDEX_NPROBE: IVF parameters (defaults 150 / 15). Used by
  IVFFlat and IVFPQ; ignored by HNSW.
- INDEX_METRIC: euclidean (default) or cosine.
- INDEX_HNSW_M: HNSW neighborhood degree (default 32).
- INDEX_HNSW_EF_CONSTRUCTION: HNSW graph-build candidate list size (default 40).
- INDEX_HNSW_EF_SEARCH: HNSW query-time candidate list size (default 64).
- INDEX_PQ_M: IVFPQ sub-quantizer count (default 8). Requires d % M == 0;
  segments with d % M != 0 fall back to IVFFlat for that segment only.
- INDEX_PQ_BITS: bits per IVFPQ sub-quantizer (default 8).
- FAISS_NUM_THREADS: OpenMP threads for training (default 6).
- KAFKA_BOOTSTRAP_SERVERS: comma-separated list of Kafka brokers. Empty
  (default) disables offset commits, which keeps static benchmarks working
  without a Kafka cluster.
- KAFKA_COMMIT_GROUP_ID: consumer group id used for offset commits (default
  vecstream_indexing).
"""

import gzip
import json
import os
import random
import re
import struct
import time
import traceback
import urllib.parse
from collections.abc import Callable
from io import BytesIO
from typing import BinaryIO

import boto3
import faiss
import numpy as np
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, TopicPartition

from vecstream.stream_search import deserialize_message
from vecstream.vector_io import VectorIO

# Optional codec dependencies. The Lambda layer (deployment/package_layer.sh)
# does not ship all of them, so import what is available and fail with a clear
# error only if a segment actually needs a missing codec.
try:
    import orjson

    _loads = orjson.loads
except ImportError:
    _loads = json.loads

try:
    import lz4.frame as _lz4f
except ImportError:
    _lz4f = None

try:
    import snappy as _snappy
except ImportError:
    _snappy = None

try:
    import zstandard as _zstd
except ImportError:
    _zstd = None

_zstd_decompressor = _zstd.ZstdDecompressor() if _zstd is not None else None

# np.save magic string, used to detect wire_protocol record values.
_NUMPY_MAGIC = b"\x93NUMPY"

# Configuration. nlist/nprobe defaults mirror the static_queries simulations
# (prepare_vecstream_data.py, vecstream_local.py), not the legacy
# streaming-kafka values (512/32).
INDEX_NLIST = int(os.environ.get("INDEX_NLIST", "150"))
INDEX_NPROBE = int(os.environ.get("INDEX_NPROBE", "15"))
INDEX_METRIC = os.environ.get("INDEX_METRIC", "euclidean")
# Index family selection. Paper §4: "the index type and parameters are
# user-configurable (e.g., quantization or HNSW)". Default "IVFFlat"
# preserves the byte-for-byte behavior of the prior single-path implementation.
INDEX_TYPE = os.environ.get("INDEX_TYPE", "IVFFlat")
INDEX_HNSW_M = int(os.environ.get("INDEX_HNSW_M", "32"))
INDEX_HNSW_EF_CONSTRUCTION = int(os.environ.get("INDEX_HNSW_EF_CONSTRUCTION", "40"))
INDEX_HNSW_EF_SEARCH = int(os.environ.get("INDEX_HNSW_EF_SEARCH", "64"))
# d % INDEX_PQ_M == 0 is required at runtime; segments that violate it fall
# back to IVFFlat so indexing never crashes on a dimension mismatch.
INDEX_PQ_M = int(os.environ.get("INDEX_PQ_M", "8"))
INDEX_PQ_BITS = int(os.environ.get("INDEX_PQ_BITS", "8"))
# Empty means: write indexes to the bucket the segment arrived in.
INDEX_STORAGE_BUCKET = os.environ.get("INDEX_STORAGE_BUCKET", "")
# Empty means: derive "{topic}/indexes" from the segment key.
INDEX_PREFIX = os.environ.get("INDEX_PREFIX", "")
FAISS_NUM_THREADS = int(os.environ.get("FAISS_NUM_THREADS", "6"))
# Below this vector count a flat IndexIDMap is built instead of IVF, so tiny
# sealed tail segments get exact search (streaming-kafka generate_index()
# used the same threshold to fall back to IndexIDMap(IndexFlatL2)).
IVF_MIN_VECTORS = 512

# Kafka offset commit configuration. Empty bootstrap servers disables commits
# so the Lambda can still be exercised in static benchmarks without Kafka.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
KAFKA_COMMIT_GROUP_ID = os.environ.get("KAFKA_COMMIT_GROUP_ID", "vecstream_indexing")

s3 = boto3.client("s3")
vector_io = VectorIO()

_commit_consumer: Consumer | None = None


def _get_commit_consumer() -> Consumer:
    """Return a cached confluent-kafka Consumer for offset commits."""
    global _commit_consumer
    if _commit_consumer is None:
        _commit_consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_COMMIT_GROUP_ID,
            "enable.auto.commit": False,
            "session.timeout.ms": 10000,
            "request.timeout.ms": 5000,
        })
    return _commit_consumer


def commit_segment_offset(topic: str, partition: int, offset: int) -> None:
    """Commit the segment end offset to Kafka for the given partition.

    The commit value is the segment's end_offset, which represents the next
    offset to read and matches Kafka's standard commit semantic. The commit
    is performed without subscribe/poll; the broker accepts an OffsetCommit
    request with generation -1 from a consumer that has not joined a group.
    """
    consumer = _get_commit_consumer()
    consumer.commit(offsets=[TopicPartition(topic, partition, offset)], asynchronous=False)


def _missing_codec(package: str) -> Callable[[bytes], bytes]:
    def _raise(_payload: bytes) -> bytes:
        raise RuntimeError(
            f"Segment uses {package} compression but the {package} package is"
            f" not available in this runtime. Add it to the Lambda layer"
            f" (deployment/package_layer.sh)."
        )

    return _raise


COMPRESSION_CODECS: dict[int, tuple[str, Callable[[bytes], bytes]]] = {
    0: ("NONE", lambda payload: payload),
    1: ("GZIP", gzip.decompress),
    2: ("SNAPPY", _snappy.decompress if _snappy is not None else _missing_codec("snappy")),
    3: ("LZ4", _lz4f.decompress if _lz4f is not None else _missing_codec("lz4")),
    4: (
        "ZSTD",
        _zstd_decompressor.decompress
        if _zstd_decompressor is not None
        else _missing_codec("zstandard"),
    ),
}


def read_varint(buffer: BinaryIO) -> int:
    """Read a zigzag-encoded VarInt from the buffer."""
    result = 0
    shift = 0
    while True:
        byte = buffer.read(1)
        if not byte:
            raise EOFError("Unexpected end of buffer while reading VarInt")
        byte = byte[0]
        result |= (byte & 0x7F) << shift
        shift += 7
        if not (byte & 0x80):
            break
        if shift > 35:
            raise ValueError("VarInt too large")
    return (result >> 1) ^ -(result & 1)


def read_varlong(buffer: BinaryIO) -> int:
    """Read a zigzag-encoded VarLong from the buffer."""
    result = 0
    shift = 0
    while True:
        byte = buffer.read(1)
        if not byte:
            raise EOFError("Unexpected end of buffer while reading VarLong")
        byte = byte[0]
        result |= (byte & 0x7F) << shift
        shift += 7
        if not (byte & 0x80):
            break
        if shift > 70:
            raise ValueError("VarLong too large")
    return (result >> 1) ^ -(result & 1)


def parse_segment_key(object_key: str) -> tuple[str, int, int, int | None]:
    """Extract (topic, partition, base_offset, end_offset) from a .log key.

    Recognized layouts:
    - {topic}/{partition}/{base_offset}-{end_offset}.log (what the
      streaming-kafka consumer parsed, obj.key.split('/')[1] as partition)
    - .../partition_{N}/{base_offset}-{end_offset}.log (VecStream layout)
    - {topic}-{partition}/{base_offset}-{end_offset}.log (Aiven tiered
      storage plugin default)

    end_offset is parsed from the filename stem: when the stem has the form
    "{base}-{end}", end_offset is the integer after the dash; when the stem
    has no dash ("{base}.log"), end_offset is None.

    Raises:
        ValueError: if the partition or base offset cannot be extracted.
    """
    parts = [p for p in object_key.split("/") if p]
    if len(parts) < 2:
        raise ValueError(f"cannot extract topic/partition from key: {object_key}")

    filename = parts[-1]
    stem = filename.rsplit(".log", 1)[0]
    stem_parts = stem.split("-")
    base_offset = int(stem_parts[0])
    end_offset = int(stem_parts[1]) if len(stem_parts) > 1 else None

    for component in parts[:-1]:
        m = re.fullmatch(r"partition_(\d+)", component)
        if m:
            return parts[0], int(m.group(1)), base_offset, end_offset

    for component in parts[1:-1]:
        if component.isdigit():
            return parts[0], int(component), base_offset, end_offset

    for component in parts[:-1]:
        m = re.fullmatch(r"(.+)-(\d+)", component)
        if m:
            return m.group(1), int(m.group(2)), base_offset, end_offset

    raise ValueError(f"cannot extract partition from key: {object_key}")


def parse_logfile_encoded(f) -> tuple[list[int], list[list[float]]]:
    """Parse a sealed Kafka log segment stream into (ids, vectors).

    Cherry-picked from streaming-kafka/streaming/decoder.py, extended to
    decode both record value formats used by VecStream producers. Parses the
    Kafka record-batch v2 binary format: an 8-byte base offset, a 4-byte
    batch length, a 61-byte header whose attributes field carries a 3-bit
    compression codec, then optionally compressed records.

    Record values are either:
    - wire_protocol messages (8-byte LE uint64 count + np.save ids +
      np.save vectors), parsed with vecstream.stream_search.deserialize_message
    - legacy orjson arrays [id, v1, ..., vd] (streaming-kafka producer)

    Raises:
        EOFError, ValueError, or the underlying decompression exception on
        corrupt or truncated segments. This function never returns partial
        results; any parse error propagates so the caller cannot index an
        incomplete segment or commit its offset.
    """
    file_data = f.read()
    f = BytesIO(file_data)
    ids: list[int] = []
    vectors: list[list[float]] = []

    while True:
        try:
            batch_start_pos = f.tell()
            # Read batch header
            if not (data := f.read(8)):
                print("END OF FILE")
                break  # End of file
            base_offset = struct.unpack(">q", data)[0]

            batch_length = struct.unpack(">i", f.read(4))[0]
            f.read(9)
            compression_bits = struct.unpack(">h", f.read(2))[0] & 0x07
            codec_name, decompress_fn = COMPRESSION_CODECS.get(
                compression_bits, ("UNKNOWN", lambda b: b)
            )
            f.read(34)
            record_count = struct.unpack(">i", f.read(4))[0]

            payload = f.read(batch_length - (f.tell() - batch_start_pos - 12))
            if compression_bits != 0:
                payload = decompress_fn(payload)

            records = BytesIO(payload)

            # Read records
            for _ in range(record_count):
                length = read_varint(records)
                attrs = records.read(1)[0]
                ts_delta = read_varlong(records)
                off_delta = read_varint(records)
                key_len = read_varint(records)
                key = records.read(key_len) if key_len >= 0 else None
                val_len = read_varint(records)
                value = records.read(val_len) if val_len >= 0 else None

                if value is None:
                    raise ValueError("record has null value")
                if value[8:14] == _NUMPY_MAGIC:
                    # VecStream wire_protocol: uint64 count + np.save ids + vectors.
                    # Check the npy magic first; a JSON text can never start with 0x93.
                    chunk_ids, chunk_vectors = deserialize_message(value)
                    ids.extend(np.asarray(chunk_ids, dtype=np.int64).tolist())
                    vectors.extend(
                        np.asarray(chunk_vectors, dtype=np.float32).tolist()
                    )
                elif value[:1] == b"[":
                    # Legacy streaming-kafka format: [id, v1, ..., vd]
                    array = _loads(value)
                    ids.append(int(array[0]))
                    vectors.append(list(map(float, array[1:])))
                else:
                    raise ValueError(
                        f"unrecognized record value format ({len(value)} bytes)"
                    )

                # Skip headers
                _ = read_varint(records)
                for _ in range(_):
                    key_len = read_varint(records)
                    records.read(key_len)
                    val_len = read_varint(records)
                    if val_len >= 0:
                        records.read(val_len)
            f.seek(batch_start_pos + 12 + batch_length)

        except EOFError:
            print("Reached end of file")
            raise
        except Exception as e:
            print(f"Error parsing log file: {e}")
            raise
    return ids, vectors


def update_index_list(
    bucket: str, prefix: str, partition: int, index_key: str
) -> dict:
    """Conditionally update {prefix}.index_list.json with the new index key.

    Registry format: dict[str, list[str]] mapping partition directory name
    (partition_{N}) to a list of full S3 index keys. The registry is read
    with a GET, merged in memory, and written back with an ETag
    compare-and-swap (IfMatch). On conflict, the operation is retried with
    bounded backoff. No S3 list operations are used.

    Idempotency: if index_key is already present in the registry, the write
    is skipped and updated=False is returned.
    """
    registry_key = f"{prefix.rstrip('/')}.index_list.json"
    partition_dir = f"partition_{partition}"
    max_attempts = 5

    for attempt in range(max_attempts):
        etag: str | None = None
        groups: dict[str, list[str]] = {}
        try:
            response = s3.get_object(Bucket=bucket, Key=registry_key)
            etag = response["ETag"]
            groups = json.loads(response["Body"].read().decode("utf-8"))
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "NoSuchKey":
                groups = {}
                try:
                    create_response = s3.put_object(
                        Bucket=bucket,
                        Key=registry_key,
                        Body=json.dumps(groups).encode("utf-8"),
                        IfNoneMatch="*",
                    )
                    etag = create_response["ETag"]
                except ClientError as create_err:
                    create_code = create_err.response.get("Error", {}).get("Code", "")
                    if create_code in ("PreconditionFailed", "ConditionalRequestConflict"):
                        time.sleep(min(random.uniform(0.05, 0.2) * (2 ** attempt), 2.0))
                        continue
                    raise
            else:
                raise
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise RuntimeError(
                f"Corrupt index registry at s3://{bucket}/{registry_key}: {e}"
            )

        if not isinstance(groups, dict):
            raise RuntimeError(
                f"Corrupt index registry at s3://{bucket}/{registry_key}: "
                "expected a JSON object"
            )

        partition_keys = groups.setdefault(partition_dir, [])
        if index_key in partition_keys:
            return {"key": registry_key, "updated": False, "attempts": attempt + 1}

        partition_keys.append(index_key)
        body = json.dumps(groups).encode("utf-8")

        try:
            s3.put_object(
                Bucket=bucket,
                Key=registry_key,
                Body=body,
                IfMatch=etag,
            )
            return {"key": registry_key, "updated": True, "attempts": attempt + 1}
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("PreconditionFailed", "ConditionalRequestConflict"):
                time.sleep(min(random.uniform(0.05, 0.2) * (2 ** attempt), 2.0))
                continue
            raise

    raise RuntimeError(
        f"Failed to update index registry s3://{bucket}/{registry_key} "
        f"after {max_attempts} attempts"
    )


def _build_ivfflat_index(
    vectors: np.ndarray,
    ids: np.ndarray,
    d: int,
    nlist: int,
    nprobe: int,
    metric: str,
) -> tuple[faiss.Index, str]:
    """Build the legacy IndexIVFFlat index (byte-identical to the prior path).

    Preserves the pre-2024 single-path behavior: nlist clamped to the vector
    count, nprobe clamped to nlist, cosine via L2-normalized IndexFlatIP +
    METRIC_INNER_PRODUCT. The caller has already L2-normalized vectors in the
    cosine path.
    """
    effective_nlist = min(nlist, len(vectors))
    if metric == "cosine":
        quantizer = faiss.IndexFlatIP(d)
        index = faiss.IndexIVFFlat(
            quantizer, d, effective_nlist, faiss.METRIC_INNER_PRODUCT
        )
    else:
        quantizer = faiss.IndexFlatL2(d)
        index = faiss.IndexIVFFlat(quantizer, d, effective_nlist, faiss.METRIC_L2)
    index.nprobe = min(effective_nlist, nprobe)
    index.train(vectors)
    index.add_with_ids(vectors, ids)
    return index, f"IndexIVFFlat(nlist={effective_nlist}, metric={metric})"


def _build_hnsw_index(
    vectors: np.ndarray,
    ids: np.ndarray,
    d: int,
    metric: str,
) -> tuple[faiss.Index, str]:
    """Build a graph-based IndexHNSWFlat wrapped in IndexIDMap.

    IndexHNSWFlat does not implement add_with_ids, so the IDMap wrapper
    translates add() calls and lets the caller keep using add_with_ids with
    its int64 Kafka offsets. efConstruction is set before adding so the
    graph-build honors it; efSearch takes effect at search time.
    """
    faiss_metric = faiss.METRIC_INNER_PRODUCT if metric == "cosine" else faiss.METRIC_L2
    hnsw = faiss.IndexHNSWFlat(d, INDEX_HNSW_M, faiss_metric)
    hnsw.hnsw.efConstruction = INDEX_HNSW_EF_CONSTRUCTION
    hnsw.hnsw.efSearch = INDEX_HNSW_EF_SEARCH
    index = faiss.IndexIDMap(hnsw)
    index.add_with_ids(vectors, ids)
    return (
        index,
        (
            f"IndexIDMap(IndexHNSWFlat(M={INDEX_HNSW_M},"
            f" efConstruction={INDEX_HNSW_EF_CONSTRUCTION},"
            f" efSearch={INDEX_HNSW_EF_SEARCH}, metric={metric}))"
        ),
    )


def _build_ivfpq_index(
    vectors: np.ndarray,
    ids: np.ndarray,
    d: int,
    nlist: int,
    nprobe: int,
    metric: str,
) -> tuple[faiss.Index, str]:
    """Build a product-quantized IndexIVFPQ index, falling back to IVFFlat on guard violations.

    Two preconditions are checked before PQ construction; if either fails the
    segment is indexed with the legacy IVFFlat path so indexing never crashes
    on a tiny tail segment or a dimension that is not divisible by INDEX_PQ_M.
    """
    if len(vectors) < 1000 or d % INDEX_PQ_M != 0:
        print(
            f"IVFPQ preconditions not met (n={len(vectors)}, d={d},"
            f" m={INDEX_PQ_M}); falling back to IVFFlat for this segment",
            flush=True,
        )
        return _build_ivfflat_index(vectors, ids, d, nlist, nprobe, metric)
    effective_nlist = min(nlist, len(vectors))
    faiss_metric = faiss.METRIC_INNER_PRODUCT if metric == "cosine" else faiss.METRIC_L2
    quantizer = faiss.IndexFlatIP(d) if metric == "cosine" else faiss.IndexFlatL2(d)
    index = faiss.IndexIVFPQ(
        quantizer, d, effective_nlist, INDEX_PQ_M, INDEX_PQ_BITS, faiss_metric
    )
    index.nprobe = min(effective_nlist, nprobe)
    index.train(vectors)
    index.add_with_ids(vectors, ids)
    return (
        index,
        (
            f"IndexIVFPQ(nlist={effective_nlist}, m={INDEX_PQ_M},"
            f" bits={INDEX_PQ_BITS}, metric={metric})"
        ),
    )


def create_index_from_log_segment(
    bucket: str,
    object_key: str,
    storage_bucket: str | None = None,
    index_prefix: str | None = None,
    nlist: int = INDEX_NLIST,
    nprobe: int = INDEX_NPROBE,
    metric: str = INDEX_METRIC,
) -> dict:
    """Download a sealed .log segment, train a FAISS index on it, upload it to S3.

    Ported from streaming-kafka/vectordb/indexing.py::generate_index() and
    aligned with the static_queries simulations (build_ivf_index): nlist
    clamped to the vector count, nprobe clamped to nlist, cosine support,
    int64 ids. The Lithops Storage calls are replaced with boto3 (download)
    and VectorIO (upload, same serialization faiss.write_index produces, so
    map_lambda's VectorIO.load_index_from_s3 can read the result).

    The index lands at {index_prefix}/partition_{partition}/index_{base_offset}.ann.
    The {index_prefix}.index_list.json registry is then updated with a
    conditional read-merge-write (no S3 list operations), and finally the
    segment end offset is committed to Kafka.

    Args:
        bucket: Source bucket holding the sealed segment.
        object_key: Segment key, {topic}/{partition}/{base_offset}-{end_offset}.log.
        storage_bucket: Destination bucket. Defaults to INDEX_STORAGE_BUCKET,
            or the source bucket when unset/empty.
        index_prefix: S3 prefix for partition_N/ directories. Defaults to
            INDEX_PREFIX, or "{topic}/indexes" when unset/empty.
        nlist: Number of IVF centroids (clamped to the vector count).
        nprobe: nprobe stored in the serialized IVF index (clamped to nlist).
        metric: "euclidean" or "cosine".

    Returns:
        Result dict with the index key, registry status, Kafka commit status,
        vector count and per-stage timings.
    """
    start = time.time()
    if metric not in ("euclidean", "cosine"):
        raise ValueError(f"unsupported metric: {metric}")
    faiss.omp_set_num_threads(FAISS_NUM_THREADS)

    topic, partition, base_offset, end_offset = parse_segment_key(object_key)
    storage_bucket = storage_bucket or INDEX_STORAGE_BUCKET or bucket
    index_prefix = (index_prefix or INDEX_PREFIX or f"{topic}/indexes").rstrip("/")

    timestamps: dict[str, float] = {}

    # Download and decode the segment
    s = time.time()
    segment = s3.get_object(Bucket=bucket, Key=object_key)["Body"]
    ids, vectors = parse_logfile_encoded(segment)
    timestamps["parse_time"] = time.time() - s
    print(
        f"Parsed {len(vectors)} vectors from s3://{bucket}/{object_key}"
        f" in {timestamps['parse_time']} seconds",
        flush=True,
    )

    if not vectors:
        timestamps["total_time"] = time.time() - start
        return {
            "bucket": bucket,
            "key": object_key,
            "status": "skipped_empty",
            "reason": "segment contained no parsable records",
            "topic": topic,
            "partition": partition,
            "base_offset": base_offset,
            "end_offset": end_offset,
            "index_bucket": storage_bucket,
            "index_key": None,
            "index_list": None,
            "committed_offset": None,
            "offset_commit": "skipped_empty_segment",
            "num_vectors": 0,
            "features": None,
            "index_type": None,
            "metric": metric,
            "timestamps": timestamps,
        }

    features = len(vectors[0])
    vectors_arr = np.asarray(vectors, dtype=np.float32)
    ids_arr = np.asarray(ids, dtype=np.int64)
    if metric == "cosine":
        faiss.normalize_L2(vectors_arr)

    # Train the index. Dispatch on INDEX_TYPE (paper §4: user-configurable).
    s = time.time()
    if len(vectors) > IVF_MIN_VECTORS:
        if INDEX_TYPE == "HNSW":
            index, index_type = _build_hnsw_index(
                vectors_arr, ids_arr, features, metric
            )
        elif INDEX_TYPE == "IVFPQ":
            index, index_type = _build_ivfpq_index(
                vectors_arr, ids_arr, features, nlist, nprobe, metric
            )
        else:
            if INDEX_TYPE != "IVFFlat":
                print(
                    f"Unknown INDEX_TYPE={INDEX_TYPE!r}; falling back to IVFFlat",
                    flush=True,
                )
            index, index_type = _build_ivfflat_index(
                vectors_arr, ids_arr, features, nlist, nprobe, metric
            )
    else:
        if metric == "cosine":
            index = faiss.IndexIDMap(faiss.IndexFlatIP(features))
        else:
            index = faiss.IndexIDMap(faiss.IndexFlatL2(features))
        index.add_with_ids(vectors_arr, ids_arr)
        index_type = f"IndexIDMap(IndexFlat{'IP' if metric == 'cosine' else 'L2'})"
    timestamps["index_time"] = time.time() - s
    print(f"Indexing time: {timestamps['index_time']} seconds", flush=True)

    # Upload the index where the query side expects it:
    # {index_prefix}/partition_{partition}/index_{base_offset}.ann
    s = time.time()
    index_key = f"{index_prefix}/partition_{partition}/index_{base_offset}.ann"
    vector_io.write_index_to_object_putobject(storage_bucket, index_key, index)
    timestamps["upload_time"] = time.time() - s
    print(f"Upload time: {timestamps['upload_time']} seconds", flush=True)

    # Update the index registry with a conditional read-merge-write.
    s = time.time()
    index_list_status = update_index_list(storage_bucket, index_prefix, partition, index_key)
    timestamps["index_list_time"] = time.time() - s
    print(
        f"Index list update time: {timestamps['index_list_time']} seconds"
        f" (updated={index_list_status['updated']}, attempts={index_list_status['attempts']})",
        flush=True,
    )

    # Commit the segment end offset to Kafka after the index is registered.
    s = time.time()
    committed_offset: int | None = None
    offset_commit_status: str
    if end_offset is None:
        offset_commit_status = "skipped_no_end_offset"
        print(
            f"Skipping Kafka offset commit for {topic}/{partition}:"
            " segment key has no end_offset",
            flush=True,
        )
    elif not KAFKA_BOOTSTRAP_SERVERS:
        offset_commit_status = "skipped_disabled"
        print(
            f"Skipping Kafka offset commit for {topic}/{partition}:"
            " KAFKA_BOOTSTRAP_SERVERS not set",
            flush=True,
        )
    else:
        commit_segment_offset(topic, partition, end_offset)
        committed_offset = end_offset
        offset_commit_status = "committed"
    timestamps["commit_time"] = time.time() - s
    print(f"Offset commit time: {timestamps['commit_time']} seconds", flush=True)

    timestamps["total_time"] = time.time() - start

    return {
        "bucket": bucket,
        "key": object_key,
        "status": "indexed",
        "topic": topic,
        "partition": partition,
        "base_offset": base_offset,
        "end_offset": end_offset,
        "index_bucket": storage_bucket,
        "index_key": index_key,
        "index_list": index_list_status,
        "committed_offset": committed_offset,
        "offset_commit": offset_commit_status,
        "num_vectors": len(vectors),
        "features": features,
        "index_type": index_type,
        "metric": metric,
        "timestamps": timestamps,
    }


def event_handler(event, context):
    """Lambda entry point, deploy as vecstream.async_index_creation.event_handler.

    Triggered by S3 ObjectCreated notifications emitted when Kafka tiered
    storage uploads a sealed segment to the source bucket. Skips the sidecar
    files tiered storage also uploads (.index, .timeindex, ...), the same way
    consumer.py::keys_filter did in the polling pipeline.
    """
    start_lambda = time.time()
    try:
        records = event.get("Records", [])
        if not records:
            # S3 sends a service test event when the notification is first
            # configured; it has no Records list.
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "No S3 records to process (test event?).",
                        "timestamps": {
                            "start_index_lambda": start_lambda,
                            "end_index_lambda": time.time(),
                        },
                    }
                ),
            }

        results = []
        for record in records:
            if record.get("eventSource", "") != "aws:s3":
                continue
            if not record.get("eventName", "").startswith("ObjectCreated"):
                continue

            bucket = record["s3"]["bucket"]["name"]
            object_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

            if not object_key.endswith(".log"):
                results.append(
                    {
                        "bucket": bucket,
                        "key": object_key,
                        "status": "skipped",
                        "reason": "not a .log segment",
                    }
                )
                continue

            try:
                results.append(create_index_from_log_segment(bucket, object_key))
            except Exception as e:
                print(f"Error indexing s3://{bucket}/{object_key}: {e}", flush=True)
                print(traceback.format_exc(), flush=True)
                results.append(
                    {
                        "bucket": bucket,
                        "key": object_key,
                        "status": "error",
                        "error": str(e),
                        "trace": traceback.format_exc(),
                        "timestamps": {},
                    }
                )

        failed = [r for r in results if r.get("status") == "error"]
        body = {
            "message": f"Processed {len(results)} record(s), {len(failed)} failed.",
            "results": results,
            "timestamps": {
                "start_index_lambda": start_lambda,
                "end_index_lambda": time.time(),
            },
        }
        return {
            "statusCode": 500 if failed else 200,
            "body": json.dumps(body),
        }

    except Exception as e:
        print(f"Error processing request: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "trace": traceback.format_exc()}),
        }
