
from abc import ABC
from typing import List

import numpy as np


class Routing(ABC):
    """Abstract base class for routing algorithms. 
    Routing algorithms determine the stream partition to write the vector to. 
    It also determines the best stream partitions to search for a given query vector.
    """

    def __init__(self, n_partitions, d):
        self.n_partitions = n_partitions
        self.d = d

    def get_partition(self, vector: np.ndarray) -> int:
        """Get the partition to write the vector to."""
        raise NotImplementedError("get_partition method not implemented")

    def get_ingest_partitions(self, vector: np.ndarray) -> List[int]:
        """Get the partition(s) to write the vector to during ingestion.

        Most routing algorithms write each vector to a single partition, so
        the default returns [get_partition(vector)]. Multi-table routing
        (e.g., MHT) overrides this to write one replica per table.
        """
        return [self.get_partition(vector)]

    def get_search_partitions(self, query_vector: np.ndarray, nprobe: int) -> List[int]:
        """Get the best partitions to search for a given query vector."""
        raise NotImplementedError("get_search_partitions method not implemented")

    def get_partitions_batch(self, vectors: np.ndarray) -> np.ndarray:
        """Get the partition for each vector in a batch."""
        raise NotImplementedError("get_partitions_batch method not implemented")

    def get_ingest_partitions_batch(self, vectors: np.ndarray) -> List[List[int]]:
        """Get the partition(s) to write each vector of a batch to.

        Default: one partition per vector, via get_partitions_batch.
        """
        return [[int(p)] for p in self.get_partitions_batch(vectors)]

    def get_search_partitions_batch(self, query_vectors: np.ndarray, nprobe: int) -> List[List[int]]:
        """Get the best partitions to search for each query vector in a batch."""
        raise NotImplementedError("get_search_partitions_batch method not implemented")



class IVFRouting(Routing):
    """IVF-based routing algorithm.
    Uses k-means clustering to determine the partition for a given vector.
    """

    def __init__(self, n_partitions: int, d: int, centroids: np.ndarray, metric: str = "euclidean"):
        super().__init__(n_partitions, d)
        if metric not in ("euclidean", "cosine"):
            raise ValueError(f"Unsupported metric: {metric}")
        self.metric = metric
        if metric == "cosine":
            norms = np.linalg.norm(centroids, axis=1, keepdims=True)
            self.centroids = (centroids / np.maximum(norms, 1e-10)).astype(centroids.dtype, copy=False)
            self._centroid_norms_sq = np.ones(self.centroids.shape[0], dtype=self.centroids.dtype)
        else:
            self.centroids = np.ascontiguousarray(centroids)
            self._centroid_norms_sq = np.einsum("ij,ij->i", self.centroids, self.centroids)

    def _sq_distances(self, vector: np.ndarray) -> np.ndarray:
        """Squared L2 distances ||c - v||^2 = ||c||^2 - 2 c·v + ||v||^2, no sqrt, no broadcast."""
        dots = self.centroids @ vector
        return self._centroid_norms_sq - 2.0 * dots + float(np.dot(vector, vector))

    def _cosine_similarities(self, vector: np.ndarray) -> np.ndarray:
        """Centroids are pre-normalized; normalize the query once and take a dot product."""
        q = vector / (float(np.linalg.norm(vector)) + 1e-10)
        return self.centroids @ q

    def get_partition(self, vector: np.ndarray) -> int:
        if self.metric == "euclidean":
            return int(np.argmin(self._sq_distances(vector)))
        return int(np.argmax(self._cosine_similarities(vector)))

    def get_search_partitions(self, query_vector: np.ndarray, nprobe: int) -> List[int]:
        if nprobe >= self.n_partitions:
            return list(range(self.n_partitions))
        if self.metric == "euclidean":
            dists = self._sq_distances(query_vector)
            return np.argpartition(dists, nprobe)[:nprobe].tolist()
        sims = self._cosine_similarities(query_vector)
        return np.argpartition(sims, -nprobe)[-nprobe:].tolist()

    def get_partitions_batch(self, vectors: np.ndarray) -> np.ndarray:
        """Assign each row of `vectors` (shape (n, d)) to its nearest centroid.
        Returns an int array of shape (n,).
        """
        if vectors.ndim != 2 or vectors.shape[1] != self.d:
            raise ValueError(
                f"vectors must have shape (n, {self.d}), got {vectors.shape}"
            )
        if self.metric == "euclidean":
            dots = self.centroids @ vectors.T
            sq_norms = np.einsum("ij,ij->i", vectors, vectors)
            dists = self._centroid_norms_sq[:, None] - 2.0 * dots + sq_norms[None, :]
            return np.argmin(dists, axis=0).astype(np.int64, copy=False)
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        q = vectors / np.maximum(norms, 1e-10)
        return np.argmax(self.centroids @ q.T, axis=0).astype(np.int64, copy=False)

    def get_search_partitions_batch(self, query_vectors: np.ndarray, nprobe: int) -> List[List[int]]:
        """For each query row, return the indices of the nprobe nearest centroids."""
        if query_vectors.ndim != 2 or query_vectors.shape[1] != self.d:
            raise ValueError(
                f"query_vectors must have shape (n, {self.d}), got {query_vectors.shape}"
            )
        n = query_vectors.shape[0]
        if nprobe >= self.n_partitions:
            full = np.tile(np.arange(self.n_partitions), (n, 1))
            return full.tolist()
        if self.metric == "euclidean":
            dots = self.centroids @ query_vectors.T
            sq_norms = np.einsum("ij,ij->i", query_vectors, query_vectors)
            dists = self._centroid_norms_sq[:, None] - 2.0 * dots + sq_norms[None, :]
            return np.argpartition(dists, nprobe, axis=0)[:nprobe].T.tolist()
        norms = np.linalg.norm(query_vectors, axis=1, keepdims=True)
        q = query_vectors / np.maximum(norms, 1e-10)
        sims = self.centroids @ q.T
        return np.argpartition(sims, -nprobe, axis=0)[-nprobe:].T.tolist()


class LSHRouting(Routing):
    """LSH-based routing (paper §3.3, hash-based family).

    Hyperplane random-projection locality-sensitive hashing. Each vector is
    mapped to an ``n_bits`` hash code by thresholding the sign of ``n_bits``
    inner products against a seeded projection matrix; the bucket-to-partition
    map ``m`` is identity when ``2 ** n_bits == n_partitions`` (bijective) and
    ``code -> code % n_partitions`` otherwise. The same input vector always
    maps to the same partition and the same ``get_search_partitions``
    ranking, so the ``Routing`` ABC contract is preserved.

    Parameters
    ----------
    n_partitions:
        Number of stream partitions (the codomain of ``m``).
    d:
        Embedding dimensionality.
    n_bits:
        Number of hash bits. The number of distinct buckets is
        ``2 ** n_bits``.
    seed:
        Seed for the projection matrix; default 42 matches the experiment
        code in ``plots/design/routing/utils/hashing.py::Hasher``.
    metric:
        ``"euclidean"`` (default) hashes raw vectors; ``"cosine"``
        L2-normalizes the vector before projecting so that
        direction-equivalent inputs collide.

    Bit packing convention (matches ``Hasher.hash`` in
    ``plots/design/routing/utils/hashing.py``):

        bit ``i`` of the hash code is 1 iff
        ``projection_i = (P_i . v) >= 0``, where ``P_i`` is the
        i-th column of the projection matrix. The integer code is
        ``sum(bit_i << i)`` (LSB corresponds to projection 0).
    """

    _MAX_N_BITS = 32

    def __init__(
        self,
        n_partitions: int,
        d: int,
        n_bits: int,
        seed: int = 42,
        metric: str = "euclidean",
    ) -> None:
        super().__init__(n_partitions, d)
        if not (isinstance(n_bits, int) and 0 < n_bits <= self._MAX_N_BITS):
            raise ValueError(
                f"n_bits must satisfy 0 < n_bits <= {self._MAX_N_BITS}, got {n_bits}"
            )
        if metric not in ("euclidean", "cosine"):
            raise ValueError(f"Unsupported metric: {metric}")
        self.n_bits = n_bits
        self.metric = metric
        self.seed = seed

        n_buckets = 1 << n_bits
        self.n_buckets = n_buckets

        rng = np.random.default_rng(seed)
        # Projection matrix: shape (d, n_bits). Each column is a hyperplane.
        self.P = rng.standard_normal((d, n_bits)).astype(np.float64, copy=False)

        # Bucket-to-partition map m (identity when bijective, else mod).
        if n_buckets == n_partitions:
            self._bucket_to_partition = np.arange(n_buckets, dtype=np.int64)
        else:
            self._bucket_to_partition = (
                np.arange(n_buckets, dtype=np.int64) % n_partitions
            )

        # Precomputed state for fast Hamming ranking.
        self._all_buckets = np.arange(n_buckets, dtype=np.int64)
        self._bit_weights = (
            np.int64(1) << np.arange(n_bits, dtype=np.int64)
        )  # weight for bit i

    def _normalize(self, vector: np.ndarray) -> np.ndarray:
        """L2-normalize when the metric requires it (cosine)."""
        if self.metric == "cosine":
            return vector / (float(np.linalg.norm(vector)) + 1e-10)
        return vector

    def _hash_code(self, vector: np.ndarray) -> int:
        """Hash a single vector to its integer bucket code (0 .. 2**n_bits - 1)."""
        v = self._normalize(vector)
        proj = v @ self.P  # (n_bits,)
        bits = (proj >= 0).astype(np.int64)
        return int(bits @ self._bit_weights)

    def _hash_codes_batch(self, vectors: np.ndarray) -> np.ndarray:
        """Hash a (n, d) batch. Returns shape (n,) int64."""
        if self.metric == "cosine":
            norms = np.linalg.norm(vectors, axis=1, keepdims=True)
            v = vectors / np.maximum(norms, 1e-10)
        else:
            v = vectors
        proj = v @ self.P  # (n, n_bits)
        bits = (proj >= 0).astype(np.int64)
        return bits @ self._bit_weights  # (n,)

    def _partition_min_distances(self, qcode: int) -> np.ndarray:
        """Per-partition min Hamming distance from ``qcode`` to any mapped bucket."""
        xors = np.int64(qcode) ^ self._all_buckets
        hd = np.bitwise_count(xors)
        out = np.full(
            self.n_partitions, np.int64(self.n_bits + 1), dtype=np.int64
        )
        # Scatter-min per partition: each partition keeps the smallest hd
        # among the buckets mapped to it. np.minimum.at is well-defined for
        # duplicate indices (the min is associative).
        np.minimum.at(out, self._bucket_to_partition, hd)
        return out

    def get_partition(self, vector: np.ndarray) -> int:
        code = self._hash_code(vector)
        return int(self._bucket_to_partition[code])

    def get_partitions_batch(self, vectors: np.ndarray) -> np.ndarray:
        if vectors.ndim != 2 or vectors.shape[1] != self.d:
            raise ValueError(
                f"vectors must have shape (n, {self.d}), got {vectors.shape}"
            )
        codes = self._hash_codes_batch(vectors)
        return self._bucket_to_partition[codes].astype(np.int64, copy=False)

    def get_search_partitions(
        self, query_vector: np.ndarray, nprobe: int
    ) -> List[int]:
        """Rank ALL partitions by min Hamming distance; return the nprobe best.

        Deterministic tie-break by partition id.
        """
        nprobe = min(nprobe, self.n_partitions)
        if nprobe <= 0:
            return []
        qcode = self._hash_code(query_vector)
        part_dists = self._partition_min_distances(qcode)
        part_ids = np.arange(self.n_partitions)
        # Primary key: distance; secondary key: partition id.
        order = np.lexsort((part_ids, part_dists))[:nprobe]
        return order.tolist()

    def get_search_partitions_batch(
        self, query_vectors: np.ndarray, nprobe: int
    ) -> List[List[int]]:
        if query_vectors.ndim != 2 or query_vectors.shape[1] != self.d:
            raise ValueError(
                f"query_vectors must have shape (n, {self.d}), got {query_vectors.shape}"
            )
        n = query_vectors.shape[0]
        if n == 0:
            return []
        nprobe = min(nprobe, self.n_partitions)
        if nprobe <= 0:
            return [[] for _ in range(n)]
        codes_q = self._hash_codes_batch(query_vectors)  # (n,)
        xors = codes_q[:, None] ^ self._all_buckets[None, :]  # (n, B)
        hd = np.bitwise_count(xors)  # (n, B) int64
        # Per-row scatter-min via np.minimum.at (loop over batch rows is
        # acceptable; B = 2**n_bits is the inner numpy dimension).
        part_dists = np.full(
            (n, self.n_partitions), np.int64(self.n_bits + 1), dtype=np.int64
        )
        for i in range(n):
            np.minimum.at(part_dists[i], self._bucket_to_partition, hd[i])
        part_ids = np.arange(self.n_partitions)
        result: List[List[int]] = []
        for i in range(n):
            order = np.lexsort((part_ids, part_dists[i]))[:nprobe]
            result.append(order.tolist())
        return result


class MHTRouting(Routing):
    """Multi-Hash Table routing (paper §3.3, multi-hash tables family).

    ``n_tables`` independent LSH tables share a single bucket-to-partition map
    ``m`` (LSHRouting's mapping). Each vector is hashed once per table;
    during ingestion it is replicated to the deduplicated partitions of every
    table. During search, the ``nprobe`` budget is split across tables: each
    table contributes ``ceil(nprobe / n_tables)`` partitions ranked by
    minimum Hamming distance from the query's table-specific hash code; the
    union keeps the minimum distance per partition; if the union is smaller
    than ``nprobe``, the result is topped up by iterating across tables in
    table-id order. The final ordering is ``(min distance across tables,
    partition id)``.

    The single-partition API (``get_partition``, ``get_partitions_batch``)
    uses table 0's assignment; the multi-replica API
    (``get_ingest_partitions``, ``get_ingest_partitions_batch``) is the
    canonical ingestion interface for MHT.

    Parameters
    ----------
    n_partitions, d, n_bits, seed, metric:
        Same semantics as ``LSHRouting``. The seeded RNG produces
        ``n_tables`` independent ``(d, n_bits)`` projection matrices, one
        per table.
    n_tables:
        Number of LSH tables (replicas).

    Bit packing convention matches ``LSHRouting``.
    """

    _MAX_N_BITS = 32

    def __init__(
        self,
        n_partitions: int,
        d: int,
        n_tables: int,
        n_bits: int,
        seed: int = 42,
        metric: str = "euclidean",
    ) -> None:
        super().__init__(n_partitions, d)
        if not (isinstance(n_tables, int) and n_tables > 0):
            raise ValueError(f"n_tables must be a positive int, got {n_tables}")
        if not (isinstance(n_bits, int) and 0 < n_bits <= self._MAX_N_BITS):
            raise ValueError(
                f"n_bits must satisfy 0 < n_bits <= {self._MAX_N_BITS}, got {n_bits}"
            )
        if metric not in ("euclidean", "cosine"):
            raise ValueError(f"Unsupported metric: {metric}")
        self.n_tables = n_tables
        self.n_bits = n_bits
        self.metric = metric
        self.seed = seed

        n_buckets = 1 << n_bits
        self.n_buckets = n_buckets

        rng = np.random.default_rng(seed)
        # T independent projection matrices, one per table.
        self.P = rng.standard_normal((n_tables, d, n_bits)).astype(
            np.float64, copy=False
        )

        # Bucket-to-partition map m (shared by all tables).
        if n_buckets == n_partitions:
            self._bucket_to_partition = np.arange(n_buckets, dtype=np.int64)
        else:
            self._bucket_to_partition = (
                np.arange(n_buckets, dtype=np.int64) % n_partitions
            )

        self._all_buckets = np.arange(n_buckets, dtype=np.int64)
        self._bit_weights = np.int64(1) << np.arange(n_bits, dtype=np.int64)

    def _hash_code(self, vector: np.ndarray, table_idx: int) -> int:
        """Hash a single vector under a single table."""
        if self.metric == "cosine":
            v = vector / (float(np.linalg.norm(vector)) + 1e-10)
        else:
            v = vector
        proj = v @ self.P[table_idx]
        bits = (proj >= 0).astype(np.int64)
        return int(bits @ self._bit_weights)

    def _hash_codes_all_tables(self, vector: np.ndarray) -> np.ndarray:
        """Hash one vector under all T tables. Returns shape (T,) int64."""
        if self.metric == "cosine":
            v = vector / (float(np.linalg.norm(vector)) + 1e-10)
        else:
            v = vector
        proj = v @ self.P  # (T, n_bits)
        bits = (proj >= 0).astype(np.int64)
        return bits @ self._bit_weights  # (T,)

    def _hash_codes_batch_all_tables(self, vectors: np.ndarray) -> np.ndarray:
        """Hash a (n, d) batch under all T tables. Returns shape (B, T) int64."""
        if self.metric == "cosine":
            norms = np.linalg.norm(vectors, axis=1, keepdims=True)
            v = vectors / np.maximum(norms, 1e-10)
        else:
            v = vectors
        # einsum: (B, d) x (T, d, n_bits) -> (B, T, n_bits). Note: ``v @ self.P``
        # with a 3D right-hand operand stacks the leading (T) axis first and
        # yields (T, B, n_bits); einsum makes the contraction explicit.
        proj = np.einsum("bd,tdk->btk", v, self.P)
        bits = (proj >= 0).astype(np.int64)
        return bits @ self._bit_weights  # (B, T)

    def _partition_distances(self, qcodes: np.ndarray) -> np.ndarray:
        """Per-table (T, n_partitions) min Hamming distance to partitions."""
        xors = qcodes[:, None] ^ self._all_buckets[None, :]  # (T, B)
        hd = np.bitwise_count(xors)  # (T, B)
        # Scatter-min per partition via np.minimum.at (loop over the small T).
        fill = np.int64(self.n_bits + 1)
        part_dists = np.full(
            (self.n_tables, self.n_partitions), fill, dtype=np.int64
        )
        for t in range(self.n_tables):
            np.minimum.at(part_dists[t], self._bucket_to_partition, hd[t])
        return part_dists

    def get_partition(self, vector: np.ndarray) -> int:
        """Single-partition API: uses table 0's assignment (see class docstring)."""
        code = self._hash_code(vector, 0)
        return int(self._bucket_to_partition[code])

    def get_ingest_partitions(self, vector: np.ndarray) -> List[int]:
        """Deduplicated partitions across all T tables (one replica per table)."""
        codes = self._hash_codes_all_tables(vector)  # (T,)
        parts = self._bucket_to_partition[codes]
        seen: set = set()
        out: List[int] = []
        for p in parts.tolist():
            if p not in seen:
                seen.add(p)
                out.append(p)
        return out

    def get_partitions_batch(self, vectors: np.ndarray) -> np.ndarray:
        """Single-partition batch: table 0's assignment per vector."""
        if vectors.ndim != 2 or vectors.shape[1] != self.d:
            raise ValueError(
                f"vectors must have shape (n, {self.d}), got {vectors.shape}"
            )
        codes = self._hash_codes_batch_all_tables(vectors)[:, 0]
        return self._bucket_to_partition[codes].astype(np.int64, copy=False)

    def get_ingest_partitions_batch(
        self, vectors: np.ndarray
    ) -> List[List[int]]:
        """Deduplicated partitions across T tables, per vector."""
        if vectors.ndim != 2 or vectors.shape[1] != self.d:
            raise ValueError(
                f"vectors must have shape (n, {self.d}), got {vectors.shape}"
            )
        codes = self._hash_codes_batch_all_tables(vectors)  # (B, T)
        parts = self._bucket_to_partition[codes]  # (B, T)
        # Per-row dedup; loop over batch rows is acceptable (T is small).
        result: List[List[int]] = []
        for row in parts:
            seen: set = set()
            out: List[int] = []
            for p in row.tolist():
                if p not in seen:
                    seen.add(p)
                    out.append(p)
            result.append(out)
        return result

    def get_search_partitions(
        self, query_vector: np.ndarray, nprobe: int
    ) -> List[int]:
        """Probe budget split across tables; deterministic (distance, id) order."""
        nprobe = min(nprobe, self.n_partitions)
        if nprobe <= 0:
            return []
        qcodes = self._hash_codes_all_tables(query_vector)  # (T,)
        part_dists = self._partition_distances(qcodes)  # (T, P)

        nprobe_per_table = (nprobe + self.n_tables - 1) // self.n_tables
        part_ids = np.arange(self.n_partitions)

        # Per-table sorted order by (distance, partition_id).
        per_table_order = np.empty(
            (self.n_tables, self.n_partitions), dtype=np.int64
        )
        for t in range(self.n_tables):
            per_table_order[t] = np.lexsort((part_ids, part_dists[t]))

        # Initial picks: top nprobe_per_table from each table in table-id order.
        selected = np.zeros(self.n_partitions, dtype=bool)
        collected: List[int] = []
        for t in range(self.n_tables):
            take = min(nprobe_per_table, self.n_partitions)
            for j in range(take):
                pid = int(per_table_order[t, j])
                if not selected[pid]:
                    selected[pid] = True
                    collected.append(pid)

        # Top-up: round-robin extend each table by one new partition per round.
        if len(collected) < nprobe:
            cursor = np.full(self.n_tables, nprobe_per_table, dtype=np.int64)
            for t in range(self.n_tables):
                cursor[t] = min(cursor[t], self.n_partitions)
            while len(collected) < nprobe:
                added_this_round = False
                for t in range(self.n_tables):
                    if len(collected) >= nprobe:
                        break
                    while cursor[t] < self.n_partitions:
                        pid = int(per_table_order[t, cursor[t]])
                        cursor[t] += 1
                        if not selected[pid]:
                            selected[pid] = True
                            collected.append(pid)
                            added_this_round = True
                            break
                if not added_this_round:
                    break  # all tables exhausted

        # Final deterministic order: (min distance across tables, partition id).
        union_dists = part_dists.min(axis=0)  # (P,)
        collected_arr = np.array(collected, dtype=np.int64)
        if collected_arr.size == 0:
            return []
        order = np.lexsort((collected_arr, union_dists[collected_arr]))[:nprobe]
        return collected_arr[order].tolist()

    def get_search_partitions_batch(
        self, query_vectors: np.ndarray, nprobe: int
    ) -> List[List[int]]:
        if query_vectors.ndim != 2 or query_vectors.shape[1] != self.d:
            raise ValueError(
                f"query_vectors must have shape (n, {self.d}), got {query_vectors.shape}"
            )
        return [self.get_search_partitions(q, nprobe) for q in query_vectors]
