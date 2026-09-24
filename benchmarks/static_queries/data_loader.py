import struct
from pathlib import Path

import numpy as np


def load_vectors(path: str, max_vectors: int = None) -> np.ndarray:
    path_obj = Path(path)
    if path_obj.suffix == ".fbin":
        with open(path, "rb") as f:
            nb, d = struct.unpack("<ii", f.read(8))
            vectors_to_load = min(nb, max_vectors) if max_vectors is not None else nb
            vectors = np.frombuffer(f.read(vectors_to_load * d * 4), dtype=np.float32).reshape(vectors_to_load, d)
        return vectors
    data = np.load(path, mmap_mode="r")
    return data[:max_vectors].copy()


def get_vector_count_and_dimension(path: str) -> tuple[int, int]:
    path_obj = Path(path)
    if path_obj.suffix == ".fbin":
        with open(path, "rb") as f:
            nb, d = struct.unpack("<ii", f.read(8))
        return nb, d
    data = np.load(path, mmap_mode="r")
    return data.shape[0], data.shape[1]


def vector_generator(path: str, batch_size: int, max_vectors: int = None, start_batch: int = 0):
    path_obj = Path(path)
    if path_obj.suffix == ".fbin":
        yield from _fbin_generator(path, batch_size, max_vectors, start_batch)
    else:
        yield from _npy_generator(path, batch_size, max_vectors, start_batch)


def _npy_generator(path: str, batch_size: int, max_vectors: int = None, start_batch: int = 0):
    data = np.load(path, mmap_mode="r")
    num_vectors = data.shape[0]
    total = min(num_vectors, max_vectors) if max_vectors is not None else num_vectors
    start_index = start_batch * batch_size
    if start_index >= total:
        return
    for start in range(start_index, total, batch_size):
        end = min(start + batch_size, total)
        batch = data[start:end].copy()
        ids = [str(i) for i in range(start, end)]
        yield batch, ids


def _fbin_generator(path: str, batch_size: int, max_vectors: int = None, start_batch: int = 0):
    with open(path, "rb") as f:
        nb, d = struct.unpack("<ii", f.read(8))
    total = min(nb, max_vectors) if max_vectors is not None else nb
    start_index = start_batch * batch_size
    if start_index >= total:
        return
    with open(path, "rb") as f:
        offset = 8 + (start_index * d * 4)
        f.seek(offset)
        for start in range(start_index, total, batch_size):
            end = min(start + batch_size, total)
            n = end - start
            batch = np.frombuffer(f.read(n * d * 4), dtype=np.float32).reshape(n, d)
            ids = [str(i) for i in range(start, end)]
            yield batch, ids
