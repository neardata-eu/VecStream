from abc import ABC, abstractmethod
from typing import Any

import numpy as np


class VectorSystem(ABC):
    @abstractmethod
    def put_vectors(self, vectors: np.ndarray, ids: list[str]) -> dict:
        ...

    @abstractmethod
    def query_vectors(self, vectors: np.ndarray, top_k: int) -> dict:
        """
        Query the nearest neighbors for the given vectors.

        Args:
            vectors: 2D numpy array of shape (num_queries, dimension) with query vectors.
            top_k: Number of nearest neighbors to retrieve per query vector.

        Returns:
            dict: Standardized response with the following structure:
            {
                "start_time": float,
                "end_time": float,
                "latency_seconds": float,
                "distance_metric": str,
                "query_vector": list[float],
                "results": list[{"id": str, "distance": float | None}],
                "system_data": dict,
            }
        """
        ...

    def close(self) -> None:
        pass

    def cleanup(self) -> None:
        pass
