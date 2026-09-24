from abc import ABC, abstractmethod
import numpy as np


class VectorSystem(ABC):
    @abstractmethod
    def put_vectors(self, vectors: np.ndarray, ids: list[str]) -> dict:
        ...

    def close(self) -> None:
        pass