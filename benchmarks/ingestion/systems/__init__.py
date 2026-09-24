from benchmarks.ingestion.systems.base import VectorSystem
from benchmarks.ingestion.systems.vecstream import VecStreamSystem

_REGISTRY: dict[str, type[VectorSystem]] = {
    "vecstream": VecStreamSystem,
}


def get_system(name: str, **kwargs) -> VectorSystem:
    if name not in _REGISTRY:
        raise ValueError(
            f"Unknown system '{name}'. Available: {list(_REGISTRY.keys())}"
        )
    return _REGISTRY[name](**kwargs)
