"""VecStream-only system registry for the streaming_queries benchmark.

The benchmark only ships a single backend (``vecstream``); the registry is kept
as a thin factory so a new backend can be plugged in by adding an entry here
without touching the query loop in ``vecstream.py``.

The ``VectorSystem`` ABC and ``VecStreamQuerySystem`` implementation live under
``benchmarks.static_queries.systems`` (shared with the static-queries suite);
this package only re-exports them.
"""

from benchmarks.static_queries.systems.base import VectorSystem
from benchmarks.static_queries.systems.vecstream import VecStreamQuerySystem

__all__ = ["VectorSystem", "VecStreamQuerySystem", "get_system"]


_REGISTRY: dict[str, type[VectorSystem]] = {
    "vecstream": VecStreamQuerySystem,
}


def get_system(name: str, **kwargs) -> VectorSystem:
    if name not in _REGISTRY:
        raise ValueError(
            f"Unknown system '{name}'. Available: {list(_REGISTRY.keys())}"
        )
    return _REGISTRY[name](**kwargs)
