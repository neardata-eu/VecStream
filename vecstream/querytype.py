from enum import Enum


class QueryType(Enum):
    CACHED_QUERY = "CACHED_QUERY"
    NON_CACHED_QUERY = "NON_CACHED_QUERY"
    LOAD_INDEX_INTO_CACHE = "LOAD_INDEX_INTO_CACHE"
    WARMUP = "WARMUP"

class MetricType(Enum):
    EUCLIDEAN = "euclidean"
    COSINE = "cosine"