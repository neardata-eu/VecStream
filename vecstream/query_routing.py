import collections
import mmh3
from vecstream.urls import l1_cache_lambda_urls, l2_cache_lambda_urls

def __split_object_list_across_lambdas(object_keys: list[str], num_lambdas: int, max_objects_per_lambda: int) -> list[list[str]]:
    """Split a list of object keys into sublists for distribution across multiple lambdas.

    Constraint: Objects of different partitions CANNOT share the same lambda.
    """
    if not object_keys:
        return []

    # 1. Group object keys by partition_id
    partition_groups = collections.defaultdict(list)
    for key in object_keys:
        parts = key.split('/')
        partition_id = parts[-2] if len(parts) >= 2 else "unknown"
        partition_groups[partition_id].append(key)

    lambdas = []

    # 2. Chunk each partition into its own dedicated Lambda payloads
    for keys in partition_groups.values():
        for i in range(0, len(keys), max_objects_per_lambda):
            chunk = keys[i:i + max_objects_per_lambda]
            lambdas.append(chunk)

    # 3. Validate against the available number of lambdas
    if len(lambdas) > num_lambdas:
        raise ValueError(
            f"Capacity exceeded: The strict partition isolation requires {len(lambdas)} "
            f"lambdas, but only {num_lambdas} were provided."
        )

    return lambdas

def build_lambda_query_routing(
    object_keys: list[str],
    max_objects_per_lambda: int = 16,
) -> tuple[dict[str, list[str]], dict[str, str]]:

    """Build a routing map for L1 cache lambdas based on object keys.

    Returns:
        map_lambda_to_objects: A dictionary mapping each L1 cache lambda URL to its assigned object keys.
        routing: A dictionary mapping each object key to its corresponding L1 cache lambda URL.
    """

    num_lambdas = len(l1_cache_lambda_urls)
    objects_per_lambda = __split_object_list_across_lambdas(object_keys, num_lambdas, max_objects_per_lambda)
    routing = {}
    map_lambda_to_objects = {url: objs for url, objs in zip(l1_cache_lambda_urls, objects_per_lambda)}
    for objects, lambda_url in zip(objects_per_lambda, l1_cache_lambda_urls):
        for object_key in objects:
            routing[object_key] = lambda_url
    return map_lambda_to_objects, routing

def l2_single_cache_routing(object_key: str) -> str:
    """Consistent hash routing for L2 cache lambdas using MurmurHash3.

    Each L2 cache lambda holds a single segment index, so MurmurHash3 maps
    every object key to exactly one L2 lambda across calls (the L2 tier is a
    deterministic, sticky routing layer).
    """
    num_lambdas = len(l2_cache_lambda_urls)
    hash_value = mmh3.hash(object_key, signed=False)
    lambda_index = hash_value % num_lambdas
    return l2_cache_lambda_urls[lambda_index]

def l2_cache_routing(object_keys: list[str]) -> dict[str, str]:
    """Consistent hash routing for L2 cache lambdas using MurmurHash3."""
    routing = {object_key: l2_single_cache_routing(object_key) for object_key in object_keys}

    return routing

