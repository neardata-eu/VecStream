
import threading

import aiohttp

from vecstream.query_routing import l2_single_cache_routing
from vecstream.querytype import QueryType


_tls = threading.local()


def get_session() -> aiohttp.ClientSession:
    """Return the aiohttp session bound to the current thread's event loop.

    Creates a fresh session on first access in this thread. This is what keeps
    multiple VecStreamQuerySystem instances (each with its own loop) from
    stepping on each other: closing the session in one system does not
    invalidate the session used by a later system on a different loop.
    """
    sess = getattr(_tls, "session", None)
    if sess is None or sess.closed:
        timeout = aiohttp.ClientTimeout(total=950)
        sess = aiohttp.ClientSession(timeout=timeout)
        _tls.session = sess
    return sess


async def post_request(url: str, payload: dict) -> dict:
    session = get_session()
    async with session.post(url, json=payload) as response:
        if response.status != 200:
            try:
                raise ValueError(
                    f"[Error] Request to {url} failed with status {response.status}, "
                    f"payload: {payload}, response: {await response.json()}"
                )
            except Exception:
                raise ValueError(
                    f"[Error] Request to {url} failed with status {response.status}, "
                    f"payload: {payload}, response: {await response.text()}"
                )
        return await response.json()

async def invoke_map_search_lambda(lambda_url: str, bucket: str, objects: list[str], query_vector: list[float], topK: int, query_type: QueryType, worker_id: int, branching_factor: int, map_invocations_per_lambda: int):
    payload = {
        "bucket": bucket,
        "objects": objects,
        "query_vector": query_vector,
        "query_type": query_type.value,
        "topK": topK,
        "worker_id": worker_id,
        "map_invocations_per_lambda": map_invocations_per_lambda,
        "branching_factor": branching_factor
    }
    return await post_request(lambda_url, payload)


async def search_l2_cache(bucket: str, object_key: str, query_vector: list[float], topK: int, query_type: QueryType, worker_id: int, branching_factor: int = 16, map_invocations_per_lambda: int = 16):
    # print(f"Searching L2 cache for object_key: {object_key} with worker_id: {worker_id}")
    """Search a single L2 cache lambda for a given object key."""
    lambda_url = l2_single_cache_routing(object_key)
    # print(f"Invoking L2 cache lambda at URL: {lambda_url} for object_key: {object_key} with worker_id: {worker_id}")
    return await invoke_map_search_lambda(
        lambda_url, 
        bucket, 
        [object_key], 
        query_vector, 
        topK, query_type, 
        worker_id, 
        branching_factor, 
        map_invocations_per_lambda
    )


async def invoke_branching_reduce_lambda(
    bucket: str,
    objects: list[str],
    query_vector: list[float],
    topK: int,
    query_type: QueryType,
    worker_id: int,
    branching_factor: int,
    map_invocations_per_lambda: int,
):
    # print(f"Invoking branching reduce lambda for objects: {objects} with worker_id: {worker_id}")
    first_object_key = objects[0]
    lambda_url = l2_single_cache_routing(first_object_key)
    # print(f"Invoking branching reduce lambda at URL: {lambda_url} for objects: {objects} with worker_id: {worker_id}")
    return await invoke_map_search_lambda(
        lambda_url,
        bucket,
        objects,
        query_vector,
        topK,
        query_type,
        worker_id,
        branching_factor,
        map_invocations_per_lambda
    )

async def load_index_into_cache(
        lambda_url: str,
        bucket: str,
        object_keys: list[str],
) -> dict:
    """Load index data into the cache of a specified lambda function.

    Args:
        lambda_url (str): The URL of the lambda function to load the index into.
        bucket (str): The S3 bucket name where the index data is stored.
        object_keys (List[str]): A list of object keys representing the index data to be loaded.
    """

    payload = {
        "bucket": bucket,
        "objects": object_keys,
        "query_type": QueryType.LOAD_INDEX_INTO_CACHE.value,
    }
    return await post_request(lambda_url, payload)

async def warmup_lambda(
        lambda_url: str,
        warmup_time: int = 1
) -> dict:
    """Warm up a specified lambda function by sending a request to it.

    Args:
        lambda_url (str): The URL of the lambda function to load the index into.
        warmup_time (int): The time to warm up the lambda function (in seconds). Default is 1 second.
    """

    payload = {
        "query_type": QueryType.WARMUP.value,
        "warmup_time": warmup_time
    }
    return await post_request(lambda_url, payload)


async def send_kafka_search_request(
    lambda_url: str,
    query_vector: list[float],
    topK: int,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    reader_id: str,
    partitions: list[int],
    # end_offsets: dict[int, int],
    metric: str,
):
    payload = {
        "query_type": QueryType.NON_CACHED_QUERY.value,
        "query_vector": query_vector,
        "topK": topK,
        "group_id": group_id,
        "client_id": reader_id,
        "bootstrap_servers": bootstrap_servers,
        "topic": topic,
        "partitions": partitions,
        "metric": metric,
        # "end_offsets": end_offsets,
    }
    return await post_request(lambda_url, payload)