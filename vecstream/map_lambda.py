import json
import time
from typing import Dict, List, Tuple
from dataclasses import dataclass
import faiss
import numpy as np
import asyncio

from vecstream.serde import serialize_array
from vecstream.search import reduce_results, search
from vecstream.vector_io import VectorIO
from vecstream.querytype import QueryType
import traceback

from vecstream.reduce_query import branching_search


loop = asyncio.new_event_loop()

@dataclass
class CachedFaissIndex:
    name: str
    # etag: str
    faiss_index: faiss.Index

vector_io = VectorIO()
vector_cache: Dict[str, Dict[str, CachedFaissIndex]] = {}

def get_cached_indexes(bucket: str, object_keys: list[str]) -> Dict[str, CachedFaissIndex]:
    if bucket in vector_cache:
        return {key: vector_cache[bucket][key] for key in object_keys if key in vector_cache[bucket]}

    return {}

def add_index_to_cache(bucket: str, object_key: str, faiss_index: faiss.Index) -> None:
    if bucket not in vector_cache:
        vector_cache[bucket] = {}
    vector_cache[bucket][object_key] = CachedFaissIndex(
        name=object_key,
        faiss_index=faiss_index,
    )

def reset_cache() -> None:
    global vector_cache
    vector_cache = {}

def load_index_into_cache(bucket: str, object_keys: list[str]) -> None:
    reset_cache()
    current_cache = get_cached_indexes(bucket, object_keys)
    for object_key in object_keys:
        if not object_key in current_cache:
            faiss_index = vector_io.load_index_from_s3(bucket, object_key)
            add_index_to_cache(bucket, object_key, faiss_index)

async def query(
    bucket: str,
    object_keys: list[str],
    vector_list: list[float],
    topK: int,
    cached: bool = True,
    worker_id: int = 0,
    branching_factor: int = 16,
    map_invocations_per_lambda: int = 16
) -> Tuple[np.ndarray, np.ndarray, Dict[str, Dict], List[str]]:
    """ CACHE search

    Returns:
        A 4-tuple ``(Ds, Is, timestamps, indexes_not_in_cache)`` where
        ``indexes_not_in_cache`` is the list of object keys that were not
        present in the L1 cache when this query ran (always empty for
        non-cached queries). The client uses it to schedule background
        L1 reloads (paper §3.4 CATI flow).
    """
    timestamps: dict[str, dict] = {}
    search_timestamps = {}
    results = []
    query_vector = np.array([vector_list], dtype=np.float32)
    start_query_time = time.time()

    if len(object_keys) == 0:
        return np.array([]), np.array([]), timestamps, []

    if cached:
        indexes = get_cached_indexes(bucket, object_keys)
    else:
        indexes = {}
    indexes_not_in_cache = [k for k in object_keys if k not in indexes]

    fan_out_task = None
    if len(indexes_not_in_cache) >= 1:
        fan_out_indexes = indexes_not_in_cache[1:]
        if fan_out_indexes:
            fan_out_task = asyncio.create_task(branching_search(bucket, fan_out_indexes, vector_list, topK, QueryType.NON_CACHED_QUERY, worker_id, branching_factor, map_invocations_per_lambda))
        
        # Map_{worker_id} results 
        D, I, t = single_non_cached_query(bucket, indexes_not_in_cache[0], query_vector, topK)
        results.append((D, I))
        search_timestamps[indexes_not_in_cache[0]] = t
        
    fan_out_invoke_time = time.time()
    
    for index_name, cached_index in indexes.items():
        D, I, t = search(cached_index.faiss_index, query_vector, topK)
        results.append((D, I))
        search_timestamps[index_name] = t

    local_search_time = time.time()

    if fan_out_task is not None:
        fanout_results, fanout_timestamps = await fan_out_task
        results.extend(fanout_results)
        timestamps.update(fanout_timestamps)

    start_reduce_cached = time.time()
    Ds, Is = reduce_results(results, topK)
    end_reduce_cached = time.time()

    timestamps[f"M{worker_id}"] = {
        "start_reduce_cached": start_reduce_cached,
        "end_reduce_cached": end_reduce_cached,
        "fan_out_search_times": search_timestamps,
        "fan_out_invoke_time": fan_out_invoke_time,
        "local_search_time": local_search_time,
        "start_query_time": start_query_time,
    }
    return Ds, Is, timestamps, indexes_not_in_cache


# TODO make async
def single_non_cached_query(
    bucket: str, object_key: str, query_vector: np.ndarray, topK: int
) -> Tuple[np.ndarray, np.ndarray, Dict[str, float]]:
    t_start_load_index_from_s3 = time.time()
    faiss_index = vector_io.load_index_from_s3(bucket, object_key)
    t_end_load_index_from_s3 = time.time()
    add_index_to_cache(bucket, object_key, faiss_index)
    D, I, ts = search(faiss_index, query_vector, topK)
    timestamps = {
        "start_load_index_from_s3": t_start_load_index_from_s3,
        "end_load_index_from_s3": t_end_load_index_from_s3
    }
    timestamps.update(ts)
    return D, I, timestamps


# def multiple_non_cached_query(
#     bucket: str, object_keys: list[str], query_vector: np.ndarray, topK: int
# ) -> Tuple[np.ndarray, np.ndarray, Dict[str, float]]:
#     results = []
#     timestamps = []
#     for object_key in object_keys:
#         D, I, timestamps = single_non_cached_query(
#             bucket, object_key, query_vector, topK
#         )
#         results.append((D, I))
#         timestamps.append(timestamps)

#     start_reduce = time.time()
#     distances, indices = reduce_results(results, topK)
#     end_reduce = time.time()
#     timestamps.append({"start_reduce": start_reduce, "end_reduce": end_reduce})

#     return distances, indices, timestamps


# def non_cached_query(
#     bucket: str, objects: list[str], query_vector: np.ndarray, topK: int
# ) -> Tuple[np.ndarray, np.ndarray, dict]:
#     if len(objects) == 0:
#         return np.array([]), np.array([]), {}

#     if len(objects) == 1:
#         return single_non_cached_query(bucket, objects[0], query_vector, topK)





def event_handler(event, context):
    try:
        start_map_lambda = time.time()
        body = event.get("body", "{}")
        body = json.loads(body) if isinstance(body, str) else body

        # TODO Add metric.
        # Params for Query: bucket, object_key list, query_vector, topK

        # If object_key list has length >1, then branching factor should be also required. 

        # Params for Load Index into Cache: bucket, object_key list

        query_type_str = body.get("query_type", "NON_CACHED_QUERY")
        query_type = QueryType(query_type_str)

        bucket = body.get("bucket", "vector-store--use1-az6--x-s3")
        objects = body.get("objects", [])
        timestamps = {}

        if query_type == QueryType.LOAD_INDEX_INTO_CACHE:
            load_index_into_cache(bucket, objects)
            end_map_lambda = time.time()
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {"message": f"Loaded {len(objects)} indexes into cache.",
                     "loaded_objects": objects,
                     "num_loaded_objects": len(objects),
                     "timestamps": {
                         "start_map_lambda": start_map_lambda,
                         "end_map_lambda": end_map_lambda,
                     }}
                ),
            }

        elif query_type in (QueryType.CACHED_QUERY, QueryType.NON_CACHED_QUERY):
            query_vector: list[float] = body.get("query_vector", [])
            topK = body.get("topK", 10)
            worker_id = body.get("worker_id", 0)
            branching_factor = body.get("branching_factor", 16)
            map_invocations_per_lambda = body.get("map_invocations_per_lambda", 16)
            cached = query_type == QueryType.CACHED_QUERY
            reduced_D, reduced_I, timestamps, cache_misses = loop.run_until_complete(query(
                bucket,
                objects,
                query_vector,
                topK,
                cached=cached,
                worker_id=worker_id,
                branching_factor=branching_factor,
                map_invocations_per_lambda=map_invocations_per_lambda,
            ))
            end_map_lambda = time.time()
            lambda_timestamps = {
                "start_map_lambda": start_map_lambda,
                "end_map_lambda": end_map_lambda
            }
            timestamps[f"M{worker_id}"].update(lambda_timestamps)


            response = {
                "D": serialize_array(reduced_D),
                "I": serialize_array(reduced_I),
                "timestamps": timestamps,
            }
            if cached:
                response["cache_misses"] = cache_misses
            return {"statusCode": 200, "body": json.dumps(response)}

        elif query_type == QueryType.WARMUP:
            # Warmup the lambda by loading a small index into cache
            warmup_time = body.get("warmup_time", 1)  # Default warmup time in seconds
            time.sleep(warmup_time)  # Simulate some work
            end_map_lambda = time.time()
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Lambda warmup complete.",
                    "timestamps": {
                        "start_map_lambda": start_map_lambda,
                        "end_map_lambda": end_map_lambda,
                    },
                }),
            }
        else:
            raise ValueError(f"Unknown query type: {query_type}")
        
    except Exception as e:
        print(f"Error processing request: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "trace": traceback.format_exc()}),
        }
