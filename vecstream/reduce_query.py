import asyncio
import time
from typing import List

import numpy as np

from vecstream.serde import deserialize_array
from vecstream.search import reduce_results
from vecstream.querytype import QueryType
from vecstream.invoke_http import invoke_branching_reduce_lambda, post_request, search_l2_cache



async def single_invoker_query(
    map_lambda_urls: List[str],
    bucket: str,
    objects: List[str],
    query_vector: List[float],
    topK: int,
    query_type: QueryType = QueryType.CACHED_QUERY,
) -> tuple:
    start_query = time.time()


    tasks: List[asyncio.Task] = []
    # start_time = time.time()

    # session_created_time = time.time()
    start_invoking_mappers = time.time()

    for i, (lamba_url, object_key) in enumerate(zip(map_lambda_urls, objects)):
        payload = {
            "bucket": bucket,
            "objects": [object_key],
            "query_vector": query_vector,
            "query_type": query_type.value,
            "topK": topK,
            "map_id": i,
        }
        tasks.append(asyncio.create_task(post_request(lamba_url, payload)))
    # requests_sent_time = time.time()
    end_invoking_mappers = time.time()
    start_gather = time.time()
    responses = await asyncio.gather(*tasks)
    end_gather = time.time()
    # responses_received_time = time.time()

    start_reduce = time.time()
    results = []
    timestamps_dict = {}
    for response in responses:
        D_bytes, I_bytes = response.get("D", b""), response.get("I", b"")
        t = response.get("timestamps", {})
        D, I = deserialize_array(D_bytes), deserialize_array(I_bytes)
        if D.size > 0 and I.size > 0:
            results.append((D, I))
        timestamps_dict.update(t)

    # responses_processed_time = time.time()

    # reduced_D, reduced_I, reduced_V = reduce_results(results)
    reduced_D, reduced_I = reduce_results(results, topK)
    # reduced_results_time = time.time()
    # return reduced_D, reduced_I, reduced_V
    # timers = {
    #     "start_time": start_time,
    #     "session_created_time": session_created_time,
    #     "requests_sent_time": requests_sent_time,
    #     "responses_received_time": responses_received_time,
    #     "responses_processed_time": responses_processed_time,
    #     "reduced_results_time": reduced_results_time
    # }
    end_reduce = time.time()

    timestamps_dict["R0"] = {
        "start_query": start_query,
        "start_invoking_mappers": start_invoking_mappers,
        "end_invoking_mappers": end_invoking_mappers,
        "start_gather": start_gather,
        "end_gather": end_gather,
        "start_reduce": start_reduce,
        "end_reduce": end_reduce,
    }
    return reduced_D, reduced_I, timestamps_dict





# TODO do not pass all the objects list to each branch

async def branching_search(
    bucket: str,
    objects: List[str],
    query_vector: List[float],
    topK: int,
    query_type: QueryType,
    worker_id: int,
    reduce_branching_factor: int,
    map_invocations_per_lambda: int,
) -> tuple[ list[tuple[np.ndarray, np.ndarray]], dict[str, dict[str, float]]]:
    start_branching = time.time()

    """
        Perform a tree-based branching search using reduce lambdas

        The first 'map_invocations_per_lambda' objects in 'objects' list will be queried using map lambdas.
        The remaining objects will be processed using branching reduce lambdas. Each reduce lambda will invoke 'reduce_branching_factor' reduce lambdas until all objects are processed.
        Reducer IDs are the parent reducer ID, plus the index of the first object assigned to that reducer, to ensure uniqueness across the tree.
    """
    # print(f"Branching factor: {reduce_branching_factor}, Map invocations per lambda: {map_invocations_per_lambda}")
    # print(f"Total objects to process: {len(objects)}")
    # print(f"Objects: {objects}")
    # print(f"Reducer ID: {reducer_id}")
    tasks = []

    get_session_time = time.time()

    if len(objects) > map_invocations_per_lambda:
        branching_objects = objects[map_invocations_per_lambda:]
        objects_per_branch = len(branching_objects) // reduce_branching_factor
        if objects_per_branch >= map_invocations_per_lambda:
            for i in range(reduce_branching_factor):
                start_idx = i * objects_per_branch
                end_idx = (i + 1) * objects_per_branch if i < reduce_branching_factor - 1 else len(branching_objects)
                branch_objects = branching_objects[start_idx:end_idx]
                branch_reducer_id = worker_id + map_invocations_per_lambda + start_idx + 1
                if branch_objects:
                    tasks.append(asyncio.create_task(
                        invoke_branching_reduce_lambda(
                            bucket,
                            branch_objects,
                            query_vector,
                            topK,
                            query_type,
                            branch_reducer_id,
                            reduce_branching_factor,
                            map_invocations_per_lambda,
                        )
                    ))
        else:
            branch_objects_list = []
            while branching_objects:
                objects_to_take = min(map_invocations_per_lambda, len(branching_objects))
                branch_objects_list.append(branching_objects[:objects_to_take])
                branching_objects = branching_objects[objects_to_take:]
            for i, branch_objects in enumerate(branch_objects_list):
                branch_reducer_id = worker_id + map_invocations_per_lambda + i * map_invocations_per_lambda + 1
                tasks.append(asyncio.create_task(
                    invoke_branching_reduce_lambda(
                        bucket,
                        branch_objects,
                        query_vector,
                        topK,
                        query_type,
                        branch_reducer_id,
                        reduce_branching_factor,
                        map_invocations_per_lambda,
                    )
                ))


    end_invoking_branches = time.time()
    start_invoking_maps = time.time()

    for i in range(min(map_invocations_per_lambda, len(objects))):
        tasks.append( asyncio.create_task(search_l2_cache(bucket, objects[i], query_vector, topK, query_type, worker_id + i + 1, reduce_branching_factor, map_invocations_per_lambda)))

    end_invoking_maps = time.time()
    start_gather = time.time()
    responses = await asyncio.gather(*tasks)
    end_gather = time.time() 
    results = []
    timestamps_dict = {}
    for response in responses:
        D_bytes, I_bytes = response.get("D", b""), response.get("I", b"")
        t = response.get("timestamps", {})
        D, I = deserialize_array(D_bytes), deserialize_array(I_bytes)
        if D.size > 0 and I.size > 0:
            results.append((D, I))
        timestamps_dict.update(t)


    timestamps_dict[f"M{worker_id}"] = {
        "start_branching": start_branching,
        "get_session_time": get_session_time,
        "end_invoking_branches": end_invoking_branches,
        "start_invoking_maps": start_invoking_maps,
        "end_invoking_maps": end_invoking_maps,
        "start_gather": start_gather,
        "end_gather": end_gather,
    }

    return results, timestamps_dict
    # return {'D': serialize_array(reduced_D), 'I': serialize_array(reduced_I)}
