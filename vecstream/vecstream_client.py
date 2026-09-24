import boto3
import asyncio
from botocore.exceptions import ClientError
import time
import numpy as np
import collections
import json


from vecstream.query_routing import build_lambda_query_routing, l2_single_cache_routing
from vecstream.querytype import QueryType
from vecstream.reduce_query import branching_search
from vecstream.reduce_s3 import list_objects_s3
from vecstream.invoke_http import load_index_into_cache, warmup_lambda

from vecstream.routing import Routing
from vecstream.search import reduce_results
from vecstream.serde import deserialize_array
from vecstream.stream_search import get_end_offsets
from vecstream.urls import l1_cache_lambda_urls, l2_cache_lambda_urls, kafka_search_lambda_urls
from vecstream.invoke_http import invoke_map_search_lambda, send_kafka_search_request



class VecStreamClient:
    def __init__(self, bucket: str, prefix: str, num_partitions: int, dimension: int, routing: Routing, bootstrap_servers: str = "localhost:9092", kafka_topic: str = "vecstream_topic", kafka_group_id: str = "vecstream_group", metric: str = "euclidean"):
        self.bucket = bucket
        self.prefix = prefix
        self.num_partitions = num_partitions
        self.dimension = dimension
        self.routing = routing
        self.bootstrap_servers = bootstrap_servers
        self.kafka_topic = kafka_topic
        self.kafka_group_id = kafka_group_id
        self.metric = metric
        self.s3_client = boto3.client("s3")
        self._background_reloads: set[asyncio.Task] = set()
        self._keepalive_task: asyncio.Task | None = None
        self._keepalive_loop: asyncio.AbstractEventLoop | None = None

    def get_index_list_key(self) -> str:
        """Get the S3 key for the index list JSON file."""
        prefix = self.prefix.rstrip('/')
        return f"{prefix}.index_list.json"

    def create_index_list(self):
        """Create a list of index object keys from the S3 bucket and prefix."""
        object_keys = list_objects_s3(self.bucket, self.prefix, self.s3_client)

        # 1. Group object keys by partition_id
        partition_groups = collections.defaultdict(list)
        for key in object_keys:
            parts = key.split('/')
            partition_id = parts[-2] if len(parts) >= 2 else "unknown"
            partition_groups[partition_id].append(key)

        # 2. Write partition_groups as a json file to S3 for later use
        s3_client = boto3.client("s3")
        index_list_key = self.get_index_list_key()
        bytes_data = json.dumps(partition_groups).encode('utf-8')
        s3_client.put_object(
            Bucket=self.bucket,
            Key=index_list_key,
            Body=bytes_data
        )

    def load_index_list(self) -> dict[str, list[str]]:
        """Load the index list from S3 and return it as a dictionary."""
        s3_client = boto3.client("s3")
        index_list_key = self.get_index_list_key()
        response = s3_client.get_object(Bucket=self.bucket, Key=index_list_key)
        bytes_data = response['Body'].read()
        partition_groups = json.loads(bytes_data.decode('utf-8'))
        return partition_groups

    async def load_l1_cache(self, index_per_lambda: int = 50):
        print(f"Loading L1 cache lambdas with index_per_lambda={index_per_lambda}...")
        try:
            partition_groups = self.load_index_list()
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'NoSuchKey':
                raise RuntimeError(
                    f"Index registry not found at s3://{self.bucket}/{self.get_index_list_key()}. "
                    "Run VecStreamClient.create_index_list() first, or let the async indexing pipeline create it."
                ) from e
            raise
        object_keys = [key for keys in partition_groups.values() for key in keys]
        self.map_lambda_to_keys, self.map_key_to_lambda = build_lambda_query_routing(object_keys, index_per_lambda)

        tasks = []
        for lambda_url, object_keys in self.map_lambda_to_keys.items():
            tasks.append(load_index_into_cache(lambda_url, self.bucket, object_keys))

        await asyncio.gather(*tasks)

    async def warmup_lambdas(self):
        tasks = []
        print("Warming up L1 cache lambdas...")
        # Warm up L1 cache lambdas
        l1_warmup_urls = self.map_lambda_to_keys.keys() if hasattr(self, 'map_lambda_to_keys') else l1_cache_lambda_urls
        for lambda_url in l1_warmup_urls:
            tasks.append(warmup_lambda(lambda_url))

        await asyncio.gather(*tasks)

        tasks = []
        print("Warming up L2 cache lambdas...")
        if hasattr(self, "map_lambda_to_keys"):
            l2_warmup_urls = {
                l2_single_cache_routing(key)
                for keys in self.map_lambda_to_keys.values()
                for key in keys
            }
        else:
            l2_warmup_urls = set(l2_cache_lambda_urls)
        for lambda_url in l2_warmup_urls:
            tasks.append(warmup_lambda(lambda_url, warmup_time=0))

        await asyncio.gather(*tasks)

        tasks = []
        print("Warming up Kafka search lambdas...")
        num_kafka_to_ping = min(len(kafka_search_lambda_urls), self.num_partitions)
        for lambda_url in kafka_search_lambda_urls[:num_kafka_to_ping]:
            tasks.append(warmup_lambda(lambda_url, warmup_time=0))

        await asyncio.gather(*tasks)

    def get_query_mapping(self, partitions_id: list[int]) -> dict[str, list[str]]:
        """Get the L1 lambdas url, and the list of index keys to search for."""
        index_list = self.load_index_list()

        object_keys_filtered = []
        for id in partitions_id:
            partition_key = f"partition_{id}"
            if partition_key in index_list:
                object_keys_filtered.extend(index_list[partition_key])

            
        url_to_keys: dict[str, list[str]] = {}
        for key in object_keys_filtered:
            if key in self.map_key_to_lambda:
                lambda_url = self.map_key_to_lambda[key]
            else:
                # Key sealed after the last L1 cache load: its consistent-hash
                # L2 lambda searches it as a cache miss (CATI §3.4).
                lambda_url = l2_single_cache_routing(key)
            if lambda_url not in url_to_keys:
                url_to_keys[lambda_url] = []
            url_to_keys[lambda_url].append(key)

        return url_to_keys

    
    async def index_search(self, query_vector: np.ndarray, topK: int, use_cache: bool, reduce_branching_factor: int = 16, map_invocations_per_lambda: int = 16, num_partitions_to_search: int = 16) -> tuple[np.ndarray, np.ndarray, dict[str, dict[str, float]]]:
        """Perform a search across the index using the provided query vector.

        Args:
            query_vector (np.ndarray): The query vector to search for.
            topK (int): The number of top results to return.
            use_cache (bool): Whether to use the L1 cache lambdas or not.
            reduce_branching_factor (int): The branching factor for the reduce lambdas.
            map_invocations_per_lambda (int): The number of map invocations per lambda.
            num_partitions_to_search (int): The number of partitions to search in the index.
        """
        start_total = time.time()
        # Assert that query_vector is a 1D numpy array of the correct dimension
        start_validate = time.time()
        if not isinstance(query_vector, np.ndarray) or query_vector.ndim != 1 or query_vector.size != self.dimension:
            raise ValueError(f"query_vector must be a 1D numpy array of size {self.dimension}")
        end_validate = time.time()
        start_tolist = time.time()
        query_vector_list = query_vector.tolist()
        end_tolist = time.time()

        start_routing = time.time()
        partitions_id_to_search = self.routing.get_search_partitions(query_vector, num_partitions_to_search)
        end_routing = time.time()

        timestamps_dict = {}

        if not use_cache:
            index_list = self.load_index_list()

            object_keys_filtered = []
            for id in partitions_id_to_search:
                partition_key = f"partition_{id}"
                if partition_key in index_list:
                    object_keys_filtered.extend(index_list[partition_key])

            print(f"Performing non-cached search for {len(object_keys_filtered)} object keys across {len(partitions_id_to_search)} partitions.")

            results, timestamps = await branching_search(
                bucket=self.bucket,
                objects=object_keys_filtered,
                query_vector=query_vector_list,
                topK=topK,
                query_type=QueryType.NON_CACHED_QUERY,
                worker_id=0,
                reduce_branching_factor=reduce_branching_factor,
                map_invocations_per_lambda=map_invocations_per_lambda)
            timestamps_dict.update(timestamps)

        else :
            start_query_mapping = time.time()
            search_mapping = self.get_query_mapping(partitions_id_to_search)
            end_query_mapping = time.time()

            start_task_build = time.time()
            tasks = []
            task_lambda_urls: list[str] = []
            worker_id = 0
            for lambda_url, object_keys in search_mapping.items():
                query_type = QueryType.CACHED_QUERY if use_cache else QueryType.NON_CACHED_QUERY
                tasks.append(asyncio.create_task(invoke_map_search_lambda(lambda_url, self.bucket, object_keys, query_vector_list, topK, query_type, worker_id, reduce_branching_factor, map_invocations_per_lambda)))
                task_lambda_urls.append(lambda_url)
                worker_id += len(object_keys)
            end_task_build = time.time()

            start_await_gather = time.time()
            responses = await asyncio.gather(*tasks)
            end_await_gather = time.time()

            # Process the results as needed, and reduce them to get the final topK results.
            start_result_processing = time.time()
            results = []
            for response in responses:
                D_bytes, I_bytes = response.get("D", b""), response.get("I", b"")
                t = response.get("timestamps", {})
                D, I = deserialize_array(D_bytes), deserialize_array(I_bytes)
                if D.size > 0 and I.size > 0:
                    results.append((D, I))
                timestamps_dict.update(t)
            end_result_processing = time.time()

            self._schedule_cache_reloads(responses, task_lambda_urls)
            tmp = {
                # "start_total": start_total,
                # "end_total": end_total,
                # "start_validate": start_validate,
                # "end_validate": end_validate,
                # "start_tolist": start_tolist,
                # "end_tolist": end_tolist,
                # "start_routing": start_routing,
                # "end_routing": end_routing,
                "start_query_mapping": start_query_mapping,
                "end_query_mapping": end_query_mapping,
                "start_task_build": start_task_build,
                "end_task_build": end_task_build,
                "start_await_gather": start_await_gather,
                "end_await_gather": end_await_gather,
                "start_result_processing": start_result_processing,
                "end_result_processing": end_result_processing,
                # "start_reduce": start_reduce,
                # "end_reduce": end_reduce,
                "n_lambdas": len(tasks),
                # "start_reduce_indexed": start_reduce,
                # "end_reduce_indexed": end_reduce,
            }
            timestamps_dict[f"vecstream_client"] = tmp

        start_reduce = time.time()
        Ds, Is = reduce_results(results, topK)
        end_reduce = time.time()
        end_total = time.time()

        if f"vecstream_client" not in timestamps_dict:
            timestamps_dict[f"vecstream_client"] = {}
        timestamps_dict[f"vecstream_client"].update({
            "start_total": start_total,
            "end_total": end_total,
            "start_validate": start_validate,
            "end_validate": end_validate,
            "start_tolist": start_tolist,
            "end_tolist": end_tolist,
            "start_routing": start_routing,
            "end_routing": end_routing,
            # "start_query_mapping": start_query_mapping,
            # "end_query_mapping": end_query_mapping,
            # "start_task_build": start_task_build,
            # "end_task_build": end_task_build,
            # "start_await_gather": start_await_gather,
            # "end_await_gather": end_await_gather,
            # "start_result_processing": start_result_processing,
            # "end_result_processing": end_result_processing,
            "start_reduce": start_reduce,
            "end_reduce": end_reduce,
            # "n_lambdas": len(tasks),
            "start_reduce_indexed": start_reduce,
            "end_reduce_indexed": end_reduce,
        })

        return Ds, Is, timestamps_dict

    def _schedule_cache_reloads(
        self,
        responses: list[dict],
        task_lambda_urls: list[str],
    ) -> None:
        """Schedule fire-and-forget L1 cache reloads for cache misses.

        Each map lambda response includes a ``cache_misses`` list (only set
        for ``CACHED_QUERY``) of object keys it had to fall back to
        non-cached search for. We kick off a ``load_index_into_cache`` task
        per affected lambda so the next query hits L1. Tasks are kept on
        ``self._background_reloads`` so the event loop does not garbage
        collect them mid-flight, and a done-callback removes them from the
        set while logging any exception so callers never see an unhandled
        error.
        """
        for response, lambda_url in zip(responses, task_lambda_urls):
            missed_keys = response.get("cache_misses") or []
            if not missed_keys:
                continue
            task = asyncio.create_task(
                load_index_into_cache(lambda_url, self.bucket, missed_keys)
            )
            self._background_reloads.add(task)
            task.add_done_callback(self._on_background_reload_done)

    def _on_background_reload_done(self, task: asyncio.Task) -> None:
        self._background_reloads.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            print(
                f"[VecStreamClient] background L1 reload failed: {exc}",
                flush=True,
            )

    async def kafka_search(self, query_vector: np.ndarray, topK: int, num_partitions_to_search: int = 16) -> tuple[np.ndarray, np.ndarray, dict[str, dict[str, float]]]:
        """Perform a search across the Kafka stream partitions

        Args:
            query_vector (np.ndarray): The query vector to search for.
            topK (int): The number of top results to return.
            num_partitions_to_search (int): The number of partitions to search in the Kafka stream.
        """
        start_total = time.time()
        # Assert that query_vector is a 1D numpy array of the correct dimension
        start_validate = time.time()
        if not isinstance(query_vector, np.ndarray) or query_vector.ndim != 1 or query_vector.size != self.dimension:
            raise ValueError(f"query_vector must be a 1D numpy array of size {self.dimension}")
        end_validate = time.time()
        start_tolist = time.time()
        query_vector_list = query_vector.tolist()
        end_tolist = time.time()

        start_routing = time.time()
        partitions_id_to_search = self.routing.get_search_partitions(query_vector, num_partitions_to_search)
        end_routing = time.time()

        # start_get_end_offsets = time.time()
        # end_offsets, get_end_offsets_timestamps = get_end_offsets(
        #     bootstrap_servers=self.bootstrap_servers,
        #     topic=self.kafka_topic,
        #     partitions=partitions_id_to_search,
        #     group_id=self.kafka_group_id,
        #     client_id="vecstream_client"
        # )
        # end_get_end_offsets = time.time()

        # print(f"Kafka search will query partitions: {partitions_id_to_search} with end_offsets: {end_offsets}")

        start_task_build = time.time()
        tasks = []
        num_kafka_lambdas = len(kafka_search_lambda_urls)
        for partition_id in partitions_id_to_search:
            lambda_url = kafka_search_lambda_urls[partition_id % num_kafka_lambdas]
            tasks.append(asyncio.create_task(send_kafka_search_request(
                lambda_url,
                query_vector_list,
                topK,
                self.bootstrap_servers,
                self.kafka_topic,
                self.kafka_group_id,
                f"reader_{partition_id}",
                [partition_id],
                # end_offsets=end_offsets,
                metric=self.metric
                )))
        end_task_build = time.time()

        start_await_gather = time.time()
        responses = await asyncio.gather(*tasks)
        end_await_gather = time.time()

        # Process the results as needed, and reduce them to get the final topK results.
        start_result_processing = time.time()
        results = []
        timestamps_dict = {}
        for response in responses:
            D_bytes, I_bytes = response.get("D", b""), response.get("I", b"")
            t = response.get("timestamps", {})
            D, I = deserialize_array(D_bytes), deserialize_array(I_bytes)
            if D.size > 0 and I.size > 0:
                results.append((D, I))
            timestamps_dict.update(t)
        end_result_processing = time.time()

        start_reduce = time.time()
        Ds, Is = reduce_results(results, topK)
        end_reduce = time.time()
        end_total = time.time()

        timestamps_dict[f"vecstream_client_kafka"] = {
            "start_total": start_total,
            "end_total": end_total,
            "start_validate": start_validate,
            "end_validate": end_validate,
            "start_tolist": start_tolist,
            "end_tolist": end_tolist,
            "start_routing": start_routing,
            "end_routing": end_routing,
            # "start_get_end_offsets": start_get_end_offsets,
            # "end_get_end_offsets": end_get_end_offsets,
            "start_task_build": start_task_build,
            "end_task_build": end_task_build,
            "start_await_gather": start_await_gather,
            "end_await_gather": end_await_gather,
            "start_result_processing": start_result_processing,
            "end_result_processing": end_result_processing,
            "start_reduce": start_reduce,
            "end_reduce": end_reduce,
            "n_lambdas": len(tasks),
            "start_reduce_kafka": start_reduce,
            "end_reduce_kafka": end_reduce,
            # "get_end_offsets": get_end_offsets_timestamps,
        }

        return Ds, Is, timestamps_dict

    def start_keepalive(self, period_seconds: float = 300.0) -> None:
        """Spawn a background task that periodically pings L1 lambdas.

        Each cycle sends a zero-second ``warmup_lambda`` ping to every URL
        in ``self.map_lambda_to_keys`` (or the full ``l1_cache_lambda_urls``
        pool if the routing map is not loaded), keeping every deployed L1
        instance warm between queries. Must be called from inside an
        asyncio event loop. Idempotent: a second call while the task is
        still running is a no-op. The task is kept on ``self`` so the GC
        does not collect it.
        """
        if self._keepalive_task is not None and not self._keepalive_task.done():
            return
        loop = asyncio.get_running_loop()
        self._keepalive_loop = loop
        self._keepalive_task = loop.create_task(self._keepalive_loop(period_seconds))

    def stop_keepalive(self) -> None:
        """Cancel the periodic keep-alive task, if any.

        Safe to call from any thread; ``Task.cancel`` schedules the
        cancellation on the loop the task is bound to. No-op if the task
        was never started or already finished.
        """
        task = self._keepalive_task
        self._keepalive_task = None
        self._keepalive_loop = None
        if task is None or task.done():
            return
        try:
            task.cancel()
        except Exception as e:
            print(f"[VecStreamClient] keepalive cancel raised: {e}", flush=True)

    async def _keepalive_loop(self, period_seconds: float) -> None:
        """Periodically ping L1 lambdas with a zero-second warmup request."""
        while True:
            await asyncio.sleep(period_seconds)
            try:
                if hasattr(self, "map_lambda_to_keys"):
                    urls = list(self.map_lambda_to_keys.keys())
                else:
                    urls = list(l1_cache_lambda_urls)
                if not urls:
                    continue
                await asyncio.gather(
                    *(warmup_lambda(url, warmup_time=0) for url in urls),
                    return_exceptions=True,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(
                    f"[VecStreamClient] keepalive ping cycle failed: {e}",
                    flush=True,
                )

    async def search(self, query_vector: np.ndarray, topK: int, use_cache: bool = True, reduce_branching_factor: int = 16, map_invocations_per_lambda: int = 16, num_partitions_to_search: int = 16) -> tuple[np.ndarray, np.ndarray, dict[str, dict[str, float]]]:
        """Perform a search across the indexes and kafka stream using the provided query vector.

        Args:
            query_vector (np.ndarray): The query vector to search for.
            topK (int): The number of top results to return.
            use_cache (bool): Whether to use the L1 cache lambdas or not.
            reduce_branching_factor (int): The branching factor for the reduce lambdas.
            map_invocations_per_lambda (int): The number of map invocations per lambda.
            num_partitions_to_search (int): The number of partitions to search in the index and kafka stream.
        """

        # Perform index search
        index_search_task = asyncio.create_task(self.index_search(
            query_vector,
            topK,
            use_cache,
            reduce_branching_factor,
            map_invocations_per_lambda,
            num_partitions_to_search
        ))

        # Perform kafka search
        kafka_search_task = asyncio.create_task(self.kafka_search(
            query_vector,
            topK,
            num_partitions_to_search
        ))
        
        Ds_index, Is_index, timestamps_index = await index_search_task
        Ds_kafka, Is_kafka, timestamps_kafka = await kafka_search_task

        # Combine results from both searches
        combined_results = [(Ds_index, Is_index), (Ds_kafka, Is_kafka)]
        Ds_final, Is_final = reduce_results(combined_results, topK)

        # Combine timestamps from both searches
        combined_timestamps = {**timestamps_index, **timestamps_kafka}

        return Ds_final, Is_final, combined_timestamps


    