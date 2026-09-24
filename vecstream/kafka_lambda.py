import json
import os
import time
import numpy as np

from vecstream.serde import serialize_array
from vecstream.querytype import QueryType
import traceback
from vecstream.stream_search import kafka_search



def event_handler(event, context):
    try:
        start_kafka_lambda = time.time()
        body = event.get("body", "{}")
        body = json.loads(body) if isinstance(body, str) else body

        query_type_str = body.get("query_type", "NON_CACHED_QUERY")
        query_type = QueryType(query_type_str)
        
        timestamps = {}

        if query_type == QueryType.WARMUP:
            time.sleep(5)
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {"message": f"Warmup completed successfully."}
                ),
            }

        elif query_type == QueryType.NON_CACHED_QUERY:
            query_vector = body.get("query_vector", [])
            topK = body.get("topK", 10)
            group_id = body.get("group_id", "vecstream_search")
            client_id = body.get("client_id", "vecstream_search")
            bootstrap_servers = body.get("bootstrap_servers", "localhost:9092")
            topic = body.get("topic", "vecstream_topic")
            partitions = body.get("partitions", [])
            metric = body.get("metric", "euclidean")
            commit_group_id = os.environ.get(
                "KAFKA_COMMIT_GROUP_ID", "vecstream_indexing"
            )
            # end_offsets = body.get("end_offsets", {p: 0 for p in partitions})

            # Print the received parameters for debugging
            print(f"Received query_vector: {query_vector}")
            print(f"Received topK: {topK}")
            print(f"Received group_id: {group_id}")
            print(f"Received client_id: {client_id}")
            print(f"Received bootstrap_servers: {bootstrap_servers}")
            print(f"Received topic: {topic}")
            print(f"Received partitions: {partitions}")
            print(f"Received metric: {metric}")
            print(f"Received commit_group_id (from env): {commit_group_id}")
            # print(f"Received end_offsets: {end_offsets}", flush=True)


            distances, indices, timestamps = kafka_search(
                query=np.array(query_vector, dtype=np.float32),
                k=topK,
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                partitions=partitions,
                # end_offsets=end_offsets,
                group_id=group_id,
                client_id=client_id,
                metric=metric,
                commit_group_id=commit_group_id,
            )
                
            end_kafka_lambda = time.time()
            timestamps["start_kafka_lambda"] = start_kafka_lambda
            timestamps["end_kafka_lambda"] = end_kafka_lambda
    
            timestamps_dict = {f"K{client_id}": timestamps}
    
            response = {
                "D": serialize_array(distances),
                "I": serialize_array(indices),
                "timestamps": timestamps_dict,
            }
    
            return {"statusCode": 200, "body": json.dumps(response)}
        else:
            raise ValueError(f"Unknown query type: {query_type}")
        
    except Exception as e:
        print(f"Error processing request: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "trace": traceback.format_exc()}),
        }
