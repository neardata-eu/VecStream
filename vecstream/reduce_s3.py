import boto3
from typing import List


def list_objects_s3(bucket: str, prefix: str, s3) -> List[str]:
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    object_keys = []
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                object_keys.append(obj["Key"])

    return object_keys


from concurrent.futures import ThreadPoolExecutor

def list_objects_s3_parallel(bucket: str, sub_prefixes: List[str], s3, n_threads=30) -> List[str]:
    def fetch_prefix(prefix: str) -> List[str]:
        paginator = s3.get_paginator("list_objects_v2")
        return [
            obj["Key"]
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
            for obj in page.get("Contents", [])
        ]

    all_keys = []
    # Use threads to fetch multiple sub-prefixes at the same time
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        results = executor.map(fetch_prefix, sub_prefixes)
        for result in results:
            all_keys.extend(result)
            
    return all_keys