import faiss
import boto3

class VectorIO:

    def __init__(self):
        self.s3 = boto3.client('s3')

    def load_index_from_s3(self, bucket: str, object_key: str) -> faiss.Index:
        buffer = self.s3.get_object(Bucket=bucket, Key=object_key)['Body']
        reader = faiss.PyCallbackIOReader(buffer.read)
        index = faiss.read_index(reader)
        return index

    def write_index_to_object_putobject(self, bucket: str, object_key: str, index: faiss.Index) -> None:
        arr = faiss.serialize_index(index)
        data = arr.tobytes()
        self.s3.put_object(Bucket=bucket, Key=object_key, Body=data)
