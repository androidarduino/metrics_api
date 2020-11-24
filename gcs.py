from google.cloud import storage
from google.api_core.exceptions import NotFound
import zlib


class GCSBucket:

    def __init__(self, bucket_name):
        self._client = storage.Client()
        self._bucket = self._client.get_bucket(bucket_name)
        self._bucket_name = bucket_name

    def get(self, path):
        blob = self._bucket.blob(path)
        try:
            blob.content_encoding = 'gzip'
            blob.content_type = 'text/plain'
            data = zlib.decompress(blob.download_as_string())
            return data
        except NotFound:
            return ""

    def put(self, path, data):
        blob = self._bucket.blob(path)
        compressed = 0
        try:
            compressed = zlib.compress(data, 9)
            blob.upload_from_string(compressed)
            return True, len(compressed)
        except Exception as e:
            print(f"error: {e}")
            return False, 0

    def remove(self, path):
        b = self._bucket.blob(path)
        try:
            self._bucket.delete_blob(b)
            return True
        except:
            return False

    def list(self, path, filter):
        files = self._bucket.list_blobs(prefix=path)
        return files