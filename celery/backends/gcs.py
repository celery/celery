"""Google Cloud Storage result store backend for Celery."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from os import getpid
from threading import RLock

from kombu.utils.encoding import bytes_to_str
from kombu.utils.functional import dictfilter
from kombu.utils.url import url_to_parts

from celery.exceptions import ImproperlyConfigured

from .base import KeyValueStoreBackend

try:
    import requests
    from google.cloud import storage
    from google.cloud.storage import Client
    from google.cloud.storage.retry import DEFAULT_RETRY
except ImportError:
    storage = None

__all__ = ('GCSBackend',)


class GCSBackend(KeyValueStoreBackend):
    """Google Cloud Storage task result backend."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._lock = RLock()
        self._pid = getpid()
        self._retry_policy = DEFAULT_RETRY
        self._client = None

        if not storage:
            raise ImproperlyConfigured(
                'You must install google-cloud-storage to use gcs backend'
            )
        conf = self.app.conf
        if self.url:
            url_params = self._params_from_url()
            conf.update(**dictfilter(url_params))

        self.bucket_name = conf.get('gcs_bucket')
        if not self.bucket_name:
            raise ImproperlyConfigured(
                'Missing bucket name: specify gcs_bucket to use gcs backend'
            )
        self.project = conf.get('gcs_project')
        if not self.project:
            raise ImproperlyConfigured(
                'Missing project:specify gcs_project to use gcs backend'
            )
        self.base_path = conf.get('gcs_base_path', '').strip('/')
        self._threadpool_maxsize = int(conf.get('gcs_threadpool_maxsize', 10))
        self.ttl = float(conf.get('gcs_ttl') or 0)
        if self.ttl < 0:
            raise ImproperlyConfigured(
                f'Invalid ttl: {self.ttl} must be greater than or equal to 0'
            )
        elif self.ttl:
            if not self._is_bucket_lifecycle_rule_exists():
                raise ImproperlyConfigured(
                    f'Missing lifecycle rule to use gcs backend with ttl on '
                    f'bucket: {self.bucket_name}'
                )

    def get(self, key):
        key = bytes_to_str(key)
        blob = self._get_blob(key)
        try:
            return blob.download_as_bytes(retry=self._retry_policy)
        except storage.blob.NotFound:
            return None

    def set(self, key, value):
        key = bytes_to_str(key)
        blob = self._get_blob(key)
        if self.ttl:
            blob.custom_time = datetime.utcnow() + timedelta(seconds=self.ttl)
        blob.upload_from_string(value, retry=self._retry_policy)

    def delete(self, key):
        key = bytes_to_str(key)
        blob = self._get_blob(key)
        if blob.exists():
            blob.delete(retry=self._retry_policy)

    def mget(self, keys):
        with ThreadPoolExecutor() as pool:
            return list(pool.map(self.get, keys))

    @property
    def client(self):
        """Returns a storage client."""

        # make sure it's thread-safe, as creating a new client is expensive
        with self._lock:
            if self._client and self._pid == getpid():
                return self._client
            # make sure each process gets its own connection after a fork
            self._client = Client(project=self.project)
            self._pid = getpid()

            # config the number of connections to the server
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=self._threadpool_maxsize,
                pool_maxsize=self._threadpool_maxsize,
                max_retries=3,
            )
            client_http = self._client._http
            client_http.mount("https://", adapter)
            client_http._auth_request.session.mount("https://", adapter)

            return self._client

    @property
    def bucket(self):
        return self.client.bucket(self.bucket_name)

    def _get_blob(self, key):
        key_bucket_path = f'{self.base_path}/{key}' if self.base_path else key
        return self.bucket.blob(key_bucket_path)

    def _is_bucket_lifecycle_rule_exists(self):
        bucket = self.bucket
        bucket.reload()
        for rule in bucket.lifecycle_rules:
            if rule['action']['type'] == 'Delete':
                return True
        return False

    def _params_from_url(self):
        url_parts = url_to_parts(self.url)

        return {
            'gcs_bucket': url_parts.hostname,
            'gcs_base_path': url_parts.path,
            **url_parts.query,
        }
