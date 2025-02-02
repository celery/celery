"""Google Cloud Storage result store backend for Celery."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from os import getpid
from threading import RLock

from kombu.utils.encoding import bytes_to_str
from kombu.utils.functional import dictfilter
from kombu.utils.url import url_to_parts

from celery.canvas import maybe_signature
from celery.exceptions import ChordError, ImproperlyConfigured
from celery.result import GroupResult, allow_join_result
from celery.utils.log import get_logger

from .base import KeyValueStoreBackend

try:
    import requests
    from google.api_core import retry
    from google.api_core.exceptions import Conflict
    from google.api_core.retry import if_exception_type
    from google.cloud import storage
    from google.cloud.storage import Client
    from google.cloud.storage.retry import DEFAULT_RETRY
except ImportError:
    storage = None

try:
    from google.cloud import firestore, firestore_admin_v1
except ImportError:
    firestore = None
    firestore_admin_v1 = None


__all__ = ('GCSBackend',)


logger = get_logger(__name__)


class GCSBackendBase(KeyValueStoreBackend):
    """Google Cloud Storage task result backend."""

    def __init__(self, **kwargs):
        if not storage:
            raise ImproperlyConfigured(
                'You must install google-cloud-storage to use gcs backend'
            )
        super().__init__(**kwargs)
        self._client_lock = RLock()
        self._pid = getpid()
        self._retry_policy = DEFAULT_RETRY
        self._client = None

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
        with self._client_lock:
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


class GCSBackend(GCSBackendBase):
    """Google Cloud Storage task result backend.

    Uses Firestore for chord ref count.
    """

    implements_incr = True
    supports_native_join = True

    # Firestore parameters
    _collection_name = 'celery'
    _field_count = 'chord_count'
    _field_expires = 'expires_at'

    def __init__(self, **kwargs):
        if not (firestore and firestore_admin_v1):
            raise ImproperlyConfigured(
                'You must install google-cloud-firestore to use gcs backend'
            )
        super().__init__(**kwargs)

        self._firestore_lock = RLock()
        self._firestore_client = None

        self.firestore_project = self.app.conf.get(
            'firestore_project', self.project
        )
        if not self._is_firestore_ttl_policy_enabled():
            raise ImproperlyConfigured(
                f'Missing TTL policy to use gcs backend with ttl on '
                f'Firestore collection: {self._collection_name} '
                f'project: {self.firestore_project}'
            )

    @property
    def firestore_client(self):
        """Returns a firestore client."""

        # make sure it's thread-safe, as creating a new client is expensive
        with self._firestore_lock:
            if self._firestore_client and self._pid == getpid():
                return self._firestore_client
            # make sure each process gets its own connection after a fork
            self._firestore_client = firestore.Client(
                project=self.firestore_project
            )
            self._pid = getpid()
        return self._firestore_client

    def _is_firestore_ttl_policy_enabled(self):
        client = firestore_admin_v1.FirestoreAdminClient()

        name = (
            f"projects/{self.firestore_project}"
            f"/databases/(default)/collectionGroups/{self._collection_name}"
            f"/fields/{self._field_expires}"
        )
        request = firestore_admin_v1.GetFieldRequest(name=name)
        field = client.get_field(request=request)

        ttl_config = field.ttl_config
        return ttl_config and ttl_config.state in {
            firestore_admin_v1.Field.TtlConfig.State.ACTIVE,
            firestore_admin_v1.Field.TtlConfig.State.CREATING,
        }

    def _apply_chord_incr(self, header_result_args, body, **kwargs):
        key = self.get_key_for_chord(header_result_args[0]).decode()
        self._expire_chord_key(key, 86400)
        return super()._apply_chord_incr(header_result_args, body, **kwargs)

    def incr(self, key: bytes) -> int:
        doc = self._firestore_document(key)
        resp = doc.set(
            {self._field_count: firestore.Increment(1)},
            merge=True,
            retry=retry.Retry(
                predicate=if_exception_type(Conflict),
                initial=1.0,
                maximum=180.0,
                multiplier=2.0,
                timeout=180.0,
            ),
        )
        return resp.transform_results[0].integer_value

    def on_chord_part_return(self, request, state, result, **kwargs):
        """Chord part return callback.

        Called for each task in the chord.
        Increments the counter stored in Firestore.
        If the counter reaches the number of tasks in the chord, the callback
        is called.
        If the callback raises an exception, the chord is marked as errored.
        If the callback returns a value, the chord is marked as successful.
        """
        app = self.app
        gid = request.group
        if not gid:
            return
        key = self.get_key_for_chord(gid)
        val = self.incr(key)
        size = request.chord.get("chord_size")
        if size is None:
            deps = self._restore_deps(gid, request)
            if deps is None:
                return
            size = len(deps)
        if val > size:  # pragma: no cover
            logger.warning(
                'Chord counter incremented too many times for %r', gid
            )
        elif val == size:
            # Read the deps once, to reduce the number of reads from GCS ($$)
            deps = self._restore_deps(gid, request)
            if deps is None:
                return
            callback = maybe_signature(request.chord, app=app)
            j = deps.join_native
            try:
                with allow_join_result():
                    ret = j(
                        timeout=app.conf.result_chord_join_timeout,
                        propagate=True,
                    )
            except Exception as exc:  # pylint: disable=broad-except
                try:
                    culprit = next(deps._failed_join_report())
                    reason = 'Dependency {0.id} raised {1!r}'.format(
                        culprit,
                        exc,
                    )
                except StopIteration:
                    reason = repr(exc)

                logger.exception('Chord %r raised: %r', gid, reason)
                self.chord_error_from_stack(callback, ChordError(reason))
            else:
                try:
                    callback.delay(ret)
                except Exception as exc:  # pylint: disable=broad-except
                    logger.exception('Chord %r raised: %r', gid, exc)
                    self.chord_error_from_stack(
                        callback,
                        ChordError(f'Callback error: {exc!r}'),
                    )
            finally:
                deps.delete()
                # Firestore doesn't have an exact ttl policy, so delete the key.
                self._delete_chord_key(key)

    def _restore_deps(self, gid, request):
        app = self.app
        try:
            deps = GroupResult.restore(gid, backend=self)
        except Exception as exc:  # pylint: disable=broad-except
            callback = maybe_signature(request.chord, app=app)
            logger.exception('Chord %r raised: %r', gid, exc)
            self.chord_error_from_stack(
                callback,
                ChordError(f'Cannot restore group: {exc!r}'),
            )
            return
        if deps is None:
            try:
                raise ValueError(gid)
            except ValueError as exc:
                callback = maybe_signature(request.chord, app=app)
                logger.exception('Chord callback %r raised: %r', gid, exc)
                self.chord_error_from_stack(
                    callback,
                    ChordError(f'GroupResult {gid} no longer exists'),
                )
        return deps

    def _delete_chord_key(self, key):
        doc = self._firestore_document(key)
        doc.delete()

    def _expire_chord_key(self, key, expires):
        """Set TTL policy for a Firestore document.

        Firestore ttl data is typically deleted within 24 hours after its
        expiration date.
        """
        val_expires = datetime.utcnow() + timedelta(seconds=expires)
        doc = self._firestore_document(key)
        doc.set({self._field_expires: val_expires}, merge=True)

    def _firestore_document(self, key):
        return self.firestore_client.collection(
            self._collection_name
        ).document(bytes_to_str(key))
