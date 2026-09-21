"""Tests for the CouchbaseBackend."""
from datetime import timedelta
from unittest.mock import MagicMock, Mock, patch, sentinel

import pytest

from celery import states
from celery.app import backends
from celery.backends import couchbase as module
from celery.backends.couchbase import CouchbaseBackend
from celery.exceptions import ImproperlyConfigured

try:
    import couchbase
except ImportError:
    couchbase = None

COUCHBASE_BUCKET = 'celery_bucket'

pytest.importorskip('couchbase')

from couchbase.exceptions import DocumentNotFoundException, TimeoutException  # noqa: E402


class test_CouchbaseBackend:

    def setup_method(self):
        self.backend = CouchbaseBackend(app=self.app)

    def test_init_no_couchbase(self):
        prev, module.Cluster = module.Cluster, None
        try:
            with pytest.raises(ImproperlyConfigured):
                CouchbaseBackend(app=self.app)
        finally:
            module.Cluster = prev

    def test_init_no_settings(self):
        self.app.conf.couchbase_backend_settings = []
        with pytest.raises(ImproperlyConfigured):
            CouchbaseBackend(app=self.app)

    def test_init_settings_is_None(self):
        self.app.conf.couchbase_backend_settings = None
        CouchbaseBackend(app=self.app)

    def test_get_connection_connection_exists(self):
        with patch('couchbase.cluster.Cluster') as mock_Cluster:
            self.backend._connection = sentinel._connection

            connection = self.backend._get_connection()

            assert sentinel._connection == connection
            mock_Cluster.assert_not_called()

    def test_get(self):
        self.app.conf.couchbase_backend_settings = {}
        x = CouchbaseBackend(app=self.app)
        x._connection = Mock()
        mocked_get = x._connection.get = Mock()
        mocked_get.return_value.content = sentinel.retval
        # should return None
        assert x.get('1f3fab') == sentinel.retval
        x._connection.get.assert_called_once_with('1f3fab')

    def test_get_missing_document_returns_None(self):
        x = CouchbaseBackend(app=self.app)
        x._connection = Mock()
        x._connection.get = Mock(
            side_effect=DocumentNotFoundException('missing'))
        # absent or expired results must surface as None, like every
        # other KV backend, not as an exception through get_task_meta
        assert x.get('1f3fab') is None
        x._connection.get.assert_called_once_with('1f3fab')

    def test_get_task_meta_pending_for_missing_document(self):
        # the user-visible contract this PR is about: AsyncResult(id).state
        # must read PENDING when the result document was never stored (or
        # has expired), instead of raising
        x = CouchbaseBackend(app=self.app)
        x._connection = Mock()
        x._connection.get = Mock(
            side_effect=DocumentNotFoundException('missing'))
        assert x.get_task_meta('1f3fab')['status'] == states.PENDING

    def test_get_unrelated_sdk_errors_propagate(self):
        # only DocumentNotFoundException maps to an absent result; any other
        # SDK error (a timeout, say) must still reach the caller instead of
        # silently reading as a pending result
        x = CouchbaseBackend(app=self.app)
        x._connection = Mock()
        x._connection.get = Mock(side_effect=TimeoutException('timed out'))
        with pytest.raises(TimeoutException):
            x.get('1f3fab')

    def test_set_no_expires(self):
        self.app.conf.couchbase_backend_settings = None
        x = CouchbaseBackend(app=self.app)
        x.expires = None
        x._connection = MagicMock()
        x._connection.set = MagicMock()
        # should return None
        assert x._set_with_state(sentinel.key, sentinel.value, states.SUCCESS) is None

    def test_set_expires(self):
        self.app.conf.couchbase_backend_settings = None
        x = CouchbaseBackend(app=self.app, expires=30)
        assert x.expires == 30
        x._connection = MagicMock()
        x._connection.set = MagicMock()
        # should return None
        assert x._set_with_state(sentinel.key, sentinel.value, states.SUCCESS) is None

    def test_delete(self):
        self.app.conf.couchbase_backend_settings = {}
        x = CouchbaseBackend(app=self.app)
        x._connection = Mock()
        mocked_delete = x._connection.remove = Mock()
        mocked_delete.return_value = None
        # should return None
        assert x.delete('1f3fab') is None
        x._connection.remove.assert_called_once_with('1f3fab')

    def test_delete_missing_document_is_noop(self):
        x = CouchbaseBackend(app=self.app)
        x._connection = Mock()
        x._connection.remove = Mock(
            side_effect=DocumentNotFoundException('missing'))
        # forget()/delete() on an already-expired or never-stored result
        # must not raise, matching the other KV backends
        assert x.delete('1f3fab') is None

    def test_config_params(self):
        self.app.conf.couchbase_backend_settings = {
            'bucket': 'mycoolbucket',
            'host': ['here.host.com', 'there.host.com'],
            'username': 'johndoe',
            'password': 'mysecret',
            'port': '1234',
        }
        x = CouchbaseBackend(app=self.app)
        assert x.bucket == 'mycoolbucket'
        assert x.host == ['here.host.com', 'there.host.com']
        assert x.username == 'johndoe'
        assert x.password == 'mysecret'
        assert x.port == 1234

    def test_backend_by_url(self, url='couchbase://myhost/mycoolbucket'):
        from celery.backends.couchbase import CouchbaseBackend
        backend, url_ = backends.by_url(url, self.app.loader)
        assert backend is CouchbaseBackend
        assert url_ == url

    def test_backend_params_by_url(self):
        url = 'couchbase://johndoe:mysecret@myhost:123/mycoolbucket'
        with self.Celery(backend=url) as app:
            x = app.backend
            assert x.bucket == 'mycoolbucket'
            assert x.host == 'myhost'
            assert x.username == 'johndoe'
            assert x.password == 'mysecret'
            assert x.port == 123

    def test_expires_defaults_to_config(self):
        self.app.conf.result_expires = 10
        b = CouchbaseBackend(expires=None, app=self.app)
        assert b.expires == 10

    def test_expires_is_int(self):
        b = CouchbaseBackend(expires=48, app=self.app)
        assert b.expires == 48

    def test_expires_is_None(self):
        b = CouchbaseBackend(expires=None, app=self.app)
        assert b.expires == self.app.conf.result_expires.total_seconds()

    def test_expires_is_timedelta(self):
        b = CouchbaseBackend(expires=timedelta(minutes=1), app=self.app)
        assert b.expires == 60
