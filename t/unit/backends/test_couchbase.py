"""Tests for the CouchbaseBackend."""
from __future__ import absolute_import, unicode_literals

from datetime import timedelta

import pytest
from case import MagicMock, Mock, patch, sentinel, skip

from celery.app import backends
from celery.backends import couchbase as module
from celery.backends.couchbase import CouchbaseBackend
from celery.exceptions import ImproperlyConfigured

try:
    import couchbase
except ImportError:
    couchbase = None  # noqa

COUCHBASE_BUCKET = 'celery_bucket'


@skip.unless_module('couchbase')
class test_CouchbaseBackend:

    def setup(self):
        self.backend = CouchbaseBackend(app=self.app)

    def test_init_no_couchbase(self):
        prev, module.Couchbase = module.Couchbase, None
        try:
            with pytest.raises(ImproperlyConfigured):
                CouchbaseBackend(app=self.app)
        finally:
            module.Couchbase = prev

    def test_init_no_settings(self):
        self.app.conf.couchbase_backend_settings = []
        with pytest.raises(ImproperlyConfigured):
            CouchbaseBackend(app=self.app)

    def test_init_settings_is_None(self):
        self.app.conf.couchbase_backend_settings = None
        CouchbaseBackend(app=self.app)

    def test_get_connection_connection_exists(self):
        with patch('couchbase.connection.Connection') as mock_Connection:
            self.backend._connection = sentinel._connection

            connection = self.backend._get_connection()

            assert sentinel._connection == connection
            mock_Connection.assert_not_called()

    def test_get(self):
        self.app.conf.couchbase_backend_settings = {}
        x = CouchbaseBackend(app=self.app)
        x._connection = Mock()
        mocked_get = x._connection.get = Mock()
        mocked_get.return_value.value = sentinel.retval
        # should return None
        assert x.get('1f3fab') == sentinel.retval
        x._connection.get.assert_called_once_with('1f3fab')

    def test_set_no_expires(self):
        self.app.conf.couchbase_backend_settings = None
        x = CouchbaseBackend(app=self.app)
        x.expires = None
        x._connection = MagicMock()
        x._connection.set = MagicMock()
        # should return None
        assert x.set(sentinel.key, sentinel.value) is None

    def test_set_expires(self):
        self.app.conf.couchbase_backend_settings = None
        x = CouchbaseBackend(app=self.app, expires=30)
        assert x.expires == 30
        x._connection = MagicMock()
        x._connection.set = MagicMock()
        # should return None
        assert x.set(sentinel.key, sentinel.value) is None

    def test_delete(self):
        self.app.conf.couchbase_backend_settings = {}
        x = CouchbaseBackend(app=self.app)
        x._connection = Mock()
        mocked_delete = x._connection.delete = Mock()
        mocked_delete.return_value = None
        # should return None
        assert x.delete('1f3fab') is None
        x._connection.delete.assert_called_once_with('1f3fab')

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
