# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import sys

import pytest

from case import MagicMock, Mock, patch, sentinel, skip
from celery.exceptions import ImproperlyConfigured

try:
    from celery.backends import riak as module
    from celery.backends.riak import RiakBackend
except ImportError:
    pass
except TypeError as e:
    if sys.version_info[0:2] >= (3, 7):
        print(e)
    else:
        raise e


RIAK_BUCKET = 'riak_bucket'


@skip.if_python_version_after(3, 7)
@skip.unless_module('riak')
class test_RiakBackend:

    def setup(self):
        self.app.conf.result_backend = 'riak://'

    @property
    def backend(self):
        return self.app.backend

    def test_init_no_riak(self):
        prev, module.riak = module.riak, None
        try:
            with pytest.raises(ImproperlyConfigured):
                RiakBackend(app=self.app)
        finally:
            module.riak = prev

    def test_init_no_settings(self):
        self.app.conf.riak_backend_settings = []
        with pytest.raises(ImproperlyConfigured):
            RiakBackend(app=self.app)

    def test_init_settings_is_None(self):
        self.app.conf.riak_backend_settings = None
        assert self.app.backend

    def test_get_client_client_exists(self):
        with patch('riak.client.RiakClient') as mock_connection:
            self.backend._client = sentinel._client
            mocked_is_alive = self.backend._client.is_alive = Mock()
            mocked_is_alive.return_value.value = True
            client = self.backend._get_client()
            assert sentinel._client == client
            mock_connection.assert_not_called()

    def test_get(self):
        self.app.conf.couchbase_backend_settings = {}
        self.backend._client = Mock(name='_client')
        self.backend._bucket = Mock(name='_bucket')
        mocked_get = self.backend._bucket.get = Mock(name='bucket.get')
        mocked_get.return_value.data = sentinel.retval
        # should return None
        assert self.backend.get('1f3fab') == sentinel.retval
        self.backend._bucket.get.assert_called_once_with('1f3fab')

    def test_set(self):
        self.app.conf.couchbase_backend_settings = None
        self.backend._client = MagicMock()
        self.backend._bucket = MagicMock()
        self.backend._bucket.set = MagicMock()
        # should return None
        assert self.backend.set(sentinel.key, sentinel.value) is None

    def test_delete(self):
        self.app.conf.couchbase_backend_settings = {}

        self.backend._client = Mock(name='_client')
        self.backend._bucket = Mock(name='_bucket')
        mocked_delete = self.backend._client.delete = Mock('client.delete')
        mocked_delete.return_value = None
        # should return None
        assert self.backend.delete('1f3fab') is None
        self.backend._bucket.delete.assert_called_once_with('1f3fab')

    def test_config_params(self):
        self.app.conf.riak_backend_settings = {
            'bucket': 'mycoolbucket',
            'host': 'there.host.com',
            'port': '1234',
        }
        assert self.backend.bucket_name == 'mycoolbucket'
        assert self.backend.host == 'there.host.com'
        assert self.backend.port == 1234

    def test_backend_by_url(self, url='riak://myhost/mycoolbucket'):
        from celery.app import backends
        from celery.backends.riak import RiakBackend
        backend, url_ = backends.by_url(url, self.app.loader)
        assert backend is RiakBackend
        assert url_ == url

    def test_backend_params_by_url(self):
        self.app.conf.result_backend = 'riak://myhost:123/mycoolbucket'
        assert self.backend.bucket_name == 'mycoolbucket'
        assert self.backend.host == 'myhost'
        assert self.backend.port == 123

    def test_non_ASCII_bucket_raises(self):
        self.app.conf.riak_backend_settings = {
            'bucket': 'héhé',
            'host': 'there.host.com',
            'port': '1234',
        }
        with pytest.raises(ValueError):
            RiakBackend(app=self.app)
