# -*- coding: utf-8 -*-

from __future__ import absolute_import, with_statement

from celery.backends import riak as module
from celery.backends.riak import RiakBackend, riak
from celery.exceptions import ImproperlyConfigured
from celery.tests.case import (
    AppCase, MagicMock, Mock, SkipTest, patch, sentinel,
)


RIAK_BUCKET = 'riak_bucket'


class test_RiakBackend(AppCase):

    def setup(self):
        if riak is None:
            raise SkipTest('riak is not installed.')
        self.app.conf.result_backend = 'riak://'

    @property
    def backend(self):
        return self.app.backend

    def test_init_no_riak(self):
        """
        test init no riak raises
        """
        prev, module.riak = module.riak, None
        try:
            with self.assertRaises(ImproperlyConfigured):
                RiakBackend(app=self.app)
        finally:
            module.riak = prev

    def test_init_no_settings(self):
        """Test init no settings."""
        self.app.conf.riak_backend_settings = []
        with self.assertRaises(ImproperlyConfigured):
            RiakBackend(app=self.app)

    def test_init_settings_is_None(self):
        """
        Test init settings is None
        """
        self.app.conf.riak_backend_settings = None
        self.assertTrue(self.app.backend)

    def test_get_client_client_exists(self):
        """Test get existing client."""
        with patch('riak.client.RiakClient') as mock_connection:
            self.backend._client = sentinel._client

            mocked_is_alive = self.backend._client.is_alive = Mock()
            mocked_is_alive.return_value.value = True
            client = self.backend._get_client()
            self.assertEquals(sentinel._client, client)
            self.assertFalse(mock_connection.called)

    def test_get(self):
        """Test get

        RiakBackend.get
        should return  and take two params
        db conn to riak is mocked
        TODO Should test on key not exists
        """
        self.app.conf.couchbase_backend_settings = {}
        self.backend._client = Mock(name='_client')
        self.backend._bucket = Mock(name='_bucket')
        mocked_get = self.backend._bucket.get = Mock(name='bucket.get')
        mocked_get.return_value.data = sentinel.retval
        # should return None
        self.assertEqual(self.backend.get('1f3fab'), sentinel.retval)
        self.backend._bucket.get.assert_called_once_with('1f3fab')

    def test_set(self):
        """Test set

        RiakBackend.set
        should return None and take two params
        db conn to couchbase is mocked.

        """
        self.app.conf.couchbase_backend_settings = None
        self.backend._client = MagicMock()
        self.backend._bucket = MagicMock()
        self.backend._bucket.set = MagicMock()
        # should return None
        self.assertIsNone(self.backend.set(sentinel.key, sentinel.value))

    def test_delete(self):
        """Test get

        RiakBackend.get
        should return  and take two params
        db conn to couchbase is mocked
        TODO Should test on key not exists

        """
        self.app.conf.couchbase_backend_settings = {}

        self.backend._client = Mock(name='_client')
        self.backend._bucket = Mock(name='_bucket')
        mocked_delete = self.backend._client.delete = Mock('client.delete')
        mocked_delete.return_value = None
        # should return None
        self.assertIsNone(self.backend.delete('1f3fab'))
        self.backend._bucket.delete.assert_called_once_with('1f3fab')

    def test_config_params(self):
        """
        test celery.conf.riak_backend_settingS
        celery.conf.riak_backend_settingS
        is properly set
        """
        self.app.conf.riak_backend_settings = {
            'bucket': 'mycoolbucket',
            'host': 'there.host.com',
            'port': '1234',
        }
        self.assertEqual(self.backend.bucket_name, 'mycoolbucket')
        self.assertEqual(self.backend.host, 'there.host.com')
        self.assertEqual(self.backend.port, 1234)

    def test_backend_by_url(self, url='riak://myhost/mycoolbucket'):
        """
        test get backend by url
        """
        from celery import backends
        from celery.backends.riak import RiakBackend
        backend, url_ = backends.get_backend_by_url(url, self.app.loader)
        self.assertIs(backend, RiakBackend)
        self.assertEqual(url_, url)

    def test_backend_params_by_url(self):
        """
        test get backend params by url
        """
        self.app.conf.result_backend = 'riak://myhost:123/mycoolbucket'
        self.assertEqual(self.backend.bucket_name, 'mycoolbucket')
        self.assertEqual(self.backend.host, 'myhost')
        self.assertEqual(self.backend.port, 123)

    def test_non_ASCII_bucket_raises(self):
        """test app.conf.riak_backend_settings and
        app.conf.riak_backend_settings
        is properly set
        """
        self.app.conf.riak_backend_settings = {
            'bucket': 'héhé',
            'host': 'there.host.com',
            'port': '1234',
        }
        with self.assertRaises(ValueError):
            RiakBackend(app=self.app)
