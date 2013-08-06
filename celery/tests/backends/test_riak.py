# -*- coding: utf-8 -*-

from __future__ import absolute_import, with_statement

from mock import MagicMock, Mock, patch, sentinel
from nose import SkipTest

from celery import Celery
from celery.backends import riak as module
from celery.backends.riak import RiakBackend, riak, NonAsciiBucket
from celery.exceptions import ImproperlyConfigured
from celery.tests.utils import AppCase


RIAK_BUCKET = 'riak_bucket'


class test_RiakBackend(AppCase):

    def setUp(self):
        if riak is None:
            raise SkipTest('riak is not installed.')
        from celery.app import current_app
        self.app = self._current_app = current_app()
        self.backend = RiakBackend(app=self.app)

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
        """
        test init no settings
        """
        celery = Celery(set_as_current=False)
        celery.conf.CELERY_RIAK_BACKEND_SETTINGS = []
        with self.assertRaises(ImproperlyConfigured):
            RiakBackend(app=celery)

    def test_init_settings_is_None(self):
        """
        Test init settings is None
        """
        celery = Celery(set_as_current=False)
        celery.conf.CELERY_RIAK_BACKEND_SETTINGS = None
        RiakBackend(app=celery)

    def test_get_client_client_exists(self):
        """
        Test get existing client
        """
        with patch('riak.client.RiakClient') as mock_connection:
            self.backend._client = sentinel._client

            mocked_is_alive = self.backend._client.is_alive = Mock()
            mocked_is_alive.return_value.value = True
            client = self.backend._get_client()
            self.assertEquals(sentinel._client, client)
            self.assertFalse(mock_connection.called)

    def test_get(self):
        """
        Test get
        RiakBackend.get
        should return  and take two params
        db conn to riak is mocked
        TODO Should test on key not exists
        """
        celery = Celery(set_as_current=False)

        celery.conf.CELERY_COUCHBASE_BACKEND_SETTINGS = {}

        backend = RiakBackend(app=celery)
        backend._client = Mock()
        backend._bucket = Mock()
        mocked_get = backend._bucket.get = Mock()
        mocked_get.return_value.data = sentinel.retval
        # should return None
        self.assertEqual(backend.get('1f3fab'), sentinel.retval)
        backend._bucket.get.assert_called_once_with('1f3fab')

    def test_set(self):
        """
        Test set
        RiakBackend.set
        should return None and take two params
        db conn to couchbase is mocked
        """
        celery = Celery(set_as_current=False)
        celery.conf.CELERY_COUCHBASE_BACKEND_SETTINGS = None
        backend = RiakBackend(app=celery)
        backend._client = MagicMock()
        backend._bucket = MagicMock()
        backend._bucket.set = MagicMock()
        # should return None
        self.assertIsNone(backend.set(sentinel.key, sentinel.value))

    def test_delete(self):
        """
        Test get
        RiakBackend.get
        should return  and take two params
        db conn to couchbase is mocked
        TODO Should test on key not exists
        """
        celery = Celery(set_as_current=False)

        celery.conf.CELERY_COUCHBASE_BACKEND_SETTINGS = {}

        backend = RiakBackend(app=celery)
        backend._client = Mock()
        backend._bucket = Mock()
        mocked_delete = backend._client.delete = Mock()
        mocked_delete.return_value = None
        # should return None
        self.assertIsNone(backend.delete('1f3fab'))
        backend._bucket.delete.assert_called_once_with('1f3fab')

    def test_config_params(self):
        """
        test celery.conf.CELERY_RIAK_BACKEND_SETTINGS
        celery.conf.CELERY_RIAK_BACKEND_SETTINGS
        is properly set
        """
        celery = Celery(set_as_current=False)
        celery.conf.CELERY_RIAK_BACKEND_SETTINGS = {'bucket': 'mycoolbucket',
                                                    'host': 'there.host.com',
                                                    'port': '1234'}
        backend = RiakBackend(app=celery)
        self.assertEqual(backend.bucket_name, "mycoolbucket")
        self.assertEqual(backend.host, 'there.host.com')
        self.assertEqual(backend.port, 1234)

    def test_backend_by_url(self, url='riak://myhost/mycoolbucket'):
        """
        test get backend by url
        """
        from celery.backends.riak import RiakBackend
        backend, url_ = backends.get_backend_by_url(url)
        self.assertIs(backend, RiakBackend)
        self.assertEqual(url_, url)

    def test_backend_params_by_url(self):
        """
        test get backend params by url
        """
        celery = Celery(set_as_current=False,
                        backend='riak://myhost:123/mycoolbucket')
        backend = celery.backend
        self.assertEqual(backend.bucket_name, "mycoolbucket")
        self.assertEqual(backend.host, "myhost")
        self.assertEqual(backend.port, 123)

    def test_non_ASCII_bucket_raises(self):
        """
        test celery.conf.CELERY_RIAK_BACKEND_SETTINGS
        celery.conf.CELERY_RIAK_BACKEND_SETTINGS
        is properly set
        """
        with self.assertRaises(NonAsciiBucket):
            celery = Celery(set_as_current=False)
            celery.conf.CELERY_RIAK_BACKEND_SETTINGS = {
                'bucket': 'héhé',
                'host': 'there.host.com',
                'port': '1234',
            }
            RiakBackend(app=celery)
