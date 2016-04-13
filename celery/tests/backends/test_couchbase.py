"""Tests for the CouchbaseBackend."""

from __future__ import absolute_import, unicode_literals

from kombu.utils.encoding import str_t

from celery.backends import couchbase as module
from celery.backends.couchbase import CouchbaseBackend
from celery.exceptions import ImproperlyConfigured
from celery import backends
from celery.tests.case import AppCase, MagicMock, Mock, patch, sentinel, skip

try:
    import couchbase
except ImportError:
    couchbase = None  # noqa

COUCHBASE_BUCKET = 'celery_bucket'


@skip.unless_module('couchbase')
class test_CouchbaseBackend(AppCase):

    def setup(self):
        self.backend = CouchbaseBackend(app=self.app)

    def test_init_no_couchbase(self):
        prev, module.Couchbase = module.Couchbase, None
        try:
            with self.assertRaises(ImproperlyConfigured):
                CouchbaseBackend(app=self.app)
        finally:
            module.Couchbase = prev

    def test_init_no_settings(self):
        self.app.conf.couchbase_backend_settings = []
        with self.assertRaises(ImproperlyConfigured):
            CouchbaseBackend(app=self.app)

    def test_init_settings_is_None(self):
        self.app.conf.couchbase_backend_settings = None
        CouchbaseBackend(app=self.app)

    def test_get_connection_connection_exists(self):
        with patch('couchbase.connection.Connection') as mock_Connection:
            self.backend._connection = sentinel._connection

            connection = self.backend._get_connection()

            self.assertEqual(sentinel._connection, connection)
            mock_Connection.assert_not_called()

    def test_get(self):
        self.app.conf.couchbase_backend_settings = {}
        x = CouchbaseBackend(app=self.app)
        x._connection = Mock()
        mocked_get = x._connection.get = Mock()
        mocked_get.return_value.value = sentinel.retval
        # should return None
        self.assertEqual(x.get('1f3fab'), sentinel.retval)
        x._connection.get.assert_called_once_with('1f3fab')

    def test_set(self):
        self.app.conf.couchbase_backend_settings = None
        x = CouchbaseBackend(app=self.app)
        x._connection = MagicMock()
        x._connection.set = MagicMock()
        # should return None
        self.assertIsNone(x.set(sentinel.key, sentinel.value))

    def test_delete(self):
        self.app.conf.couchbase_backend_settings = {}
        x = CouchbaseBackend(app=self.app)
        x._connection = Mock()
        mocked_delete = x._connection.delete = Mock()
        mocked_delete.return_value = None
        # should return None
        self.assertIsNone(x.delete('1f3fab'))
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
        self.assertEqual(x.bucket, 'mycoolbucket')
        self.assertEqual(x.host, ['here.host.com', 'there.host.com'],)
        self.assertEqual(x.username, 'johndoe',)
        self.assertEqual(x.password, 'mysecret')
        self.assertEqual(x.port, 1234)

    def test_backend_by_url(self, url='couchbase://myhost/mycoolbucket'):
        from celery.backends.couchbase import CouchbaseBackend
        backend, url_ = backends.get_backend_by_url(url, self.app.loader)
        self.assertIs(backend, CouchbaseBackend)
        self.assertEqual(url_, url)

    def test_backend_params_by_url(self):
        url = 'couchbase://johndoe:mysecret@myhost:123/mycoolbucket'
        with self.Celery(backend=url) as app:
            x = app.backend
            self.assertEqual(x.bucket, 'mycoolbucket')
            self.assertEqual(x.host, 'myhost')
            self.assertEqual(x.username, 'johndoe')
            self.assertEqual(x.password, 'mysecret')
            self.assertEqual(x.port, 123)

    def test_correct_key_types(self):
        keys = [
            self.backend.get_key_for_task('task_id', bytes('key')),
            self.backend.get_key_for_chord('group_id', bytes('key')),
            self.backend.get_key_for_group('group_id', bytes('key')),
            self.backend.get_key_for_task('task_id', 'key'),
            self.backend.get_key_for_chord('group_id', 'key'),
            self.backend.get_key_for_group('group_id', 'key'),
        ]
        for key in keys:
            self.assertIsInstance(key, str_t)
