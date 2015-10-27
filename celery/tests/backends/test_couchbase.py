"""Tests for the CouchBaseBackend."""

from __future__ import absolute_import

from kombu.utils.encoding import str_t

from celery.backends import couchbase as module
from celery.backends.couchbase import CouchBaseBackend
from celery.exceptions import ImproperlyConfigured
from celery import backends
from celery.tests.case import (
    AppCase, MagicMock, Mock, SkipTest, patch, sentinel,
)

try:
    import couchbase
except ImportError:
    couchbase = None  # noqa

COUCHBASE_BUCKET = 'celery_bucket'


class test_CouchBaseBackend(AppCase):

    """CouchBaseBackend TestCase."""

    def setup(self):
        """Skip the test if couchbase cannot be imported."""
        if couchbase is None:
            raise SkipTest('couchbase is not installed.')
        self.backend = CouchBaseBackend(app=self.app)

    def test_init_no_couchbase(self):
        """
        Test init no couchbase raises.

        If celery.backends.couchbase cannot import the couchbase client, it
        sets the couchbase.Couchbase to None and then handles this in the
        CouchBaseBackend __init__ method.
        """
        prev, module.Couchbase = module.Couchbase, None
        try:
            with self.assertRaises(ImproperlyConfigured):
                CouchBaseBackend(app=self.app)
        finally:
            module.Couchbase = prev

    def test_init_no_settings(self):
        """Test init no settings."""
        self.app.conf.couchbase_backend_settings = []
        with self.assertRaises(ImproperlyConfigured):
            CouchBaseBackend(app=self.app)

    def test_init_settings_is_None(self):
        """Test init settings is None."""
        self.app.conf.couchbase_backend_settings = None
        CouchBaseBackend(app=self.app)

    def test_get_connection_connection_exists(self):
        """Test _get_connection works."""
        with patch('couchbase.connection.Connection') as mock_Connection:
            self.backend._connection = sentinel._connection

            connection = self.backend._get_connection()

            self.assertEqual(sentinel._connection, connection)
            self.assertFalse(mock_Connection.called)

    def test_get(self):
        """
        Test get method.

        CouchBaseBackend.get should return  and take two params
        db conn to couchbase is mocked.

        TODO Should test on key not exists
        """
        self.app.conf.couchbase_backend_settings = {}
        x = CouchBaseBackend(app=self.app)
        x._connection = Mock()
        mocked_get = x._connection.get = Mock()
        mocked_get.return_value.value = sentinel.retval
        # should return None
        self.assertEqual(x.get('1f3fab'), sentinel.retval)
        x._connection.get.assert_called_once_with('1f3fab')

    def test_set(self):
        """
        Test set method.

        CouchBaseBackend.set should return None and take two params
        db conn to couchbase is mocked.
        """
        self.app.conf.couchbase_backend_settings = None
        x = CouchBaseBackend(app=self.app)
        x._connection = MagicMock()
        x._connection.set = MagicMock()
        # should return None
        self.assertIsNone(x.set(sentinel.key, sentinel.value))

    def test_delete(self):
        """
        Test delete method.

        CouchBaseBackend.delete should return and take two params
        db conn to couchbase is mocked.

        TODO Should test on key not exists.
        """
        self.app.conf.couchbase_backend_settings = {}
        x = CouchBaseBackend(app=self.app)
        x._connection = Mock()
        mocked_delete = x._connection.delete = Mock()
        mocked_delete.return_value = None
        # should return None
        self.assertIsNone(x.delete('1f3fab'))
        x._connection.delete.assert_called_once_with('1f3fab')

    def test_config_params(self):
        """
        Test config params are correct.

        app.conf.couchbase_backend_settings is properly set.
        """
        self.app.conf.couchbase_backend_settings = {
            'bucket': 'mycoolbucket',
            'host': ['here.host.com', 'there.host.com'],
            'username': 'johndoe',
            'password': 'mysecret',
            'port': '1234',
        }
        x = CouchBaseBackend(app=self.app)
        self.assertEqual(x.bucket, 'mycoolbucket')
        self.assertEqual(x.host, ['here.host.com', 'there.host.com'],)
        self.assertEqual(x.username, 'johndoe',)
        self.assertEqual(x.password, 'mysecret')
        self.assertEqual(x.port, 1234)

    def test_backend_by_url(self, url='couchbase://myhost/mycoolbucket'):
        """Test that a CouchBaseBackend is loaded from the couchbase url."""
        from celery.backends.couchbase import CouchBaseBackend
        backend, url_ = backends.get_backend_by_url(url, self.app.loader)
        self.assertIs(backend, CouchBaseBackend)
        self.assertEqual(url_, url)

    def test_backend_params_by_url(self):
        """Test config params are correct from config url."""
        url = 'couchbase://johndoe:mysecret@myhost:123/mycoolbucket'
        with self.Celery(backend=url) as app:
            x = app.backend
            self.assertEqual(x.bucket, 'mycoolbucket')
            self.assertEqual(x.host, 'myhost')
            self.assertEqual(x.username, 'johndoe')
            self.assertEqual(x.password, 'mysecret')
            self.assertEqual(x.port, 123)

    def test_correct_key_types(self):
        """
        Test that the key is the correct type for the couchbase python API.

        We check that get_key_for_task, get_key_for_chord, and
        get_key_for_group always returns a python string. Need to use str_t
        for cross Python reasons.
        """
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
