from __future__ import absolute_import, unicode_literals

from celery.backends import couchdb as module
from celery.backends.couchdb import CouchBackend
from celery.exceptions import ImproperlyConfigured
from celery import backends
from celery.tests.case import AppCase, Mock, patch, sentinel, skip

try:
    import pycouchdb
except ImportError:
    pycouchdb = None  # noqa

COUCHDB_CONTAINER = 'celery_container'


@skip.unless_module('pycouchdb')
class test_CouchBackend(AppCase):

    def setup(self):
        self.backend = CouchBackend(app=self.app)

    def test_init_no_pycouchdb(self):
        """test init no pycouchdb raises"""
        prev, module.pycouchdb = module.pycouchdb, None
        try:
            with self.assertRaises(ImproperlyConfigured):
                CouchBackend(app=self.app)
        finally:
            module.pycouchdb = prev

    def test_get_container_exists(self):
        with patch('pycouchdb.client.Database') as mock_Connection:
            self.backend._connection = sentinel._connection

            connection = self.backend._get_connection()

            self.assertEqual(sentinel._connection, connection)
            mock_Connection.assert_not_called()

    def test_get(self):
        """test_get

        CouchBackend.get should return  and take two params
        db conn to couchdb is mocked.
        TODO Should test on key not exists

        """
        x = CouchBackend(app=self.app)
        x._connection = Mock()
        mocked_get = x._connection.get = Mock()
        mocked_get.return_value = sentinel.retval
        # should return None
        self.assertEqual(x.get('1f3fab'), sentinel.retval)
        x._connection.get.assert_called_once_with('1f3fab')

    def test_delete(self):
        """test_delete

        CouchBackend.delete should return and take two params
        db conn to pycouchdb is mocked.
        TODO Should test on key not exists

        """
        x = CouchBackend(app=self.app)
        x._connection = Mock()
        mocked_delete = x._connection.delete = Mock()
        mocked_delete.return_value = None
        # should return None
        self.assertIsNone(x.delete('1f3fab'))
        x._connection.delete.assert_called_once_with('1f3fab')

    def test_backend_by_url(self, url='couchdb://myhost/mycoolcontainer'):
        from celery.backends.couchdb import CouchBackend
        backend, url_ = backends.get_backend_by_url(url, self.app.loader)
        self.assertIs(backend, CouchBackend)
        self.assertEqual(url_, url)

    def test_backend_params_by_url(self):
        url = 'couchdb://johndoe:mysecret@myhost:123/mycoolcontainer'
        with self.Celery(backend=url) as app:
            x = app.backend
            self.assertEqual(x.container, 'mycoolcontainer')
            self.assertEqual(x.host, 'myhost')
            self.assertEqual(x.username, 'johndoe')
            self.assertEqual(x.password, 'mysecret')
            self.assertEqual(x.port, 123)
