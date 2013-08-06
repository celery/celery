from __future__ import absolute_import

from datetime import timedelta

from mock import MagicMock, Mock, patch, sentinel
from nose import SkipTest

from celery import Celery
from celery import current_app
from celery import states
from celery.backends import couchbase as module
from celery.backends.couchbase import CouchBaseBackend, couchbase
from celery.datastructures import AttributeDict
from celery.exceptions import ImproperlyConfigured
from celery.result import AsyncResult
from celery.task import subtask
from celery.utils.timeutils import timedelta_seconds
from celery.backends.base import KeyValueStoreBackend
from celery import backends
from celery.tests.case import AppCase
from celery.app import app_or_default
from pprint import pprint

COUCHBASE_BUCKET = 'celery_bucket'

class test_CouchBaseBackend(AppCase):

    def setUp(self):
        if couchbase is None:
            raise SkipTest('couchbase is not installed.')
        from celery.app import current_app
        app = self.app = self._current_app = current_app()
        self.backend = CouchBaseBackend(app=self.app)
        
    def test_init_no_couchbase(self):
        """
        test init no couchbase raises
        """
        prev, module.couchbase = module.couchbase, None
        try:
            with self.assertRaises(ImproperlyConfigured):
                CouchBaseBackend(app=self.app)
        finally:
            module.couchbase = prev

    def test_init_no_settings(self):
        """
        test init no settings
        """
        celery = Celery(set_as_current=False)
        celery.conf.CELERY_COUCHBASE_BACKEND_SETTINGS = []
        with self.assertRaises(ImproperlyConfigured):
            CouchBaseBackend(app=celery)

    def test_init_settings_is_None(self):
        """
        Test init settings is None
        """
        celery = Celery(set_as_current=False)
        celery.conf.CELERY_COUCHBASE_BACKEND_SETTINGS = None
        CouchBaseBackend(app=celery)

    def test_get_connection_connection_exists(self):
        """
        Test get existing connection
        """
        with patch('couchbase.connection.Connection') as mock_Connection:
            self.backend._connection = sentinel._connection

            connection = self.backend._get_connection()

            self.assertEquals(sentinel._connection, connection)
            self.assertFalse(mock_Connection.called)

    def test_get(self):
        """
        Test get
        CouchBaseBackend.get
        should return  and take two params
        db conn to couchbase is mocked
        TODO Should test on key not exists
        """
        celery = Celery(set_as_current=False)
        
        celery.conf.CELERY_COUCHBASE_BACKEND_SETTINGS = {}

        x = CouchBaseBackend(app=celery)
        x._connection = Mock()
        mocked_get = x._connection.get = Mock()
        mocked_get.return_value.value = sentinel.retval
        # should return None
        self.assertEqual(x.get('1f3fab'), sentinel.retval)
        x._connection.get.assert_called_once_with('1f3fab')

    # betta
    def test_set(self):
        """
        Test set
        CouchBaseBackend.set
        should return None and take two params
        db conn to couchbase is mocked
        """
        celery = Celery(set_as_current=False)
        celery.conf.CELERY_COUCHBASE_BACKEND_SETTINGS = None
        x = CouchBaseBackend(app=celery)
        x._connection = MagicMock()
        x._connection.set = MagicMock()
        # should return None
        self.assertIsNone(x.set(sentinel.key, sentinel.value))

    def test_delete(self):
        """
        Test get
        CouchBaseBackend.get
        should return  and take two params
        db conn to couchbase is mocked
        TODO Should test on key not exists
        """
        celery = Celery(set_as_current=False)
        
        celery.conf.CELERY_COUCHBASE_BACKEND_SETTINGS = {}

        x = CouchBaseBackend(app=celery)
        x._connection = Mock()
        mocked_delete = x._connection.delete = Mock()
        mocked_delete.return_value = None
        # should return None
        self.assertIsNone(x.delete('1f3fab'))
        x._connection.delete.assert_called_once_with('1f3fab')

    def test_config_params(self):
        """
        test celery.conf.CELERY_COUCHBASE_BACKEND_SETTINGS
        celery.conf.CELERY_COUCHBASE_BACKEND_SETTINGS
        is properly set
        """
        celery = Celery(set_as_current=False)
        celery.conf.CELERY_COUCHBASE_BACKEND_SETTINGS = {'bucket':'mycoolbucket', 
                                                         'host':['here.host.com',
                                                                 'there.host.com'],
                                                         'username':'johndoe',
                                                         'password':'mysecret',
                                                         'port': '1234'}
        x = CouchBaseBackend(app=celery)
        self.assertEqual(x.bucket, "mycoolbucket")
        self.assertEqual(x.host, ['here.host.com', 'there.host.com'],)
        self.assertEqual(x.username, "johndoe",)
        self.assertEqual(x.password, 'mysecret')
        self.assertEqual(x.port, 1234)

    def test_backend_by_url(self, url='couchbase://myhost/mycoolbucket'):
        """
        test get backend by url
        """
        from celery.backends.couchbase import CouchBaseBackend
        backend, url_ = backends.get_backend_by_url(url)
        self.assertIs(backend, CouchBaseBackend)
        self.assertEqual(url_, url)

    def test_backend_params_by_url(self):
        """
        test get backend params by url
        """
        celery = Celery(set_as_current=False,
                        backend='couchbase://johndoe:mysecret@myhost:123/mycoolbucket')
        x = celery.backend
        self.assertEqual(x.bucket, "mycoolbucket")
        self.assertEqual(x.host, "myhost")
        self.assertEqual(x.username, "johndoe")
        self.assertEqual(x.password, "mysecret")
        self.assertEqual(x.port, 123)

