import uuid

from mock import MagicMock, Mock, patch, sentinel
from nose import SkipTest

from celery import states
from celery.backends.mongodb import MongoBackend
from celery.tests.utils import unittest


try:
    import pymongo
except ImportError:
    pymongo = None


COLLECTION = "taskmeta_celery"
TASK_ID = str(uuid.uuid1())
MONGODB_HOST = "localhost"
MONGODB_PORT = 27017
MONGODB_USER = "mongo"
MONGODB_PASSWORD = "1234"
MONGODB_DATABASE = "testing"
MONGODB_COLLECTION = "collection1"


@patch("celery.backends.mongodb.MongoBackend.decode", Mock())
@patch("celery.backends.mongodb.MongoBackend.encode", Mock())
@patch("pymongo.binary.Binary", Mock())
@patch("datetime.datetime", Mock())
class TestBackendMongoDb(unittest.TestCase):

    def setUp(self):
        if pymongo is None:
            raise SkipTest("pymongo is not installed.")

        self.backend = MongoBackend()

    @patch("pymongo.connection.Connection")
    def test_get_connection_connection_exists(self, mock_Connection):
        self.backend._connection = sentinel._connection

        connection = self.backend._get_connection()

        self.assertEquals(sentinel._connection, connection)
        self.assertFalse(mock_Connection.called)

    @patch("pymongo.connection.Connection")
    def test_get_connection_no_connection_host(self, mock_Connection):
        self.backend._connection = None
        self.backend.mongodb_host = MONGODB_HOST
        self.backend.mongodb_port = MONGODB_PORT
        mock_Connection.return_value = sentinel.connection

        connection = self.backend._get_connection()
        mock_Connection.assert_called_once_with(
            MONGODB_HOST, MONGODB_PORT)
        self.assertEquals(sentinel.connection, connection)

    @patch("pymongo.connection.Connection")
    def test_get_connection_no_connection_mongodb_uri(self, mock_Connection):
        mongodb_uri = "mongodb://%s:%d" % (MONGODB_HOST, MONGODB_PORT)
        self.backend._connection = None
        self.backend.mongodb_host = mongodb_uri

        mock_Connection.return_value = sentinel.connection

        connection = self.backend._get_connection()
        mock_Connection.assert_called_once_with(mongodb_uri)
        self.assertEquals(sentinel.connection, connection)

    @patch("celery.backends.mongodb.MongoBackend._get_connection")
    def test_get_database_no_existing(self, mock_get_connection):
        # Should really check for combinations of these two, to be complete.
        self.backend.mongodb_user = MONGODB_USER
        self.backend.mongodb_password = MONGODB_PASSWORD

        mock_database = Mock()
        mock_connection = MagicMock(spec=['__getitem__'])
        mock_connection.__getitem__.return_value = mock_database
        mock_get_connection.return_value = mock_connection

        database = self.backend._get_database()

        self.assertTrue(database is mock_database)
        self.assertTrue(self.backend._database is mock_database)
        mock_database.authenticate.assert_called_once_with(
            MONGODB_USER, MONGODB_PASSWORD)

    @patch("celery.backends.mongodb.MongoBackend._get_connection")
    def test_get_database_no_existing_no_auth(self, mock_get_connection):
        # Should really check for combinations of these two, to be complete.
        self.backend.mongodb_user = None
        self.backend.mongodb_password = None

        mock_database = Mock()
        mock_connection = MagicMock(spec=['__getitem__'])
        mock_connection.__getitem__.return_value = mock_database
        mock_get_connection.return_value = mock_connection

        database = self.backend._get_database()

        self.assertTrue(database is mock_database)
        self.assertFalse(mock_database.authenticate.called)
        self.assertTrue(self.backend._database is mock_database)

    def test_process_cleanup(self):
        self.backend._connection = None
        self.backend.process_cleanup()
        self.assertEquals(self.backend._connection, None)

        self.backend._connection = "not none"
        self.backend.process_cleanup()
        self.assertEquals(self.backend._connection, None)

    @patch("celery.backends.mongodb.MongoBackend._get_database")
    def test_store_result(self, mock_get_database):
        self.backend.mongodb_taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        ret_val = self.backend._store_result(
            sentinel.task_id, sentinel.result, sentinel.status)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(MONGODB_COLLECTION)
        mock_collection.save.assert_called_once()
        self.assertEquals(sentinel.result, ret_val)

    @patch("celery.backends.mongodb.MongoBackend._get_database")
    def test_get_task_meta_for(self, mock_get_database):
        self.backend.mongodb_taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()
        mock_collection.find_one.return_value = MagicMock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        ret_val = self.backend._get_task_meta_for(sentinel.task_id)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(MONGODB_COLLECTION)
        self.assertEquals(
            ['status', 'date_done', 'traceback', 'result', 'task_id'],
            ret_val.keys())

    @patch("celery.backends.mongodb.MongoBackend._get_database")
    def test_get_task_meta_for_no_result(self, mock_get_database):
        self.backend.mongodb_taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()
        mock_collection.find_one.return_value = None

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        ret_val = self.backend._get_task_meta_for(sentinel.task_id)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(MONGODB_COLLECTION)
        self.assertEquals({"status": states.PENDING, "result": None}, ret_val)

    @patch("celery.backends.mongodb.MongoBackend._get_database")
    def test_save_taskset(self, mock_get_database):
        self.backend.mongodb_taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        ret_val = self.backend._save_taskset(
            sentinel.taskset_id, sentinel.result)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(MONGODB_COLLECTION)
        mock_collection.save.assert_called_once()
        self.assertEquals(sentinel.result, ret_val)

    @patch("celery.backends.mongodb.MongoBackend._get_database")
    def test_restore_taskset(self, mock_get_database):
        self.backend.mongodb_taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()
        mock_collection.find_one.return_value = MagicMock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        ret_val = self.backend._restore_taskset(sentinel.taskset_id)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(MONGODB_COLLECTION)
        mock_collection.find_one.assert_called_once_with(
            {"_id": sentinel.taskset_id})
        self.assertEquals(['date_done', 'result', 'task_id'], ret_val.keys())

    @patch("celery.backends.mongodb.MongoBackend._get_database")
    def test_delete_taskset(self, mock_get_database):
        self.backend.mongodb_taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        ret_val = self.backend._delete_taskset(sentinel.taskset_id)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(MONGODB_COLLECTION)
        mock_collection.remove.assert_called_once_with(
            {"_id": sentinel.taskset_id})

    @patch("celery.backends.mongodb.MongoBackend._get_database")
    def test_forget(self, mock_get_database):
        self.backend.mongodb_taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        self.backend._forget(sentinel.task_id)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(
            MONGODB_COLLECTION)
        mock_collection.remove.assert_called_once_with(
            {"_id": sentinel.task_id}, safe=True)

    @patch("celery.backends.mongodb.MongoBackend._get_database")
    def test_cleanup(self, mock_get_database):
        self.backend.mongodb_taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection
        
        self.backend.cleanup()

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(
            MONGODB_COLLECTION)
        mock_collection.assert_called_once()
