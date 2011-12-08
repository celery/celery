import uuid

from mock import MagicMock, Mock, patch, sentinel
from nose import SkipTest

from celery.backends.mongodb import MongoBackend
from celery.tests.utils import unittest


try:
    import pymongo
except ImportError:
    pymongo = None


COLLECTION = "taskmeta_celery"
TASK_ID = str(uuid.uuid1())


class TestBackendMongoDb(unittest.TestCase):

    def setUp(self):
        if pymongo is None:
            raise SkipTest("pymongo is not installed.")

        self.backend = MongoBackend()
        self.backend.mongodb_taskmeta_collection = sentinel.collection

    @patch("celery.backends.mongodb.MongoBackend._get_database")
    def test_forget(self, mock_get_database):
        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        self.backend._forget(sentinel.task_id)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(sentinel.collection)
        mock_collection.remove.assert_called_once_with(
            {"_id": sentinel.task_id}, safe=True)

    def test_save__restore__delete_taskset(self):
        res = {u"foo": "bar"}
        self.assertEqual(self.backend.save_taskset(TASK_ID, res), res)

        res2 = self.backend.restore_taskset(TASK_ID)
        self.assertEqual(res2, res)

        self.backend.delete_taskset(TASK_ID)
        self.assertIsNone(self.backend.restore_taskset(TASK_ID))

        self.assertIsNone(self.backend.restore_taskset("xxx-nonexisting-id"))
