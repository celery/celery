"""Tests for the ArangoDb."""
import datetime
from unittest.mock import MagicMock, Mock, patch, sentinel

import pytest

from celery.app import backends
from celery.backends import arangodb as module
from celery.backends.arangodb import ArangoDbBackend
from celery.exceptions import ImproperlyConfigured

try:
    import pyArango
except ImportError:
    pyArango = None

pytest.importorskip('pyArango')


class test_ArangoDbBackend:

    def setup_method(self):
        self.backend = ArangoDbBackend(app=self.app)

    def test_init_no_arangodb(self):
        prev, module.py_arango_connection = module.py_arango_connection, None
        try:
            with pytest.raises(ImproperlyConfigured):
                ArangoDbBackend(app=self.app)
        finally:
            module.py_arango_connection = prev

    def test_init_no_settings(self):
        self.app.conf.arangodb_backend_settings = []
        with pytest.raises(ImproperlyConfigured):
            ArangoDbBackend(app=self.app)

    def test_init_settings_is_None(self):
        self.app.conf.arangodb_backend_settings = None
        ArangoDbBackend(app=self.app)

    def test_init_url(self):
        url = None
        expected_database = "celery"
        expected_collection = "celery"
        backend = ArangoDbBackend(app=self.app, url=url)
        assert backend.database == expected_database
        assert backend.collection == expected_collection

        url = "arangodb://localhost:27017/celery-database/celery-collection"
        expected_database = "celery-database"
        expected_collection = "celery-collection"
        backend = ArangoDbBackend(app=self.app, url=url)
        assert backend.database == expected_database
        assert backend.collection == expected_collection

    def test_get_connection_connection_exists(self):
        with patch('pyArango.connection.Connection') as mock_Connection:
            self.backend._connection = sentinel.connection
            connection = self.backend.connection
            assert connection == sentinel.connection
            mock_Connection.assert_not_called()

            expected_connection = mock_Connection()
            mock_Connection.reset_mock()  # So the assert_called_once below is accurate.
            self.backend._connection = None
            connection = self.backend.connection
            assert connection == expected_connection
            mock_Connection.assert_called_once()

    def test_get(self):
        self.backend._connection = MagicMock(spec=["__getitem__"])

        assert self.backend.get(None) is None
        self.backend.db.AQLQuery.assert_not_called()

        assert self.backend.get(sentinel.task_id) is None
        self.backend.db.AQLQuery.assert_called_once_with(
            "RETURN DOCUMENT(@@collection, @key).task",
            rawResults=True,
            bindVars={
                "@collection": self.backend.collection,
                "key": sentinel.task_id,
            },
        )

        self.backend.get = Mock(return_value=sentinel.retval)
        assert self.backend.get(sentinel.task_id) == sentinel.retval
        self.backend.get.assert_called_once_with(sentinel.task_id)

    def test_set(self):
        self.backend._connection = MagicMock(spec=["__getitem__"])

        assert self.backend.set(sentinel.key, sentinel.value) is None
        self.backend.db.AQLQuery.assert_called_once_with(
            """
            UPSERT {_key: @key}
            INSERT {_key: @key, task: @value}
            UPDATE {task: @value} IN @@collection
            """,
            bindVars={
                "@collection": self.backend.collection,
                "key": sentinel.key,
                "value": sentinel.value,
            },
        )

    def test_mget(self):
        self.backend._connection = MagicMock(spec=["__getitem__"])

        result = list(self.backend.mget(None))
        expected_result = []
        assert result == expected_result
        self.backend.db.AQLQuery.assert_not_called()

        Query = MagicMock(spec=pyArango.query.Query)
        query = Query()
        query.nextBatch = MagicMock(side_effect=StopIteration())
        self.backend.db.AQLQuery = Mock(return_value=query)

        keys = [sentinel.task_id_0, sentinel.task_id_1]
        result = list(self.backend.mget(keys))
        expected_result = []
        assert result == expected_result
        self.backend.db.AQLQuery.assert_called_once_with(
            "FOR k IN @keys RETURN DOCUMENT(@@collection, k).task",
            rawResults=True,
            bindVars={
                "@collection": self.backend.collection,
                "keys": keys,
            },
        )

        values = [sentinel.value_0, sentinel.value_1]
        query.__iter__.return_value = iter([sentinel.value_0, sentinel.value_1])
        result = list(self.backend.mget(keys))
        expected_result = values
        assert result == expected_result

    def test_delete(self):
        self.backend._connection = MagicMock(spec=["__getitem__"])

        assert self.backend.delete(None) is None
        self.backend.db.AQLQuery.assert_not_called()

        assert self.backend.delete(sentinel.task_id) is None
        self.backend.db.AQLQuery.assert_called_once_with(
            "REMOVE {_key: @key} IN @@collection",
            bindVars={
                "@collection": self.backend.collection,
                "key": sentinel.task_id,
            },
        )

    def test_config_params(self):
        self.app.conf.arangodb_backend_settings = {
            'host': 'test.arangodb.com',
            'port': '8529',
            'username': 'johndoe',
            'password': 'mysecret',
            'database': 'celery_database',
            'collection': 'celery_collection',
            'http_protocol': 'https',
            'verify': True
        }
        x = ArangoDbBackend(app=self.app)
        assert x.host == 'test.arangodb.com'
        assert x.port == 8529
        assert x.username == 'johndoe'
        assert x.password == 'mysecret'
        assert x.database == 'celery_database'
        assert x.collection == 'celery_collection'
        assert x.http_protocol == 'https'
        assert x.arangodb_url == 'https://test.arangodb.com:8529'
        assert x.verify is True

    def test_backend_by_url(
        self, url="arangodb://username:password@host:port/database/collection"
    ):
        from celery.backends.arangodb import ArangoDbBackend
        backend, url_ = backends.by_url(url, self.app.loader)
        assert backend is ArangoDbBackend
        assert url_ == url

    def test_backend_params_by_url(self):
        url = (
            "arangodb://johndoe:mysecret@test.arangodb.com:8529/"
            "celery_database/celery_collection"
        )
        with self.Celery(backend=url) as app:
            x = app.backend
            assert x.host == 'test.arangodb.com'
            assert x.port == 8529
            assert x.username == 'johndoe'
            assert x.password == 'mysecret'
            assert x.database == 'celery_database'
            assert x.collection == 'celery_collection'
            assert x.http_protocol == 'http'
            assert x.arangodb_url == 'http://test.arangodb.com:8529'
            assert x.verify is False

    def test_backend_cleanup(self):
        self.backend._connection = MagicMock(spec=["__getitem__"])

        self.backend.expires = None
        self.backend.cleanup()
        self.backend.db.AQLQuery.assert_not_called()

        self.backend.expires = 0
        self.backend.cleanup()
        self.backend.db.AQLQuery.assert_not_called()

        now = datetime.datetime.utcnow()
        self.backend.app.now = Mock(return_value=now)
        self.backend.expires = 86400
        expected_checkpoint = (now - self.backend.expires_delta).isoformat()
        self.backend.cleanup()
        self.backend.db.AQLQuery.assert_called_once_with(
            """
            FOR record IN @@collection
                FILTER record.task.date_done < @checkpoint
                REMOVE record IN @@collection
            """,
            bindVars={
                "@collection": self.backend.collection,
                "checkpoint": expected_checkpoint,
            },
        )
