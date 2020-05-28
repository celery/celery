"""Tests for the ArangoDb."""
from __future__ import absolute_import, unicode_literals

import datetime

import pytest
from case import Mock, patch, sentinel, skip

from celery.app import backends
from celery.backends import arangodb as module
from celery.backends.arangodb import ArangoDbBackend
from celery.exceptions import ImproperlyConfigured

try:
    import pyArango
except ImportError:
    pyArango = None  # noqa


@skip.unless_module('pyArango')
class test_ArangoDbBackend:

    def setup(self):
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

    def test_get_connection_connection_exists(self):
        with patch('pyArango.connection.Connection') as mock_Connection:
            self.backend._connection = sentinel._connection

            connection = self.backend._connection

            assert sentinel._connection == connection
            mock_Connection.assert_not_called()

    def test_get(self):
        self.app.conf.arangodb_backend_settings = {}
        x = ArangoDbBackend(app=self.app)
        x.get = Mock()
        x.get.return_value = sentinel.retval
        assert x.get('1f3fab') == sentinel.retval
        x.get.assert_called_once_with('1f3fab')

    def test_delete(self):
        self.app.conf.arangodb_backend_settings = {}
        x = ArangoDbBackend(app=self.app)
        x.delete = Mock()
        x.delete.return_value = None
        assert x.delete('1f3fab') is None

    def test_config_params(self):
        self.app.conf.arangodb_backend_settings = {
            'host': 'test.arangodb.com',
            'port': '8529',
            'username': 'johndoe',
            'password': 'mysecret',
            'database': 'celery_database',
            'collection': 'celery_collection',
            'http_protocol': 'https'
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

    def test_backend_cleanup(self):
        now = datetime.datetime.utcnow()
        self.backend.app.now = Mock(return_value=now)
        self.backend._connection = {
            'celery': Mock(),
        }

        self.backend.cleanup()

        expected_date = (now - self.backend.expires_delta).isoformat()
        expected_query = (
            'FOR item IN celery '
            'FILTER item.task.date_done < "{date}" '
            'REMOVE item IN celery'
        ).format(date=expected_date)
        self.backend.db.AQLQuery.assert_called_once_with(expected_query)
