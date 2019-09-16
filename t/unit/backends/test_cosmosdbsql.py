from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock, call, patch, skip
from celery.backends import cosmosdbsql
from celery.backends.cosmosdbsql import CosmosDBSQLBackend
from celery.exceptions import ImproperlyConfigured

MODULE_TO_MOCK = "celery.backends.cosmosdbsql"


@skip.unless_module("pydocumentdb")
class test_DocumentDBBackend:
    def setup(self):
        self.url = "cosmosdbsql://:key@endpoint"
        self.backend = CosmosDBSQLBackend(app=self.app, url=self.url)

    def test_missing_third_party_sdk(self):
        pydocumentdb = cosmosdbsql.pydocumentdb
        try:
            cosmosdbsql.pydocumentdb = None
            with pytest.raises(ImproperlyConfigured):
                CosmosDBSQLBackend(app=self.app, url=self.url)
        finally:
            cosmosdbsql.pydocumentdb = pydocumentdb

    def test_bad_connection_url(self):
        with pytest.raises(ImproperlyConfigured):
            CosmosDBSQLBackend._parse_url(
                "cosmosdbsql://:key@")

        with pytest.raises(ImproperlyConfigured):
            CosmosDBSQLBackend._parse_url(
                "cosmosdbsql://:@host")

        with pytest.raises(ImproperlyConfigured):
            CosmosDBSQLBackend._parse_url(
                "cosmosdbsql://corrupted")

    def test_default_connection_url(self):
        endpoint, password = CosmosDBSQLBackend._parse_url(
            "cosmosdbsql://:key@host")

        assert password == "key"
        assert endpoint == "https://host:443"

        endpoint, password = CosmosDBSQLBackend._parse_url(
            "cosmosdbsql://:key@host:443")

        assert password == "key"
        assert endpoint == "https://host:443"

        endpoint, password = CosmosDBSQLBackend._parse_url(
            "cosmosdbsql://:key@host:8080")

        assert password == "key"
        assert endpoint == "http://host:8080"

    def test_bad_partition_key(self):
        with pytest.raises(ValueError):
            CosmosDBSQLBackend._get_partition_key("")

        with pytest.raises(ValueError):
            CosmosDBSQLBackend._get_partition_key("   ")

        with pytest.raises(ValueError):
            CosmosDBSQLBackend._get_partition_key(None)

    def test_bad_consistency_level(self):
        with pytest.raises(ImproperlyConfigured):
            CosmosDBSQLBackend(app=self.app, url=self.url,
                               consistency_level="DoesNotExist")

    @patch(MODULE_TO_MOCK + ".DocumentClient")
    def test_create_client(self, mock_factory):
        mock_instance = Mock()
        mock_factory.return_value = mock_instance
        backend = CosmosDBSQLBackend(app=self.app, url=self.url)

        # ensure database and collection get created on client access...
        assert mock_instance.CreateDatabase.call_count == 0
        assert mock_instance.CreateCollection.call_count == 0
        assert backend._client is not None
        assert mock_instance.CreateDatabase.call_count == 1
        assert mock_instance.CreateCollection.call_count == 1

        # ...but only once per backend instance
        assert backend._client is not None
        assert mock_instance.CreateDatabase.call_count == 1
        assert mock_instance.CreateCollection.call_count == 1

    @patch(MODULE_TO_MOCK + ".CosmosDBSQLBackend._client")
    def test_get(self, mock_client):
        self.backend.get(b"mykey")

        mock_client.ReadDocument.assert_has_calls(
            [call("dbs/celerydb/colls/celerycol/docs/mykey",
                  {"partitionKey": "mykey"}),
             call().get("value")])

    @patch(MODULE_TO_MOCK + ".CosmosDBSQLBackend._client")
    def test_get_missing(self, mock_client):
        mock_client.ReadDocument.side_effect = \
            cosmosdbsql.HTTPFailure(cosmosdbsql.ERROR_NOT_FOUND)

        assert self.backend.get(b"mykey") is None

    @patch(MODULE_TO_MOCK + ".CosmosDBSQLBackend._client")
    def test_set(self, mock_client):
        self.backend.set(b"mykey", "myvalue")

        mock_client.CreateDocument.assert_called_once_with(
            "dbs/celerydb/colls/celerycol",
            {"id": "mykey", "value": "myvalue"},
            {"partitionKey": "mykey"})

    @patch(MODULE_TO_MOCK + ".CosmosDBSQLBackend._client")
    def test_mget(self, mock_client):
        keys = [b"mykey1", b"mykey2"]

        self.backend.mget(keys)

        mock_client.ReadDocument.assert_has_calls(
            [call("dbs/celerydb/colls/celerycol/docs/mykey1",
                  {"partitionKey": "mykey1"}),
             call().get("value"),
             call("dbs/celerydb/colls/celerycol/docs/mykey2",
                  {"partitionKey": "mykey2"}),
             call().get("value")])

    @patch(MODULE_TO_MOCK + ".CosmosDBSQLBackend._client")
    def test_delete(self, mock_client):
        self.backend.delete(b"mykey")

        mock_client.DeleteDocument.assert_called_once_with(
            "dbs/celerydb/colls/celerycol/docs/mykey",
            {"partitionKey": "mykey"})
