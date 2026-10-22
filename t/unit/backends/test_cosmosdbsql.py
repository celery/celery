from unittest.mock import Mock, call, patch

import pytest

from celery import states, uuid
from celery.backends import cosmosdbsql
from celery.backends.cosmosdbsql import CosmosDBSQLBackend
from celery.exceptions import ImproperlyConfigured

MODULE_TO_MOCK = "celery.backends.cosmosdbsql"

pytest.importorskip('pydocumentdb')


def fake_document_store(mock_client):
    # Like CosmosDB, refuse to create a document whose id is taken.
    documents = {}

    def create_document(collection_link, document, options):
        if document["id"] in documents:
            raise cosmosdbsql.HTTPFailure(cosmosdbsql.ERROR_EXISTS)
        documents[document["id"]] = document

    def upsert_document(collection_link, document, options):
        documents[document["id"]] = document

    def read_document(document_link, options):
        try:
            return documents[options["partitionKey"]]
        except KeyError:
            raise cosmosdbsql.HTTPFailure(cosmosdbsql.ERROR_NOT_FOUND)

    mock_client.CreateDocument.side_effect = create_document
    mock_client.UpsertDocument.side_effect = upsert_document
    mock_client.ReadDocument.side_effect = read_document


class test_DocumentDBBackend:
    def setup_method(self):
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
        self.backend._set_with_state(b"mykey", "myvalue", states.SUCCESS)

        mock_client.UpsertDocument.assert_called_once_with(
            "dbs/celerydb/colls/celerycol",
            {"id": "mykey", "value": "myvalue"},
            {"partitionKey": "mykey"})

    @patch(MODULE_TO_MOCK + ".CosmosDBSQLBackend._client")
    def test_store_result_overwrites_earlier_state(self, mock_client):
        fake_document_store(mock_client)

        # A task stores STARTED (task_track_started), RETRY or a custom
        # update_state() before its final state, all under the same key.
        task_id = uuid()
        self.backend.mark_as_started(task_id)
        self.backend.mark_as_done(task_id, 42)

        meta = self.backend.get_task_meta(task_id, cache=False)
        assert meta["status"] == states.SUCCESS
        assert meta["result"] == 42

    @patch(MODULE_TO_MOCK + ".CosmosDBSQLBackend._client")
    def test_store_result_keeps_success(self, mock_client):
        fake_document_store(mock_client)

        # Once SUCCESS is stored, _store_result() skips later states, such
        # as STARTED from a redelivered task.
        task_id = uuid()
        self.backend.mark_as_done(task_id, 42)
        self.backend.mark_as_started(task_id)

        meta = self.backend.get_task_meta(task_id, cache=False)
        assert meta["status"] == states.SUCCESS
        assert meta["result"] == 42
        mock_client.UpsertDocument.assert_called_once()

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

    @patch(MODULE_TO_MOCK + ".CosmosDBSQLBackend._client")
    def test_forget_missing_task_is_noop(self, mock_client):
        mock_client.DeleteDocument.side_effect = \
            cosmosdbsql.HTTPFailure(cosmosdbsql.ERROR_NOT_FOUND)

        # forget() on a result that was expired, already forgotten or never
        # stored must not raise HTTPFailure
        self.backend.delete(b"mykey")

    @patch(MODULE_TO_MOCK + ".CosmosDBSQLBackend._client")
    def test_forget_twice_is_noop(self, mock_client):
        mock_client.DeleteDocument.side_effect = [
            None,
            cosmosdbsql.HTTPFailure(cosmosdbsql.ERROR_NOT_FOUND),
        ]

        self.backend.delete(b"mykey")
        self.backend.delete(b"mykey")

    @patch(MODULE_TO_MOCK + ".CosmosDBSQLBackend._client")
    def test_delete_reraises_other_http_failures(self, mock_client):
        mock_client.DeleteDocument.side_effect = \
            cosmosdbsql.HTTPFailure(500)

        with pytest.raises(cosmosdbsql.HTTPFailure):
            self.backend.delete(b"mykey")
