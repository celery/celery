from __future__ import absolute_import, unicode_literals

import pytest
from case import Mock, patch, sentinel, skip, call
from celery import states
import datetime
from elasticsearch import exceptions
from kombu.utils.encoding import bytes_to_str

from celery.app import backends
from celery.backends import elasticsearch as module
from celery.backends.elasticsearch import ElasticsearchBackend
from celery.exceptions import ImproperlyConfigured


@skip.unless_module('elasticsearch')
class test_ElasticsearchBackend:

    def setup(self):
        self.backend = ElasticsearchBackend(app=self.app)

    def test_init_no_elasticsearch(self):
        prev, module.elasticsearch = module.elasticsearch, None
        try:
            with pytest.raises(ImproperlyConfigured):
                ElasticsearchBackend(app=self.app)
        finally:
            module.elasticsearch = prev

    def test_get(self):
        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        x._server.get = Mock()
        # expected result
        r = {'found': True, '_source': {'result': sentinel.result}}
        x._server.get.return_value = r
        dict_result = x.get(sentinel.task_id)

        assert dict_result == sentinel.result
        x._server.get.assert_called_once_with(
            doc_type=x.doc_type,
            id=sentinel.task_id,
            index=x.index,
        )

    def test_get_none(self):
        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        x._server.get = Mock()
        x._server.get.return_value = sentinel.result
        none_result = x.get(sentinel.task_id)

        assert none_result is None
        x._server.get.assert_called_once_with(
            doc_type=x.doc_type,
            id=sentinel.task_id,
            index=x.index,
        )

    def test_get_task_not_found(self):
        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        x._server.get.side_effect = [
            exceptions.NotFoundError(404, '{"_index":"celery","_type":"_doc","_id":"toto","found":false}',
                                     {'_index': 'celery', '_type': '_doc', '_id': 'toto', 'found': False})
        ]

        res = x.get(sentinel.task_id)
        assert res is None

    def test_delete(self):
        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        x._server.delete = Mock()
        x._server.delete.return_value = sentinel.result

        assert x.delete(sentinel.task_id) is None
        x._server.delete.assert_called_once_with(
            doc_type=x.doc_type,
            id=sentinel.task_id,
            index=x.index,
        )

    def test_backend_by_url(self, url='elasticsearch://localhost:9200/index'):
        backend, url_ = backends.by_url(url, self.app.loader)

        assert backend is ElasticsearchBackend
        assert url_ == url

    @patch('celery.backends.elasticsearch.datetime')
    def test_index_conflict(self, datetime_mock):
        expected_dt = datetime.datetime(2020, 6, 1, 18, 43, 24, 123456, None)
        datetime_mock.utcnow.return_value = expected_dt

        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        x._server.index.side_effect = [
            exceptions.ConflictError(409, "concurrent update", {})
        ]

        x._server.get.return_value = {
            'found': True,
            '_source': {
                'result': """{"status":"RETRY","result":{"exc_type":"Exception","exc_message":["failed"],"exc_module":"builtins"}}"""
            },
            '_seq_no': 2,
            '_primary_term': 1,
        }

        x._server.update.return_value = {
            'result': 'updated'
        }

        x.set(sentinel.task_id, sentinel.result, sentinel.state)

        assert x._server.get.call_count == 1
        x._server.index.assert_called_once_with(
            id=sentinel.task_id,
            index=x.index,
            doc_type=x.doc_type,
            body={'result': sentinel.result, '@timestamp': expected_dt.isoformat()[:-3] + 'Z'},
            params={'op_type': 'create'},
        )
        x._server.update.assert_called_once_with(
            id=sentinel.task_id,
            index=x.index,
            doc_type=x.doc_type,
            body={'doc': {'result': sentinel.result, '@timestamp': expected_dt.isoformat()[:-3] + 'Z'}},
            params={'if_seq_no': 2, 'if_primary_term': 1}
        )

    @patch('celery.backends.elasticsearch.datetime')
    def test_index_conflict_with_existing_success(self, datetime_mock):
        expected_dt = datetime.datetime(2020, 6, 1, 18, 43, 24, 123456, None)
        datetime_mock.utcnow.return_value = expected_dt

        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        x._server.index.side_effect = [
            exceptions.ConflictError(409, "concurrent update", {})
        ]

        x._server.get.return_value = {
            'found': True,
            '_source': {
                'result': """{"status":"SUCCESS","result":42}"""
            },
            '_seq_no': 2,
            '_primary_term': 1,
        }

        x._server.update.return_value = {
            'result': 'updated'
        }

        x.set(sentinel.task_id, sentinel.result, sentinel.state)

        assert x._server.get.call_count == 1
        x._server.index.assert_called_once_with(
            id=sentinel.task_id,
            index=x.index,
            doc_type=x.doc_type,
            body={'result': sentinel.result, '@timestamp': expected_dt.isoformat()[:-3] + 'Z'},
            params={'op_type': 'create'},
        )
        x._server.update.assert_not_called()

    @patch('celery.backends.elasticsearch.datetime')
    def test_index_conflict_with_existing_ready_state(self, datetime_mock):
        expected_dt = datetime.datetime(2020, 6, 1, 18, 43, 24, 123456, None)
        datetime_mock.utcnow.return_value = expected_dt

        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        x._server.index.side_effect = [
            exceptions.ConflictError(409, "concurrent update", {})
        ]

        x._server.get.return_value = {
            'found': True,
            '_source': {
                'result': """{"status":"FAILURE","result":{"exc_type":"Exception","exc_message":["failed"],"exc_module":"builtins"}}"""
            },
            '_seq_no': 2,
            '_primary_term': 1,
        }

        x._server.update.return_value = {
            'result': 'updated'
        }

        x.set(sentinel.task_id, sentinel.result, states.RETRY)

        assert x._server.get.call_count == 1
        x._server.index.assert_called_once_with(
            id=sentinel.task_id,
            index=x.index,
            doc_type=x.doc_type,
            body={'result': sentinel.result, '@timestamp': expected_dt.isoformat()[:-3] + 'Z'},
            params={'op_type': 'create'},
        )
        x._server.update.assert_not_called()

    @patch('celery.backends.elasticsearch.datetime')
    @patch('celery.backends.base.datetime')
    def test_backend_concurrent_update(self, base_datetime_mock, es_datetime_mock):
        expected_dt = datetime.datetime(2020, 6, 1, 18, 43, 24, 123456, None)
        es_datetime_mock.utcnow.return_value = expected_dt

        expected_done_dt = datetime.datetime(2020, 6, 1, 18, 45, 34, 654321, None)
        base_datetime_mock.utcnow.return_value = expected_done_dt

        self.app.conf.result_backend_always_retry, prev = True, self.app.conf.result_backend_always_retry
        try:
            x = ElasticsearchBackend(app=self.app)

            task_id = str(sentinel.task_id)
            encoded_task_id = bytes_to_str(x.get_key_for_task(task_id))
            result = str(sentinel.result)

            sleep_mock = Mock()
            x._sleep = sleep_mock
            x._server = Mock()
            x._server.index.side_effect = exceptions.ConflictError(409, "concurrent update", {})

            x._server.get.side_effect = [
                {
                    'found': True,
                    '_source': {
                        'result': """{"status":"RETRY","result":{"exc_type":"Exception","exc_message":["failed"],"exc_module":"builtins"}}"""
                    },
                    '_seq_no': 2,
                    '_primary_term': 1,
                },
                {
                    'found': True,
                    '_source': {
                        'result': """{"status":"RETRY","result":{"exc_type":"Exception","exc_message":["failed"],"exc_module":"builtins"}}"""
                    },
                    '_seq_no': 2,
                    '_primary_term': 1,
                },
                {
                    'found': True,
                    '_source': {
                        'result': """{"status":"FAILURE","result":{"exc_type":"Exception","exc_message":["failed"],"exc_module":"builtins"}}"""
                    },
                    '_seq_no': 3,
                    '_primary_term': 1,
                },
                {
                    'found': True,
                    '_source': {
                        'result': """{"status":"FAILURE","result":{"exc_type":"Exception","exc_message":["failed"],"exc_module":"builtins"}}"""
                    },
                    '_seq_no': 3,
                    '_primary_term': 1,
                },
            ]

            x._server.update.side_effect = [
                {'result': 'noop'},
                {'result': 'updated'}
            ]
            result_meta = x._get_result_meta(result, states.SUCCESS, None, None)
            result_meta['task_id'] = bytes_to_str(task_id)

            expected_result = x.encode(result_meta)

            x.store_result(task_id, result, states.SUCCESS)
            x._server.index.assert_has_calls([
                call(
                    id=encoded_task_id,
                    index=x.index,
                    doc_type=x.doc_type,
                    body={
                        'result': expected_result,
                        '@timestamp': expected_dt.isoformat()[:-3] + 'Z'
                    },
                    params={'op_type': 'create'}
                ),
                call(
                    id=encoded_task_id,
                    index=x.index,
                    doc_type=x.doc_type,
                    body={
                        'result': expected_result,
                        '@timestamp': expected_dt.isoformat()[:-3] + 'Z'
                    },
                    params={'op_type': 'create'}
                ),
            ])
            x._server.update.assert_has_calls([
                call(
                    id=encoded_task_id,
                    index=x.index,
                    doc_type=x.doc_type,
                    body={
                        'doc': {
                            'result': expected_result,
                            '@timestamp': expected_dt.isoformat()[:-3] + 'Z'
                        }
                    },
                    params={'if_seq_no': 2, 'if_primary_term': 1}
                ),
                call(
                    id=encoded_task_id,
                    index=x.index,
                    doc_type=x.doc_type,
                    body={
                        'doc': {
                            'result': expected_result,
                            '@timestamp': expected_dt.isoformat()[:-3] + 'Z'
                        }
                    },
                    params={'if_seq_no': 3, 'if_primary_term': 1}
                ),
            ])

            assert sleep_mock.call_count == 1
        finally:
            self.app.conf.result_backend_always_retry = prev

    def test_backend_params_by_url(self):
        url = 'elasticsearch://localhost:9200/index/doc_type'
        with self.Celery(backend=url) as app:
            x = app.backend

            assert x.index == 'index'
            assert x.doc_type == 'doc_type'
            assert x.scheme == 'http'
            assert x.host == 'localhost'
            assert x.port == 9200

    @patch('elasticsearch.Elasticsearch')
    def test_get_server_with_auth(self, mock_es_client):
        url = 'elasticsearch+https://fake_user:fake_pass@localhost:9200/index/doc_type'
        with self.Celery(backend=url) as app:
            x = app.backend

            assert x.username == 'fake_user'
            assert x.password == 'fake_pass'
            assert x.scheme == 'https'

            x._get_server()
            mock_es_client.assert_called_once_with(
                'localhost:9200',
                http_auth=('fake_user', 'fake_pass'),
                max_retries=x.es_max_retries,
                retry_on_timeout=x.es_retry_on_timeout,
                scheme='https',
                timeout=x.es_timeout,
            )

    @patch('elasticsearch.Elasticsearch')
    def test_get_server_without_auth(self, mock_es_client):
        url = 'elasticsearch://localhost:9200/index/doc_type'
        with self.Celery(backend=url) as app:
            x = app.backend
            x._get_server()
            mock_es_client.assert_called_once_with(
                'localhost:9200',
                http_auth=None,
                max_retries=x.es_max_retries,
                retry_on_timeout=x.es_retry_on_timeout,
                scheme='http',
                timeout=x.es_timeout,
            )

    def test_index(self):
        x = ElasticsearchBackend(app=self.app)
        x.doc_type = 'test-doc-type'
        x._server = Mock()
        x._server.index = Mock()
        expected_result = {
            '_id': sentinel.task_id,
            '_source': {'result': sentinel.result}
        }
        x._server.index.return_value = expected_result

        body = {"field1": "value1"}
        x._index(
            id=str(sentinel.task_id).encode(),
            body=body,
            kwarg1='test1'
        )
        x._server.index.assert_called_once_with(
            id=str(sentinel.task_id),
            doc_type=x.doc_type,
            index=x.index,
            body=body,
            params={'op_type': 'create'},
            kwarg1='test1'
        )

    def test_index_bytes_key(self):
        x = ElasticsearchBackend(app=self.app)
        x.doc_type = 'test-doc-type'
        x._server = Mock()
        x._server.index = Mock()
        expected_result = {
            '_id': sentinel.task_id,
            '_source': {'result': sentinel.result}
        }
        x._server.index.return_value = expected_result

        body = {b"field1": "value1"}
        x._index(
            id=str(sentinel.task_id).encode(),
            body=body,
            kwarg1='test1'
        )
        x._server.index.assert_called_once_with(
            id=str(sentinel.task_id),
            doc_type=x.doc_type,
            index=x.index,
            body={"field1": "value1"},
            params={'op_type': 'create'},
            kwarg1='test1'
        )

    def test_config_params(self):
        self.app.conf.elasticsearch_max_retries = 10
        self.app.conf.elasticsearch_timeout = 20.0
        self.app.conf.elasticsearch_retry_on_timeout = True

        self.backend = ElasticsearchBackend(app=self.app)

        assert self.backend.es_max_retries == 10
        assert self.backend.es_timeout == 20.0
        assert self.backend.es_retry_on_timeout is True
