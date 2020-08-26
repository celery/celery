import datetime
from unittest.mock import Mock, call, patch, sentinel

import pytest
from billiard.einfo import ExceptionInfo
from kombu.utils.encoding import bytes_to_str

from celery import states

try:
    from elasticsearch import exceptions
except ImportError:
    exceptions = None

from celery.app import backends
from celery.backends import elasticsearch as module
from celery.backends.elasticsearch import ElasticsearchBackend
from celery.exceptions import ImproperlyConfigured

_RESULT_RETRY = (
    '{"status":"RETRY","result":'
    '{"exc_type":"Exception","exc_message":["failed"],"exc_module":"builtins"}}'
)
_RESULT_FAILURE = (
    '{"status":"FAILURE","result":'
    '{"exc_type":"Exception","exc_message":["failed"],"exc_module":"builtins"}}'
)

pytest.importorskip('elasticsearch')


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

    def test_get_task_not_found_without_throw(self):
        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        # this should not happen as if not found elasticsearch python library
        # will raise elasticsearch.exceptions.NotFoundError.
        x._server.get.return_value = {'_index': 'celery', '_type': '_doc', '_id': 'toto', 'found': False}

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
            '_source': {"result": _RESULT_RETRY},
            '_seq_no': 2,
            '_primary_term': 1,
        }

        x._server.update.return_value = {
            'result': 'updated'
        }

        x._set_with_state(sentinel.task_id, sentinel.result, sentinel.state)

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
    def test_index_conflict_without_state(self, datetime_mock):
        expected_dt = datetime.datetime(2020, 6, 1, 18, 43, 24, 123456, None)
        datetime_mock.utcnow.return_value = expected_dt

        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        x._server.index.side_effect = [
            exceptions.ConflictError(409, "concurrent update", {})
        ]

        x._server.get.return_value = {
            'found': True,
            '_source': {"result": _RESULT_RETRY},
            '_seq_no': 2,
            '_primary_term': 1,
        }

        x._server.update.return_value = {
            'result': 'updated'
        }

        x.set(sentinel.task_id, sentinel.result)

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
    def test_index_conflict_with_ready_state_on_backend_without_state(self, datetime_mock):
        """Even if the backend already have a ready state saved (FAILURE in this test case)
        as we are calling ElasticsearchBackend.set directly, it does not have state,
        so it cannot protect overriding a ready state by any other state.
        As a result, server.update will be called no matter what.
        """
        expected_dt = datetime.datetime(2020, 6, 1, 18, 43, 24, 123456, None)
        datetime_mock.utcnow.return_value = expected_dt

        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        x._server.index.side_effect = [
            exceptions.ConflictError(409, "concurrent update", {})
        ]

        x._server.get.return_value = {
            'found': True,
            '_source': {"result": _RESULT_FAILURE},
            '_seq_no': 2,
            '_primary_term': 1,
        }

        x._server.update.return_value = {
            'result': 'updated'
        }

        x.set(sentinel.task_id, sentinel.result)

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

        x._set_with_state(sentinel.task_id, sentinel.result, sentinel.state)

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
            '_source': {"result": _RESULT_FAILURE},
            '_seq_no': 2,
            '_primary_term': 1,
        }

        x._server.update.return_value = {
            'result': 'updated'
        }

        x._set_with_state(sentinel.task_id, sentinel.result, states.RETRY)

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
        x_server_get_side_effect = [
            {
                'found': True,
                '_source': {'result': _RESULT_RETRY},
                '_seq_no': 2,
                '_primary_term': 1,
            },
            {
                'found': True,
                '_source': {'result': _RESULT_RETRY},
                '_seq_no': 2,
                '_primary_term': 1,
            },
            {
                'found': True,
                '_source': {'result': _RESULT_FAILURE},
                '_seq_no': 3,
                '_primary_term': 1,
            },
            {
                'found': True,
                '_source': {'result': _RESULT_FAILURE},
                '_seq_no': 3,
                '_primary_term': 1,
            },
        ]

        try:
            x = ElasticsearchBackend(app=self.app)

            task_id = str(sentinel.task_id)
            encoded_task_id = bytes_to_str(x.get_key_for_task(task_id))
            result = str(sentinel.result)

            sleep_mock = Mock()
            x._sleep = sleep_mock
            x._server = Mock()
            x._server.index.side_effect = exceptions.ConflictError(409, "concurrent update", {})
            x._server.get.side_effect = x_server_get_side_effect
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

    @patch('celery.backends.elasticsearch.datetime')
    @patch('celery.backends.base.datetime')
    def test_backend_index_conflicting_document_removed(self, base_datetime_mock, es_datetime_mock):
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
            x._server.index.side_effect = [
                exceptions.ConflictError(409, "concurrent update", {}),
                {'result': 'created'}
            ]

            x._server.get.side_effect = [
                {
                    'found': True,
                    '_source': {"result": _RESULT_RETRY},
                    '_seq_no': 2,
                    '_primary_term': 1,
                },
                exceptions.NotFoundError(404,
                                         '{"_index":"celery","_type":"_doc","_id":"toto","found":false}',
                                         {'_index': 'celery', '_type': '_doc',
                                          '_id': 'toto', 'found': False}),
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
            x._server.update.assert_not_called()
            sleep_mock.assert_not_called()
        finally:
            self.app.conf.result_backend_always_retry = prev

    @patch('celery.backends.elasticsearch.datetime')
    @patch('celery.backends.base.datetime')
    def test_backend_index_conflicting_document_removed_not_throwing(self, base_datetime_mock, es_datetime_mock):
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
            x._server.index.side_effect = [
                exceptions.ConflictError(409, "concurrent update", {}),
                {'result': 'created'}
            ]

            x._server.get.side_effect = [
                {
                    'found': True,
                    '_source': {'result': _RESULT_RETRY},
                    '_seq_no': 2,
                    '_primary_term': 1,
                },
                {'_index': 'celery', '_type': '_doc', '_id': 'toto', 'found': False},
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
            x._server.update.assert_not_called()
            sleep_mock.assert_not_called()
        finally:
            self.app.conf.result_backend_always_retry = prev

    @patch('celery.backends.elasticsearch.datetime')
    @patch('celery.backends.base.datetime')
    def test_backend_index_corrupted_conflicting_document(self, base_datetime_mock, es_datetime_mock):
        expected_dt = datetime.datetime(2020, 6, 1, 18, 43, 24, 123456, None)
        es_datetime_mock.utcnow.return_value = expected_dt

        expected_done_dt = datetime.datetime(2020, 6, 1, 18, 45, 34, 654321, None)
        base_datetime_mock.utcnow.return_value = expected_done_dt

        # self.app.conf.result_backend_always_retry, prev = True, self.app.conf.result_backend_always_retry
        # try:
        x = ElasticsearchBackend(app=self.app)

        task_id = str(sentinel.task_id)
        encoded_task_id = bytes_to_str(x.get_key_for_task(task_id))
        result = str(sentinel.result)

        sleep_mock = Mock()
        x._sleep = sleep_mock
        x._server = Mock()
        x._server.index.side_effect = [
            exceptions.ConflictError(409, "concurrent update", {})
        ]

        x._server.update.side_effect = [
            {'result': 'updated'}
        ]

        x._server.get.return_value = {
            'found': True,
            '_source': {},
            '_seq_no': 2,
            '_primary_term': 1,
        }

        result_meta = x._get_result_meta(result, states.SUCCESS, None, None)
        result_meta['task_id'] = bytes_to_str(task_id)

        expected_result = x.encode(result_meta)

        x.store_result(task_id, result, states.SUCCESS)
        x._server.index.assert_called_once_with(
            id=encoded_task_id,
            index=x.index,
            doc_type=x.doc_type,
            body={
                'result': expected_result,
                '@timestamp': expected_dt.isoformat()[:-3] + 'Z'
            },
            params={'op_type': 'create'}
        )
        x._server.update.assert_called_once_with(
            id=encoded_task_id,
            index=x.index,
            doc_type=x.doc_type,
            body={
                'doc': {
                    'result': expected_result,
                    '@timestamp': expected_dt.isoformat()[:-3] + 'Z'
                }
            },
            params={'if_primary_term': 1, 'if_seq_no': 2}
        )
        sleep_mock.assert_not_called()

    def test_backend_params_by_url(self):
        url = 'elasticsearch://localhost:9200/index/doc_type'
        with self.Celery(backend=url) as app:
            x = app.backend

            assert x.index == 'index'
            assert x.doc_type == 'doc_type'
            assert x.scheme == 'http'
            assert x.host == 'localhost'
            assert x.port == 9200

    def test_backend_url_no_params(self):
        url = 'elasticsearch:///'
        with self.Celery(backend=url) as app:
            x = app.backend

            assert x.index == 'celery'
            assert x.doc_type == 'backend'
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

    def test_encode_as_json(self):
        self.app.conf.elasticsearch_save_meta_as_text, prev = False, self.app.conf.elasticsearch_save_meta_as_text
        try:
            x = ElasticsearchBackend(app=self.app)
            result_meta = x._get_result_meta({'solution': 42}, states.SUCCESS, None, None)
            assert x.encode(result_meta) == result_meta
        finally:
            self.app.conf.elasticsearch_save_meta_as_text = prev

    def test_encode_none_as_json(self):
        self.app.conf.elasticsearch_save_meta_as_text, prev = False, self.app.conf.elasticsearch_save_meta_as_text
        try:
            x = ElasticsearchBackend(app=self.app)
            result_meta = x._get_result_meta(None, states.SUCCESS, None, None)
            assert x.encode(result_meta) == result_meta
        finally:
            self.app.conf.elasticsearch_save_meta_as_text = prev

    def test_encode_exception_as_json(self):
        self.app.conf.elasticsearch_save_meta_as_text, prev = False, self.app.conf.elasticsearch_save_meta_as_text
        try:
            x = ElasticsearchBackend(app=self.app)
            try:
                raise Exception("failed")
            except Exception as exc:
                einfo = ExceptionInfo()
                result_meta = x._get_result_meta(
                    x.encode_result(exc, states.FAILURE),
                    states.FAILURE,
                    einfo.traceback,
                    None,
                )
                assert x.encode(result_meta) == result_meta
        finally:
            self.app.conf.elasticsearch_save_meta_as_text = prev

    def test_decode_from_json(self):
        self.app.conf.elasticsearch_save_meta_as_text, prev = False, self.app.conf.elasticsearch_save_meta_as_text
        try:
            x = ElasticsearchBackend(app=self.app)
            result_meta = x._get_result_meta({'solution': 42}, states.SUCCESS, None, None)
            result_meta['result'] = x._encode(result_meta['result'])[2]
            assert x.decode(result_meta) == result_meta
        finally:
            self.app.conf.elasticsearch_save_meta_as_text = prev

    def test_decode_none_from_json(self):
        self.app.conf.elasticsearch_save_meta_as_text, prev = False, self.app.conf.elasticsearch_save_meta_as_text
        try:
            x = ElasticsearchBackend(app=self.app)
            result_meta = x._get_result_meta(None, states.SUCCESS, None, None)
            # result_meta['result'] = x._encode(result_meta['result'])[2]
            assert x.decode(result_meta) == result_meta
        finally:
            self.app.conf.elasticsearch_save_meta_as_text = prev

    def test_decode_encoded_from_json(self):
        self.app.conf.elasticsearch_save_meta_as_text, prev = False, self.app.conf.elasticsearch_save_meta_as_text
        try:
            x = ElasticsearchBackend(app=self.app)
            result_meta = x._get_result_meta({'solution': 42}, states.SUCCESS, None, None)
            assert x.decode(x.encode(result_meta)) == result_meta
        finally:
            self.app.conf.elasticsearch_save_meta_as_text = prev

    def test_decode_encoded_exception_as_json(self):
        self.app.conf.elasticsearch_save_meta_as_text, prev = False, self.app.conf.elasticsearch_save_meta_as_text
        try:
            x = ElasticsearchBackend(app=self.app)
            try:
                raise Exception("failed")
            except Exception as exc:
                einfo = ExceptionInfo()
                result_meta = x._get_result_meta(
                    x.encode_result(exc, states.FAILURE),
                    states.FAILURE,
                    einfo.traceback,
                    None,
                )
                assert x.decode(x.encode(result_meta)) == result_meta
        finally:
            self.app.conf.elasticsearch_save_meta_as_text = prev

    @patch("celery.backends.base.KeyValueStoreBackend.decode")
    def test_decode_not_dict(self, kv_decode_mock):
        self.app.conf.elasticsearch_save_meta_as_text, prev = False, self.app.conf.elasticsearch_save_meta_as_text
        try:
            kv_decode_mock.return_value = sentinel.decoded
            x = ElasticsearchBackend(app=self.app)
            assert x.decode(sentinel.encoded) == sentinel.decoded
            kv_decode_mock.assert_called_once()
        finally:
            self.app.conf.elasticsearch_save_meta_as_text = prev

    def test_config_params(self):
        self.app.conf.elasticsearch_max_retries = 10
        self.app.conf.elasticsearch_timeout = 20.0
        self.app.conf.elasticsearch_retry_on_timeout = True

        self.backend = ElasticsearchBackend(app=self.app)

        assert self.backend.es_max_retries == 10
        assert self.backend.es_timeout == 20.0
        assert self.backend.es_retry_on_timeout is True

    def test_lazy_server_init(self):
        x = ElasticsearchBackend(app=self.app)
        x._get_server = Mock()
        x._get_server.return_value = sentinel.server

        assert x.server == sentinel.server
        x._get_server.assert_called_once()

    def test_mget(self):
        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        x._server.get.side_effect = [
            {'found': True, '_id': sentinel.task_id1, '_source': {'result': sentinel.result1}},
            {'found': True, '_id': sentinel.task_id2, '_source': {'result': sentinel.result2}},
        ]
        assert x.mget([sentinel.task_id1, sentinel.task_id2]) == [sentinel.result1, sentinel.result2]
        x._server.get.assert_has_calls([
            call(index=x.index, doc_type=x.doc_type, id=sentinel.task_id1),
            call(index=x.index, doc_type=x.doc_type, id=sentinel.task_id2),
        ])

    def test_exception_safe_to_retry(self):
        x = ElasticsearchBackend(app=self.app)
        assert not x.exception_safe_to_retry(Exception("failed"))
        assert not x.exception_safe_to_retry(BaseException("failed"))
        assert x.exception_safe_to_retry(exceptions.ConflictError(409, "concurrent update", {}))
        assert x.exception_safe_to_retry(exceptions.ConnectionError(503, "service unavailable", {}))
        assert x.exception_safe_to_retry(exceptions.TransportError(429, "too many requests", {}))
        assert not x.exception_safe_to_retry(exceptions.NotFoundError(404, "not found", {}))
