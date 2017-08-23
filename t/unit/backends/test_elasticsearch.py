from __future__ import absolute_import, unicode_literals
import pytest
from case import Mock, sentinel, skip
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
        r = dict(found=True, _source={'result': sentinel.result})
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

    def test_backend_params_by_url(self):
        url = 'elasticsearch://localhost:9200/index/doc_type'
        with self.Celery(backend=url) as app:
            x = app.backend

            assert x.index == 'index'
            assert x.doc_type == 'doc_type'
            assert x.scheme == 'elasticsearch'
            assert x.host == 'localhost'
            assert x.port == 9200

    def test_index(self):
        x = ElasticsearchBackend(app=self.app)
        x.doc_type = 'test-doc-type'
        x._server = Mock()
        x._server.index = Mock()
        expected_result = dict(
            _id=sentinel.task_id,
            _source={'result': sentinel.result}
        )
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
            kwarg1='test1'
        )

    def test_index_bytes_key(self):
        x = ElasticsearchBackend(app=self.app)
        x.doc_type = 'test-doc-type'
        x._server = Mock()
        x._server.index = Mock()
        expected_result = dict(
            _id=sentinel.task_id,
            _source={'result': sentinel.result}
        )
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
