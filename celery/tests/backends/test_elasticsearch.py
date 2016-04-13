from __future__ import absolute_import, unicode_literals

from celery import backends
from celery.backends import elasticsearch as module
from celery.backends.elasticsearch import ElasticsearchBackend
from celery.exceptions import ImproperlyConfigured

from celery.tests.case import AppCase, Mock, sentinel, skip


@skip.unless_module('elasticsearch')
class test_ElasticsearchBackend(AppCase):

    def setup(self):
        self.backend = ElasticsearchBackend(app=self.app)

    def test_init_no_elasticsearch(self):
        prev, module.elasticsearch = module.elasticsearch, None
        try:
            with self.assertRaises(ImproperlyConfigured):
                ElasticsearchBackend(app=self.app)
        finally:
            module.elasticsearch = prev

    def test_get(self):
        x = ElasticsearchBackend(app=self.app)
        x._server = Mock()
        x._server.get = Mock()
        # expected result
        r = dict(found=True, _source={sentinel.task_id: sentinel.result})
        x._server.get.return_value = r
        dict_result = x.get(sentinel.task_id)

        self.assertEqual(dict_result, sentinel.result)
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

        self.assertEqual(none_result, None)
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

        self.assertIsNone(x.delete(sentinel.task_id), sentinel.result)
        x._server.delete.assert_called_once_with(
            doc_type=x.doc_type,
            id=sentinel.task_id,
            index=x.index,
        )

    def test_backend_by_url(self, url='elasticsearch://localhost:9200/index'):
        backend, url_ = backends.get_backend_by_url(url, self.app.loader)

        self.assertIs(backend, ElasticsearchBackend)
        self.assertEqual(url_, url)

    def test_backend_params_by_url(self):
        url = 'elasticsearch://localhost:9200/index/doc_type'
        with self.Celery(backend=url) as app:
            x = app.backend

            self.assertEqual(x.index, 'index')
            self.assertEqual(x.doc_type, 'doc_type')
            self.assertEqual(x.scheme, 'elasticsearch')
            self.assertEqual(x.host, 'localhost')
            self.assertEqual(x.port, 9200)
