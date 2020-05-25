# -* coding: utf-8 -*-
"""Elasticsearch result store backend."""
from __future__ import absolute_import, unicode_literals

from datetime import datetime

from celery import states
from kombu.utils.encoding import bytes_to_str
from kombu.utils.url import _parse_url

from celery.exceptions import ImproperlyConfigured
from celery.five import items

from .base import KeyValueStoreBackend

try:
    import elasticsearch
except ImportError:
    elasticsearch = None  # noqa

__all__ = ('ElasticsearchBackend',)

E_LIB_MISSING = """\
You need to install the elasticsearch library to use the Elasticsearch \
result backend.\
"""


class ElasticsearchBackend(KeyValueStoreBackend):
    """Elasticsearch Backend.

    Raises:
        celery.exceptions.ImproperlyConfigured:
            if module :pypi:`elasticsearch` is not available.
    """

    index = 'celery'
    doc_type = 'backend'
    scheme = 'http'
    host = 'localhost'
    port = 9200
    username = None
    password = None
    es_retry_on_timeout = False
    es_timeout = 10
    es_max_retries = 3

    def __init__(self, url=None, *args, **kwargs):
        super(ElasticsearchBackend, self).__init__(*args, **kwargs)
        self.url = url
        _get = self.app.conf.get

        if elasticsearch is None:
            raise ImproperlyConfigured(E_LIB_MISSING)

        index = doc_type = scheme = host = port = username = password = None

        if url:
            scheme, host, port, username, password, path, _ = _parse_url(url)  # noqa
            if scheme == 'elasticsearch':
                scheme = None
            if path:
                path = path.strip('/')
                index, _, doc_type = path.partition('/')

        self.index = index or self.index
        self.doc_type = doc_type or self.doc_type
        self.scheme = scheme or self.scheme
        self.host = host or self.host
        self.port = port or self.port
        self.username = username or self.username
        self.password = password or self.password

        self.es_retry_on_timeout = (
            _get('elasticsearch_retry_on_timeout') or self.es_retry_on_timeout
        )

        es_timeout = _get('elasticsearch_timeout')
        if es_timeout is not None:
            self.es_timeout = es_timeout

        es_max_retries = _get('elasticsearch_max_retries')
        if es_max_retries is not None:
            self.es_max_retries = es_max_retries

        self._server = None

    def get(self, key):
        try:
            res = self._get(key)
            try:
                if res['found']:
                    return res['_source']['result']
            except (TypeError, KeyError):
                pass
        except elasticsearch.exceptions.NotFoundError:
            pass

    def _get(self, key):
        return self.server.get(
            index=self.index,
            doc_type=self.doc_type,
            id=key,
        )

    def set(self, key, value):
        body = {
            'result': value,
            '@timestamp': '{0}Z'.format(
                datetime.utcnow().isoformat()[:-3]
            ),
        }
        try:
            self._index(
                id=key,
                body=body,
            )
        except elasticsearch.exceptions.ConflictError:
            # document already exists, update it
            self._update(id=key, body=body)

    def _index(self, id, body, **kwargs):
        body = {bytes_to_str(k): v for k, v in items(body)}
        return self.server.index(
            id=bytes_to_str(id),
            index=self.index,
            doc_type=self.doc_type,
            body=body,
            params={'op_type': 'create'},
            **kwargs
        )

    def _update(self, id, body, **kwargs):
        body = {bytes_to_str(k): v for k, v in items(body)}
        retries = 3
        while retries > 0:
            retries -= 1
            try:
                res_get = self._get(key=id)
                if not res_get['found']:
                    return self._index(id, body, **kwargs)
                parsed_result = self.decode_result(res_get['_source']['result'])
                if parsed_result['status'] in states.READY_STATES:
                    # if stored state is already in ready state, do nothing
                    return {'result': 'noop'}

                # get current sequence number and primary term
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/optimistic-concurrency-control.html
                seq_no = res_get.get('_seq_no', 1)
                prim_term = res_get.get('_primary_term', 1)

                # try to update document with current seq_no and primary_term
                res = self.server.update(
                    id=bytes_to_str(id),
                    index=self.index,
                    body={'doc': body},
                    params={'if_primary_term': prim_term, 'if_seq_no': seq_no},
                    **kwargs
                )
                # result is elastic search update query result
                # noop = query did not update any document
                # updated = at least one document got updated
                if res['result'] != 'noop':
                    return res
            except Exception:
                if retries == 0:
                    raise
        raise Exception('too many retries to update backend')

    def mget(self, keys):
        return [self.get(key) for key in keys]

    def delete(self, key):
        self.server.delete(index=self.index, doc_type=self.doc_type, id=key)

    def _get_server(self):
        """Connect to the Elasticsearch server."""
        http_auth = None
        if self.username and self.password:
            http_auth = (self.username, self.password)
        return elasticsearch.Elasticsearch(
            '%s:%s' % (self.host, self.port),
            retry_on_timeout=self.es_retry_on_timeout,
            max_retries=self.es_max_retries,
            timeout=self.es_timeout,
            scheme=self.scheme,
            http_auth=http_auth,
        )

    @property
    def server(self):
        if self._server is None:
            self._server = self._get_server()
        return self._server
