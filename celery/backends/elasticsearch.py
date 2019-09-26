# -* coding: utf-8 -*-
"""Elasticsearch result store backend."""
from __future__ import absolute_import, unicode_literals

from datetime import datetime

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
            res = self.server.get(
                index=self.index,
                doc_type=self.doc_type,
                id=key,
            )
            try:
                if res['found']:
                    return res['_source']['result']
            except (TypeError, KeyError):
                pass
        except elasticsearch.exceptions.NotFoundError:
            pass

    def set(self, key, value):
        try:
            self._index(
                id=key,
                body={
                    'result': value,
                    '@timestamp': '{0}Z'.format(
                        datetime.utcnow().isoformat()[:-3]
                    ),
                },
            )
        except elasticsearch.exceptions.ConflictError:
            # document already exists, update it
            data = self.get(key)
            data[key] = value
            self._index(key, data, refresh=True)

    def _index(self, id, body, **kwargs):
        body = {bytes_to_str(k): v for k, v in items(body)}
        return self.server.index(
            id=bytes_to_str(id),
            index=self.index,
            doc_type=self.doc_type,
            body=body,
            **kwargs
        )

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
