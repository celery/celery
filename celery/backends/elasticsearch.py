# -* coding: utf-8 -*-
"""
    celery.backends.elasticsearch
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Elasticsearch result store backend.
    Based on CouchDB backend.

"""
from __future__ import absolute_import

try:
    import elasticsearch
except ImportError:
    elasticsearch = None  # noqa

from .base import KeyValueStoreBackend

import datetime

from kombu.utils.url import _parse_url

from celery.exceptions import ImproperlyConfigured

__all__ = ['ElasticsearchBackend']

ERR_LIB_MISSING = """\
You need to install the elasticsearch library to use the Elasticsearch \
result backend\
"""

class ElasticsearchBackend(KeyValueStoreBackend):

    index = 'celery'
    doc_type = 'backend'
    scheme = 'http'
    host = 'localhost'
    port = 9200


    def __init__(self, url=None, *args, **kwargs):
        """Initialize Elasticsearch backend instance.

        :raises celery.exceptions.ImproperlyConfigured: if
            module :mod:`elasticsearch` is not available.

        """
        super(ElasticsearchBackend, self).__init__(*args, **kwargs)

        if elasticsearch is None:
            raise ImproperlyConfigured(ERR_LIB_MISSING)

        uindex = udoc_type = uscheme = uhost = uport = None
        
        if url:
            uscheme, uhost, uport, _, _, uuri, _ = _parse_url(url)  # noqa
            uuri = uuri.strip('/') if uuri else None
            uuris = uuri.split("/")
            uindex = uuris[0] if len(uuris) > 0 else None
            udoc_type = uuris[1] if len(uuris) > 1 else None

        self.index = uindex or self.index
        self.doc_type = udoc_type or self.doc_type
        self.scheme = uscheme or self.scheme
        self.host = uhost or self.host
        self.port = uport or self.port

        self._server = None


    def _get_server(self):
        """Connect to the Elasticsearch server."""
        return elasticsearch.Elasticsearch(self.host)


    @property
    def server(self):
        if self._server is None:
            self._server = self._get_server()
        return self._server


    def get(self, key):
        try:
            out = self.server.get(index=self.index,\
                                  doc_type=self.doc_type,\
                                  id=key)
            if isinstance(out, dict) \
                    and "found" in out and out["found"] \
                    and "_source" in out and key in out["_source"]:
                return out["_source"][key]
            else:
                return None
        except elasticsearch.exceptions.NotFoundError:
            return None


    def set(self, key, value):
        try:
            data = {}
            data['@timestamp'] = "{0}Z".format(datetime.datetime.utcnow()\
                                       .strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
            data[key] = value
            self.server.index(index=self.index, doc_type=self.doc_type,\
                              id=key, body=data)
        except elasticsearch.exceptions.ConflictError:
            # document already exists, update it
            data = self.get(key)
            data[key] = value
            self.server.index(index=self.index, doc_type=self.doc_type,\
                              id=key, body=data, refresh=True)


    def mget(self, keys):
        return [self.get(key) for key in keys]


    def delete(self, key):
        self.server.delete(index=self.index, doc_type=self.doc_type, id=key)

