# -*- coding: utf-8 -*-
"""Couchbase result store backend."""
from __future__ import absolute_import, unicode_literals

import logging

try:
    from couchbase import Couchbase
    from couchbase.connection import Connection
    from couchbase.exceptions import NotFoundError
except ImportError:
    Couchbase = Connection = NotFoundError = None   # noqa

from kombu.utils.encoding import str_t
from kombu.utils.url import _parse_url

from celery.exceptions import ImproperlyConfigured

from .base import KeyValueStoreBackend

__all__ = ['CouchbaseBackend']


class CouchbaseBackend(KeyValueStoreBackend):
    """Couchbase backend.

    Raises:
        celery.exceptions.ImproperlyConfigured:
            if module :pypi:`couchbase` is not available.
    """

    bucket = 'default'
    host = 'localhost'
    port = 8091
    username = None
    password = None
    quiet = False
    timeout = 2.5

    # Use str as couchbase key not bytes
    key_t = str_t

    def __init__(self, url=None, *args, **kwargs):
        super(CouchbaseBackend, self).__init__(*args, **kwargs)
        self.url = url

        if Couchbase is None:
            raise ImproperlyConfigured(
                'You need to install the couchbase library to use the '
                'Couchbase backend.',
            )

        uhost = uport = uname = upass = ubucket = None
        if url:
            _, uhost, uport, uname, upass, ubucket, _ = _parse_url(url)
            ubucket = ubucket.strip('/') if ubucket else None

        config = self.app.conf.get('couchbase_backend_settings', None)
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured(
                    'Couchbase backend settings should be grouped in a dict',
                )
        else:
            config = {}

        self.host = uhost or config.get('host', self.host)
        self.port = int(uport or config.get('port', self.port))
        self.bucket = ubucket or config.get('bucket', self.bucket)
        self.username = uname or config.get('username', self.username)
        self.password = upass or config.get('password', self.password)

        self._connection = None

    def _get_connection(self):
        """Connect to the Couchbase server."""
        if self._connection is None:
            kwargs = {'bucket': self.bucket, 'host': self.host}

            if self.port:
                kwargs.update({'port': self.port})
            if self.username:
                kwargs.update({'username': self.username})
            if self.password:
                kwargs.update({'password': self.password})

            logging.debug('couchbase settings %r', kwargs)
            self._connection = Connection(**kwargs)
        return self._connection

    @property
    def connection(self):
        return self._get_connection()

    def get(self, key):
        try:
            return self.connection.get(key).value
        except NotFoundError:
            return None

    def set(self, key, value):
        self.connection.set(key, value)

    def mget(self, keys):
        return [self.get(key) for key in keys]

    def delete(self, key):
        self.connection.delete(key)
