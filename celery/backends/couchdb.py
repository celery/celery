# -*- coding: utf-8 -*-
"""
    celery.backends.couchdb
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    CouchDB result store backend.

"""
from __future__ import absolute_import

import logging

try:
    import pycouchdb
except ImportError:
    pycouchdb = None  # noqa

from kombu.utils.url import _parse_url

from celery.exceptions import ImproperlyConfigured
from celery.utils.timeutils import maybe_timedelta

from .base import KeyValueStoreBackend

__all__ = ['CouchDBBackend']


class CouchDBBackend(KeyValueStoreBackend):
    container = 'default'
    scheme = 'http'
    host = 'localhost'
    port = 5984
    username = None
    password = None
    quiet = False
    conncache = None
    unlock_gil = True
    timeout = 2.5
    transcoder = None
    # supports_autoexpire = False

    def __init__(self, url=None, *args, **kwargs):
        """Initialize CouchDB backend instance.

        :raises celery.exceptions.ImproperlyConfigured: if
            module :mod:`pycouchdb` is not available.

        """
        super(CouchDBBackend, self).__init__(*args, **kwargs)

        self.expires = kwargs.get('expires') or maybe_timedelta(
            self.app.conf.CELERY_TASK_RESULT_EXPIRES)

        if pycouchdb is None:
            raise ImproperlyConfigured(
                'You need to install the pycouchdb library to use the '
                'CouchDB backend.',
            )

        uscheme = uhost = uport = uname = upass = ucontainer = None
        if url:
            _, uhost, uport, uname, upass, ucontainer , _ = _parse_url(url)  # noqa
            ucontainer = ucontainer.strip('/') if ucontainer else None

        config = self.app.conf.get('CELERY_COUCHDB_BACKEND_SETTINGS', None)
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured(
                    'CouchDB backend settings should be grouped in a dict',
                )
        else:
            config = {}

        self.scheme = uscheme or config.get('scheme', self.scheme)
        self.host = uhost or config.get('host', self.host)
        self.port = int(uport or config.get('port', self.port))
        self.container = ucontainer or config.get('container', self.container)
        self.username = uname or config.get('username', self.username)
        self.password = upass or config.get('password', self.password)

        self._connection = None

    def _get_connection(self):
        """Connect to the CouchDB server."""
        if self._connection is None:
            if self.username and self.password:
                conn_string = '%s://%s:%s@%s:%s' % (
                    self.scheme, self.username, self.password,
                    self.host, str(self.port))
                server = pycouchdb.Server(conn_string, authmethod='basic')
            else:
                conn_string = '%s://%s:%s' % (
                    self.scheme, self.host, str(self.port))
                server = pycouchdb.Server(conn_string)

            logging.debug('couchdb conn string: %s', conn_string)
            try:
                self._connection = server.database(self.container)
            except pycouchdb.exceptions.NotFound:
                self._connection = server.create(self.container)
        return self._connection

    @property
    def connection(self):
        return self._get_connection()

    def get(self, key):
        try:
            return self.connection.get(key)['value']
        except pycouchdb.exceptions.NotFound:
            return None

    def set(self, key, value):
        data = {'_id': key, 'value': value}
        try:
            self.connection.save(data)
        except pycouchdb.exceptions.Conflict:
            # document already exists, update it
            data = self.connection.get(key)
            data['value'] = value
            self.connection.save(data)

    def mget(self, keys):
        return [self.get(key) for key in keys]

    def delete(self, key):
        self.connection.delete(key)
