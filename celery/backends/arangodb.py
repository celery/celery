# -*- coding: utf-8 -*-
"""ArangoDb result store backend."""
from __future__ import absolute_import, unicode_literals

import logging
import json
from kombu.utils.url import _parse_url

from celery.exceptions import ImproperlyConfigured

from .base import KeyValueStoreBackend

try:
    from pyArango import connection as pyArangoConnection
    from pyArango.theExceptions import AQLQueryError
except ImportError:
    pyArangoConnection = AQLQueryError = None   # noqa

__all__ = ('ArangoDbBackend',)


class ArangoDbBackend(KeyValueStoreBackend):
    """ArangoDb backend.

    Sample url
    "arangodb://username:password@host:port/database/collection"
    *arangodb_backend_settings* is where the settings are present
    (in the app.conf)
    Settings should contain the host, port, username, password, database name,
    collection name else the default will be chosen.
    Default database name and collection name is celery.

    Raises
    ------
    celery.exceptions.ImproperlyConfigured:
        if module :pypi:`pyArango` is not available.

    """

    host = '127.0.0.1'
    port = '8529'
    database = 'celery'
    collection = 'celery'
    username = None
    password = None

    def __init__(self, url=None, *args, **kwargs):
        # kwargs.setdefault('expires_type', int)
        super(ArangoDbBackend, self).__init__(*args, **kwargs)
        self.url = url

        if url is None:
            host = port = database = collection = username = password = None
        else:
            (
                _schema, host, port, username, password,
                database_collection, _query
            ) = _parse_url(url)
            logging.debug(
                "_shema: %s, host: %s, port: %s, username: %s, password: %s, "
                "database_collection: %s, _query: %s",
                _schema, host, port, username, password,
                database_collection, _query
            )
            if database_collection is None:
                database = collection = None
            else:
                database, collection = database_collection.split('/')

        if pyArangoConnection is None:
            raise ImproperlyConfigured(
                'You need to install the pyArango library to use the '
                'ArangoDb backend.',
            )

        config = self.app.conf.get('arangodb_backend_settings', None)
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured(
                    'ArangoDb backend settings should be grouped in a dict',
                )
        else:
            config = {}

        self.host = host or config.get('host', self.host)
        self.port = int(port or config.get('port', self.port))
        self.database = database or config.get('database', self.database)
        self.collection = \
            collection or config.get('collection', self.collection)
        self.username = username or config.get('username', self.username)
        self.password = password or config.get('password', self.password)
        self.arangodb_url = "http://{host}:{port}".format(
            host=self.host, port=self.port
        )
        self._connection = None

    @property
    def connection(self):
        """Connect to the arangodb server."""
        if self._connection is None:
            self._connection = pyArangoConnection.Connection(
                arangoURL=self.arangodb_url, username=self.username,
                password=self.password
            )
        return self._connection

    @property
    def db(self):
        """Database Object to the given database."""
        return self.connection[self.database]

    def get(self, key):
        try:
            logging.debug(
                'RETURN DOCUMENT("{collection}/{key}").task'.format(
                    collection=self.collection, key=key
                )
            )
            query = self.db.AQLQuery(
                'RETURN DOCUMENT("{collection}/{key}").task'.format(
                    collection=self.collection, key=key
                )
            )
            result = query.response["result"][0]
            if result is None:
                return None
            return json.dumps(result)
        except AQLQueryError as err:
            logging.debug(err)
            return None

    def set(self, key, value):
        """Insert a doc with value into task attribute and _key as key."""
        try:
            logging.debug(
                'INSERT {{ task: {task}, _key: "{key}" }} INTO {collection}'
                .format(
                    collection=self.collection, key=key, task=value
                )
            )
            self.db.AQLQuery(
                'INSERT {{ task: {task}, _key: "{key}" }} INTO {collection}'
                .format(
                    collection=self.collection, key=key, task=value
                )
            )
        except AQLQueryError as err:
            logging.debug(err)
            return None

    def mget(self, keys):
        try:
            logging.debug(
                """
                FOR key in {keys}
                    RETURN DOCUMENT(CONCAT("{collection}/", key).task
                """.format(
                    collection=self.collection, keys=json.dumps(keys)
                )
            )
            query = self.db.AQLQuery(
                """
                FOR key in {keys}
                    RETURN DOCUMENT(CONCAT("{collection}/", key).task
                """.format(
                    collection=self.collection, keys=json.dumps(keys)
                )
            )
            results = []
            while True:
                results.extend(query.response['result'])
                query.nextBatch()
        except StopIteration:
            [
                result if result is None else json.dumps(result)
                for result in results
            ]
        except AQLQueryError as err:
            logging.debug(err)
            return None

    def delete(self, key):
        try:
            logging.debug(
                'REMOVE {{ _key: "{key}" }} IN {collection}'.format(
                    key=key, collection=self.collection
                )
            )
            self.db.AQLQuery(
                'REMOVE {{ _key: "{key}" }} IN {collection}'.format(
                    key=key, collection=self.collection
                )
            )
        except AQLQueryError as err:
            logging.debug(err)
            return None
