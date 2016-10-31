# -*- coding: utf-8 -*-
"""MongoDB result store backend."""
from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta
from kombu.utils.objects import cached_property
from kombu.utils.url import maybe_sanitize_url
from kombu.exceptions import EncodeError
from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.five import string_t, items
from .base import BaseBackend

try:
    import pymongo
except ImportError:  # pragma: no cover
    pymongo = None   # noqa

if pymongo:
    try:
        from bson.binary import Binary
    except ImportError:                     # pragma: no cover
        from pymongo.binary import Binary   # noqa
    from pymongo.errors import InvalidDocument  # noqa
else:                                       # pragma: no cover
    Binary = None                           # noqa

    class InvalidDocument(Exception):       # noqa
        pass

__all__ = ['MongoBackend']


class MongoBackend(BaseBackend):
    """MongoDB result backend.

    Raises:
        celery.exceptions.ImproperlyConfigured:
            if module :pypi:`pymongo` is not available.
    """

    mongo_host = None
    host = 'localhost'
    port = 27017
    user = None
    password = None
    database_name = 'celery'
    taskmeta_collection = 'celery_taskmeta'
    groupmeta_collection = 'celery_groupmeta'
    max_pool_size = 10
    options = None

    supports_autoexpire = False

    _connection = None

    def __init__(self, app=None, **kwargs):
        self.options = {}

        super(MongoBackend, self).__init__(app, **kwargs)

        if not pymongo:
            raise ImproperlyConfigured(
                'You need to install the pymongo library to use the '
                'MongoDB backend.')

        # Set option defaults
        for key, value in items(self._prepare_client_options()):
            self.options.setdefault(key, value)

        # update conf with mongo uri data, only if uri was given
        if self.url:
            if self.url == 'mongodb://':
                self.url += 'localhost'

            uri_data = pymongo.uri_parser.parse_uri(self.url)
            # build the hosts list to create a mongo connection
            hostslist = [
                '{0}:{1}'.format(x[0], x[1]) for x in uri_data['nodelist']
            ]
            self.user = uri_data['username']
            self.password = uri_data['password']
            self.mongo_host = hostslist
            if uri_data['database']:
                # if no database is provided in the uri, use default
                self.database_name = uri_data['database']

            self.options.update(uri_data['options'])

        # update conf with specific settings
        config = self.app.conf.get('mongodb_backend_settings')
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured(
                    'MongoDB backend settings should be grouped in a dict')
            config = dict(config)  # don't modify original

            if 'host' in config or 'port' in config:
                # these should take over uri conf
                self.mongo_host = None

            self.host = config.pop('host', self.host)
            self.port = config.pop('port', self.port)
            self.mongo_host = config.pop('mongo_host', self.mongo_host)
            self.user = config.pop('user', self.user)
            self.password = config.pop('password', self.password)
            self.database_name = config.pop('database', self.database_name)
            self.taskmeta_collection = config.pop(
                'taskmeta_collection', self.taskmeta_collection,
            )
            self.groupmeta_collection = config.pop(
                'groupmeta_collection', self.groupmeta_collection,
            )

            self.options.update(config.pop('options', {}))
            self.options.update(config)

    def _prepare_client_options(self):
            if pymongo.version_tuple >= (3,):
                return {'maxPoolSize': self.max_pool_size}
            else:  # pragma: no cover
                return {'max_pool_size': self.max_pool_size,
                        'auto_start_request': False}

    def _get_connection(self):
        """Connect to the MongoDB server."""
        if self._connection is None:
            from pymongo import MongoClient

            host = self.mongo_host
            if not host:
                # The first pymongo.Connection() argument (host) can be
                # a list of ['host:port'] elements or a mongodb connection
                # URI.  If this is the case, don't use self.port
                # but let pymongo get the port(s) from the URI instead.
                # This enables the use of replica sets and sharding.
                # See pymongo.Connection() for more info.
                host = self.host
                if isinstance(host, string_t) \
                   and not host.startswith('mongodb://'):
                    host = 'mongodb://{0}:{1}'.format(host, self.port)
            # don't change self.options
            conf = dict(self.options)
            conf['host'] = host

            self._connection = MongoClient(**conf)

        return self._connection

    def encode(self, data):
        if self.serializer == 'bson':
            # mongodb handles serialization
            return data
        return super(MongoBackend, self).encode(data)

    def decode(self, data):
        if self.serializer == 'bson':
            return data
        return super(MongoBackend, self).decode(data)

    def _store_result(self, task_id, result, state,
                      traceback=None, request=None, **kwargs):
        """Store return value and state of an executed task."""
        meta = {
            '_id': task_id,
            'status': state,
            'result': self.encode(result),
            'date_done': datetime.utcnow(),
            'traceback': self.encode(traceback),
            'children': self.encode(
                self.current_task_children(request),
            ),
        }

        try:
            self.collection.save(meta)
        except InvalidDocument as exc:
            raise EncodeError(exc)

        return result

    def _get_task_meta_for(self, task_id):
        """Get task meta-data for a task by id."""
        obj = self.collection.find_one({'_id': task_id})
        if obj:
            return self.meta_from_decoded({
                'task_id': obj['_id'],
                'status': obj['status'],
                'result': self.decode(obj['result']),
                'date_done': obj['date_done'],
                'traceback': self.decode(obj['traceback']),
                'children': self.decode(obj['children']),
            })
        return {'status': states.PENDING, 'result': None}

    def _save_group(self, group_id, result):
        """Save the group result."""
        self.group_collection.save({
            '_id': group_id,
            'result': self.encode([i.id for i in result]),
            'date_done': datetime.utcnow(),
        })
        return result

    def _restore_group(self, group_id):
        """Get the result for a group by id."""
        obj = self.group_collection.find_one({'_id': group_id})
        if obj:
            return {
                'task_id': obj['_id'],
                'date_done': obj['date_done'],
                'result': [
                    self.app.AsyncResult(task)
                    for task in self.decode(obj['result'])
                ],
            }

    def _delete_group(self, group_id):
        """Delete a group by id."""
        self.group_collection.remove({'_id': group_id})

    def _forget(self, task_id):
        """Remove result from MongoDB.

        Raises:
            pymongo.exceptions.OperationsError:
                if the task_id could not be removed.
        """
        # By using safe=True, this will wait until it receives a response from
        # the server.  Likewise, it will raise an OperationsError if the
        # response was unable to be completed.
        self.collection.remove({'_id': task_id})

    def cleanup(self):
        """Delete expired meta-data."""
        self.collection.remove(
            {'date_done': {'$lt': self.app.now() - self.expires_delta}},
        )
        self.group_collection.remove(
            {'date_done': {'$lt': self.app.now() - self.expires_delta}},
        )

    def __reduce__(self, args=(), kwargs={}):
        return super(MongoBackend, self).__reduce__(
            args, dict(kwargs, expires=self.expires, url=self.url))

    def _get_database(self):
        conn = self._get_connection()
        db = conn[self.database_name]
        if self.user and self.password:
            if not db.authenticate(self.user, self.password):
                raise ImproperlyConfigured(
                    'Invalid MongoDB username or password.')
        return db

    @cached_property
    def database(self):
        """Get database from MongoDB connection.

        performs authentication if necessary.
        """
        return self._get_database()

    @cached_property
    def collection(self):
        """Get the meta-data task collection."""
        collection = self.database[self.taskmeta_collection]

        # Ensure an index on date_done is there, if not process the index
        # in the background.  Once completed cleanup will be much faster
        collection.ensure_index('date_done', background='true')
        return collection

    @cached_property
    def group_collection(self):
        """Get the meta-data task collection."""
        collection = self.database[self.groupmeta_collection]

        # Ensure an index on date_done is there, if not process the index
        # in the background.  Once completed cleanup will be much faster
        collection.ensure_index('date_done', background='true')
        return collection

    @cached_property
    def expires_delta(self):
        return timedelta(seconds=self.expires)

    def as_uri(self, include_password=False):
        """Return the backend as an URI.

        Arguments:
            include_password (bool): Password censored if disabled.
        """
        if not self.url:
            return 'mongodb://'
        if include_password:
            return self.url

        if ',' not in self.url:
            return maybe_sanitize_url(self.url)

        uri1, remainder = self.url.split(',', 1)
        return ','.join([maybe_sanitize_url(uri1), remainder])
