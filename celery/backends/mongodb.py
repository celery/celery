# -*- coding: utf-8 -*-
"""
    celery.backends.mongodb
    ~~~~~~~~~~~~~~~~~~~~~~~

    MongoDB result store backend.

"""
from __future__ import absolute_import

from datetime import datetime, timedelta

from kombu.syn import detect_environment
from kombu.utils import cached_property
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
    InvalidDocument = None                  # noqa

__all__ = ['MongoBackend']


class Bunch(object):

    def __init__(self, **kw):
        self.__dict__.update(kw)


class MongoBackend(BaseBackend):

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

    def __init__(self, app=None, url=None, **kwargs):
        """Initialize MongoDB backend instance.

        :raises celery.exceptions.ImproperlyConfigured: if
            module :mod:`pymongo` is not available.

        """
        self.options = {}

        super(MongoBackend, self).__init__(app, **kwargs)

        if not pymongo:
            raise ImproperlyConfigured(
                'You need to install the pymongo library to use the '
                'MongoDB backend.')

        self.url = url

        # Set option defaults
        for key, value in items(self._prepare_client_options()):
            self.options.setdefault(key, value)

        # update conf with mongo uri data, only if uri was given
        if self.url:
            uri_data = pymongo.uri_parser.parse_uri(self.url)
            # build the hosts list to create a mongo connection
            hostslist = [
                "{0}:{1}".format(x[0], x[1]) for x in uri_data['nodelist']
            ]
            self.user = uri_data['username']
            self.password = uri_data['password']
            self.mongo_host = hostslist
            if uri_data['database']:
                # if no database is provided in the uri, use default
                self.database_name = uri_data['database']

            self.options.update(uri_data['options'])

        # update conf with specific settings
        config = self.app.conf.get('CELERY_MONGODB_BACKEND_SETTINGS')
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured(
                    'MongoDB backend settings should be grouped in a dict')
            config = dict(config)  # do not modify original

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
                # URI. If this is the case, don't use self.port
                # but let pymongo get the port(s) from the URI instead.
                # This enables the use of replica sets and sharding.
                # See pymongo.Connection() for more info.
                host = self.host
                if isinstance(host, string_t) \
                   and not host.startswith('mongodb://'):
                    host = 'mongodb://{0}:{1}'.format(host, self.port)

                if host == 'mongodb://':
                    host += 'localhost'

            # don't change self.options
            conf = dict(self.options)
            conf['host'] = host

            if detect_environment() != 'default':
                conf['use_greenlets'] = True

            self._connection = MongoClient(**conf)

        return self._connection

    def process_cleanup(self):
        if self._connection is not None:
            # MongoDB connection will be closed automatically when object
            # goes out of scope
            del(self.collection)
            del(self.database)
            self._connection = None

    def encode(self, data):
        if self.serializer == 'bson':
            # mongodb handles serialization
            return data
        return super(MongoBackend, self).encode(data)

    def decode(self, data):
        if self.serializer == 'bson':
            return data
        return super(MongoBackend, self).decode(data)

    def _store_result(self, task_id, result, status,
                      traceback=None, request=None, **kwargs):
        """Store return value and status of an executed task."""

        meta = {'_id': task_id,
                'status': status,
                'result': self.encode(result),
                'date_done': datetime.utcnow(),
                'traceback': self.encode(traceback),
                'children': self.encode(
                    self.current_task_children(request),
                )}

        try:
            self.collection.save(meta)
        except InvalidDocument as exc:
            raise EncodeError(exc)

        return result

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
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

        task_ids = [i.id for i in result]

        meta = {'_id': group_id,
                'result': self.encode(task_ids),
                'date_done': datetime.utcnow()}
        self.group_collection.save(meta)

        return result

    def _restore_group(self, group_id):
        """Get the result for a group by id."""
        obj = self.group_collection.find_one({'_id': group_id})
        if obj:
            tasks = [self.app.AsyncResult(task)
                     for task in self.decode(obj['result'])]

            return {
                'task_id': obj['_id'],
                'result': tasks,
                'date_done': obj['date_done'],
            }

    def _delete_group(self, group_id):
        """Delete a group by id."""
        self.group_collection.remove({'_id': group_id})

    def _forget(self, task_id):
        """
        Remove result from MongoDB.

        :raises celery.exceptions.OperationsError: if the task_id could not be
                                                   removed.
        """
        # By using safe=True, this will wait until it receives a response from
        # the server.  Likewise, it will raise an OperationsError if the
        # response was unable to be completed.
        self.collection.remove({'_id': task_id})

    def cleanup(self):
        """Delete expired metadata."""
        self.collection.remove(
            {'date_done': {'$lt': self.app.now() - self.expires_delta}},
        )
        self.group_collection.remove(
            {'date_done': {'$lt': self.app.now() - self.expires_delta}},
        )

    def __reduce__(self, args=(), kwargs={}):
        return super(MongoBackend, self).__reduce__(
            args, dict(kwargs, expires=self.expires, url=self.url),
        )

    def _get_database(self):
        conn = self._get_connection()
        db = conn[self.database_name]
        if self.user and self.password:
            if not db.authenticate(self.user,
                                   self.password):
                raise ImproperlyConfigured(
                    'Invalid MongoDB username or password.')
        return db

    @cached_property
    def database(self):
        """Get database from MongoDB connection and perform authentication
        if necessary."""
        return self._get_database()

    @cached_property
    def collection(self):
        """Get the metadata task collection."""
        collection = self.database[self.taskmeta_collection]

        # Ensure an index on date_done is there, if not process the index
        # in the background. Once completed cleanup will be much faster
        collection.ensure_index('date_done', background='true')
        return collection

    @cached_property
    def group_collection(self):
        """Get the metadata task collection."""
        collection = self.database[self.groupmeta_collection]

        # Ensure an index on date_done is there, if not process the index
        # in the background. Once completed cleanup will be much faster
        collection.ensure_index('date_done', background='true')
        return collection

    @cached_property
    def expires_delta(self):
        return timedelta(seconds=self.expires)
