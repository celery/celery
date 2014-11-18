# -*- coding: utf-8 -*-
"""
    celery.backends.mongodb
    ~~~~~~~~~~~~~~~~~~~~~~~

    MongoDB result store backend.

"""
from __future__ import absolute_import

from datetime import datetime, timedelta

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

from kombu.syn import detect_environment
from kombu.utils import cached_property
from kombu.exceptions import EncodeError
from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.five import string_t

from .base import BaseBackend

__all__ = ['MongoBackend']


class Bunch(object):

    def __init__(self, **kw):
        self.__dict__.update(kw)


class MongoBackend(BaseBackend):

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

        config = self.app.conf.get('CELERY_MONGODB_BACKEND_SETTINGS')
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured(
                    'MongoDB backend settings should be grouped in a dict')
            config = dict(config)  # do not modify original

            self.host = config.pop('host', self.host)
            self.port = int(config.pop('port', self.port))
            self.user = config.pop('user', self.user)
            self.password = config.pop('password', self.password)
            self.database_name = config.pop('database', self.database_name)
            self.taskmeta_collection = config.pop(
                'taskmeta_collection', self.taskmeta_collection,
            )
            self.groupmeta_collection = config.pop(
                'groupmeta_collection', self.groupmeta_collection,
            )

            self.options = dict(config, **config.pop('options', None) or {})

            # Set option defaults
            self.options.setdefault('max_pool_size', self.max_pool_size)
            self.options.setdefault('auto_start_request', False)

        self.url = url
        if self.url:
            # Specifying backend as an URL
            self.host = self.url

    def _get_connection(self):
        """Connect to the MongoDB server."""
        if self._connection is None:
            from pymongo import MongoClient

            # The first pymongo.Connection() argument (host) can be
            # a list of ['host:port'] elements or a mongodb connection
            # URI. If this is the case, don't use self.port
            # but let pymongo get the port(s) from the URI instead.
            # This enables the use of replica sets and sharding.
            # See pymongo.Connection() for more info.
            url = self.host
            if isinstance(url, string_t) \
                    and not url.startswith('mongodb://'):
                url = 'mongodb://{0}:{1}'.format(url, self.port)
            if url == 'mongodb://':
                url = url + 'localhost'
            if detect_environment() != 'default':
                self.options['use_greenlets'] = True
            self._connection = MongoClient(host=url, **self.options)

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
