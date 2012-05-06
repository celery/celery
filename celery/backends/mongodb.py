
# -*- coding: utf-8 -*-
"""MongoDB backend for celery."""
from __future__ import absolute_import

import warnings

from datetime import datetime

try:
    import pymongo
except ImportError:  # pragma: no cover
    pymongo = None   # noqa

from kombu.utils import cached_property

from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.utils.timeutils import maybe_timedelta

from .base import BaseDictBackend


class Bunch(object):

    def __init__(self, **kw):
        self.__dict__.update(kw)


class MongoBackend(BaseDictBackend):
    mongodb_host = "localhost"
    mongodb_port = 27017
    mongodb_user = None
    mongodb_password = None
    mongodb_database = "celery"
    mongodb_taskmeta_collection = "celery_taskmeta"

    def __init__(self, *args, **kwargs):
        """Initialize MongoDB backend instance.

        :raises celery.exceptions.ImproperlyConfigured: if
            module :mod:`pymongo` is not available.

        """
        super(MongoBackend, self).__init__(*args, **kwargs)
        self.expires = kwargs.get("expires") or maybe_timedelta(
                                    self.app.conf.CELERY_TASK_RESULT_EXPIRES)

        if not pymongo:
            raise ImproperlyConfigured(
                "You need to install the pymongo library to use the "
                "MongoDB backend.")

        config = self.app.conf.get("CELERY_MONGODB_BACKEND_SETTINGS", None)
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured(
                    "MongoDB backend settings should be grouped in a dict")

            self.mongodb_host = config.get("host", self.mongodb_host)
            self.mongodb_port = int(config.get("port", self.mongodb_port))
            self.mongodb_user = config.get("user", self.mongodb_user)
            self.mongodb_password = config.get(
                    "password", self.mongodb_password)
            self.mongodb_database = config.get(
                    "database", self.mongodb_database)
            self.mongodb_taskmeta_collection = config.get(
                "taskmeta_collection", self.mongodb_taskmeta_collection)

        self._connection = None

    def _get_connection(self):
        """Connect to the MongoDB server."""
        if self._connection is None:
            from pymongo.connection import Connection

            # The first pymongo.Connection() argument (host) can be
            # a list of ['host:port'] elements or a mongodb connection
            # URI. If this is the case, don't use self.mongodb_port
            # but let pymongo get the port(s) from the URI instead.
            # This enables the use of replica sets and sharding.
            # See pymongo.Connection() for more info.
            args = [self.mongodb_host]
            if isinstance(self.mongodb_host, basestring) \
                    and not self.mongodb_host.startswith("mongodb://"):
                args.append(self.mongodb_port)

            self._connection = Connection(*args)

        return self._connection

    def process_cleanup(self):
        if self._connection is not None:
            # MongoDB connection will be closed automatically when object
            # goes out of scope
            self._connection = None

    def _store_result(self, task_id, result, status, traceback=None):
        """Store return value and status of an executed task."""
        try:
            from bson.binary import Binary
        except ImportError:
            from pymongo.binary import Binary
            warnings.warn("pymongo.binary has been removed in pymongo>2.2 in favor of bson.binary")
            
        meta = {"_id": task_id,
                "status": status,
                "result": Binary(self.encode(result)),
                "date_done": datetime.utcnow(),
                "traceback": Binary(self.encode(traceback))}
        self.collection.save(meta, safe=True)

        return result

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""

        obj = self.collection.find_one({"_id": task_id})
        if not obj:
            return {"status": states.PENDING, "result": None}

        meta = {
            "task_id": obj["_id"],
            "status": obj["status"],
            "result": self.decode(obj["result"]),
            "date_done": obj["date_done"],
            "traceback": self.decode(obj["traceback"]),
        }

        return meta

    def _save_taskset(self, taskset_id, result):
        """Save the taskset result."""
        try:
            from bson.binary import Binary
        except ImportError:
            from pymongo.binary import Binary
            warnings.warn("pymongo.binary has been removed pymongo >2.1 in favor of bson.binary")
            
        meta = {"_id": taskset_id,
                "result": Binary(self.encode(result)),
                "date_done": datetime.utcnow()}
        self.collection.save(meta, safe=True)

        return result

    def _restore_taskset(self, taskset_id):
        """Get the result for a taskset by id."""
        obj = self.collection.find_one({"_id": taskset_id})
        if not obj:
            return

        meta = {
            "task_id": obj["_id"],
            "result": self.decode(obj["result"]),
            "date_done": obj["date_done"],
        }

        return meta

    def _delete_taskset(self, taskset_id):
        """Delete a taskset by id."""
        self.collection.remove({"_id": taskset_id})

    def _forget(self, task_id):
        """
        Remove result from MongoDB.

        :raises celery.exceptions.OperationsError: if the task_id could not be
                                                   removed.
        """
        # By using safe=True, this will wait until it receives a response from
        # the server.  Likewise, it will raise an OperationsError if the
        # response was unable to be completed.
        self.collection.remove({"_id": task_id}, safe=True)

    def cleanup(self):
        """Delete expired metadata."""
        self.collection.remove({
                "date_done": {
                    "$lt": self.app.now() - self.expires,
                 }
        })

    def __reduce__(self, args=(), kwargs={}):
        kwargs.update(
            dict(expires=self.expires))
        return super(MongoBackend, self).__reduce__(args, kwargs)

    def _get_database(self):
        conn = self._get_connection()
        db = conn[self.mongodb_database]
        if self.mongodb_user and self.mongodb_password:
            if not db.authenticate(self.mongodb_user,
                                   self.mongodb_password):
                raise ImproperlyConfigured(
                    "Invalid MongoDB username or password.")
        return db

    @cached_property
    def database(self):
        """Get database from MongoDB connection and perform authentication
        if necessary."""
        return self._get_database()

    @cached_property
    def collection(self):
        """Get the metadata task collection."""
        collection = self.database[self.mongodb_taskmeta_collection]

        # Ensure an index on date_done is there, if not process the index
        # in the background. Once completed cleanup will be much faster
        collection.ensure_index('date_done', background='true')
        return collection
