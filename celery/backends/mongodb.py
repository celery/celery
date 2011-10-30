# -*- coding: utf-8 -*-
"""MongoDB backend for celery."""
from __future__ import absolute_import

from datetime import datetime

try:
    import pymongo
except ImportError:
    pymongo = None  # noqa

from .. import states
from ..exceptions import ImproperlyConfigured
from ..utils.timeutils import maybe_timedelta

from .base import BaseDictBackend


class Bunch:

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
        self._database = None

    def _get_connection(self):
        """Connect to the MongoDB server."""
        if self._connection is None:
            from pymongo.connection import Connection
            self._connection = Connection(self.mongodb_host,
                                          self.mongodb_port)
        return self._connection

    def _get_database(self):
        """"Get database from MongoDB connection and perform authentication
        if necessary."""
        if self._database is None:
            conn = self._get_connection()
            db = conn[self.mongodb_database]
            if self.mongodb_user and self.mongodb_password:
                auth = db.authenticate(self.mongodb_user,
                                       self.mongodb_password)
                if not auth:
                    raise ImproperlyConfigured(
                        "Invalid MongoDB username or password.")
            self._database = db

        return self._database

    def process_cleanup(self):
        if self._connection is not None:
            # MongoDB connection will be closed automatically when object
            # goes out of scope
            self._connection = None

    def _store_result(self, task_id, result, status, traceback=None):
        """Store return value and status of an executed task."""
        from pymongo.binary import Binary

        meta = {"_id": task_id,
                "status": status,
                "result": Binary(self.encode(result)),
                "date_done": datetime.now(),
                "traceback": Binary(self.encode(traceback))}

        db = self._get_database()
        taskmeta_collection = db[self.mongodb_taskmeta_collection]
        taskmeta_collection.save(meta, safe=True)

        return result

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""

        db = self._get_database()
        taskmeta_collection = db[self.mongodb_taskmeta_collection]
        obj = taskmeta_collection.find_one({"_id": task_id})
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

    def cleanup(self):
        """Delete expired metadata."""
        db = self._get_database()
        taskmeta_collection = db[self.mongodb_taskmeta_collection]
        taskmeta_collection.remove({
                "date_done": {
                    "$lt": datetime.now() - self.expires,
                 }
        })

    def __reduce__(self, args=(), kwargs={}):
        kwargs.update(
            dict(expires=self.expires))
        return super(MongoBackend, self).__reduce__(args, kwargs)
