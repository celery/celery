"""MongoDB backend for celery."""
from datetime import datetime

from django.core.exceptions import ImproperlyConfigured
from billiard.serialization import pickle
try:
    import pymongo
except ImportError:
    pymongo = None

from celery import conf
from celery import states
from celery.backends.base import BaseBackend
from celery.loaders import load_settings


class Bunch:

    def __init__(self, **kw):
        self.__dict__.update(kw)


class MongoBackend(BaseBackend):

    capabilities = ["ResultStore"]

    mongodb_host = 'localhost'
    mongodb_port = 27017
    mongodb_user = None
    mongodb_password = None
    mongodb_database = 'celery'
    mongodb_taskmeta_collection = 'celery_taskmeta'

    def __init__(self, *args, **kwargs):
        """Initialize MongoDB backend instance.

        :raises django.core.exceptions.ImproperlyConfigured: if
            module :mod:`pymongo` is not available.

        """

        if not pymongo:
            raise ImproperlyConfigured(
                "You need to install the pymongo library to use the "
                "MongoDB backend.")

        settings = load_settings()

        conf = getattr(settings, "CELERY_MONGODB_BACKEND_SETTINGS", None)
        if conf is not None:
            if not isinstance(conf, dict):
                raise ImproperlyConfigured(
                    "MongoDB backend settings should be grouped in a dict")

            self.mongodb_host = conf.get('host', self.mongodb_host)
            self.mongodb_port = int(conf.get('port', self.mongodb_port))
            self.mongodb_user = conf.get('user', self.mongodb_user)
            self.mongodb_password = conf.get(
                    'password', self.mongodb_password)
            self.mongodb_database = conf.get(
                    'database', self.mongodb_database)
            self.mongodb_taskmeta_collection = conf.get(
                'taskmeta_collection', self.mongodb_taskmeta_collection)

        super(MongoBackend, self).__init__(*args, **kwargs)
        self._cache = {}
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

    def store_result(self, task_id, result, status, traceback=None):
        """Store return value and status of an executed task."""
        from pymongo.binary import Binary

        result = self.encode_result(result, status)

        meta = {"_id": task_id,
                "status": status,
                "result": Binary(pickle.dumps(result)),
                "date_done": datetime.now(),
                "traceback": Binary(pickle.dumps(traceback))}

        db = self._get_database()
        taskmeta_collection = db[self.mongodb_taskmeta_collection]

        taskmeta_collection.save(meta, safe=True)

    def get_status(self, task_id):
        """Get status of a task."""
        return self._get_task_meta_for(task_id)["status"]

    def get_traceback(self, task_id):
        """Get the traceback of a failed task."""
        meta = self._get_task_meta_for(task_id)
        return meta["traceback"]

    def get_result(self, task_id):
        """Get the result for a task."""
        meta = self._get_task_meta_for(task_id)
        if meta["status"] in states.EXCEPTION_STATES:
            return self.exception_to_python(meta["result"])
        else:
            return meta["result"]

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        if task_id in self._cache:
            return self._cache[task_id]

        db = self._get_database()
        taskmeta_collection = db[self.mongodb_taskmeta_collection]
        obj = taskmeta_collection.find_one({"_id": task_id})
        if not obj:
            return {"status": states.PENDING, "result": None}

        meta = {
            "task_id": obj["_id"],
            "status": obj["status"],
            "result": pickle.loads(str(obj["result"])),
            "date_done": obj["date_done"],
            "traceback": pickle.loads(str(obj["traceback"])),
        }
        if meta["status"] == states.SUCCESS:
            self._cache[task_id] = meta

        return meta

    def cleanup(self):
        """Delete expired metadata."""
        db = self._get_database()
        taskmeta_collection = db[self.mongodb_taskmeta_collection]
        taskmeta_collection.remove({
                "date_done": {
                    "$lt": datetime.now() - conf.TASK_RESULT_EXPIRES,
                 }
        })
