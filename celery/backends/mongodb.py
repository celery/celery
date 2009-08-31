"""MongoDB backend for celery."""

import random
from datetime import datetime, timedelta

from django.core.exceptions import ImproperlyConfigured
from celery.serialization import pickle
from celery.backends.base import BaseBackend
from celery.loaders import settings
from celery.conf import TASK_RESULT_EXPIRES
from celery.registry import tasks

try:
    import pymongo
except ImportError:
    pymongo = None

# taken from celery.managers.PeriodicTaskManager
SERVER_DRIFT = timedelta(seconds=random.vonmisesvariate(1, 4))


class Bunch:

    def __init__(self, **kw):
        self.__dict__.update(kw)


class Backend(BaseBackend):

    capabilities = ("ResultStore", "PeriodicStatus")

    mongodb_host = 'localhost'
    mongodb_port = 27017
    mongodb_user = None
    mongodb_password = None
    mongodb_database = 'celery'
    mongodb_taskmeta_collection = 'celery_taskmeta'
    mongodb_periodictaskmeta_collection = 'celery_periodictaskmeta'

    def __init__(self, *args, **kwargs):
        """Initialize MongoDB backend instance.

        :raises django.core.exceptions.ImproperlyConfigured: if
            module :mod:`pymongo` is not available.

        """

        if not pymongo:
            raise ImproperlyConfigured(
                "You need to install the pymongo library to use the "
                "MongoDB backend.")

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
            self.mongodb_collection_periodictaskmeta = conf.get(
                'periodictaskmeta_collection',
                self.mongodb_periodictaskmeta_collection)

        super(Backend, self).__init__(*args, **kwargs)
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

    def init_periodic_tasks(self):
        """Create collection for periodic tasks in database."""
        db = self._get_database()
        collection = db[self.mongodb_periodictaskmeta_collection]
        collection.ensure_index("name", pymongo.ASCENDING, unique=True)

        periodic_tasks = tasks.get_all_periodic()
        for task_name in periodic_tasks.keys():
            if not collection.find_one({"name": task_name}):
                collection.save({"name": task_name,
                                 "last_run_at": datetime.fromtimestamp(0),
                                 "total_run_count": 0}, safe=True)

    def run_periodic_tasks(self):
        """Run all waiting periodic tasks.

        :returns: a list of ``(task, task_id)`` tuples containing
            the task class and id for the resulting tasks applied.
        """
        db = self._get_database()
        collection = db[self.mongodb_periodictaskmeta_collection]

        waiting_tasks = self._get_waiting_tasks()
        task_id_tuples = []
        for waiting_task in waiting_tasks:
            task = tasks[waiting_task['name']]
            resp = task.delay()
            collection.update({'_id': waiting_task['_id']},
                              {"$inc": {"total_run_count": 1}})

            task_meta = Bunch(name=waiting_task['name'],
                              last_run_at=waiting_task['last_run_at'],
                              total_run_count=waiting_task['total_run_count'])
            task_id_tuples.append((task_meta, resp.task_id))

        return task_id_tuples

    def _is_time(self, last_run_at, run_every):
        """Check if if it is time to run the periodic task.

        :param last_run_at: Last time the periodic task was run.
        :param run_every: How often to run the periodic task.

        :rtype bool:

        """
        # code taken from celery.managers.PeriodicTaskManager
        run_every_drifted = run_every + SERVER_DRIFT
        run_at = last_run_at + run_every_drifted
        if datetime.now() > run_at:
            return True
        return False

    def _get_waiting_tasks(self):
        """Get all waiting periodic tasks."""
        db = self._get_database()
        collection = db[self.mongodb_periodictaskmeta_collection]

        periodic_tasks = tasks.get_all_periodic()

        # find all periodic tasks to be run
        waiting = []
        for task_meta in collection.find():
            if task_meta['name'] in periodic_tasks:
                task = periodic_tasks[task_meta['name']]
                run_every = task.run_every
                if self._is_time(task_meta['last_run_at'], run_every):
                    collection.update(
                        {"name": task_meta['name'],
                         "last_run_at": task_meta['last_run_at']},
                        {"$set": {"last_run_at": datetime.utcnow()}})

                    if db.last_status()['updatedExisting']:
                        waiting.append(task_meta)

        return waiting

    def store_result(self, task_id, result, status, traceback=None):
        """Store return value and status of an executed task."""
        from pymongo.binary import Binary

        if status == 'DONE':
            result = self.prepare_result(result)
        elif status == 'FAILURE':
            result = self.prepare_exception(result)

        meta = {"_id": task_id,
                "status": status,
                "result": Binary(pickle.dumps(result)),
                "date_done": datetime.utcnow(),
                "traceback": Binary(pickle.dumps(traceback))}

        db = self._get_database()
        taskmeta_collection = db[self.mongodb_taskmeta_collection]

        taskmeta_collection.save(meta, safe=True)

    def is_done(self, task_id):
        """Returns ``True`` if the task executed successfully."""
        return self.get_status(task_id) == "DONE"

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
        if meta["status"] == "FAILURE":
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
            return {"status": "PENDING", "result": None}

        meta = {
            "task_id": obj["_id"],
            "status": obj["status"],
            "result": pickle.loads(str(obj["result"])),
            "date_done": obj["date_done"],
            "traceback": pickle.loads(str(obj["traceback"])),
        }
        if meta["status"] == "DONE":
            self._cache[task_id] = meta

        return meta

    def cleanup(self):
        """Delete expired metadata."""
        db = self._get_database()
        taskmeta_collection = db[self.mongodb_taskmeta_collection]
        taskmeta_collection.remove({
                "date_done": {
                    "$lt": datetime.now() - TASK_RESULT_EXPIRES,
                 }
        })
