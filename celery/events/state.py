import time
import heapq

from carrot.utils import partition

from celery import states
from celery.datastructures import LocalCache

HEARTBEAT_EXPIRE = 150 # 2 minutes, 30 seconds


class Element(dict):
    """Base class for types."""
    visited = False

    def __init__(self, **fields):
        dict.__init__(self, fields)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError("'%s' object has no attribute '%s'" % (
                    self.__class__.__name__, key))

    def __setattr__(self, key, value):
        self[key] = value


class Worker(Element):
    """Worker State."""

    def __init__(self, **fields):
        super(Worker, self).__init__(**fields)
        self.heartbeats = []

    def on_online(self, timestamp=None, **kwargs):
        self._heartpush(timestamp)

    def on_offline(self, **kwargs):
        self.heartbeats = []

    def on_heartbeat(self, timestamp=None, **kwargs):
        self._heartpush(timestamp)

    def _heartpush(self, timestamp):
        if timestamp:
            heapq.heappush(self.heartbeats, timestamp)

    @property
    def alive(self):
        return (self.heartbeats and
                time.time() < self.heartbeats[-1] + HEARTBEAT_EXPIRE)


class Task(Element):
    """Task State."""
    _info_fields = ("args", "kwargs", "retries",
                    "result", "eta", "runtime",
                    "exception")

    _defaults = dict(uuid=None,
                     name=None,
                     state=states.PENDING,
                     received=False,
                     started=False,
                     succeeded=False,
                     failed=False,
                     retried=False,
                     revoked=False,
                     args=None,
                     kwargs=None,
                     eta=None,
                     retries=None,
                     worker=None,
                     timestamp=None)

    def __init__(self, **fields):
        super(Task, self).__init__(**dict(self._defaults, **fields))

    def info(self, fields=None, extra=[]):
        if fields is None:
            fields = self._info_fields
        fields = list(fields) + list(extra)
        return dict((key, getattr(self, key, None))
                        for key in fields
                            if getattr(self, key, None) is not None)

    @property
    def ready(self):
        return self.state in states.READY_STATES

    def update(self, d, **extra):
        if self.worker:
            self.worker.on_heartbeat(timestamp=time.time())
        return super(Task, self).update(d, **extra)

    def on_received(self, timestamp=None, **fields):
        print("ON RECEIVED")
        self.received = timestamp
        self.state = "RECEIVED"
        print(fields)
        self.update(fields, timestamp=timestamp)

    def on_started(self, timestamp=None, **fields):
        self.state = states.STARTED
        self.started = timestamp
        self.update(fields, timestamp=timestamp)

    def on_failed(self, timestamp=None, **fields):
        self.state = states.FAILURE
        self.failed = timestamp
        self.update(fields, timestamp=timestamp)

    def on_retried(self, timestamp=None, **fields):
        self.state = states.RETRY
        self.retried = timestamp
        self.update(fields, timestamp=timestamp)

    def on_succeeded(self, timestamp=None, **fields):
        self.state = states.SUCCESS
        self.succeeded = timestamp
        self.update(fields, timestamp=timestamp)

    def on_revoked(self, timestamp=None, **fields):
        self.state = states.REVOKED
        self.revoked = timestamp
        self.update(fields, timestamp=timestamp)


class State(object):
    """Represents a snapshot of a clusters state."""
    event_count = 0
    task_count = 0

    def __init__(self, callback=None,
            max_workers_in_memory=5000, max_tasks_in_memory=10000):
        self.workers = LocalCache(max_workers_in_memory)
        self.tasks = LocalCache(max_tasks_in_memory)
        self.event_callback = callback
        self.group_handlers = {"worker": self.worker_event,
                               "task": self.task_event}

    def get_or_create_worker(self, hostname, **kwargs):
        """Get or create worker by hostname."""
        try:
            worker = self.workers[hostname]
            worker.update(kwargs)
        except KeyError:
            worker = self.workers[hostname] = Worker(
                    hostname=hostname, **kwargs)
        return worker

    def get_or_create_task(self, uuid, **kwargs):
        """Get or create task by uuid."""
        try:
            task = self.tasks[uuid]
            task.update(kwargs)
        except KeyError:
            task = self.tasks[uuid] = Task(uuid=uuid, **kwargs)
        return task

    def worker_event(self, type, fields):
        """Process worker event."""
        hostname = fields.pop("hostname")
        worker = self.get_or_create_worker(hostname)
        handler = getattr(worker, "on_%s" % type)
        if handler:
            handler(**fields)

    def task_event(self, type, fields):
        """Process task event."""
        uuid = fields.pop("uuid")
        hostname = fields.pop("hostname")
        worker = self.get_or_create_worker(hostname)
        task = self.get_or_create_task(uuid)
        handler = getattr(task, "on_%s" % type)
        if type == "received":
            self.task_count += 1
        if handler:
            handler(**fields)
        task.worker = worker

    def event(self, event):
        """Process event."""
        event = dict((key.encode("utf-8"), value)
                        for key, value in event.items())
        self.event_count += 1
        group, _, type = partition(event.pop("type"), "-")
        self.group_handlers[group](type, event)
        if self.event_callback:
            self.event_callback(self, event)

    def tasks_by_timestamp(self):
        """Get tasks by timestamp.

        Returns a list of ``(uuid, task)`` tuples.

        """
        return self._sort_tasks_by_time(self.tasks.items())

    def _sort_tasks_by_time(self, tasks):
        """Sort task items by time."""
        return sorted(tasks, key=lambda t: t[1].timestamp, reverse=True)

    def tasks_by_type(self, name):
        """Get all tasks by type.

        Returns a list of ``(uuid, task)`` tuples.

        """
        return self._sort_tasks_by_time([(uuid, task)
                for uuid, task in self.tasks.items()
                    if task.name == name])

    def tasks_by_worker(self, hostname):
        """Get all tasks by worker.

        Returns a list of ``(uuid, task)`` tuples.

        """
        return self._sort_tasks_by_time([(uuid, task)
                for uuid, task in self.tasks.items()
                    if task.worker.hostname == hostname])

    def task_types(self):
        """Returns a list of all seen task types."""
        return list(set(task.name for task in self.tasks.values()))

    def alive_workers(self):
        """Returns a list of (seemingly) alive workers."""
        return [w for w in self.workers.values() if w.alive]


state = State()
