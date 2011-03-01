import time
import heapq

from threading import Lock

from kombu.utils import partition

from celery import states
from celery.datastructures import AttributeDict, LocalCache
from celery.utils import kwdict

HEARTBEAT_EXPIRE = 150                      # 2 minutes, 30 seconds


class Element(AttributeDict):
    """Base class for types."""
    visited = False

    def __init__(self, **fields):
        dict.__init__(self, fields)


class Worker(Element):
    """Worker State."""
    heartbeat_max = 4

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
            if len(self.heartbeats) > self.heartbeat_max:
                self.heartbeats = self.heartbeats[self.heartbeat_max:]

    def __repr__(self):
        return "<Worker: %s (%s)" % (self.hostname,
                                     self.alive and "ONLINE" or "OFFLINE")

    @property
    def alive(self):
        return (self.heartbeats and
                time.time() < self.heartbeats[-1] + HEARTBEAT_EXPIRE)


class Task(Element):
    """Task State."""
    _info_fields = ("args", "kwargs", "retries",
                    "result", "eta", "runtime", "expires",
                    "exception")

    merge_rules = {states.RECEIVED: ("name", "args", "kwargs",
                                     "retries", "eta", "expires")}

    _defaults = dict(uuid=None,
                     name=None,
                     state=states.PENDING,
                     received=False,
                     sent=False,
                     started=False,
                     succeeded=False,
                     failed=False,
                     retried=False,
                     revoked=False,
                     args=None,
                     kwargs=None,
                     eta=None,
                     expires=None,
                     retries=None,
                     worker=None,
                     result=None,
                     exception=None,
                     timestamp=None,
                     runtime=None,
                     traceback=None)

    def __init__(self, **fields):
        super(Task, self).__init__(**dict(self._defaults, **fields))

    def info(self, fields=None, extra=[]):
        if fields is None:
            fields = self._info_fields
        fields = list(fields) + list(extra)
        return dict((key, getattr(self, key, None))
                        for key in fields
                            if getattr(self, key, None) is not None)

    def update(self, state, timestamp, fields):
        if self.worker:
            self.worker.on_heartbeat(timestamp=timestamp)
        if state != states.RETRY and self.state != states.RETRY and \
                states.state(state) < states.state(self.state):
            self.merge(state, timestamp, fields)
        else:
            self.state = state
            self.timestamp = timestamp
            super(Task, self).update(fields)

    def merge(self, state, timestamp, fields):
        keep = self.merge_rules.get(state)
        if keep is not None:
            fields = dict((key, fields.get(key)) for key in keep)
            super(Task, self).update(fields)

    def on_sent(self, timestamp=None, **fields):
        self.sent = timestamp
        self.update(states.PENDING, timestamp, fields)

    def on_received(self, timestamp=None, **fields):
        self.received = timestamp
        self.update(states.RECEIVED, timestamp, fields)

    def on_started(self, timestamp=None, **fields):
        self.started = timestamp
        self.update(states.STARTED, timestamp, fields)

    def on_failed(self, timestamp=None, **fields):
        self.failed = timestamp
        self.update(states.FAILURE, timestamp, fields)

    def on_retried(self, timestamp=None, **fields):
        self.retried = timestamp
        self.update(states.RETRY, timestamp, fields)

    def on_succeeded(self, timestamp=None, **fields):
        self.succeeded = timestamp
        self.update(states.SUCCESS, timestamp, fields)

    def on_revoked(self, timestamp=None, **fields):
        self.revoked = timestamp
        self.update(states.REVOKED, timestamp, fields)

    def __repr__(self):
        return "<Task: %s(%s) %s>" % (self.name, self.uuid, self.state)

    @property
    def ready(self):
        return self.state in states.READY_STATES


class State(object):
    """Records clusters state."""
    event_count = 0
    task_count = 0

    def __init__(self, callback=None,
            max_workers_in_memory=5000, max_tasks_in_memory=10000):
        self.workers = LocalCache(max_workers_in_memory)
        self.tasks = LocalCache(max_tasks_in_memory)
        self.event_callback = callback
        self.group_handlers = {"worker": self.worker_event,
                               "task": self.task_event}
        self._mutex = Lock()

    def freeze_while(self, fun, *args, **kwargs):
        clear_after = kwargs.pop("clear_after", False)
        self._mutex.acquire()
        try:
            return fun(*args, **kwargs)
        finally:
            if clear_after:
                self._clear()
            self._mutex.release()

    def clear_tasks(self, ready=True):
        self._mutex.acquire()
        try:
            return self._clear_tasks(ready)
        finally:
            self._mutex.release()

    def _clear_tasks(self, ready=True):
        if ready:
            self.tasks = dict((uuid, task)
                                for uuid, task in self.tasks.items()
                                    if task.state not in states.READY_STATES)
        else:
            self.tasks.clear()

    def _clear(self, ready=True):
        self.workers.clear()
        self._clear_tasks(ready)
        self.event_count = 0
        self.task_count = 0

    def clear(self, ready=True):
        self._mutex.acquire()
        try:
            return self._clear(ready)
        finally:
            self._mutex.release()

    def get_or_create_worker(self, hostname, **kwargs):
        """Get or create worker by hostname."""
        try:
            worker = self.workers[hostname]
            worker.update(kwargs)
        except KeyError:
            worker = self.workers[hostname] = Worker(
                    hostname=hostname, **kwargs)
        return worker

    def get_or_create_task(self, uuid):
        """Get or create task by uuid."""
        try:
            return self.tasks[uuid]
        except KeyError:
            task = self.tasks[uuid] = Task(uuid=uuid)
            return task

    def worker_event(self, type, fields):
        """Process worker event."""
        hostname = fields.pop("hostname", None)
        if hostname:
            worker = self.get_or_create_worker(hostname)
            handler = getattr(worker, "on_%s" % type, None)
            if handler:
                handler(**fields)

    def task_event(self, type, fields):
        """Process task event."""
        uuid = fields.pop("uuid")
        hostname = fields.pop("hostname")
        worker = self.get_or_create_worker(hostname)
        task = self.get_or_create_task(uuid)
        handler = getattr(task, "on_%s" % type, None)
        if type == "received":
            self.task_count += 1
        if handler:
            handler(**fields)
        task.worker = worker

    def event(self, event):
        self._mutex.acquire()
        try:
            return self._dispatch_event(event)
        finally:
            self._mutex.release()

    def _dispatch_event(self, event):
        self.event_count += 1
        event = kwdict(event)
        group, _, type = partition(event.pop("type"), "-")
        self.group_handlers[group](type, event)
        if self.event_callback:
            self.event_callback(self, event)

    def tasks_by_timestamp(self, limit=None):
        """Get tasks by timestamp.

        Returns a list of `(uuid, task)` tuples.

        """
        return self._sort_tasks_by_time(self.tasks.items()[:limit])

    def _sort_tasks_by_time(self, tasks):
        """Sort task items by time."""
        return sorted(tasks, key=lambda t: t[1].timestamp,
                      reverse=True)

    def tasks_by_type(self, name, limit=None):
        """Get all tasks by type.

        Returns a list of `(uuid, task)` tuples.

        """
        return self._sort_tasks_by_time([(uuid, task)
                for uuid, task in self.tasks.items()[:limit]
                    if task.name == name])

    def tasks_by_worker(self, hostname, limit=None):
        """Get all tasks by worker.

        Returns a list of `(uuid, task)` tuples.

        """
        return self._sort_tasks_by_time([(uuid, task)
                for uuid, task in self.tasks.items()[:limit]
                    if task.worker.hostname == hostname])

    def task_types(self):
        """Returns a list of all seen task types."""
        return list(sorted(set(task.name for task in self.tasks.values())))

    def alive_workers(self):
        """Returns a list of (seemingly) alive workers."""
        return [w for w in self.workers.values() if w.alive]

    def __repr__(self):
        return "<ClusterState: events=%s tasks=%s>" % (self.event_count,
                                                       self.task_count)


state = State()
