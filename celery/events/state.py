# -*- coding: utf-8 -*-
"""
    celery.events.state
    ~~~~~~~~~~~~~~~~~~~

    This module implements a datastructure used to keep
    track of the state of a cluster of workers and the tasks
    it is working on (by consuming events).

    For every event consumed the state is updated,
    so the state represents the state of the cluster
    at the time of the last event.

    Snapshots (:mod:`celery.events.snapshot`) can be used to
    take "pictures" of this state at regular intervals
    to e.g. store that in a database.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import time
import heapq

from threading import Lock

from .. import states
from ..datastructures import AttributeDict, LRUCache
from ..utils import kwdict

#: Hartbeat expiry time in seconds.  The worker will be considered offline
#: if no heartbeat is received within this time.
#: Default is 2:30 minutes.
HEARTBEAT_EXPIRE = 150


class Element(AttributeDict):
    """Base class for worker state elements."""


class Worker(Element):
    """Worker State."""
    heartbeat_max = 4

    def __init__(self, **fields):
        super(Worker, self).__init__(**fields)
        self.heartbeats = []

    def on_online(self, timestamp=None, **kwargs):
        """Callback for the `worker-online` event."""
        self._heartpush(timestamp)

    def on_offline(self, **kwargs):
        """Callback for the `worker-offline` event."""
        self.heartbeats = []

    def on_heartbeat(self, timestamp=None, **kwargs):
        """Callback for the `worker-heartbeat` event."""
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

    #: How to merge out of order events.
    #: Disorder is detected by logical ordering (e.g. task-received must have
    #: happened before a task-failed event).
    #:
    #: A merge rule consists of a state and a list of fields to keep from
    #: that state. ``(RECEIVED, ("name", "args")``, means the name and args
    #: fields are always taken from the RECEIVED state, and any values for
    #: these fields received before or after is simply ignored.
    merge_rules = {states.RECEIVED: ("name", "args", "kwargs",
                                     "retries", "eta", "expires")}

    #: meth:`info` displays these fields by default.
    _info_fields = ("args", "kwargs", "retries", "result",
                    "eta", "runtime", "expires", "exception")

    #: Default values.
    _defaults = dict(uuid=None, name=None, state=states.PENDING,
                     received=False, sent=False, started=False,
                     succeeded=False, failed=False, retried=False,
                     revoked=False, args=None, kwargs=None, eta=None,
                     expires=None, retries=None, worker=None, result=None,
                     exception=None, timestamp=None, runtime=None,
                     traceback=None)

    def __init__(self, **fields):
        super(Task, self).__init__(**dict(self._defaults, **fields))

    def update(self, state, timestamp, fields):
        """Update state from new event.

        :param state: State from event.
        :param timestamp: Timestamp from event.
        :param fields: Event data.

        """
        if self.worker:
            self.worker.on_heartbeat(timestamp=timestamp)
        if state != states.RETRY and self.state != states.RETRY and \
                states.state(state) < states.state(self.state):
            # this state logically happens-before the current state, so merge.
            self.merge(state, timestamp, fields)
        else:
            self.state = state
            self.timestamp = timestamp
            super(Task, self).update(fields)

    def merge(self, state, timestamp, fields):
        """Merge with out of order event."""
        keep = self.merge_rules.get(state)
        if keep is not None:
            fields = dict((key, fields.get(key)) for key in keep)
            super(Task, self).update(fields)

    def on_sent(self, timestamp=None, **fields):
        """Callback for the ``task-sent`` event."""
        self.sent = timestamp
        self.update(states.PENDING, timestamp, fields)

    def on_received(self, timestamp=None, **fields):
        """Callback for the ``task-received`` event."""
        self.received = timestamp
        self.update(states.RECEIVED, timestamp, fields)

    def on_started(self, timestamp=None, **fields):
        """Callback for the ``task-started`` event."""
        self.started = timestamp
        self.update(states.STARTED, timestamp, fields)

    def on_failed(self, timestamp=None, **fields):
        """Callback for the ``task-failed`` event."""
        self.failed = timestamp
        self.update(states.FAILURE, timestamp, fields)

    def on_retried(self, timestamp=None, **fields):
        """Callback for the ``task-retried`` event."""
        self.retried = timestamp
        self.update(states.RETRY, timestamp, fields)

    def on_succeeded(self, timestamp=None, **fields):
        """Callback for the ``task-succeeded`` event."""
        self.succeeded = timestamp
        self.update(states.SUCCESS, timestamp, fields)

    def on_revoked(self, timestamp=None, **fields):
        """Callback for the ``task-revoked`` event."""
        self.revoked = timestamp
        self.update(states.REVOKED, timestamp, fields)

    def on_unknown_event(self, type, timestamp=None, **fields):
        self.update(type.upper(), timestamp, fields)

    def info(self, fields=None, extra=[]):
        """Information about this task suitable for on-screen display."""
        if fields is None:
            fields = self._info_fields
        return dict((key, getattr(self, key, None))
                        for key in list(fields) + list(extra)
                            if getattr(self, key, None) is not None)

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
        self.workers = LRUCache(limit=max_workers_in_memory)
        self.tasks = LRUCache(limit=max_tasks_in_memory)
        self.event_callback = callback
        self.group_handlers = {"worker": self.worker_event,
                               "task": self.task_event}
        self._mutex = Lock()

    def freeze_while(self, fun, *args, **kwargs):
        clear_after = kwargs.pop("clear_after", False)
        with self._mutex:
            try:
                return fun(*args, **kwargs)
            finally:
                if clear_after:
                    self._clear()

    def clear_tasks(self, ready=True):
        with self._mutex:
            return self._clear_tasks(ready)

    def _clear_tasks(self, ready=True):
        if ready:
            in_progress = dict((uuid, task) for uuid, task in self.itertasks()
                                if task.state not in states.READY_STATES)
            self.tasks.clear()
            self.tasks.update(in_progress)
        else:
            self.tasks.clear()

    def _clear(self, ready=True):
        self.workers.clear()
        self._clear_tasks(ready)
        self.event_count = 0
        self.task_count = 0

    def clear(self, ready=True):
        with self._mutex:
            return self._clear(ready)

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
        else:
            task.on_unknown_event(type, **fields)
        task.worker = worker

    def event(self, event):
        with self._mutex:
            return self._dispatch_event(event)

    def _dispatch_event(self, event):
        self.event_count += 1
        event = kwdict(event)
        group, _, type = event.pop("type").partition("-")
        self.group_handlers[group](type, event)
        if self.event_callback:
            self.event_callback(self, event)

    def itertasks(self, limit=None):
        for index, row in enumerate(self.tasks.iteritems()):
            yield row
            if limit and index >= limit:
                break

    def tasks_by_timestamp(self, limit=None):
        """Get tasks by timestamp.

        Returns a list of `(uuid, task)` tuples.

        """
        return self._sort_tasks_by_time(self.itertasks(limit))

    def _sort_tasks_by_time(self, tasks):
        """Sort task items by time."""
        return sorted(tasks, key=lambda t: t[1].timestamp,
                      reverse=True)

    def tasks_by_type(self, name, limit=None):
        """Get all tasks by type.

        Returns a list of `(uuid, task)` tuples.

        """
        return self._sort_tasks_by_time([(uuid, task)
                for uuid, task in self.itertasks(limit)
                    if task.name == name])

    def tasks_by_worker(self, hostname, limit=None):
        """Get all tasks by worker.

        Returns a list of `(uuid, task)` tuples.

        """
        return self._sort_tasks_by_time([(uuid, task)
                for uuid, task in self.itertasks(limit)
                    if task.worker.hostname == hostname])

    def task_types(self):
        """Returns a list of all seen task types."""
        return list(sorted(set(task.name for task in self.tasks.itervalues())))

    def alive_workers(self):
        """Returns a list of (seemingly) alive workers."""
        return [w for w in self.workers.values() if w.alive]

    def __repr__(self):
        return "<ClusterState: events=%s tasks=%s>" % (self.event_count,
                                                       self.task_count)


state = State()
