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

"""
from __future__ import absolute_import

import threading

from heapq import heappush
from itertools import islice
from operator import itemgetter
from time import time

from kombu.utils import kwdict

from celery import states
from celery.datastructures import AttributeDict, LRUCache
from celery.five import items, values
from celery.utils.log import get_logger

# The window (in percentage) is added to the workers heartbeat
# frequency.  If the time between updates exceeds this window,
# then the worker is considered to be offline.
HEARTBEAT_EXPIRE_WINDOW = 200

# Max drift between event timestamp and time of event received
# before we alert that clocks may be unsynchronized.
HEARTBEAT_DRIFT_MAX = 16

DRIFT_WARNING = """\
Substantial drift from %s may mean clocks are out of sync.  Current drift is
%s seconds (including message overhead).\
"""

logger = get_logger(__name__)
warn = logger.warn


def heartbeat_expires(timestamp, freq=60,
                      expire_window=HEARTBEAT_EXPIRE_WINDOW):
    return timestamp + freq * (expire_window / 1e2)


class _lamportinfo(tuple):
    __slots__ = ()

    def __new__(cls, clock, timestamp, id, obj):
        return tuple.__new__(cls, (clock, timestamp, id, obj))

    def __repr__(self):
        return '_lamport(clock={0}, timestamp={1}, id={2} {3!r}'.format(*self)

    def __getnewargs__(self):
        return tuple(self)

    def __lt__(self, other):
        # 0: clock 1: timestamp 3: process id
        try:
            A, B = self[0], other[0]
            # uses logical clock value first
            if A and B:  # use logical clock if available
                if A == B:  # equal clocks use lower process id
                    return self[3] < other[3]
                return A < B
            return self[1] < other[1]  # ... or use timestamp
        except IndexError:
            return NotImplemented
    __gt__ = lambda self, other: other < self
    __le__ = lambda self, other: not other < self
    __ge__ = lambda self, other: not self < other

    clock = property(itemgetter(0))
    timestamp = property(itemgetter(1))
    id = property(itemgetter(2))
    obj = property(itemgetter(3))


class Element(AttributeDict):
    """Base class for worker state elements."""


class Worker(Element):
    """Worker State."""
    heartbeat_max = 4
    expire_window = HEARTBEAT_EXPIRE_WINDOW
    pid = None

    def __init__(self, **fields):
        fields.setdefault('freq', 60)
        super(Worker, self).__init__(**fields)
        self.heartbeats = []

    def on_online(self, timestamp=None, local_received=None, **kwargs):
        """Callback for the :event:`worker-online` event."""
        self.update(**kwargs)
        self.update_heartbeat(local_received, timestamp)

    def on_offline(self, **kwargs):
        """Callback for the :event:`worker-offline` event."""
        self.update(**kwargs)
        self.heartbeats = []

    def on_heartbeat(self, timestamp=None, local_received=None, **kwargs):
        """Callback for the :event:`worker-heartbeat` event."""
        self.update(**kwargs)
        self.update_heartbeat(local_received, timestamp)

    def update_heartbeat(self, received, timestamp):
        if not received or not timestamp:
            return
        drift = received - timestamp
        if drift > HEARTBEAT_DRIFT_MAX:
            warn(DRIFT_WARNING, self.hostname, drift)
        heartbeats, hbmax = self.heartbeats, self.heartbeat_max
        if not heartbeats or (received and received > heartbeats[-1]):
            heappush(heartbeats, received)
            if len(heartbeats) > hbmax:
                heartbeats[:] = heartbeats[hbmax:]

    def __repr__(self):
        return '<Worker: {0.hostname} ({0.status_string})'.format(self)

    @property
    def status_string(self):
        return 'ONLINE' if self.alive else 'OFFLINE'

    @property
    def heartbeat_expires(self):
        return heartbeat_expires(self.heartbeats[-1],
                                 self.freq, self.expire_window)

    @property
    def alive(self):
        return bool(self.heartbeats and time() < self.heartbeat_expires)

    @property
    def id(self):
        return '{0.hostname}.{0.pid}'.format(self)


class Task(Element):
    """Task State."""

    #: How to merge out of order events.
    #: Disorder is detected by logical ordering (e.g. :event:`task-received`
    #: must have happened before a :event:`task-failed` event).
    #:
    #: A merge rule consists of a state and a list of fields to keep from
    #: that state. ``(RECEIVED, ('name', 'args')``, means the name and args
    #: fields are always taken from the RECEIVED state, and any values for
    #: these fields received before or after is simply ignored.
    merge_rules = {states.RECEIVED: ('name', 'args', 'kwargs',
                                     'retries', 'eta', 'expires')}

    #: meth:`info` displays these fields by default.
    _info_fields = ('args', 'kwargs', 'retries', 'result',
                    'eta', 'runtime', 'expires', 'exception',
                    'exchange', 'routing_key')

    #: Default values.
    _defaults = dict(uuid=None, name=None, state=states.PENDING,
                     received=False, sent=False, started=False,
                     succeeded=False, failed=False, retried=False,
                     revoked=False, args=None, kwargs=None, eta=None,
                     expires=None, retries=None, worker=None, result=None,
                     exception=None, timestamp=None, runtime=None,
                     traceback=None, exchange=None, routing_key=None,
                     clock=0)

    def __init__(self, **fields):
        super(Task, self).__init__(**dict(self._defaults, **fields))

    def update(self, state, timestamp, fields):
        """Update state from new event.

        :param state: State from event.
        :param timestamp: Timestamp from event.
        :param fields: Event data.

        """
        time_received = fields.get('local_received') or 0
        if self.worker and time_received:
            self.worker.update_heartbeat(time_received, timestamp)
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
        """Callback for the :event:`task-sent` event."""
        self.sent = timestamp
        self.update(states.PENDING, timestamp, fields)

    def on_received(self, timestamp=None, **fields):
        """Callback for the :event:`task-received` event."""
        self.received = timestamp
        self.update(states.RECEIVED, timestamp, fields)

    def on_started(self, timestamp=None, **fields):
        """Callback for the :event:`task-started` event."""
        self.started = timestamp
        self.update(states.STARTED, timestamp, fields)

    def on_failed(self, timestamp=None, **fields):
        """Callback for the :event:`task-failed` event."""
        self.failed = timestamp
        self.update(states.FAILURE, timestamp, fields)

    def on_retried(self, timestamp=None, **fields):
        """Callback for the :event:`task-retried` event."""
        self.retried = timestamp
        self.update(states.RETRY, timestamp, fields)

    def on_succeeded(self, timestamp=None, **fields):
        """Callback for the :event:`task-succeeded` event."""
        self.succeeded = timestamp
        self.update(states.SUCCESS, timestamp, fields)

    def on_revoked(self, timestamp=None, **fields):
        """Callback for the :event:`task-revoked` event."""
        self.revoked = timestamp
        self.update(states.REVOKED, timestamp, fields)

    def on_unknown_event(self, type, timestamp=None, **fields):
        self.update(type.upper(), timestamp, fields)

    def info(self, fields=None, extra=[]):
        """Information about this task suitable for on-screen display."""
        fields = self._info_fields if fields is None else fields

        def _keys():
            for key in list(fields) + list(extra):
                value = getattr(self, key, None)
                if value is not None:
                    yield key, value

        return dict(_keys())

    def __repr__(self):
        return '<Task: {0.name}({0.uuid}) {0.state}>'.format(self)

    @property
    def ready(self):
        return self.state in states.READY_STATES


class State(object):
    """Records clusters state."""
    event_count = 0
    task_count = 0

    def __init__(self, callback=None,
                 max_workers_in_memory=5000, max_tasks_in_memory=10000):
        self.max_workers_in_memory = max_workers_in_memory
        self.max_tasks_in_memory = max_tasks_in_memory
        self.workers = LRUCache(limit=self.max_workers_in_memory)
        self.tasks = LRUCache(limit=self.max_tasks_in_memory)
        self._taskheap = []
        self.event_callback = callback
        self._mutex = threading.Lock()

    def freeze_while(self, fun, *args, **kwargs):
        clear_after = kwargs.pop('clear_after', False)
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
            in_progress = dict(
                (uuid, task) for uuid, task in self.itertasks()
                if task.state not in states.READY_STATES)
            self.tasks.clear()
            self.tasks.update(in_progress)
        else:
            self.tasks.clear()
        self._taskheap[:] = []

    def _clear(self, ready=True):
        self.workers.clear()
        self._clear_tasks(ready)
        self.event_count = 0
        self.task_count = 0

    def clear(self, ready=True):
        with self._mutex:
            return self._clear(ready)

    def get_or_create_worker(self, hostname, **kwargs):
        """Get or create worker by hostname.

        Returns tuple of ``(worker, was_created)``.
        """
        try:
            worker = self.workers[hostname]
            worker.update(kwargs)
            return worker, False
        except KeyError:
            worker = self.workers[hostname] = Worker(
                hostname=hostname, **kwargs)
            return worker, True

    def get_or_create_task(self, uuid):
        """Get or create task by uuid."""
        try:
            return self.tasks[uuid], True
        except KeyError:
            task = self.tasks[uuid] = Task(uuid=uuid)
            return task, False

    def worker_event(self, type, fields):
        """Process worker event."""
        try:
            hostname = fields['hostname']
        except KeyError:
            pass
        else:
            worker, created = self.get_or_create_worker(hostname)
            handler = getattr(worker, 'on_' + type, None)
            if handler:
                handler(**fields)
            return worker, created

    def task_event(self, type, fields):
        """Process task event."""
        uuid = fields.pop('uuid')
        hostname = fields.pop('hostname')
        worker, _ = self.get_or_create_worker(hostname)
        task, created = self.get_or_create_task(uuid)
        task.worker = worker

        taskheap = self._taskheap
        timestamp = fields.get('timestamp') or 0
        clock = 0 if type == 'sent' else fields.get('clock')
        heappush(taskheap, _lamportinfo(clock, timestamp, worker.id, task))
        curcount = len(self.tasks)
        if len(taskheap) > self.max_tasks_in_memory * 2:
            taskheap[:] = taskheap[curcount:]

        handler = getattr(task, 'on_' + type, None)
        if type == 'received':
            self.task_count += 1
        if handler:
            handler(**fields)
        else:
            task.on_unknown_event(type, **fields)
        return created

    def event(self, event):
        with self._mutex:
            return self._dispatch_event(event)

    def _dispatch_event(self, event):
        self.event_count += 1
        event = kwdict(event)
        group, _, subject = event.pop('type').partition('-')
        getattr(self, group + '_event')(subject, event)
        if self.event_callback:
            self.event_callback(self, event)

    def itertasks(self, limit=None):
        for index, row in enumerate(items(self.tasks)):
            yield row
            if limit and index + 1 >= limit:
                break

    def tasks_by_time(self, limit=None):
        """Generator giving tasks ordered by time,
        in ``(uuid, Task)`` tuples."""
        seen = set()
        for evtup in islice(reversed(self._taskheap), 0, limit):
            uuid = evtup[3].uuid
            if uuid not in seen:
                yield uuid, evtup[3]
                seen.add(uuid)
    tasks_by_timestamp = tasks_by_time

    def tasks_by_type(self, name, limit=None):
        """Get all tasks by type.

        Returns a list of ``(uuid, Task)`` tuples.

        """
        return islice(
            ((uuid, task) for uuid, task in self.tasks_by_time()
             if task.name == name),
            0, limit,
        )

    def tasks_by_worker(self, hostname, limit=None):
        """Get all tasks by worker.

        """
        return islice(
            ((uuid, task) for uuid, task in self.tasks_by_time()
             if task.worker.hostname == hostname),
            0, limit,
        )

    def task_types(self):
        """Returns a list of all seen task types."""
        return list(sorted(set(task.name for task in values(self.tasks))))

    def alive_workers(self):
        """Returns a list of (seemingly) alive workers."""
        return [w for w in values(self.workers) if w.alive]

    def __repr__(self):
        return '<State: events={0.event_count} tasks={0.task_count}>' \
            .format(self)

    def __getstate__(self):
        d = dict(vars(self))
        d.pop('_mutex')
        return d

    def __setstate__(self, state):
        self.__dict__ = state
        self._mutex = threading.Lock()


state = State()
