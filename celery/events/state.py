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

import sys
import threading

from datetime import datetime
from heapq import heappush, heappop
from itertools import islice
from operator import itemgetter
from time import time

from kombu.clocks import timetuple

from celery import states
from celery.five import items, values
from celery.utils.functional import LRUCache
from celery.utils.log import get_logger

PYPY = hasattr(sys, 'pypy_version_info')

# The window (in percentage) is added to the workers heartbeat
# frequency.  If the time between updates exceeds this window,
# then the worker is considered to be offline.
HEARTBEAT_EXPIRE_WINDOW = 200

# Max drift between event timestamp and time of event received
# before we alert that clocks may be unsynchronized.
HEARTBEAT_DRIFT_MAX = 16

DRIFT_WARNING = """\
Substantial drift from %s may mean clocks are out of sync.  Current drift is
%s seconds.  [orig: %s recv: %s]
"""

CAN_KWDICT = sys.version_info >= (2, 6, 5)

logger = get_logger(__name__)
warn = logger.warning

R_STATE = '<State: events={0.event_count} tasks={0.task_count}>'
R_WORKER = '<Worker: {0.hostname} ({0.status_string} clock:{0.clock})'
R_TASK = '<Task: {0.name}({0.uuid}) {0.state} clock:{0.clock}>'

__all__ = ['Worker', 'Task', 'State', 'heartbeat_expires']


def heartbeat_expires(timestamp, freq=60,
                      expire_window=HEARTBEAT_EXPIRE_WINDOW):
    return timestamp + freq * (expire_window / 1e2)


def with_unique_field(attr):

    def _decorate_cls(cls):

        def __eq__(this, other):
            if isinstance(other, this.__class__):
                return getattr(this, attr) == getattr(other, attr)
            return NotImplemented
        cls.__eq__ = __eq__

        def __ne__(this, other):
            return not this.__eq__(other)
        cls.__ne__ = __ne__

        def __hash__(this):
            return hash(getattr(this, attr))
        cls.__hash__ = __hash__

        return cls
    return _decorate_cls


@with_unique_field('hostname')
class Worker(object):
    """Worker State."""
    heartbeat_max = 4
    expire_window = HEARTBEAT_EXPIRE_WINDOW
    pid = None
    _defaults = {'hostname': None, 'pid': None, 'freq': 60}

    if not PYPY:
        __slots__ = ('hostname', 'pid', 'freq',
                     'heartbeats', 'clock', '__dict__')

    def __init__(self, hostname=None, pid=None, freq=60):
        self.hostname = hostname
        self.pid = pid
        self.freq = freq
        self.heartbeats = []
        self.clock = 0
        self.event = self._create_event_handler()

    def _create_event_handler(self):
        _set = object.__setattr__
        heartbeats = self.heartbeats
        hbmax = self.heartbeat_max

        def event(type_, timestamp=None,
                  local_received=None, fields=None,
                  max_drift=HEARTBEAT_DRIFT_MAX, items=items, abs=abs,
                  heappush=heappush, heappop=heappop, int=int, len=len):
            fields = fields or {}
            for k, v in items(fields):
                _set(self, k, v)
            if type_ == 'offline':
                heartbeats[:] = []
            else:
                if not local_received or not timestamp:
                    return
                drift = abs(int(local_received) - int(timestamp))
                if drift > HEARTBEAT_DRIFT_MAX:
                    warn(DRIFT_WARNING, self.hostname, drift,
                         datetime.fromtimestamp(local_received),
                         datetime.fromtimestamp(timestamp))
                if not heartbeats or (
                        local_received and local_received > heartbeats[-1]):
                    heappush(heartbeats, local_received)
                    if len(heartbeats) > hbmax:
                        heappop(heartbeats)
        return event

    def update(self, f, **kw):
        for k, v in items(dict(f, **kw) if kw else f):
            setattr(self, k, v)

    def update_heartbeat(self, received, timestamp):
        self.event(None, timestamp, received)

    def on_online(self, timestamp=None, local_received=None, **fields):
        """Deprecated, to be removed in 3.1, use:
        ``.event('online', timestamp, local_received, fields)``."""
        self.event('online', timestamp, local_received, fields)

    def on_offline(self, timestamp=None, local_received=None, **fields):
        """Deprecated, to be removed in 3.1, use:
        ``.event('offline', timestamp, local_received, fields)``."""
        self.event('offline', timestamp, local_received, fields)

    def on_heartbeat(self, timestamp=None, local_received=None, **fields):
        """Deprecated, to be removed in 3.1, use:
        ``.event('heartbeat', timestamp, local_received, fields)``."""
        self.event('heartbeat', timestamp, local_received, fields)

    def __repr__(self):
        return R_WORKER.format(self)

    @property
    def status_string(self):
        return 'ONLINE' if self.alive else 'OFFLINE'

    @property
    def heartbeat_expires(self):
        return heartbeat_expires(self.heartbeats[-1],
                                 self.freq, self.expire_window)

    @property
    def alive(self, nowfun=time):
        return bool(self.heartbeats and nowfun() < self.heartbeat_expires)

    @property
    def id(self):
        return '{0.hostname}.{0.pid}'.format(self)


@with_unique_field('uuid')
class Task(object):
    """Task State."""
    if not PYPY:
        __slots__ = (
            'uuid', 'name', 'state', 'received', 'sent', 'started',
            'succeeded', 'failed', 'retried', 'revoked', 'args', 'kwargs',
            'eta', 'expires', 'retries', 'worker', 'result',
            'exception', 'timestamp', 'runtime',
            'traceback', 'exchange', 'routing_key', 'clock',
            '__dict__',
        )

    #: How to merge out of order events.
    #: Disorder is detected by logical ordering (e.g. :event:`task-received`
    #: must have happened before a :event:`task-failed` event).
    #:
    #: A merge rule consists of a state and a list of fields to keep from
    #: that state. ``(RECEIVED, ('name', 'args')``, means the name and args
    #: fields are always taken from the RECEIVED state, and any values for
    #: these fields received before or after is simply ignored.
    merge_rules = {
        states.RECEIVED: ('name', 'args', 'kwargs',
                          'retries', 'eta', 'expires'),
    }

    #: meth:`info` displays these fields by default.
    _info_fields = ('args', 'kwargs', 'retries', 'result',
                    'eta', 'runtime', 'expires', 'exception',
                    'exchange', 'routing_key')

    def __init__(self, uuid=None, name=None, state=states.PENDING,
                 received=None, sent=None, started=None, succeeded=None,
                 failed=None, retried=None, revoked=None, args=None,
                 kwargs=None, eta=None, expires=None, retries=None,
                 worker=None, result=None, exception=None, timestamp=None,
                 runtime=None, traceback=None, exchange=None,
                 routing_key=None, clock=0, **fields):
        self.uuid = uuid
        self.name = name
        self.state = state
        self.received = received
        self.sent = sent
        self.started = started
        self.succeeded = succeeded
        self.failed = failed
        self.retried = retried
        self.revoked = revoked
        self.args = args
        self.kwargs = kwargs
        self.eta = eta
        self.expires = expires
        self.retries = retries
        self.worker = worker
        self.result = result
        self.exception = exception
        self.timestamp = timestamp
        self.runtime = runtime
        self.traceback = traceback
        self.exchange = exchange
        self.routing_key = routing_key
        self.clock = clock

    def update(self, state, timestamp, fields,
               _state=states.state, RETRY=states.RETRY):
        """Update state from new event.

        :param state: State from event.
        :param timestamp: Timestamp from event.
        :param fields: Event data.

        """
        time_received = fields.get('local_received') or 0
        if self.worker and time_received:
            self.worker.update_heartbeat(time_received, timestamp)
        if state != RETRY and self.state != RETRY and \
                _state(state) < _state(self.state):
            # this state logically happens-before the current state, so merge.
            self.merge(state, timestamp, fields)
        else:
            self.state = state
            self.timestamp = timestamp
            for key, value in items(self.fields):
                setattr(self, key, value)

    def merge(self, state, timestamp, fields):
        """Merge with out of order event."""
        keep = self.merge_rules.get(state)
        if keep is not None:
            fields = dict((k, v) for k, v in items(fields) if k in keep)
        for key, value in items(fields):
            setattr(self, key, value)

    def event(self, type_, timestamp=None, local_received=None, fields=None,
              precedence=states.precedence, items=items, dict=dict,
              PENDING=states.PENDING, RECEIVED=states.RECEIVED,
              STARTED=states.STARTED, FAILURE=states.FAILURE,
              RETRY=states.RETRY, SUCCESS=states.SUCCESS,
              REVOKED=states.REVOKED):
        fields = fields or {}
        if type_ == 'sent':
            state, self.sent = PENDING, timestamp
        elif type_ == 'received':
            state, self.received = RECEIVED, timestamp
        elif type_ == 'started':
            state, self.started = STARTED, timestamp
        elif type_ == 'failed':
            state, self.failed = FAILURE, timestamp
        elif type_ == 'retried':
            state, self.retried = RETRY, timestamp
        elif type_ == 'succeeded':
            state, self.succeeded = SUCCESS, timestamp
        elif type_ == 'revoked':
            state, self.revoked = REVOKED, timestamp
        else:
            state = type_.upper()

        # note that precedence here is reversed
        # see implementation in celery.states.state.__lt__
        if state != RETRY and self.state != RETRY and \
                precedence(state) > precedence(self.state):
            # this state logically happens-before the current state, so merge.
            keep = self.merge_rules.get(state)
            if keep is not None:
                fields = dict(
                    (k, v) for k, v in items(fields) if k in keep
                )
            for key, value in items(fields):
                setattr(self, key, value)
        else:
            self.state = state
            self.timestamp = timestamp
            for key, value in items(fields):
                setattr(self, key, value)

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
        return R_TASK.format(self)

    @property
    def ready(self):
        return self.state in states.READY_STATES

    def on_sent(self, timestamp=None, **fields):
        """Deprecated, to be removed in 3.2, use:
        ``.event('sent', timestamp, fields)``."""
        self.event('sent', timestamp, fields)

    def on_received(self, timestamp=None, **fields):
        """Deprecated, to be removed in 3.2, use:
        ``.event('received', timestamp, fields)``."""
        self.event('received', timestamp, fields)

    def on_started(self, timestamp=None, **fields):
        """Deprecated, to be removed in 3.2, use:
        ``.event('started', timestamp, fields)``."""
        self.event('started', timestamp, fields)

    def on_failed(self, timestamp=None, **fields):
        """Deprecated, to be removed in 3.2, use:
        ``.event('failed', timestamp, fields)``."""
        self.event('failed', timestamp, fields)

    def on_retried(self, timestamp=None, **fields):
        """Deprecated, to be removed in 3.2, use:
        ``.event('retried', timestamp, fields)``."""
        self.event('retried', timestamp, fields)

    def on_succeeded(self, timestamp=None, **fields):
        """Deprecated, to be removed in 3.2, use:
        ``.event('succeeded', timestamp, fields)``."""
        self.event('succeeded', timestamp, fields)

    def on_revoked(self, timestamp=None, **fields):
        """Deprecated, to be removed in 3.2, use:
        ``.event('revoked', timestamp, fields)``."""
        self.event('revoked', timestamp, fields)

    def on_unknown_event(self, shortype, timestamp=None, **fields):
        """Deprecated, to be removed in 3.2, use:
        ``.event(type, timestamp, fields)``."""
        self.event(shortype, timestamp, fields)


class State(object):
    """Records clusters state."""
    event_count = 0
    task_count = 0

    def __init__(self, callback=None,
                 workers=None, tasks=None, taskheap=None,
                 max_workers_in_memory=5000, max_tasks_in_memory=10000):
        self.event_callback = callback
        self.workers = (LRUCache(max_workers_in_memory)
                        if workers is None else workers)
        self.tasks = (LRUCache(max_tasks_in_memory)
                      if tasks is None else tasks)
        self._taskheap = [] if taskheap is None else taskheap
        self.max_workers_in_memory = max_workers_in_memory
        self.max_tasks_in_memory = max_tasks_in_memory
        self._mutex = threading.Lock()
        self.handlers = {}
        self._event = self._create_dispatcher()

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

        Return tuple of ``(worker, was_created)``.
        """
        try:
            worker = self.workers[hostname]
            if kwargs:
                worker.update(kwargs)
            return worker, False
        except KeyError:
            worker = self.workers[hostname] = Worker(
                hostname=hostname, **kwargs)
            return worker, True

    def get_or_create_task(self, uuid):
        """Get or create task by uuid."""
        try:
            return self.tasks[uuid], False
        except KeyError:
            task = self.tasks[uuid] = Task(uuid=uuid)
            return task, True

    def event(self, event):
        with self._mutex:
            return self._event(event)

    def task_event(self, type_, fields):
        """Deprecated, use :meth:`event`."""
        return self._event(dict(fields, type='-'.join(['task', type_])))

    def worker_event(self, type_, fields):
        """Deprecated, use :meth:`event`."""
        return self._event(dict(fields, type='-'.join(['worker', type_])))

    def _create_dispatcher(self):
        get_handler = self.handlers.__getitem__
        event_callback = self.event_callback
        get_or_create_worker = self.get_or_create_worker
        get_or_create_task = self.get_or_create_task
        wfields = itemgetter('hostname', 'timestamp', 'local_received')
        tfields = itemgetter('uuid', 'hostname', 'timestamp', 'local_received')
        taskheap = self._taskheap
        maxtasks = self.max_tasks_in_memory * 2

        def _event(event, timetuple=timetuple):
            self.event_count += 1
            if event_callback:
                event_callback(self, event)
            group, _, subject = event['type'].partition('-')
            try:
                handler = get_handler(group)
            except KeyError:
                pass
            else:
                return handler(subject, event)

            if group == 'worker':
                try:
                    hostname, timestamp, local_received = wfields(event)
                except KeyError:
                    pass
                else:
                    worker, created = get_or_create_worker(hostname)
                    worker.event(subject, timestamp, local_received, event)
                    return created
            elif group == 'task':
                uuid, hostname, timestamp, local_received = tfields(event)
                # task-sent event is sent by client, not worker
                is_client_event = subject == 'sent'
                task, created = get_or_create_task(uuid)
                if not is_client_event:
                    worker, _ = get_or_create_worker(hostname)
                    task.worker = worker
                    if worker is not None and local_received:
                        worker.event(None, local_received, timestamp)
                clock = 0 if is_client_event else event.get('clock')
                heappush(
                    taskheap, timetuple(clock, timestamp, worker.id, task),
                )
                while len(taskheap) > maxtasks:
                    heappop(taskheap)
                #if len(taskheap) > maxtasks:
                #    heappop(taskheap)
                if subject == 'received':
                    self.task_count += 1
                task.event(subject, timestamp, local_received, event)
                return created
        return _event

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

        Return a list of ``(uuid, Task)`` tuples.

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
        """Return a list of all seen task types."""
        return list(sorted(set(task.name for task in values(self.tasks)
                               if task.name is not None)))

    def alive_workers(self):
        """Return a list of (seemingly) alive workers."""
        return [w for w in values(self.workers) if w.alive]

    def __repr__(self):
        return R_STATE.format(self)

    def __reduce__(self):
        return self.__class__, (
            self.event_callback, self.workers, self.tasks, self._taskheap,
            self.max_workers_in_memory, self.max_tasks_in_memory,
        )
