# -*- coding: utf-8 -*-
"""In-memory representation of cluster state.

This module implements a data-structure used to keep
track of the state of a cluster of workers and the tasks
it is working on (by consuming events).

For every event consumed the state is updated,
so the state represents the state of the cluster
at the time of the last event.

Snapshots (:mod:`celery.events.snapshot`) can be used to
take "pictures" of this state at regular intervals
to for example, store that in a database.
"""
import bisect
import sys
import threading
from collections import Callable, defaultdict
from datetime import datetime
from decimal import Decimal
from itertools import islice
from operator import itemgetter
from time import time
from typing import Any, Iterator, List, Mapping, Set, Sequence, Tuple
from weakref import WeakSet, ref

from kombu.clocks import timetuple
from kombu.utils.objects import cached_property

from celery import states
from celery.utils.functional import LRUCache, memoize, pass1
from celery.utils.log import get_logger

__all__ = ('Worker', 'Task', 'State', 'heartbeat_expires')

# pylint: disable=redefined-outer-name
# We cache globals and attribute lookups, so disable this warning.
# pylint: disable=too-many-function-args
# For some reason pylint thinks ._event is a method, when it's a property.

#: Set if running PyPy
PYPY = hasattr(sys, 'pypy_version_info')

#: The window (in percentage) is added to the workers heartbeat
#: frequency.  If the time between updates exceeds this window,
#: then the worker is considered to be offline.
HEARTBEAT_EXPIRE_WINDOW = 200

#: Max drift between event timestamp and time of event received
#: before we alert that clocks may be unsynchronized.
HEARTBEAT_DRIFT_MAX = 16

DRIFT_WARNING = """\
Substantial drift from %s may mean clocks are out of sync.  Current drift is
%s seconds.  [orig: %s recv: %s]
"""

logger = get_logger(__name__)
warn = logger.warning

R_STATE = '<State: events={0.event_count} tasks={0.task_count}>'
R_WORKER = '<Worker: {0.hostname} ({0.status_string} clock:{0.clock})'
R_TASK = '<Task: {0.name}({0.uuid}) {0.state} clock:{0.clock}>'

#: Mapping of task event names to task state.
TASK_EVENT_TO_STATE: Mapping[str, str] = {
    'sent': states.PENDING,
    'received': states.RECEIVED,
    'started': states.STARTED,
    'failed': states.FAILURE,
    'retried': states.RETRY,
    'succeeded': states.SUCCESS,
    'revoked': states.REVOKED,
    'rejected': states.REJECTED,
}


class CallableDefaultdict(defaultdict):
    """:class:`~collections.defaultdict` with configurable __call__.

    We use this for backwards compatibility in State.tasks_by_type
    etc, which used to be a method but is now an index instead.

    So you can do::

        >>> add_tasks = state.tasks_by_type['proj.tasks.add']

    while still supporting the method call::

        >>> add_tasks = list(state.tasks_by_type(
        ...     'proj.tasks.add', reverse=True))
    """

    def __init__(self, fun: Callable, *args, **kwargs) -> None:
        self.fun = fun
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs) -> Any:
        return self.fun(*args, **kwargs)


Callable.register(CallableDefaultdict)  # noqa: E305


@memoize(maxsize=1000, keyfun=lambda a, _: a[0])
def _warn_drift(hostname: str, drift: float,
                local_received: float, timestamp: float) -> None:
    # we use memoize here so the warning is only logged once per hostname
    warn(DRIFT_WARNING, hostname, drift,
         datetime.fromtimestamp(local_received),
         datetime.fromtimestamp(timestamp))


def heartbeat_expires(timestamp: float,
                      freq: float = 60.0,
                      expire_window: float = HEARTBEAT_EXPIRE_WINDOW,
                      *,
                      Decimal: Callable = Decimal,
                      float: Callable = float,
                      isinstance: Callable = isinstance) -> float:
    """Return time when heartbeat expires."""
    # some json implementations returns decimal.Decimal objects,
    # which aren't compatible with float.
    freq = float(freq) if isinstance(freq, Decimal) else freq
    if isinstance(timestamp, Decimal):
        timestamp = float(timestamp)
    return timestamp + (freq * (expire_window / 1e2))


def _depickle_task(cls: type, fields: Mapping) -> 'Task':
    return cls(**fields)


def with_unique_field(attr: str) -> Callable:

    def _decorate_cls(cls: type) -> type:

        def __eq__(this, other: Any) -> bool:
            if isinstance(other, this.__class__):
                return getattr(this, attr) == getattr(other, attr)
            return NotImplemented
        cls.__eq__ = __eq__

        def __ne__(this, other: Any) -> bool:
            res = this.__eq__(other)
            return True if res is NotImplemented else not res
        cls.__ne__ = __ne__

        def __hash__(this: Any) -> int:
            return hash(getattr(this, attr))
        cls.__hash__ = __hash__

        return cls
    return _decorate_cls


@with_unique_field('hostname')
class Worker:
    """Worker State."""

    heartbeat_max = 4
    expire_window = HEARTBEAT_EXPIRE_WINDOW

    _fields = ('hostname', 'pid', 'freq', 'heartbeats', 'clock',
               'active', 'processed', 'loadavg', 'sw_ident',
               'sw_ver', 'sw_sys')
    if not PYPY:  # pragma: no cover
        __slots__ = _fields + ('event', '__dict__', '__weakref__')

    def __init__(self,
                 hostname: str = None,
                 pid: int = None,
                 freq: float = 60.0,
                 heartbeats: Sequence[float] = None,
                 clock: int = 0,
                 active: int = None,
                 processed: int = None,
                 loadavg: Tuple[float, float, float] = None,
                 sw_ident: str = None,
                 sw_ver: str = None,
                 sw_sys: str = None) -> None:
        self.hostname = hostname
        self.pid = pid
        self.freq = freq
        self.heartbeats = [] if heartbeats is None else heartbeats
        self.clock = clock or 0
        self.active = active
        self.processed = processed
        self.loadavg = loadavg
        self.sw_ident = sw_ident
        self.sw_ver = sw_ver
        self.sw_sys = sw_sys
        self.event = self._create_event_handler()

    def __reduce__(self) -> Tuple:
        return self.__class__, (self.hostname, self.pid, self.freq,
                                self.heartbeats, self.clock, self.active,
                                self.processed, self.loadavg, self.sw_ident,
                                self.sw_ver, self.sw_sys)

    def _create_event_handler(self) -> Callable:
        _set = object.__setattr__
        hbmax = self.heartbeat_max
        heartbeats = self.heartbeats
        hb_pop = self.heartbeats.pop
        hb_append = self.heartbeats.append

        def event(type_: str,
                  timestamp: float = None,
                  local_received: float = None,
                  fields: Mapping = None,
                  max_drift: float = HEARTBEAT_DRIFT_MAX,
                  abs: Callable = abs,
                  int: Callable = int,
                  insort: Callable = bisect.insort,
                  len: Callable = len) -> None:
            fields = fields or {}
            for k, v in fields.items():
                _set(self, k, v)
            if type_ == 'offline':
                heartbeats[:] = []
            else:
                if not local_received or not timestamp:
                    return
                drift = abs(int(local_received) - int(timestamp))
                if drift > max_drift:
                    _warn_drift(self.hostname, drift,
                                local_received, timestamp)
                if local_received:  # pragma: no cover
                    hearts = len(heartbeats)
                    if hearts > hbmax - 1:
                        hb_pop(0)
                    if hearts and local_received > heartbeats[-1]:
                        hb_append(local_received)
                    else:
                        insort(heartbeats, local_received)
        return event

    def update(self, f: Mapping, **kw) -> None:
        for k, v in (dict(f, **kw) if kw else f).items():
            setattr(self, k, v)

    def __repr__(self) -> str:
        return R_WORKER.format(self)

    @property
    def status_string(self) -> str:
        return 'ONLINE' if self.alive else 'OFFLINE'

    @property
    def heartbeat_expires(self) -> float:
        return heartbeat_expires(self.heartbeats[-1],
                                 self.freq, self.expire_window)

    @property
    def alive(self, *, nowfun: Callable = time) -> bool:
        return bool(self.heartbeats and nowfun() < self.heartbeat_expires)

    @property
    def id(self) -> str:
        return '{0.hostname}.{0.pid}'.format(self)


@with_unique_field('uuid')
class Task:
    """Task State."""

    name = received = sent = started = succeeded = failed = retried = \
        revoked = rejected = args = kwargs = eta = expires = retries = \
        worker = result = exception = timestamp = runtime = traceback = \
        exchange = routing_key = root_id = parent_id = client = None
    state = states.PENDING
    clock = 0

    _fields = (
        'uuid', 'name', 'state', 'received', 'sent', 'started', 'rejected',
        'succeeded', 'failed', 'retried', 'revoked', 'args', 'kwargs',
        'eta', 'expires', 'retries', 'worker', 'result', 'exception',
        'timestamp', 'runtime', 'traceback', 'exchange', 'routing_key',
        'clock', 'client', 'root', 'root_id', 'parent', 'parent_id',
        'children',
    )
    if not PYPY:  # pragma: no cover
        __slots__ = ('__dict__', '__weakref__')

    #: How to merge out of order events.
    #: Disorder is detected by logical ordering (e.g., :event:`task-received`
    #: must've happened before a :event:`task-failed` event).
    #:
    #: A merge rule consists of a state and a list of fields to keep from
    #: that state. ``(RECEIVED, ('name', 'args')``, means the name and args
    #: fields are always taken from the RECEIVED state, and any values for
    #: these fields received before or after is simply ignored.
    merge_rules = {
        states.RECEIVED: (
            'name', 'args', 'kwargs', 'parent_id',
            'root_id' 'retries', 'eta', 'expires',
        ),
    }

    #: meth:`info` displays these fields by default.
    _info_fields = (
        'args', 'kwargs', 'retries', 'result', 'eta', 'runtime',
        'expires', 'exception', 'exchange', 'routing_key',
        'root_id', 'parent_id',
    )

    def __init__(self,
                 uuid: str = None,
                 cluster_state: 'State' = None,
                 children: Sequence[str] = None,
                 **kwargs) -> None:
        self.uuid = uuid
        self.cluster_state = cluster_state
        if self.cluster_state is not None:
            self.children = WeakSet(
                self.cluster_state.tasks.get(task_id)
                for task_id in children or ()
                if task_id in self.cluster_state.tasks
            )
        else:
            self.children = WeakSet()
        self._serializer_handlers = {
            'children': self._serializable_children,
            'root': self._serializable_root,
            'parent': self._serializable_parent,
        }
        if kwargs:
            self.__dict__.update(kwargs)

    def event(self, type_: str,
              timestamp: float = None,
              local_received: float = None,
              fields: Mapping = None,
              precedence: Callable = states.precedence,
              setattr: Callable = setattr,
              task_event_to_state: Callable = TASK_EVENT_TO_STATE.get,
              RETRY: str = states.RETRY) -> None:
        fields = fields or {}

        # using .get is faster than catching KeyError in this case.
        state = task_event_to_state(type_)
        if state is not None:
            # sets, for example, self.succeeded to the timestamp.
            setattr(self, type_, timestamp)
        else:
            state = type_.upper()  # custom state

        # note that precedence here is reversed
        # see implementation in celery.states.state.__lt__
        if state != RETRY and self.state != RETRY and \
                precedence(state) > precedence(self.state):
            # this state logically happens-before the current state, so merge.
            keep = self.merge_rules.get(state)
            if keep is not None:
                fields = {
                    k: v for k, v in fields.items() if k in keep
                }
        else:
            fields.update(state=state, timestamp=timestamp)

        # update current state with info from this event.
        self.__dict__.update(fields)

    def info(self, fields: Sequence[str] = None,
             extra: Sequence[str] = []) -> Mapping:
        """Information about this task suitable for on-screen display."""
        fields = self._info_fields if fields is None else fields

        def _keys() -> Iterator[Tuple[str, Any]]:
            for key in list(fields) + list(extra):
                value = getattr(self, key, None)
                if value is not None:
                    yield key, value

        return dict(_keys())

    def __repr__(self) -> str:
        return R_TASK.format(self)

    def as_dict(self) -> Mapping:
        get = object.__getattribute__
        handler = self._serializer_handlers.get
        return {
            k: handler(k, pass1)(get(self, k)) for k in self._fields
        }

    def _serializable_children(self, value: Any) -> Sequence[str]:
        return [task.id for task in self.children]

    def _serializable_root(self, value: Any) -> str:
        return self.root_id

    def _serializable_parent(self, value: Any) -> str:
        return self.parent_id

    def __reduce__(self) -> Tuple:
        return _depickle_task, (self.__class__, self.as_dict())

    @property
    def id(self) -> str:
        return self.uuid

    @property
    def origin(self) -> str:
        return self.client if self.worker is None else self.worker.id

    @property
    def ready(self) -> bool:
        return self.state in states.READY_STATES

    @cached_property
<<<<<<< HEAD
    def parent(self):
        # issue github.com/mher/flower/issues/648
        try:
            return self.parent_id and self.cluster_state.tasks[self.parent_id]
        except KeyError:
            return None

    @cached_property
    def root(self):
        # issue github.com/mher/flower/issues/648
        try:
            return self.root_id and self.cluster_state.tasks[self.root_id]
        except KeyError:
            return None
=======
    def parent(self) -> 'Task':
        return self.parent_id and self.cluster_state.tasks[self.parent_id]

    @cached_property
    def root(self) -> 'Task':
        return self.root_id and self.cluster_state.tasks[self.root_id]
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726


class State:
    """Records clusters state."""

    Worker = Worker
    Task = Task
    event_count = 0
    task_count = 0
    heap_multiplier = 4

    def __init__(self,
                 callback: Callable = None,
                 workers: Sequence[Worker] = None,
                 tasks: Sequence[Task] = None,
                 taskheap: List = None,
                 max_workers_in_memory: int = 5000,
                 max_tasks_in_memory: int = 10000,
                 on_node_join: Callable = None,
                 on_node_leave: Callable = None,
                 tasks_by_type: Mapping = None,
                 tasks_by_worker: Mapping = None) -> None:
        self.event_callback = callback
        self.workers = (LRUCache(max_workers_in_memory)
                        if workers is None else workers)
        self.tasks = (LRUCache(max_tasks_in_memory)
                      if tasks is None else tasks)
        self._taskheap = [] if taskheap is None else taskheap
        self.max_workers_in_memory = max_workers_in_memory
        self.max_tasks_in_memory = max_tasks_in_memory
        self.on_node_join = on_node_join
        self.on_node_leave = on_node_leave
        self._mutex = threading.Lock()
        self.handlers = {}
        self._seen_types = set()
        self._tasks_to_resolve = {}
        self.rebuild_taskheap()

        self.tasks_by_type: Mapping[str, Set[Task]] = CallableDefaultdict(
            self._tasks_by_type, WeakSet)
        self.tasks_by_type.update(
            _deserialize_Task_WeakSet_Mapping(tasks_by_type, self.tasks))

        self.tasks_by_worker: Mapping[str, Set[Task] = CallableDefaultdict(
            self._tasks_by_worker, WeakSet)
        self.tasks_by_worker.update(
            _deserialize_Task_WeakSet_Mapping(tasks_by_worker, self.tasks))

    @cached_property
    def _event(self) -> Callable:
        return self._create_dispatcher()

    def freeze_while(self, fun: Callable, *args, **kwargs) -> Any:
        clear_after = kwargs.pop('clear_after', False)
        with self._mutex:
            try:
                return fun(*args, **kwargs)
            finally:
                if clear_after:
                    self._clear()

    def clear_tasks(self, ready: bool = True) -> None:
        with self._mutex:
            self._clear_tasks(ready)

    def _clear_tasks(self, ready: bool = True) -> None:
        if ready:
            in_progress = {
                uuid: task for uuid, task in self.itertasks()
                if task.state not in states.READY_STATES
            }
            self.tasks.clear()
            self.tasks.update(in_progress)
        else:
            self.tasks.clear()
        self._taskheap[:] = []

    def _clear(self, ready: bool = True) -> None:
        self.workers.clear()
        self._clear_tasks(ready)
        self.event_count = 0
        self.task_count = 0

    def clear(self, ready: bool = True) -> None:
        with self._mutex:
            self._clear(ready)

    def get_or_create_worker(self, hostname: str,
                             **kwargs) -> Tuple[Worker, bool]:
        """Get or create worker by hostname.

        Returns:
            Tuple: of ``(worker, was_created)`` pairs.
        """
        try:
            worker = self.workers[hostname]
            if kwargs:
                worker.update(kwargs)
            return worker, False
        except KeyError:
            worker = self.workers[hostname] = self.Worker(
                hostname, **kwargs)
            return worker, True

    def get_or_create_task(self, uuid: str) -> Tuple[Task, bool]:
        """Get or create task by uuid."""
        try:
            return self.tasks[uuid], False
        except KeyError:
            task = self.tasks[uuid] = self.Task(uuid, cluster_state=self)
            return task, True

    def event(self, event: Mapping) -> Tuple[Any, str]:
        with self._mutex:
            return self._event(event)

    def _create_dispatcher(self) -> Callable:
        # noqa: C901
        # pylint: disable=too-many-statements
        # This code is highly optimized, but not for reusability.
        get_handler = self.handlers.__getitem__
        event_callback = self.event_callback
        wfields = itemgetter('hostname', 'timestamp', 'local_received')
        tfields = itemgetter('uuid', 'hostname', 'timestamp',
                             'local_received', 'clock')
        taskheap = self._taskheap
        th_append = taskheap.append
        th_pop = taskheap.pop
        # Removing events from task heap is an O(n) operation,
        # so easier to just account for the common number of events
        # for each task (PENDING->RECEIVED->STARTED->final)
        #: an O(n) operation
        max_events_in_heap = self.max_tasks_in_memory * self.heap_multiplier
        add_type = self._seen_types.add
        on_node_join, on_node_leave = self.on_node_join, self.on_node_leave
        tasks, Task = self.tasks, self.Task
        workers, Worker = self.workers, self.Worker
        # avoid updating LRU entry at getitem
        get_worker, get_task = workers.data.__getitem__, tasks.data.__getitem__

        get_task_by_type_set = self.tasks_by_type.__getitem__
        get_task_by_worker_set = self.tasks_by_worker.__getitem__

        def _event(event: Mapping,
                   *,
                   timetuple: Callable = timetuple,
                   KeyError: type = KeyError,
                   insort: Callable = bisect.insort,
                   created: bool = True) -> Tuple[Any, str]:
            self.event_count += 1
            if event_callback:
                event_callback(self, event)
            group, _, subject = event['type'].partition('-')
            try:
                handler = get_handler(group)
            except KeyError:
                pass
            else:
                return handler(subject, event), subject

            if group == 'worker':
                try:
                    hostname, timestamp, local_received = wfields(event)
                except KeyError:
                    pass
                else:
                    is_offline = subject == 'offline'
                    try:
                        worker, created = get_worker(hostname), False
                    except KeyError:
                        if is_offline:
                            worker, created = Worker(hostname), False
                        else:
                            worker = workers[hostname] = Worker(hostname)
                    worker.event(subject, timestamp, local_received, event)
                    if on_node_join and (created or subject == 'online'):
                        on_node_join(worker)
                    if on_node_leave and is_offline:
                        on_node_leave(worker)
                        workers.pop(hostname, None)
                    return (worker, created), subject
            elif group == 'task':
                (uuid, hostname, timestamp,
                 local_received, clock) = tfields(event)
                # task-sent event is sent by client, not worker
                is_client_event = subject == 'sent'
                try:
                    task, task_created = get_task(uuid), False
                except KeyError:
                    task = tasks[uuid] = Task(uuid, cluster_state=self)
                    task_created = True
                if is_client_event:
                    task.client = hostname
                else:
                    try:
                        worker = get_worker(hostname)
                    except KeyError:
                        worker = workers[hostname] = Worker(hostname)
                    task.worker = worker
                    if worker is not None and local_received:
                        worker.event(None, local_received, timestamp)

                origin = hostname if is_client_event else worker.id

                # remove oldest event if exceeding the limit.
                heaps = len(taskheap)
                if heaps + 1 > max_events_in_heap:
                    th_pop(0)

                # most events will be dated later than the previous.
                timetup = timetuple(clock, timestamp, origin, ref(task))
                if heaps and timetup > taskheap[-1]:
                    th_append(timetup)
                else:
                    insort(taskheap, timetup)

                if subject == 'received':
                    self.task_count += 1
                task.event(subject, timestamp, local_received, event)
                task_name = task.name
                if task_name is not None:
                    add_type(task_name)
                    if task_created:  # add to tasks_by_type index
                        get_task_by_type_set(task_name).add(task)
                        get_task_by_worker_set(hostname).add(task)
                if task.parent_id:
                    try:
                        parent_task = self.tasks[task.parent_id]
                    except KeyError:
                        self._add_pending_task_child(task)
                    else:
                        parent_task.children.add(task)
                try:
                    _children = self._tasks_to_resolve.pop(uuid)
                except KeyError:
                    pass
                else:
                    task.children.update(_children)

                return (task, task_created), subject
        return _event

    def _add_pending_task_child(self, task: Task) -> None:
        try:
            ch = self._tasks_to_resolve[task.parent_id]
        except KeyError:
            ch = self._tasks_to_resolve[task.parent_id] = WeakSet()
        ch.add(task)

    def rebuild_taskheap(self, *, timetuple: Callable = timetuple) -> None:
        heap = self._taskheap[:] = [
            timetuple(t.clock, t.timestamp, t.origin, ref(t))
            for t in self.tasks.values()
        ]
        heap.sort()

    def itertasks(self, limit: int = None) -> Iterator[Task]:
        for index, row in enumerate(self.tasks.items()):
            yield row
            if limit and index + 1 >= limit:
                break

    def tasks_by_time(self,
                      limit: int = None,
                      reverse: bool = True) -> Iterator[Tuple[str, Task]]:
        """Generator yielding tasks ordered by time.

        Yields:
            Tuples of ``(uuid, Task)``.
        """
        _heap = self._taskheap
        if reverse:
            _heap = reversed(_heap)

        seen = set()
        for evtup in islice(_heap, 0, limit):
            task = evtup[3]()
            if task is not None:
                uuid = task.uuid
                if uuid not in seen:
                    yield uuid, task
                    seen.add(uuid)
    tasks_by_timestamp = tasks_by_time

    def _tasks_by_type(self, name: str,
                       limit: int = None,
                       reverse: bool = True) -> Iterator[str, Task]:
        """Get all tasks by type.

        This is slower than accessing :attr:`tasks_by_type`,
        but will be ordered by time.

        Returns:
            Generator: giving ``(uuid, Task)`` pairs.
        """
        return islice(
            ((uuid, task) for uuid, task in self.tasks_by_time(reverse=reverse)
             if task.name == name),
            0, limit,
        )

    def _tasks_by_worker(self, hostname: str,
                         limit: int = None,
                         reverse: bool = True) -> Iterator[str, Task]:
        """Get all tasks by worker.

        Slower than accessing :attr:`tasks_by_worker`, but ordered by time.
        """
        return islice(
            ((uuid, task) for uuid, task in self.tasks_by_time(reverse=reverse)
             if task.worker.hostname == hostname),
            0, limit,
        )

    def task_types(self) -> List[str]:
        """Return a list of all seen task types."""
        return sorted(self._seen_types)

    def alive_workers(self) -> Iterator[Worker]:
        """Return a list of (seemingly) alive workers."""
        return (w for w in self.workers.values() if w.alive)

    def __repr__(self) -> str:
        return R_STATE.format(self)

    def __reduce__(self) -> Tuple:
        return self.__class__, (
            self.event_callback, self.workers, self.tasks, None,
            self.max_workers_in_memory, self.max_tasks_in_memory,
            self.on_node_join, self.on_node_leave,
            _serialize_Task_WeakSet_Mapping(self.tasks_by_type),
            _serialize_Task_WeakSet_Mapping(self.tasks_by_worker),
        )


def _serialize_Task_WeakSet_Mapping(mapping: Mapping) -> Mapping:
    return {name: [t.id for t in tasks] for name, tasks in mapping.items()}


def _deserialize_Task_WeakSet_Mapping(
        mapping: Mapping, tasks: Sequence[Task]) -> Mapping[str, Set[Task]]:
    return {name: WeakSet(tasks[i] for i in ids if i in tasks)
            for name, ids in (mapping or {}).items()}
