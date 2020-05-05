from __future__ import absolute_import, unicode_literals

import pickle
from decimal import Decimal
from itertools import count
from random import shuffle
from time import time

from case import Mock, patch, skip

from celery import states, uuid
from celery.events import Event
from celery.events.state import (HEARTBEAT_DRIFT_MAX, HEARTBEAT_EXPIRE_WINDOW,
                                 State, Task, Worker, heartbeat_expires)
from celery.five import range


class replay(object):

    def __init__(self, state):
        self.state = state
        self.rewind()
        self.setup()
        self.current_clock = 0

    def setup(self):
        pass

    def next_event(self):
        ev = self.events[next(self.position)]
        ev['local_received'] = ev['timestamp']
        try:
            self.current_clock = ev['clock']
        except KeyError:
            ev['clock'] = self.current_clock = self.current_clock + 1
        return ev

    def __iter__(self):
        return self

    def __next__(self):
        try:
            self.state.event(self.next_event())
        except IndexError:
            raise StopIteration()
    next = __next__

    def rewind(self):
        self.position = count(0)
        return self

    def play(self):
        for _ in self:
            pass


class ev_worker_online_offline(replay):

    def setup(self):
        self.events = [
            Event('worker-online', hostname='utest1'),
            Event('worker-offline', hostname='utest1'),
        ]


class ev_worker_heartbeats(replay):

    def setup(self):
        self.events = [
            Event('worker-heartbeat', hostname='utest1',
                  timestamp=time() - HEARTBEAT_EXPIRE_WINDOW * 2),
            Event('worker-heartbeat', hostname='utest1'),
        ]


class ev_task_states(replay):

    def setup(self):
        tid = self.tid = uuid()
        tid2 = self.tid2 = uuid()
        self.events = [
            Event('task-received', uuid=tid, name='task1',
                  args='(2, 2)', kwargs="{'foo': 'bar'}",
                  retries=0, eta=None, hostname='utest1'),
            Event('task-started', uuid=tid, hostname='utest1'),
            Event('task-revoked', uuid=tid, hostname='utest1'),
            Event('task-retried', uuid=tid, exception="KeyError('bar')",
                  traceback='line 2 at main', hostname='utest1'),
            Event('task-failed', uuid=tid, exception="KeyError('foo')",
                  traceback='line 1 at main', hostname='utest1'),
            Event('task-succeeded', uuid=tid, result='4',
                  runtime=0.1234, hostname='utest1'),
            Event('foo-bar'),

            Event('task-received', uuid=tid2, name='task2',
                  args='(4, 4)', kwargs="{'foo': 'bar'}",
                  retries=0, eta=None, parent_id=tid, root_id=tid,
                  hostname='utest1'),
        ]


def QTEV(type, uuid, hostname, clock, name=None, timestamp=None):
    """Quick task event."""
    return Event('task-{0}'.format(type), uuid=uuid, hostname=hostname,
                 clock=clock, name=name, timestamp=timestamp or time())


class ev_logical_clock_ordering(replay):

    def __init__(self, state, offset=0, uids=None):
        self.offset = offset or 0
        self.uids = self.setuids(uids)
        super(ev_logical_clock_ordering, self).__init__(state)

    def setuids(self, uids):
        uids = self.tA, self.tB, self.tC = uids or [uuid(), uuid(), uuid()]
        return uids

    def setup(self):
        offset = self.offset
        tA, tB, tC = self.uids
        self.events = [
            QTEV('received', tA, 'w1', name='tA', clock=offset + 1),
            QTEV('received', tB, 'w2', name='tB', clock=offset + 1),
            QTEV('started', tA, 'w1', name='tA', clock=offset + 3),
            QTEV('received', tC, 'w2', name='tC', clock=offset + 3),
            QTEV('started', tB, 'w2', name='tB', clock=offset + 5),
            QTEV('retried', tA, 'w1', name='tA', clock=offset + 7),
            QTEV('succeeded', tB, 'w2', name='tB', clock=offset + 9),
            QTEV('started', tC, 'w2', name='tC', clock=offset + 10),
            QTEV('received', tA, 'w3', name='tA', clock=offset + 13),
            QTEV('succeded', tC, 'w2', name='tC', clock=offset + 12),
            QTEV('started', tA, 'w3', name='tA', clock=offset + 14),
            QTEV('succeeded', tA, 'w3', name='TA', clock=offset + 16),
        ]

    def rewind_with_offset(self, offset, uids=None):
        self.offset = offset
        self.uids = self.setuids(uids or self.uids)
        self.setup()
        self.rewind()


class ev_snapshot(replay):

    def setup(self):
        self.events = [
            Event('worker-online', hostname='utest1'),
            Event('worker-online', hostname='utest2'),
            Event('worker-online', hostname='utest3'),
        ]
        for i in range(20):
            worker = not i % 2 and 'utest2' or 'utest1'
            type = not i % 2 and 'task2' or 'task1'
            self.events.append(Event('task-received', name=type,
                                     uuid=uuid(), hostname=worker))


class test_Worker:

    def test_equality(self):
        assert Worker(hostname='foo').hostname == 'foo'
        assert Worker(hostname='foo') == Worker(hostname='foo')
        assert Worker(hostname='foo') != Worker(hostname='bar')
        assert hash(Worker(hostname='foo')) == hash(Worker(hostname='foo'))
        assert hash(Worker(hostname='foo')) != hash(Worker(hostname='bar'))

    def test_heartbeat_expires__Decimal(self):
        assert heartbeat_expires(
            Decimal(344313.37), freq=60, expire_window=200) == 344433.37

    def test_compatible_with_Decimal(self):
        w = Worker('george@vandelay.com')
        timestamp, local_received = Decimal(time()), time()
        w.event('worker-online', timestamp, local_received, fields={
            'hostname': 'george@vandelay.com',
            'timestamp': timestamp,
            'local_received': local_received,
            'freq': Decimal(5.6335431),
        })
        assert w.alive

    def test_eq_ne_other(self):
        assert Worker('a@b.com') == Worker('a@b.com')
        assert Worker('a@b.com') != Worker('b@b.com')
        assert Worker('a@b.com') != object()

    def test_reduce_direct(self):
        w = Worker('george@vandelay.com')
        w.event('worker-online', 10.0, 13.0, fields={
            'hostname': 'george@vandelay.com',
            'timestamp': 10.0,
            'local_received': 13.0,
            'freq': 60,
        })
        fun, args = w.__reduce__()
        w2 = fun(*args)
        assert w2.hostname == w.hostname
        assert w2.pid == w.pid
        assert w2.freq == w.freq
        assert w2.heartbeats == w.heartbeats
        assert w2.clock == w.clock
        assert w2.active == w.active
        assert w2.processed == w.processed
        assert w2.loadavg == w.loadavg
        assert w2.sw_ident == w.sw_ident

    def test_update(self):
        w = Worker('george@vandelay.com')
        w.update({'idx': '301'}, foo=1, clock=30, bah='foo')
        assert w.idx == '301'
        assert w.foo == 1
        assert w.clock == 30
        assert w.bah == 'foo'

    def test_survives_missing_timestamp(self):
        worker = Worker(hostname='foo')
        worker.event('heartbeat')
        assert worker.heartbeats == []

    def test_repr(self):
        assert repr(Worker(hostname='foo'))

    def test_drift_warning(self):
        worker = Worker(hostname='foo')
        with patch('celery.events.state.warn') as warn:
            worker.event(None, time() + (HEARTBEAT_DRIFT_MAX * 2), time())
            warn.assert_called()
            assert 'Substantial drift' in warn.call_args[0][0]

    def test_updates_heartbeat(self):
        worker = Worker(hostname='foo')
        worker.event(None, time(), time())
        assert len(worker.heartbeats) == 1
        h1 = worker.heartbeats[0]
        worker.event(None, time(), time() - 10)
        assert len(worker.heartbeats) == 2
        assert worker.heartbeats[-1] == h1


class test_Task:

    def test_equality(self):
        assert Task(uuid='foo').uuid == 'foo'
        assert Task(uuid='foo') == Task(uuid='foo')
        assert Task(uuid='foo') != Task(uuid='bar')
        assert hash(Task(uuid='foo')) == hash(Task(uuid='foo'))
        assert hash(Task(uuid='foo')) != hash(Task(uuid='bar'))

    def test_info(self):
        task = Task(uuid='abcdefg',
                    name='tasks.add',
                    args='(2, 2)',
                    kwargs='{}',
                    retries=2,
                    result=42,
                    eta=1,
                    runtime=0.0001,
                    expires=1,
                    parent_id='bdefc',
                    root_id='dedfef',
                    foo=None,
                    exception=1,
                    received=time() - 10,
                    started=time() - 8,
                    exchange='celery',
                    routing_key='celery',
                    succeeded=time())
        assert sorted(list(task._info_fields)) == sorted(task.info().keys())

        assert (sorted(list(task._info_fields + ('received',))) ==
                sorted(task.info(extra=('received',))))

        assert (sorted(['args', 'kwargs']) ==
                sorted(task.info(['args', 'kwargs']).keys()))
        assert not list(task.info('foo'))

    def test_reduce_direct(self):
        task = Task(uuid='uuid', name='tasks.add', args='(2, 2)')
        fun, args = task.__reduce__()
        task2 = fun(*args)
        assert task == task2

    def test_ready(self):
        task = Task(uuid='abcdefg',
                    name='tasks.add')
        task.event('received', time(), time())
        assert not task.ready
        task.event('succeeded', time(), time())
        assert task.ready

    def test_sent(self):
        task = Task(uuid='abcdefg',
                    name='tasks.add')
        task.event('sent', time(), time())
        assert task.state == states.PENDING

    def test_merge(self):
        task = Task()
        task.event('failed', time(), time())
        task.event('started', time(), time())
        task.event('received', time(), time(), {
            'name': 'tasks.add', 'args': (2, 2),
        })
        assert task.state == states.FAILURE
        assert task.name == 'tasks.add'
        assert task.args == (2, 2)
        task.event('retried', time(), time())
        assert task.state == states.RETRY

    def test_repr(self):
        assert repr(Task(uuid='xxx', name='tasks.add'))


class test_State:

    def test_repr(self):
        assert repr(State())

    def test_pickleable(self):
        state = State()
        r = ev_logical_clock_ordering(state)
        r.play()
        assert pickle.loads(pickle.dumps(state))

    def test_task_logical_clock_ordering(self):
        state = State()
        r = ev_logical_clock_ordering(state)
        tA, tB, tC = r.uids
        r.play()
        now = list(state.tasks_by_time())
        assert now[0][0] == tA
        assert now[1][0] == tC
        assert now[2][0] == tB
        for _ in range(1000):
            shuffle(r.uids)
            tA, tB, tC = r.uids
            r.rewind_with_offset(r.current_clock + 1, r.uids)
            r.play()
        now = list(state.tasks_by_time())
        assert now[0][0] == tA
        assert now[1][0] == tC
        assert now[2][0] == tB

    @skip.todo(reason='not working')
    def test_task_descending_clock_ordering(self):
        state = State()
        r = ev_logical_clock_ordering(state)
        tA, tB, tC = r.uids
        r.play()
        now = list(state.tasks_by_time(reverse=False))
        assert now[0][0] == tA
        assert now[1][0] == tB
        assert now[2][0] == tC
        for _ in range(1000):
            shuffle(r.uids)
            tA, tB, tC = r.uids
            r.rewind_with_offset(r.current_clock + 1, r.uids)
            r.play()
        now = list(state.tasks_by_time(reverse=False))
        assert now[0][0] == tB
        assert now[1][0] == tC
        assert now[2][0] == tA

    def test_get_or_create_task(self):
        state = State()
        task, created = state.get_or_create_task('id1')
        assert task.uuid == 'id1'
        assert created
        task2, created2 = state.get_or_create_task('id1')
        assert task2 is task
        assert not created2

    def test_get_or_create_worker(self):
        state = State()
        worker, created = state.get_or_create_worker('george@vandelay.com')
        assert worker.hostname == 'george@vandelay.com'
        assert created
        worker2, created2 = state.get_or_create_worker('george@vandelay.com')
        assert worker2 is worker
        assert not created2

    def test_get_or_create_worker__with_defaults(self):
        state = State()
        worker, created = state.get_or_create_worker(
            'george@vandelay.com', pid=30,
        )
        assert worker.hostname == 'george@vandelay.com'
        assert worker.pid == 30
        assert created
        worker2, created2 = state.get_or_create_worker(
            'george@vandelay.com', pid=40,
        )
        assert worker2 is worker
        assert worker2.pid == 40
        assert not created2

    def test_worker_online_offline(self):
        r = ev_worker_online_offline(State())
        next(r)
        assert list(r.state.alive_workers())
        assert r.state.workers['utest1'].alive
        r.play()
        assert not list(r.state.alive_workers())
        assert not r.state.workers['utest1'].alive

    def test_itertasks(self):
        s = State()
        s.tasks = {'a': 'a', 'b': 'b', 'c': 'c', 'd': 'd'}
        assert len(list(s.itertasks(limit=2))) == 2

    def test_worker_heartbeat_expire(self):
        r = ev_worker_heartbeats(State())
        next(r)
        assert not list(r.state.alive_workers())
        assert not r.state.workers['utest1'].alive
        r.play()
        assert list(r.state.alive_workers())
        assert r.state.workers['utest1'].alive

    def test_task_states(self):
        r = ev_task_states(State())

        # RECEIVED
        next(r)
        assert r.tid in r.state.tasks
        task = r.state.tasks[r.tid]
        assert task.state == states.RECEIVED
        assert task.received
        assert task.timestamp == task.received
        assert task.worker.hostname == 'utest1'

        # STARTED
        next(r)
        assert r.state.workers['utest1'].alive
        assert task.state == states.STARTED
        assert task.started
        assert task.timestamp == task.started
        assert task.worker.hostname == 'utest1'

        # REVOKED
        next(r)
        assert task.state == states.REVOKED
        assert task.revoked
        assert task.timestamp == task.revoked
        assert task.worker.hostname == 'utest1'

        # RETRY
        next(r)
        assert task.state == states.RETRY
        assert task.retried
        assert task.timestamp == task.retried
        assert task.worker.hostname, 'utest1'
        assert task.exception == "KeyError('bar')"
        assert task.traceback == 'line 2 at main'

        # FAILURE
        next(r)
        assert task.state == states.FAILURE
        assert task.failed
        assert task.timestamp == task.failed
        assert task.worker.hostname == 'utest1'
        assert task.exception == "KeyError('foo')"
        assert task.traceback == 'line 1 at main'

        # SUCCESS
        next(r)
        assert task.state == states.SUCCESS
        assert task.succeeded
        assert task.timestamp == task.succeeded
        assert task.worker.hostname == 'utest1'
        assert task.result == '4'
        assert task.runtime == 0.1234

        # children, parent, root
        r.play()
        assert r.tid2 in r.state.tasks
        task2 = r.state.tasks[r.tid2]

        assert task2.parent is task
        assert task2.root is task
        assert task2 in task.children

    def test_task_children_set_if_received_in_wrong_order(self):
        r = ev_task_states(State())
        r.events.insert(0, r.events.pop())
        r.play()
        assert r.state.tasks[r.tid2] in r.state.tasks[r.tid].children
        assert r.state.tasks[r.tid2].root is r.state.tasks[r.tid]
        assert r.state.tasks[r.tid2].parent is r.state.tasks[r.tid]

    def assertStateEmpty(self, state):
        assert not state.tasks
        assert not state.workers
        assert not state.event_count
        assert not state.task_count

    def assertState(self, state):
        assert state.tasks
        assert state.workers
        assert state.event_count
        assert state.task_count

    def test_freeze_while(self):
        s = State()
        r = ev_snapshot(s)
        r.play()

        def work():
            pass

        s.freeze_while(work, clear_after=True)
        assert not s.event_count

        s2 = State()
        r = ev_snapshot(s2)
        r.play()
        s2.freeze_while(work, clear_after=False)
        assert s2.event_count

    def test_clear_tasks(self):
        s = State()
        r = ev_snapshot(s)
        r.play()
        assert s.tasks
        s.clear_tasks(ready=False)
        assert not s.tasks

    def test_clear(self):
        r = ev_snapshot(State())
        r.play()
        assert r.state.event_count
        assert r.state.workers
        assert r.state.tasks
        assert r.state.task_count

        r.state.clear()
        assert not r.state.event_count
        assert not r.state.workers
        assert r.state.tasks
        assert not r.state.task_count

        r.state.clear(False)
        assert not r.state.tasks

    def test_task_types(self):
        r = ev_snapshot(State())
        r.play()
        assert sorted(r.state.task_types()) == ['task1', 'task2']

    def test_tasks_by_time(self):
        r = ev_snapshot(State())
        r.play()
        assert len(list(r.state.tasks_by_time())) == 20
        assert len(list(r.state.tasks_by_time(reverse=False))) == 20

    def test_tasks_by_type(self):
        r = ev_snapshot(State())
        r.play()
        assert len(list(r.state.tasks_by_type('task1'))) == 10
        assert len(list(r.state.tasks_by_type('task2'))) == 10

        assert len(r.state.tasks_by_type['task1']) == 10
        assert len(r.state.tasks_by_type['task2']) == 10

    def test_alive_workers(self):
        r = ev_snapshot(State())
        r.play()
        assert len(list(r.state.alive_workers())) == 3

    def test_tasks_by_worker(self):
        r = ev_snapshot(State())
        r.play()
        assert len(list(r.state.tasks_by_worker('utest1'))) == 10
        assert len(list(r.state.tasks_by_worker('utest2'))) == 10

        assert len(r.state.tasks_by_worker['utest1']) == 10
        assert len(r.state.tasks_by_worker['utest2']) == 10

    def test_survives_unknown_worker_event(self):
        s = State()
        s.event({
            'type': 'worker-unknown-event-xxx',
            'foo': 'bar',
        })
        s.event({
            'type': 'worker-unknown-event-xxx',
            'hostname': 'xxx',
            'foo': 'bar',
        })

    def test_survives_unknown_worker_leaving(self):
        s = State(on_node_leave=Mock(name='on_node_leave'))
        (worker, created), subject = s.event({
            'type': 'worker-offline',
            'hostname': 'unknown@vandelay.com',
            'timestamp': time(),
            'local_received': time(),
            'clock': 301030134894833,
        })
        assert worker == Worker('unknown@vandelay.com')
        assert not created
        assert subject == 'offline'
        assert 'unknown@vandelay.com' not in s.workers
        s.on_node_leave.assert_called_with(worker)

    def test_on_node_join_callback(self):
        s = State(on_node_join=Mock(name='on_node_join'))
        (worker, created), subject = s.event({
            'type': 'worker-online',
            'hostname': 'george@vandelay.com',
            'timestamp': time(),
            'local_received': time(),
            'clock': 34314,
        })
        assert worker
        assert created
        assert subject == 'online'
        assert 'george@vandelay.com' in s.workers
        s.on_node_join.assert_called_with(worker)

    def test_survives_unknown_task_event(self):
        s = State()
        s.event({
            'type': 'task-unknown-event-xxx',
            'foo': 'bar',
            'uuid': 'x',
            'hostname': 'y',
            'timestamp': time(),
            'local_received': time(),
            'clock': 0,
        })

    def test_limits_maxtasks(self):
        s = State(max_tasks_in_memory=1)
        s.heap_multiplier = 2
        s.event({
            'type': 'task-unknown-event-xxx',
            'foo': 'bar',
            'uuid': 'x',
            'hostname': 'y',
            'clock': 3,
            'timestamp': time(),
            'local_received': time(),
        })
        s.event({
            'type': 'task-unknown-event-xxx',
            'foo': 'bar',
            'uuid': 'y',
            'hostname': 'y',
            'clock': 4,
            'timestamp': time(),
            'local_received': time(),
        })
        s.event({
            'type': 'task-unknown-event-xxx',
            'foo': 'bar',
            'uuid': 'z',
            'hostname': 'y',
            'clock': 5,
            'timestamp': time(),
            'local_received': time(),
        })
        assert len(s._taskheap) == 2
        assert s._taskheap[0].clock == 4
        assert s._taskheap[1].clock == 5

        s._taskheap.append(s._taskheap[0])
        assert list(s.tasks_by_time())

    def test_callback(self):
        scratch = {}

        def callback(state, event):
            scratch['recv'] = True

        s = State(callback=callback)
        s.event({'type': 'worker-online'})
        assert scratch.get('recv')

    def test_deepcopy(self):
        import copy
        s = State()
        s.event({
            'type': 'task-success',
            'root_id': 'x',
            'uuid': 'x',
            'hostname': 'y',
            'clock': 3,
            'timestamp': time(),
            'local_received': time(),
        })
        s.event({
            'type': 'task-success',
            'root_id': 'y',
            'uuid': 'y',
            'hostname': 'y',
            'clock': 4,
            'timestamp': time(),
            'local_received': time(),
        })
        copy.deepcopy(s)
