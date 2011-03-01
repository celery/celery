from time import time
from celery.tests.utils import unittest

from itertools import count

from celery import states
from celery.events import Event
from celery.events.state import State, Worker, Task, HEARTBEAT_EXPIRE
from celery.utils import gen_unique_id


class replay(object):

    def __init__(self, state):
        self.state = state
        self.rewind()

    def __iter__(self):
        return self

    def next(self):
        try:
            self.state.event(self.events[self.position()])
        except IndexError:
            raise StopIteration()

    def rewind(self):
        self.position = count(0).next
        return self

    def play(self):
        for _ in self:
            pass


class ev_worker_online_offline(replay):
    events = [
        Event("worker-online", hostname="utest1"),
        Event("worker-offline", hostname="utest1"),
    ]


class ev_worker_heartbeats(replay):
    events = [
        Event("worker-heartbeat", hostname="utest1",
              timestamp=time() - HEARTBEAT_EXPIRE * 2),
        Event("worker-heartbeat", hostname="utest1"),
    ]


class ev_task_states(replay):
    uuid = gen_unique_id()
    events = [
        Event("task-received", uuid=uuid, name="task1",
              args="(2, 2)", kwargs="{'foo': 'bar'}",
              retries=0, eta=None, hostname="utest1"),
        Event("task-started", uuid=uuid, hostname="utest1"),
        Event("task-revoked", uuid=uuid, hostname="utest1"),
        Event("task-retried", uuid=uuid, exception="KeyError('bar')",
              traceback="line 2 at main", hostname="utest1"),
        Event("task-failed", uuid=uuid, exception="KeyError('foo')",
              traceback="line 1 at main", hostname="utest1"),
        Event("task-succeeded", uuid=uuid, result="4",
              runtime=0.1234, hostname="utest1"),
    ]


class ev_snapshot(replay):
    events = [
        Event("worker-online", hostname="utest1"),
        Event("worker-online", hostname="utest2"),
        Event("worker-online", hostname="utest3"),
    ]
    for i in range(20):
        worker = not i % 2 and "utest2" or "utest1"
        type = not i % 2 and "task2" or "task1"
        events.append(Event("task-received", name=type,
                      uuid=gen_unique_id(), hostname=worker))


class test_Worker(unittest.TestCase):

    def test_survives_missing_timestamp(self):
        worker = Worker(hostname="foo")
        worker.on_heartbeat(timestamp=None)
        self.assertEqual(worker.heartbeats, [])

    def test_repr(self):
        self.assertTrue(repr(Worker(hostname="foo")))


class test_Task(unittest.TestCase):

    def test_info(self):
        task = Task(uuid="abcdefg",
                    name="tasks.add",
                    args="(2, 2)",
                    kwargs="{}",
                    retries=2,
                    result=42,
                    eta=1,
                    runtime=0.0001,
                    expires=1,
                    exception=1,
                    received=time() - 10,
                    started=time() - 8,
                    succeeded=time())
        self.assertItemsEqual(list(task._info_fields),
                              task.info().keys())

        self.assertItemsEqual(list(task._info_fields + ("received", )),
                              task.info(extra=("received", )))

        self.assertItemsEqual(["args", "kwargs"],
                              task.info(["args", "kwargs"]).keys())

    def test_ready(self):
        task = Task(uuid="abcdefg",
                    name="tasks.add")
        task.on_received(timestamp=time())
        self.assertFalse(task.ready)
        task.on_succeeded(timestamp=time())
        self.assertTrue(task.ready)

    def test_sent(self):
        task = Task(uuid="abcdefg",
                    name="tasks.add")
        task.on_sent(timestamp=time())
        self.assertEqual(task.state, states.PENDING)

    def test_merge(self):
        task = Task()
        task.on_failed(timestamp=time())
        task.on_started(timestamp=time())
        task.on_received(timestamp=time(), name="tasks.add", args=(2, 2))
        self.assertEqual(task.state, states.FAILURE)
        self.assertEqual(task.name, "tasks.add")
        self.assertTupleEqual(task.args, (2, 2))
        task.on_retried(timestamp=time())
        self.assertEqual(task.state, states.RETRY)

    def test_repr(self):
        self.assertTrue(repr(Task(uuid="xxx", name="tasks.add")))


class test_State(unittest.TestCase):

    def test_repr(self):
        self.assertTrue(repr(State()))

    def test_worker_online_offline(self):
        r = ev_worker_online_offline(State())
        r.next()
        self.assertTrue(r.state.alive_workers())
        self.assertTrue(r.state.workers["utest1"].alive)
        r.play()
        self.assertFalse(r.state.alive_workers())
        self.assertFalse(r.state.workers["utest1"].alive)

    def test_worker_heartbeat_expire(self):
        r = ev_worker_heartbeats(State())
        r.next()
        self.assertFalse(r.state.alive_workers())
        self.assertFalse(r.state.workers["utest1"].alive)
        r.play()
        self.assertTrue(r.state.alive_workers())
        self.assertTrue(r.state.workers["utest1"].alive)

    def test_task_states(self):
        r = ev_task_states(State())

        # RECEIVED
        r.next()
        self.assertTrue(r.uuid in r.state.tasks)
        task = r.state.tasks[r.uuid]
        self.assertEqual(task.state, states.RECEIVED)
        self.assertTrue(task.received)
        self.assertEqual(task.timestamp, task.received)
        self.assertEqual(task.worker.hostname, "utest1")

        # STARTED
        r.next()
        self.assertTrue(r.state.workers["utest1"].alive,
                "any task event adds worker heartbeat")
        self.assertEqual(task.state, states.STARTED)
        self.assertTrue(task.started)
        self.assertEqual(task.timestamp, task.started)
        self.assertEqual(task.worker.hostname, "utest1")

        # REVOKED
        r.next()
        self.assertEqual(task.state, states.REVOKED)
        self.assertTrue(task.revoked)
        self.assertEqual(task.timestamp, task.revoked)
        self.assertEqual(task.worker.hostname, "utest1")

        # RETRY
        r.next()
        self.assertEqual(task.state, states.RETRY)
        self.assertTrue(task.retried)
        self.assertEqual(task.timestamp, task.retried)
        self.assertEqual(task.worker.hostname, "utest1")
        self.assertEqual(task.exception, "KeyError('bar')")
        self.assertEqual(task.traceback, "line 2 at main")

        # FAILURE
        r.next()
        self.assertEqual(task.state, states.FAILURE)
        self.assertTrue(task.failed)
        self.assertEqual(task.timestamp, task.failed)
        self.assertEqual(task.worker.hostname, "utest1")
        self.assertEqual(task.exception, "KeyError('foo')")
        self.assertEqual(task.traceback, "line 1 at main")

        # SUCCESS
        r.next()
        self.assertEqual(task.state, states.SUCCESS)
        self.assertTrue(task.succeeded)
        self.assertEqual(task.timestamp, task.succeeded)
        self.assertEqual(task.worker.hostname, "utest1")
        self.assertEqual(task.result, "4")
        self.assertEqual(task.runtime, 0.1234)

    def assertStateEmpty(self, state):
        self.assertFalse(state.tasks)
        self.assertFalse(state.workers)
        self.assertFalse(state.event_count)
        self.assertFalse(state.task_count)

    def assertState(self, state):
        self.assertTrue(state.tasks)
        self.assertTrue(state.workers)
        self.assertTrue(state.event_count)
        self.assertTrue(state.task_count)

    def test_freeze_while(self):
        s = State()
        r = ev_snapshot(s)
        r.play()

        def work():
            pass

        s.freeze_while(work, clear_after=True)
        self.assertFalse(s.event_count)

        s2 = State()
        r = ev_snapshot(s2)
        r.play()
        s2.freeze_while(work, clear_after=False)
        self.assertTrue(s2.event_count)

    def test_clear_tasks(self):
        s = State()
        r = ev_snapshot(s)
        r.play()
        self.assertTrue(s.tasks)
        s.clear_tasks(ready=False)
        self.assertFalse(s.tasks)

    def test_clear(self):
        r = ev_snapshot(State())
        r.play()
        self.assertTrue(r.state.event_count)
        self.assertTrue(r.state.workers)
        self.assertTrue(r.state.tasks)
        self.assertTrue(r.state.task_count)

        r.state.clear()
        self.assertFalse(r.state.event_count)
        self.assertFalse(r.state.workers)
        self.assertTrue(r.state.tasks)
        self.assertFalse(r.state.task_count)

        r.state.clear(False)
        self.assertFalse(r.state.tasks)

    def test_task_types(self):
        r = ev_snapshot(State())
        r.play()
        self.assertItemsEqual(r.state.task_types(), ["task1", "task2"])

    def test_tasks_by_timestamp(self):
        r = ev_snapshot(State())
        r.play()
        self.assertEqual(len(r.state.tasks_by_timestamp()), 20)

    def test_tasks_by_type(self):
        r = ev_snapshot(State())
        r.play()
        self.assertEqual(len(r.state.tasks_by_type("task1")), 10)
        self.assertEqual(len(r.state.tasks_by_type("task2")), 10)

    def test_alive_workers(self):
        r = ev_snapshot(State())
        r.play()
        self.assertEqual(len(r.state.alive_workers()), 3)

    def test_tasks_by_worker(self):
        r = ev_snapshot(State())
        r.play()
        self.assertEqual(len(r.state.tasks_by_worker("utest1")), 10)
        self.assertEqual(len(r.state.tasks_by_worker("utest2")), 10)

    def test_survives_unknown_worker_event(self):
        s = State()
        s.worker_event("worker-unknown-event-xxx", {"foo": "bar"})
        s.worker_event("worker-unknown-event-xxx", {"hostname": "xxx",
                                                    "foo": "bar"})

    def test_survives_unknown_task_event(self):
        s = State()
        s.task_event("task-unknown-event-xxx", {"foo": "bar",
                                                "uuid": "x",
                                                "hostname": "y"})

    def test_callback(self):
        scratch = {}

        def callback(state, event):
            scratch["recv"] = True

        s = State(callback=callback)
        s.event({"type": "worker-online"})
        self.assertTrue(scratch.get("recv"))
