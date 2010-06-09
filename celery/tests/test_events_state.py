import time
import unittest2 as unittest

from itertools import count

from celery import states
from celery.events import Event
from celery.events.state import State, HEARTBEAT_EXPIRE
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
              timestamp=time.time() - HEARTBEAT_EXPIRE * 2),
        Event("worker-heartbeat", hostname="utest1"),
    ]


class ev_task_states(replay):
    uuid = gen_unique_id()
    events = [
        Event("task-received", uuid=uuid, name="task1",
              args="(2, 2)", kwargs="{'foo': 'bar'}",
              retries=0, eta=None, hostname="utest1"),
        Event("task-started", uuid=uuid, hostname="utest1"),
        Event("task-succeeded", uuid=uuid, result="4",
              runtime=0.1234, hostname="utest1"),
        Event("task-failed", uuid=uuid, exception="KeyError('foo')",
              traceback="line 1 at main", hostname="utest1"),
        Event("task-retried", uuid=uuid, exception="KeyError('bar')",
              traceback="line 2 at main", hostname="utest1"),
        Event("task-revoked", uuid=uuid, hostname="utest1"),
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


class test_State(unittest.TestCase):

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
        self.assertEqual(task.state, "RECEIVED")
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

        # SUCCESS
        r.next()
        self.assertEqual(task.state, states.SUCCESS)
        self.assertTrue(task.succeeded)
        self.assertEqual(task.timestamp, task.succeeded)
        self.assertEqual(task.worker.hostname, "utest1")
        self.assertEqual(task.result, "4")
        self.assertEqual(task.runtime, 0.1234)

        # FAILURE
        r.next()
        self.assertEqual(task.state, states.FAILURE)
        self.assertTrue(task.failed)
        self.assertEqual(task.timestamp, task.failed)
        self.assertEqual(task.worker.hostname, "utest1")
        self.assertEqual(task.exception, "KeyError('foo')")
        self.assertEqual(task.traceback, "line 1 at main")

        # RETRY
        r.next()
        self.assertEqual(task.state, states.RETRY)
        self.assertTrue(task.retried)
        self.assertEqual(task.timestamp, task.retried)
        self.assertEqual(task.worker.hostname, "utest1")
        self.assertEqual(task.exception, "KeyError('bar')")
        self.assertEqual(task.traceback, "line 2 at main")

        # REVOKED
        r.next()
        self.assertEqual(task.state, states.REVOKED)
        self.assertTrue(task.revoked)
        self.assertEqual(task.timestamp, task.revoked)
        self.assertEqual(task.worker.hostname, "utest1")

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
