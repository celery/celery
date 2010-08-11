import socket
import unittest2 as unittest

from celery.utils.timer2 import Timer

from celery import conf
from celery.decorators import task
from celery.registry import tasks
from celery.task.builtins import PingTask
from celery.utils import gen_unique_id
from celery.worker import control
from celery.worker.buckets import FastQueue
from celery.worker.job import TaskRequest
from celery.worker.state import revoked

hostname = socket.gethostname()


@task(rate_limit=200) # for extra info in dump_tasks
def mytask():
    pass


class Dispatcher(object):
    enabled = None

    def __init__(self, *args, **kwargs):
        self.sent = []

    def enable(self):
        self.enabled = True

    def disable(self):
        self.enabled = False

    def send(self, event):
        self.sent.append(event)


class Listener(object):

    def __init__(self):
        self.ready_queue = FastQueue()
        self.ready_queue.put(TaskRequest(task_name=mytask.name,
                                         task_id=gen_unique_id(),
                                         args=(2, 2),
                                         kwargs={}))
        self.eta_schedule = Timer()
        self.event_dispatcher = Dispatcher()


class test_ControlPanel(unittest.TestCase):

    def setUp(self):
        self.panel = self.create_panel(listener=Listener())

    def create_panel(self, **kwargs):
        return control.ControlDispatch(hostname=hostname, **kwargs)

    def test_disable_events(self):
        listener = Listener()
        panel = self.create_panel(listener=listener)
        listener.event_dispatcher.enabled = True
        panel.execute("disable_events")
        self.assertEqual(listener.event_dispatcher.enabled, False)
        self.assertIn("worker-offline", listener.event_dispatcher.sent)

    def test_enable_events(self):
        listener = Listener()
        panel = self.create_panel(listener=listener)
        listener.event_dispatcher.enabled = False
        panel.execute("enable_events")
        self.assertEqual(listener.event_dispatcher.enabled, True)
        self.assertIn("worker-online", listener.event_dispatcher.sent)

    def test_dump_tasks(self):
        info = "\n".join(self.panel.execute("dump_tasks"))
        self.assertIn("mytask", info)
        self.assertIn("rate_limit=200", info)

    def test_dump_schedule(self):
        listener = Listener()
        panel = self.create_panel(listener=listener)
        self.assertFalse(panel.execute("dump_schedule"))
        import operator
        listener.eta_schedule.schedule.enter(100, operator.add, (2, 2))
        self.assertTrue(panel.execute("dump_schedule"))

    def test_dump_reserved(self):
        listener = Listener()
        panel = self.create_panel(listener=listener)
        response = panel.execute("dump_reserved", {"safe": True})
        self.assertDictContainsSubset({"name": mytask.name,
                                       "args": (2, 2),
                                       "kwargs": {},
                                       "hostname": socket.gethostname()},
                                       response[0])
        listener.ready_queue = FastQueue()
        self.assertFalse(panel.execute("dump_reserved"))

    def test_rate_limit_when_disabled(self):
        conf.DISABLE_RATE_LIMITS = True
        try:
            e = self.panel.execute("rate_limit", kwargs=dict(
                 task_name=mytask.name, rate_limit="100/m"))
            self.assertIn("rate limits disabled", e.get("error"))
        finally:
            conf.DISABLE_RATE_LIMITS = False

    def test_rate_limit_invalid_rate_limit_string(self):
        e = self.panel.execute("rate_limit", kwargs=dict(
            task_name="tasks.add", rate_limit="x1240301#%!"))
        self.assertIn("Invalid rate limit string", e.get("error"))

    def test_rate_limit(self):

        class Listener(object):

            class ReadyQueue(object):
                fresh = False

                def refresh(self):
                    self.fresh = True

            def __init__(self):
                self.ready_queue = self.ReadyQueue()

        listener = Listener()
        panel = self.create_panel(listener=listener)

        task = tasks[PingTask.name]
        old_rate_limit = task.rate_limit
        try:
            panel.execute("rate_limit", kwargs=dict(task_name=task.name,
                                                    rate_limit="100/m"))
            self.assertEqual(task.rate_limit, "100/m")
            self.assertTrue(listener.ready_queue.fresh)
            listener.ready_queue.fresh = False
            panel.execute("rate_limit", kwargs=dict(task_name=task.name,
                                                    rate_limit=0))
            self.assertEqual(task.rate_limit, 0)
            self.assertTrue(listener.ready_queue.fresh)
        finally:
            task.rate_limit = old_rate_limit

    def test_rate_limit_nonexistant_task(self):
        self.panel.execute("rate_limit", kwargs={
                                "task_name": "xxxx.does.not.exist",
                                "rate_limit": "1000/s"})

    def test_unexposed_command(self):
        self.panel.execute("foo", kwargs={})

    def test_revoke_with_name(self):
        uuid = gen_unique_id()
        m = {"command": "revoke",
             "destination": hostname,
             "task_id": uuid,
             "task_name": mytask.name}
        self.panel.dispatch_from_message(m)
        self.assertIn(uuid, revoked)

    def test_revoke_with_name_not_in_registry(self):
        uuid = gen_unique_id()
        m = {"command": "revoke",
             "destination": hostname,
             "task_id": uuid,
             "task_name": "xxxxxxxxx33333333388888"}
        self.panel.dispatch_from_message(m)
        self.assertIn(uuid, revoked)

    def test_revoke(self):
        uuid = gen_unique_id()
        m = {"command": "revoke",
             "destination": hostname,
             "task_id": uuid}
        self.panel.dispatch_from_message(m)
        self.assertIn(uuid, revoked)

        m = {"command": "revoke",
             "destination": "does.not.exist",
             "task_id": uuid + "xxx"}
        self.panel.dispatch_from_message(m)
        self.assertNotIn(uuid + "xxx", revoked)

    def test_ping(self):
        m = {"command": "ping",
             "destination": hostname}
        r = self.panel.dispatch_from_message(m)
        self.assertEqual(r, "pong")

    def test_shutdown(self):
        m = {"command": "shutdown",
             "destination": hostname}
        self.assertRaises(SystemExit, self.panel.dispatch_from_message, m)

    def test_panel_reply(self):

        replies = []

        class MockReplyPublisher(object):

            def __init__(self, *args, **kwargs):
                pass

            def send(self, reply, **kwargs):
                replies.append(reply)

            def close(self):
                pass

        class _Dispatch(control.ControlDispatch):
            ReplyPublisher = MockReplyPublisher

        panel = _Dispatch(hostname, listener=Listener())

        r = panel.execute("ping", reply_to={"exchange": "x",
                                            "routing_key": "x"})
        self.assertEqual(r, "pong")
        self.assertDictEqual(replies[0], {panel.hostname: "pong"})
