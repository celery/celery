import socket
import unittest2 as unittest

from datetime import datetime, timedelta

from kombu import pidbox

from celery.utils.timer2 import Timer

from celery.app import app_or_default
from celery.datastructures import AttributeDict
from celery.decorators import task
from celery.registry import tasks
from celery.task.builtins import PingTask
from celery.utils import gen_unique_id
from celery.worker.buckets import FastQueue
from celery.worker.job import TaskRequest
from celery.worker.state import revoked
from celery.worker.control.registry import Panel

hostname = socket.gethostname()


@task(rate_limit=200)                   # for extra info in dump_tasks
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


class Consumer(object):

    def __init__(self):
        self.ready_queue = FastQueue()
        self.ready_queue.put(TaskRequest(task_name=mytask.name,
                                         task_id=gen_unique_id(),
                                         args=(2, 2),
                                         kwargs={}))
        self.eta_schedule = Timer()
        self.app = app_or_default()
        self.event_dispatcher = Dispatcher()


class test_ControlPanel(unittest.TestCase):

    def setUp(self):
        self.app = app_or_default()
        self.panel = self.create_panel(consumer=Consumer())

    def create_state(self, **kwargs):
        kwargs.setdefault("logger", self.app.log.get_default_logger())
        return AttributeDict(kwargs)

    def create_panel(self, **kwargs):
        return self.app.control.mailbox.Node(hostname=hostname,
                                             state=self.create_state(**kwargs),
                                             handlers=Panel.data)

    def test_disable_events(self):
        consumer = Consumer()
        panel = self.create_panel(consumer=consumer)
        consumer.event_dispatcher.enabled = True
        panel.handle("disable_events")
        self.assertEqual(consumer.event_dispatcher.enabled, False)
        self.assertIn("worker-offline", consumer.event_dispatcher.sent)

    def test_enable_events(self):
        consumer = Consumer()
        panel = self.create_panel(consumer=consumer)
        consumer.event_dispatcher.enabled = False
        panel.handle("enable_events")
        self.assertEqual(consumer.event_dispatcher.enabled, True)
        self.assertIn("worker-online", consumer.event_dispatcher.sent)

    def test_dump_tasks(self):
        info = "\n".join(self.panel.handle("dump_tasks"))
        self.assertIn("mytask", info)
        self.assertIn("rate_limit=200", info)

    def test_dump_schedule(self):
        consumer = Consumer()
        panel = self.create_panel(consumer=consumer)
        self.assertFalse(panel.handle("dump_schedule"))
        r = TaskRequest("celery.ping", "CAFEBABE", (), {})
        consumer.eta_schedule.schedule.enter(
                consumer.eta_schedule.Entry(lambda x: x, (r, )),
                    datetime.now() + timedelta(seconds=10))
        self.assertTrue(panel.handle("dump_schedule"))

    def test_dump_reserved(self):
        consumer = Consumer()
        panel = self.create_panel(consumer=consumer)
        response = panel.handle("dump_reserved", {"safe": True})
        self.assertDictContainsSubset({"name": mytask.name,
                                       "args": (2, 2),
                                       "kwargs": {},
                                       "hostname": socket.gethostname()},
                                       response[0])
        consumer.ready_queue = FastQueue()
        self.assertFalse(panel.handle("dump_reserved"))

    def test_rate_limit_when_disabled(self):
        app = app_or_default()
        app.conf.CELERY_DISABLE_RATE_LIMITS = True
        try:
            e = self.panel.handle("rate_limit", arguments=dict(
                 task_name=mytask.name, rate_limit="100/m"))
            self.assertIn("rate limits disabled", e.get("error"))
        finally:
            app.conf.CELERY_DISABLE_RATE_LIMITS = False

    def test_rate_limit_invalid_rate_limit_string(self):
        e = self.panel.handle("rate_limit", arguments=dict(
            task_name="tasks.add", rate_limit="x1240301#%!"))
        self.assertIn("Invalid rate limit string", e.get("error"))

    def test_rate_limit(self):

        class Consumer(object):

            class ReadyQueue(object):
                fresh = False

                def refresh(self):
                    self.fresh = True

            def __init__(self):
                self.ready_queue = self.ReadyQueue()

        consumer = Consumer()
        panel = self.create_panel(consumer=consumer)

        task = tasks[PingTask.name]
        old_rate_limit = task.rate_limit
        try:
            panel.handle("rate_limit", arguments=dict(task_name=task.name,
                                                      rate_limit="100/m"))
            self.assertEqual(task.rate_limit, "100/m")
            self.assertTrue(consumer.ready_queue.fresh)
            consumer.ready_queue.fresh = False
            panel.handle("rate_limit", arguments=dict(task_name=task.name,
                                                      rate_limit=0))
            self.assertEqual(task.rate_limit, 0)
            self.assertTrue(consumer.ready_queue.fresh)
        finally:
            task.rate_limit = old_rate_limit

    def test_rate_limit_nonexistant_task(self):
        self.panel.handle("rate_limit", arguments={
                                "task_name": "xxxx.does.not.exist",
                                "rate_limit": "1000/s"})

    def test_unexposed_command(self):
        self.assertRaises(KeyError, self.panel.handle, "foo", arguments={})

    def test_revoke_with_name(self):
        uuid = gen_unique_id()
        m = {"method": "revoke",
             "destination": hostname,
             "arguments": {"task_id": uuid,
                           "task_name": mytask.name}}
        self.panel.dispatch_from_message(m)
        self.assertIn(uuid, revoked)

    def test_revoke_with_name_not_in_registry(self):
        uuid = gen_unique_id()
        m = {"method": "revoke",
             "destination": hostname,
             "arguments": {"task_id": uuid,
                           "task_name": "xxxxxxxxx33333333388888"}}
        self.panel.dispatch_from_message(m)
        self.assertIn(uuid, revoked)

    def test_revoke(self):
        uuid = gen_unique_id()
        m = {"method": "revoke",
             "destination": hostname,
             "arguments": {"task_id": uuid}}
        self.panel.dispatch_from_message(m)
        self.assertIn(uuid, revoked)

        m = {"method": "revoke",
             "destination": "does.not.exist",
             "arguments": {"task_id": uuid + "xxx"}}
        self.panel.dispatch_from_message(m)
        self.assertNotIn(uuid + "xxx", revoked)

    def test_ping(self):
        m = {"method": "ping",
             "destination": hostname}
        r = self.panel.dispatch_from_message(m)
        self.assertEqual(r, "pong")

    def test_shutdown(self):
        m = {"method": "shutdown",
             "destination": hostname}
        self.assertRaises(SystemExit, self.panel.dispatch_from_message, m)

    def test_panel_reply(self):

        replies = []

        class _Node(pidbox.Node):

            def reply(self, data, exchange, routing_key, **kwargs):
                replies.append(data)

        panel = _Node(hostname=hostname,
                      state=self.create_state(consumer=Consumer()),
                      handlers=Panel.data,
                      mailbox=self.app.control.mailbox)
        r = panel.dispatch("ping", reply_to={"exchange": "x",
                                             "routing_key": "x"})
        self.assertEqual(r, "pong")
        self.assertDictEqual(replies[0], {panel.hostname: "pong"})
