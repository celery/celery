from __future__ import absolute_import
from __future__ import with_statement

import anyjson

from celery.app import app_or_default
from celery.task import Task
from celery.task.sets import subtask, TaskSet

from celery.tests.utils import Case


class MockTask(Task):
    name = "tasks.add"

    def run(self, x, y, **kwargs):
        return x + y

    @classmethod
    def apply_async(cls, args, kwargs, **options):
        return (args, kwargs, options)

    @classmethod
    def apply(cls, args, kwargs, **options):
        return (args, kwargs, options)


class test_subtask(Case):

    def test_behaves_like_type(self):
        s = subtask("tasks.add", (2, 2), {"cache": True},
                    {"routing_key": "CPU-bound"})
        self.assertDictEqual(subtask(s), s)

    def test_task_argument_can_be_task_cls(self):
        s = subtask(MockTask, (2, 2))
        self.assertEqual(s.task, MockTask.name)

    def test_apply_async(self):
        s = MockTask.subtask((2, 2), {"cache": True},
                {"routing_key": "CPU-bound"})
        args, kwargs, options = s.apply_async()
        self.assertTupleEqual(args, (2, 2))
        self.assertDictEqual(kwargs, {"cache": True})
        self.assertDictEqual(options, {"routing_key": "CPU-bound"})

    def test_delay_argmerge(self):
        s = MockTask.subtask((2, ), {"cache": True},
                {"routing_key": "CPU-bound"})
        args, kwargs, options = s.delay(10, cache=False, other="foo")
        self.assertTupleEqual(args, (10, 2))
        self.assertDictEqual(kwargs, {"cache": False, "other": "foo"})
        self.assertDictEqual(options, {"routing_key": "CPU-bound"})

    def test_apply_async_argmerge(self):
        s = MockTask.subtask((2, ), {"cache": True},
                {"routing_key": "CPU-bound"})
        args, kwargs, options = s.apply_async((10, ),
                                              {"cache": False, "other": "foo"},
                                              routing_key="IO-bound",
                                              exchange="fast")

        self.assertTupleEqual(args, (10, 2))
        self.assertDictEqual(kwargs, {"cache": False, "other": "foo"})
        self.assertDictEqual(options, {"routing_key": "IO-bound",
                                        "exchange": "fast"})

    def test_apply_argmerge(self):
        s = MockTask.subtask((2, ), {"cache": True},
                {"routing_key": "CPU-bound"})
        args, kwargs, options = s.apply((10, ),
                                        {"cache": False, "other": "foo"},
                                        routing_key="IO-bound",
                                        exchange="fast")

        self.assertTupleEqual(args, (10, 2))
        self.assertDictEqual(kwargs, {"cache": False, "other": "foo"})
        self.assertDictEqual(options, {"routing_key": "IO-bound",
                                        "exchange": "fast"})

    def test_is_JSON_serializable(self):
        s = MockTask.subtask((2, ), {"cache": True},
                {"routing_key": "CPU-bound"})
        s.args = list(s.args)                   # tuples are not preserved
                                                # but this doesn't matter.
        self.assertEqual(s,
                         subtask(anyjson.deserialize(
                             anyjson.serialize(s))))

    def test_repr(self):
        s = MockTask.subtask((2, ), {"cache": True})
        self.assertIn("2", repr(s))
        self.assertIn("cache=True", repr(s))

    def test_reduce(self):
        s = MockTask.subtask((2, ), {"cache": True})
        cls, args, _ = s.__reduce__()
        self.assertDictEqual(dict(cls(*args)), dict(s))


class test_TaskSet(Case):

    def test_task_arg_can_be_iterable__compat(self):
        ts = TaskSet([MockTask.subtask((i, i))
                        for i in (2, 4, 8)])
        self.assertEqual(len(ts), 3)

    def test_respects_ALWAYS_EAGER(self):
        app = app_or_default()

        class MockTaskSet(TaskSet):
            applied = 0

            def apply(self, *args, **kwargs):
                self.applied += 1

        ts = MockTaskSet([MockTask.subtask((i, i))
                        for i in (2, 4, 8)])
        app.conf.CELERY_ALWAYS_EAGER = True
        try:
            ts.apply_async()
        finally:
            app.conf.CELERY_ALWAYS_EAGER = False
        self.assertEqual(ts.applied, 1)

    def test_apply_async(self):

        applied = [0]

        class mocksubtask(subtask):

            def apply_async(self, *args, **kwargs):
                applied[0] += 1

        ts = TaskSet([mocksubtask(MockTask, (i, i))
                        for i in (2, 4, 8)])
        ts.apply_async()
        self.assertEqual(applied[0], 3)

        class Publisher(object):

            def send(self, *args, **kwargs):
                pass

        ts.apply_async(publisher=Publisher())

    def test_apply(self):

        applied = [0]

        class mocksubtask(subtask):

            def apply(self, *args, **kwargs):
                applied[0] += 1

        ts = TaskSet([mocksubtask(MockTask, (i, i))
                        for i in (2, 4, 8)])
        ts.apply()
        self.assertEqual(applied[0], 3)
