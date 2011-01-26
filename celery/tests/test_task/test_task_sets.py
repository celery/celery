import anyjson
import warnings

from celery.app import app_or_default
from celery.task import Task
from celery.task.sets import subtask, TaskSet

from celery.tests.utils import unittest
from celery.tests.utils import execute_context
from celery.tests.compat import catch_warnings


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


class test_subtask(unittest.TestCase):

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


class test_TaskSet(unittest.TestCase):

    def test_interface__compat(self):
        warnings.resetwarnings()

        def with_catch_warnings(log):
            ts = TaskSet(MockTask, [[(2, 2)], [(4, 4)], [(8, 8)]])
            self.assertTrue(log)
            self.assertIn("Using this invocation of TaskSet is deprecated",
                          log[0].message.args[0])
            self.assertListEqual(ts.tasks,
                                 [MockTask.subtask((i, i))
                                    for i in (2, 4, 8)])
            return ts

        context = catch_warnings(record=True)
        execute_context(context, with_catch_warnings)

        # TaskSet.task (deprecated)
        def with_catch_warnings2(log):
            ts = TaskSet(MockTask, [[(2, 2)], [(4, 4)], [(8, 8)]])
            self.assertEqual(ts.task.name, MockTask.name)
            self.assertTrue(log)
            self.assertIn("TaskSet.task is deprecated",
                          log[0].message.args[0])

        execute_context(catch_warnings(record=True), with_catch_warnings2)

        # TaskSet.task_name (deprecated)
        def with_catch_warnings3(log):
            ts = TaskSet(MockTask, [[(2, 2)], [(4, 4)], [(8, 8)]])
            self.assertEqual(ts.task_name, MockTask.name)
            self.assertTrue(log)
            self.assertIn("TaskSet.task_name is deprecated",
                          log[0].message.args[0])

        execute_context(catch_warnings(record=True), with_catch_warnings3)

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

    def test_apply(self):

        applied = [0]

        class mocksubtask(subtask):

            def apply(self, *args, **kwargs):
                applied[0] += 1

        ts = TaskSet([mocksubtask(MockTask, (i, i))
                        for i in (2, 4, 8)])
        ts.apply()
        self.assertEqual(applied[0], 3)
