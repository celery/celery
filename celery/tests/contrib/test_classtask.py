from __future__ import absolute_import

from datetime import datetime, timedelta

from kombu import Queue

from celery import Task

from celery.contrib.classtask import ClassTask, auto_init
from celery.exceptions import Retry
from celery.five import items, string_t, with_metaclass
from celery.result import EagerResult
from celery.utils import uuid
from celery.utils.timeutils import parse_iso8601

from celery.tests.case import AppCase, depends_on_current_app, patch


@with_metaclass(ClassTask)
class MockApplyTask(object):
    abstract = True
    applied = 0

    def run(self):
        return self.x * self.y

    def apply_async(self, *args, **kwargs):
        self.applied += 1


class ClassTasksCase(AppCase):

    def setup(self):

        self.app.set_current()

        @with_metaclass(ClassTask)
        class MyTask(object):
            shared = False

            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

            def run(self):
                return True
        self.MyTask = MyTask

        @with_metaclass(ClassTask)
        class IncrementCounter(object):
            count = 0
            shared = False

            def __init__(self, increment_by=1):
                self.increment_by = increment_by

            def run(self):
                IncrementCounter.count += self.increment_by or 1
                return IncrementCounter.count
        self.IncrementCounter = IncrementCounter

        @with_metaclass(ClassTask)
        class Raising(object):
            shared = False

            def run(self):
                raise KeyError('foo')
        self.Raising = Raising

        @with_metaclass(ClassTask)
        class RetryTask(object):
            iterations = 0
            shared = False
            max_retries = 3

            def __init__(self, arg1, arg2, kwarg=1, max_retries=None,
                         care=True):
                self.rmax = max_retries or self.max_retries
                self.care = care
                self.arg1 = arg1
                self.arg2 = arg2
                self.kwarg = kwarg

            def run(self):
                self.get_task().iterations += 1
                assert repr(self.request)
                retries = self.request.retries
                if self.care and retries >= self.rmax:
                    return self.arg1
                else:
                    raise self.retry(countdown=0, max_retries=self.rmax)
        self.RetryTask = RetryTask

        @with_metaclass(ClassTask)
        class RetryTaskNoargs(object):
            max_retries = 3
            iterations = 0
            shared = False

            def __init__(self):
                pass

            def run(self):
                self.get_task().iterations += 1
                if self.request.retries >= 3:
                    return 42
                else:
                    raise self.retry(countdown=0)
        self.RetryTaskNoargs = RetryTaskNoargs

        class RetryTaskMockapply(MockApplyTask):
            max_retries = 3
            iterations = 0
            shared = False

            def __init__(self, arg1, arg2, kwarg=1):
                self.arg1 = arg1
                self.arg2 = arg2
                self.kwarg = kwarg

            def run(self):
                self.get_task().iterations += 1

                retries = self.request.retries
                if retries >= 3:
                    return self.arg1
                raise self.retry(countdown=0)
        self.RetryTaskMockapply = RetryTaskMockapply

        @with_metaclass(ClassTask)
        class RetryTaskCustomexc(object):
            max_retries = 3
            iterations = 0
            shared = False

            def __init__(self, arg1, arg2, kwarg=1, **kwargs):
                self.arg1 = arg1
                self.arg2 = arg2
                self.kwarg = kwarg
                self.kwargs = kwargs

            def run(self):
                self.get_task().iterations += 1

                retries = self.request.retries
                if retries >= 3:
                    return self.arg1 + self.kwarg
                else:
                    try:
                        raise MyCustomException('Elaine Marie Benes')
                    except MyCustomException as exc:
                        self.kwargs.update(kwarg=self.kwarg)
                        raise self.retry(countdown=0, exc=exc)
        self.RetryTaskCustomexc = RetryTaskCustomexc

        @with_metaclass(ClassTask)
        class Math(object):
            def __init__(self, arg1, arg2):
                self.arg1 = arg1
                self.arg2 = arg2

            def add(self):
                return self.arg1 + self.arg2

            @property
            def subtract(self):
                return self.arg1 - self.arg2

            @auto_init
            def multiply(self):
                return self.arg1 * self.arg2

            def run(self):
                return self.add()
        self.Math = Math


class MyCustomException(Exception):
    """Random custom exception."""


class test_task_retries(ClassTasksCase):

    def test_retry(self):
        self.RetryTask.get_task().iterations = 0
        self.RetryTask(0xFF, 0xFFFF).apply()
        self.assertEqual(self.RetryTask.get_task().iterations, 4)

        self.RetryTask.get_task().max_retries = 3
        self.RetryTask.get_task().iterations = 0
        self.RetryTask(0xFF, 0xFFFF, max_retries=10).apply()
        self.assertEqual(self.RetryTask.get_task().iterations, 11)

    def test_retry_no_args(self):
        self.RetryTaskNoargs.get_task().iterations = 0
        self.RetryTaskNoargs().apply(propagate=True).get()
        self.assertEqual(self.RetryTaskNoargs.get_task().iterations, 4)

    def test_retry_kwargs_can_be_empty(self):
        this_task = self.RetryTaskMockapply(4, 4)
        this_task.push_request()
        try:
            with self.assertRaises(Retry):
                this_task.retry()
        finally:
            this_task.pop_request()

    def test_retry_not_eager(self):
        this_task = self.RetryTaskMockapply(4, 4)
        this_task.push_request()
        try:
            this_task.request.called_directly = False
            exc = Exception('baz')
            try:
                this_task.retry(exc=exc, throw=False)
                self.assertTrue(this_task.applied)
            finally:
                this_task.applied = 0

            try:
                with self.assertRaises(Retry):
                    this_task.retry(exc=exc, throw=True)
                self.assertTrue(this_task.applied)
            finally:
                this_task.applied = 0
        finally:
            this_task.pop_request()

    def test_retry_with_kwargs(self):
        self.RetryTaskCustomexc(*[0xFF, 0xFFFF], **{'kwarg': 0xF}).apply()
        self.assertEqual(self.RetryTaskCustomexc.get_task().iterations, 4)

    def test_retry_with_custom_exception(self):
        self.RetryTaskCustomexc.max_retries = 2
        result = self.RetryTaskCustomexc(*[0xFF, 0xFFFF],
                                         **{'kwarg': 0xF}).apply()
        with self.assertRaises(MyCustomException):
            result.get()
        self.assertEqual(self.RetryTaskCustomexc.get_task().iterations, 3)

    def test_max_retries_exceeded_Retry(self):
        self.RetryTask.max_retries = 2
        self.RetryTask.get_task().iterations = 0
        result = self.RetryTask(*[0xFF, 0xFFFF], care=False).apply()
        with self.assertRaises(self.RetryTask.MaxRetriesExceededError):
            result.get()
        self.assertEqual(self.RetryTask.get_task().iterations, 3)

        self.RetryTask.max_retries = 1
        self.RetryTask.get_task().iterations = 0
        result = self.RetryTask(*[0xFF, 0xFFFF], **{'care': False}).apply()
        with self.assertRaises(self.RetryTask.MaxRetriesExceededError):
            result.get()
        self.assertEqual(self.RetryTask.get_task().iterations, 2)


class test_canvas_utils(ClassTasksCase):

    def test_si(self):
        self.assertTrue(self.RetryTask().si())
        self.assertTrue(self.RetryTask().si().immutable)

    def test_chunks(self):
        self.assertTrue(self.RetryTask().chunks(range(100), 10))

    def test_map(self):
        self.assertTrue(self.RetryTask().map(range(100)))

    def test_starmap(self):
        self.assertTrue(self.RetryTask().starmap(range(100)))

    def test_on_success(self):
        self.RetryTask().on_success(1, 1, (), {})


@with_metaclass(ClassTask)
class Xxx(object):
    shared = True

    def run(self):
        pass


class test_tasks(ClassTasksCase):

    def now(self):
        return self.app.now()

    @depends_on_current_app
    def test_unpickle_task(self):
        import pickle
        self.assertIs(pickle.loads(pickle.dumps(Xxx())),
                      Xxx.app.tasks[Xxx.name])

    def test_AsyncResult(self):
        task_id = uuid()
        result = self.RetryTask().AsyncResult(task_id)
        self.assertEqual(result.backend, self.RetryTask().backend)
        self.assertEqual(result.id, task_id)

    def assertNextTaskDataEqual(self, consumer, presult, task_name,
                                test_eta=False, test_expires=False, **kwargs):
        next_task = consumer.queues[0].get(accept=['pickle'])
        task_data = next_task.decode()
        self.assertEqual(task_data['id'], presult.id)
        self.assertEqual(task_data['task'], task_name)
        task_kwargs = task_data.get('kwargs', {})
        if test_eta:
            self.assertIsInstance(task_data.get('eta'), string_t)
            to_datetime = parse_iso8601(task_data.get('eta'))
            self.assertIsInstance(to_datetime, datetime)
        if test_expires:
            self.assertIsInstance(task_data.get('expires'), string_t)
            to_datetime = parse_iso8601(task_data.get('expires'))
            self.assertIsInstance(to_datetime, datetime)
        for arg_name, arg_value in items(kwargs):
            self.assertEqual(task_kwargs.get(arg_name), arg_value)

    def test_incomplete_task_cls(self):

        @with_metaclass(ClassTask)
        class IncompleteTask(object):
            app = self.app
            name = 'c.unittest.t.itask'

        with self.assertRaises(NotImplementedError):
            IncompleteTask().run()

    def test_regular_task(self):
        self.assertIsInstance(self.MyTask(), Task)
        self.assertTrue(self.MyTask().init().run())
        self.assertTrue(
            callable(self.MyTask()), 'Task class is callable()',
        )
        self.assertTrue(self.MyTask(), 'Task class runs run() when called')

        with self.app.connection_or_acquire() as conn:
            consumer = self.app.amqp.TaskConsumer(conn)
            with self.assertRaises(NotImplementedError):
                consumer.receive('foo', 'foo')
            consumer.purge()
            self.assertIsNone(consumer.queues[0].get())
            self.app.amqp.TaskConsumer(conn, queues=[Queue('foo')])

            # Without arguments.
            presult = self.MyTask().delay()
            self.assertNextTaskDataEqual(consumer, presult, self.MyTask.name)

            # With arguments.
            presult2 = self.MyTask(name='George Costanza 1').enqueue()
            self.assertNextTaskDataEqual(
                consumer, presult2, self.MyTask.name, name='George Costanza 1',
            )

            # send_task
            sresult = self.app.send_task(self.MyTask.name,
                                         kwargs=dict(name='Elaine M. Benes'))
            self.assertNextTaskDataEqual(
                consumer, sresult, self.MyTask.name, name='Elaine M. Benes',
            )

            # With eta.
            eta_task = self.MyTask(name='George Costanza 2')
            eta_eta = self.now() + timedelta(days=1)
            eta_expires = self.now() + timedelta(days=2)
            presult2 = eta_task.apply_async(eta=eta_eta, expires=eta_expires)
            self.assertNextTaskDataEqual(
                consumer, presult2, self.MyTask.name,
                name='George Costanza 2', test_eta=True, test_expires=True,
            )

            # With countdown.
            presult2 = self.MyTask(name='George Costanza 3').enqueue(
                countdown=10, expires=12,
            )
            self.assertNextTaskDataEqual(
                consumer, presult2, self.MyTask.name,
                name='George Costanza 3', test_eta=True, test_expires=True,
            )

            # Discarding all tasks.
            consumer.purge()
            self.MyTask().apply_async()
            self.assertEqual(consumer.purge(), 1)
            self.assertIsNone(consumer.queues[0].get())

            self.assertFalse(presult.successful())
            self.MyTask().backend.mark_as_done(presult.id, result=None)
            self.assertTrue(presult.successful())

    def test_repr_v2_compat(self):
        self.MyTask.get_task().__v2_compat__ = True
        self.assertIn('v2 compatible', repr(self.MyTask.get_task()))

    def test_context_get(self):
        this_task = self.MyTask()
        this_task.push_request()
        try:
            request = this_task.request
            request.foo = 32
            self.assertEqual(request.get('foo'), 32)
            self.assertEqual(request.get('bar', 36), 36)
            request.clear()
        finally:
            this_task.pop_request()

    def test_task_class_repr(self):
        self.assertIn('class Task of', repr(self.MyTask.app.Task))
        self.MyTask.app.Task._app = None
        self.assertIn('unbound', repr(self.MyTask.app.Task, ))

    def test_bind_no_magic_kwargs(self):
        self.MyTask.accept_magic_kwargs = None
        self.MyTask.bind(self.MyTask.app)

    def test_annotate(self):
        with patch('celery.app.task.resolve_all_annotations') as anno:
            anno.return_value = [{'FOO': 'BAR'}]

            @with_metaclass(ClassTask)
            class ThisTask(object):
                shared = False

                def run(self):
                    pass

            ThisTask.annotate()
            self.assertEqual(ThisTask.FOO, 'BAR')

    def test_after_return(self):
        this_task = self.MyTask()
        this_task.push_request()
        try:
            this_task.request.chord = this_task.s()
            this_task.after_return('SUCCESS', 1.0, 'foobar', (), {}, None)
            this_task.request.clear()
        finally:
            this_task.pop_request()

    def test_update_state(self):
        @with_metaclass(ClassTask)
        class Yyy(object):
            shared = False

            def run(self):
                pass

        yyy = Yyy()
        yyy.push_request()
        try:
            tid = uuid()
            yyy.update_state(tid, 'FROBULATING', {'fooz': 'baaz'})
            self.assertEqual(yyy.AsyncResult(tid).status, 'FROBULATING')
            self.assertDictEqual(yyy.AsyncResult(tid).result, {'fooz': 'baaz'})

            yyy.request.id = tid
            yyy.update_state(state='FROBUZATING', meta={'fooz': 'baaz'})
            self.assertEqual(yyy.AsyncResult(tid).status, 'FROBUZATING')
            self.assertDictEqual(yyy.AsyncResult(tid).result, {'fooz': 'baaz'})
        finally:
            yyy.pop_request()

    def test_repr(self):
        @with_metaclass(ClassTask)
        class task_test_repr(object):
            shared = False

        self.assertIn('task_test_repr', repr(task_test_repr()))

    def test_has___name__(self):
        @with_metaclass(ClassTask)
        class yyy2(object):
            shared = False

        self.assertTrue(yyy2.__name__)


class test_apply_task(ClassTasksCase):

    def test_apply_throw(self):
        with self.assertRaises(KeyError):
            self.Raising().apply(throw=True)

    def test_apply_with_CELERY_EAGER_PROPAGATES_EXCEPTIONS(self):
        self.app.conf.CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
        with self.assertRaises(KeyError):
            self.Raising().apply()

    def test_apply(self):
        self.IncrementCounter.count = 0

        e = self.IncrementCounter(2).apply()
        self.assertIsInstance(e, EagerResult)
        self.assertEqual(e.get(), 2)

        e = self.IncrementCounter().apply()
        self.assertEqual(e.get(), 3)

        e = self.IncrementCounter(increment_by=3).apply()
        self.assertEqual(e.get(), 6)

        self.assertTrue(e.successful())
        self.assertTrue(e.ready())
        self.assertTrue(repr(e).startswith('<EagerResult:'))

        f = self.Raising().apply()
        self.assertTrue(f.ready())
        self.assertFalse(f.successful())
        self.assertTrue(f.traceback)
        with self.assertRaises(KeyError):
            f.get()


class test_classtask_specifics(ClassTasksCase):

    def test_multiple_init_raises(self):
        this_task = self.Math(2, 2)
        this_task.init()
        with self.assertRaises(RuntimeError):
            this_task.init()
        with self.assertRaises(RuntimeError):
            this_task.init()

    def test_auto_init(self):
        this_task = self.Math(2, 3)
        with self.assertRaises(AttributeError):
            this_task.add()
        with self.assertRaises(AttributeError):
            this_task.subtract
        self.assertEquals(6, this_task.multiply())
        self.assertEquals(5, this_task.add())
        self.assertEqual(-1, this_task.subtract)
