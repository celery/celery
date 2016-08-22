from __future__ import absolute_import, unicode_literals

import pytest

from datetime import datetime, timedelta

from case import ContextMock, MagicMock, Mock, patch
from kombu import Queue

from celery import Task, group, uuid
from celery.app.task import _reprtask
from celery.exceptions import Ignore, Retry
from celery.five import items, range, string_t
from celery.result import EagerResult
from celery.utils.time import parse_iso8601


def return_True(*args, **kwargs):
    # Task run functions can't be closures/lambdas, as they're pickled.
    return True


class MockApplyTask(Task):
    abstract = True
    applied = 0

    def run(self, x, y):
        return x * y

    def apply_async(self, *args, **kwargs):
        self.applied += 1


class TasksCase:

    def setup(self):
        self.app.conf.task_protocol = 1  # XXX  Still using proto1
        self.mytask = self.app.task(shared=False)(return_True)

        @self.app.task(bind=True, count=0, shared=False)
        def increment_counter(self, increment_by=1):
            self.count += increment_by or 1
            return self.count

        self.increment_counter = increment_counter

        @self.app.task(shared=False)
        def raising():
            raise KeyError('foo')

        self.raising = raising

        @self.app.task(bind=True, max_retries=3, iterations=0, shared=False)
        def retry_task(self, arg1, arg2, kwarg=1, max_retries=None, care=True):
            self.iterations += 1
            rmax = self.max_retries if max_retries is None else max_retries

            assert repr(self.request)
            retries = self.request.retries
            if care and retries >= rmax:
                return arg1
            else:
                raise self.retry(countdown=0, max_retries=rmax)

        self.retry_task = retry_task

        @self.app.task(bind=True, max_retries=3, iterations=0, shared=False)
        def retry_task_noargs(self, **kwargs):
            self.iterations += 1

            if self.request.retries >= 3:
                return 42
            else:
                raise self.retry(countdown=0)

        self.retry_task_noargs = retry_task_noargs

        @self.app.task(bind=True, max_retries=3, iterations=0,
                       base=MockApplyTask, shared=False)
        def retry_task_mockapply(self, arg1, arg2, kwarg=1):
            self.iterations += 1

            retries = self.request.retries
            if retries >= 3:
                return arg1
            raise self.retry(countdown=0)

        self.retry_task_mockapply = retry_task_mockapply

        @self.app.task(bind=True, max_retries=3, iterations=0, shared=False)
        def retry_task_customexc(self, arg1, arg2, kwarg=1, **kwargs):
            self.iterations += 1

            retries = self.request.retries
            if retries >= 3:
                return arg1 + kwarg
            else:
                try:
                    raise MyCustomException('Elaine Marie Benes')
                except MyCustomException as exc:
                    kwargs.update(kwarg=kwarg)
                    raise self.retry(countdown=0, exc=exc)

        self.retry_task_customexc = retry_task_customexc

        @self.app.task(bind=True, autoretry_for=(ZeroDivisionError,),
                       shared=False)
        def autoretry_task_no_kwargs(self, a, b):
            self.iterations += 1
            return a / b

        self.autoretry_task_no_kwargs = autoretry_task_no_kwargs

        @self.app.task(bind=True, autoretry_for=(ZeroDivisionError,),
                       retry_kwargs={'max_retries': 5}, shared=False)
        def autoretry_task(self, a, b):
            self.iterations += 1
            return a / b

        self.autoretry_task = autoretry_task

        # memove all messages from memory-transport
        from kombu.transport.memory import Channel
        Channel.queues.clear()


class MyCustomException(Exception):
    """Random custom exception."""


class test_task_retries(TasksCase):

    def test_retry(self):
        self.retry_task.max_retries = 3
        self.retry_task.iterations = 0
        self.retry_task.apply([0xFF, 0xFFFF])
        assert self.retry_task.iterations == 4

        self.retry_task.max_retries = 3
        self.retry_task.iterations = 0
        self.retry_task.apply([0xFF, 0xFFFF], {'max_retries': 10})
        assert self.retry_task.iterations == 11

    def test_retry_no_args(self):
        self.retry_task_noargs.max_retries = 3
        self.retry_task_noargs.iterations = 0
        self.retry_task_noargs.apply(propagate=True).get()
        assert self.retry_task_noargs.iterations == 4

    def test_signature_from_request__passes_headers(self):
        self.retry_task.push_request()
        self.retry_task.request.headers = {'custom': 10.1}
        sig = self.retry_task.signature_from_request()
        assert sig.options['headers']['custom'] == 10.1

    def test_signature_from_request__delivery_info(self):
        self.retry_task.push_request()
        self.retry_task.request.delivery_info = {
            'exchange': 'testex',
            'routing_key': 'testrk',
        }
        sig = self.retry_task.signature_from_request()
        assert sig.options['exchange'] == 'testex'
        assert sig.options['routing_key'] == 'testrk'

    def test_retry_kwargs_can_be_empty(self):
        self.retry_task_mockapply.push_request()
        try:
            with pytest.raises(Retry):
                import sys
                try:
                    sys.exc_clear()
                except AttributeError:
                    pass
                self.retry_task_mockapply.retry(args=[4, 4], kwargs=None)
        finally:
            self.retry_task_mockapply.pop_request()

    def test_retry_not_eager(self):
        self.retry_task_mockapply.push_request()
        try:
            self.retry_task_mockapply.request.called_directly = False
            exc = Exception('baz')
            try:
                self.retry_task_mockapply.retry(
                    args=[4, 4], kwargs={'task_retries': 0},
                    exc=exc, throw=False,
                )
                assert self.retry_task_mockapply.applied
            finally:
                self.retry_task_mockapply.applied = 0

            try:
                with pytest.raises(Retry):
                    self.retry_task_mockapply.retry(
                        args=[4, 4], kwargs={'task_retries': 0},
                        exc=exc, throw=True)
                assert self.retry_task_mockapply.applied
            finally:
                self.retry_task_mockapply.applied = 0
        finally:
            self.retry_task_mockapply.pop_request()

    def test_retry_with_kwargs(self):
        self.retry_task_customexc.max_retries = 3
        self.retry_task_customexc.iterations = 0
        self.retry_task_customexc.apply([0xFF, 0xFFFF], {'kwarg': 0xF})
        assert self.retry_task_customexc.iterations == 4

    def test_retry_with_custom_exception(self):
        self.retry_task_customexc.max_retries = 2
        self.retry_task_customexc.iterations = 0
        result = self.retry_task_customexc.apply(
            [0xFF, 0xFFFF], {'kwarg': 0xF},
        )
        with pytest.raises(MyCustomException):
            result.get()
        assert self.retry_task_customexc.iterations == 3

    def test_max_retries_exceeded(self):
        self.retry_task.max_retries = 2
        self.retry_task.iterations = 0
        result = self.retry_task.apply([0xFF, 0xFFFF], {'care': False})
        with pytest.raises(self.retry_task.MaxRetriesExceededError):
            result.get()
        assert self.retry_task.iterations == 3

        self.retry_task.max_retries = 1
        self.retry_task.iterations = 0
        result = self.retry_task.apply([0xFF, 0xFFFF], {'care': False})
        with pytest.raises(self.retry_task.MaxRetriesExceededError):
            result.get()
        assert self.retry_task.iterations == 2

    def test_autoretry_no_kwargs(self):
        self.autoretry_task_no_kwargs.max_retries = 3
        self.autoretry_task_no_kwargs.iterations = 0
        self.autoretry_task_no_kwargs.apply((1, 0))
        assert self.autoretry_task_no_kwargs.iterations == 4

    def test_autoretry(self):
        self.autoretry_task.max_retries = 3
        self.autoretry_task.iterations = 0
        self.autoretry_task.apply((1, 0))
        assert self.autoretry_task.iterations == 6


class test_canvas_utils(TasksCase):

    def test_si(self):
        assert self.retry_task.si()
        assert self.retry_task.si().immutable

    def test_chunks(self):
        assert self.retry_task.chunks(range(100), 10)

    def test_map(self):
        assert self.retry_task.map(range(100))

    def test_starmap(self):
        assert self.retry_task.starmap(range(100))

    def test_on_success(self):
        self.retry_task.on_success(1, 1, (), {})


class test_tasks(TasksCase):

    def now(self):
        return self.app.now()

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_unpickle_task(self):
        import pickle

        @self.app.task(shared=True)
        def xxx():
            pass

        assert pickle.loads(pickle.dumps(xxx)) is xxx.app.tasks[xxx.name]

    @patch('celery.app.task.current_app')
    @pytest.mark.usefixtures('depends_on_current_app')
    def test_bind__no_app(self, current_app):

        class XTask(Task):
            _app = None

        XTask._app = None
        XTask.__bound__ = False
        XTask.bind = Mock(name='bind')
        assert XTask.app is current_app
        XTask.bind.assert_called_with(current_app)

    def test_reprtask__no_fmt(self):
        assert _reprtask(self.mytask)

    def test_AsyncResult(self):
        task_id = uuid()
        result = self.retry_task.AsyncResult(task_id)
        assert result.backend == self.retry_task.backend
        assert result.id == task_id

    def assert_next_task_data_equal(self, consumer, presult, task_name,
                                    test_eta=False, test_expires=False,
                                    **kwargs):
        next_task = consumer.queues[0].get(accept=['pickle', 'json'])
        task_data = next_task.decode()
        assert task_data['id'] == presult.id
        assert task_data['task'] == task_name
        task_kwargs = task_data.get('kwargs', {})
        if test_eta:
            assert isinstance(task_data.get('eta'), string_t)
            to_datetime = parse_iso8601(task_data.get('eta'))
            assert isinstance(to_datetime, datetime)
        if test_expires:
            assert isinstance(task_data.get('expires'), string_t)
            to_datetime = parse_iso8601(task_data.get('expires'))
            assert isinstance(to_datetime, datetime)
        for arg_name, arg_value in items(kwargs):
            assert task_kwargs.get(arg_name) == arg_value

    def test_incomplete_task_cls(self):

        class IncompleteTask(Task):
            app = self.app
            name = 'c.unittest.t.itask'

        with pytest.raises(NotImplementedError):
            IncompleteTask().run()

    def test_task_kwargs_must_be_dictionary(self):
        with pytest.raises(TypeError):
            self.increment_counter.apply_async([], 'str')

    def test_task_args_must_be_list(self):
        with pytest.raises(ValueError):
            self.increment_counter.apply_async('s', {})

    def test_regular_task(self):
        assert isinstance(self.mytask, Task)
        assert self.mytask.run()
        assert callable(self.mytask)
        assert self.mytask(), 'Task class runs run() when called'

        with self.app.connection_or_acquire() as conn:
            consumer = self.app.amqp.TaskConsumer(conn)
            with pytest.raises(NotImplementedError):
                consumer.receive('foo', 'foo')
            consumer.purge()
            assert consumer.queues[0].get() is None
            self.app.amqp.TaskConsumer(conn, queues=[Queue('foo')])

            # Without arguments.
            presult = self.mytask.delay()
            self.assert_next_task_data_equal(
                consumer, presult, self.mytask.name)

            # With arguments.
            presult2 = self.mytask.apply_async(
                kwargs=dict(name='George Costanza'),
            )
            self.assert_next_task_data_equal(
                consumer, presult2, self.mytask.name, name='George Costanza',
            )

            # send_task
            sresult = self.app.send_task(self.mytask.name,
                                         kwargs=dict(name='Elaine M. Benes'))
            self.assert_next_task_data_equal(
                consumer, sresult, self.mytask.name, name='Elaine M. Benes',
            )

            # With ETA.
            presult2 = self.mytask.apply_async(
                kwargs=dict(name='George Costanza'),
                eta=self.now() + timedelta(days=1),
                expires=self.now() + timedelta(days=2),
            )
            self.assert_next_task_data_equal(
                consumer, presult2, self.mytask.name,
                name='George Costanza', test_eta=True, test_expires=True,
            )

            # With countdown.
            presult2 = self.mytask.apply_async(
                kwargs=dict(name='George Costanza'), countdown=10, expires=12,
            )
            self.assert_next_task_data_equal(
                consumer, presult2, self.mytask.name,
                name='George Costanza', test_eta=True, test_expires=True,
            )

            # Discarding all tasks.
            consumer.purge()
            self.mytask.apply_async()
            assert consumer.purge() == 1
            assert consumer.queues[0].get() is None

            assert not presult.successful()
            self.mytask.backend.mark_as_done(presult.id, result=None)
            assert presult.successful()

    def test_send_event(self):
        mytask = self.mytask._get_current_object()
        mytask.app.events = Mock(name='events')
        mytask.app.events.attach_mock(ContextMock(), 'default_dispatcher')
        mytask.request.id = 'fb'
        mytask.send_event('task-foo', id=3122)
        mytask.app.events.default_dispatcher().send.assert_called_with(
            'task-foo', uuid='fb', id=3122)

    def test_replace(self):
        sig1 = Mock(name='sig1')
        sig1.options = {}
        with pytest.raises(Ignore):
            self.mytask.replace(sig1)

    def test_replace_callback(self):
        c = group([self.mytask.s()], app=self.app)
        c.freeze = Mock(name='freeze')
        c.delay = Mock(name='delay')
        self.mytask.request.id = 'id'
        self.mytask.request.group = 'group'
        self.mytask.request.root_id = 'root_id'
        self.mytask.request.callbacks = 'callbacks'
        self.mytask.request.errbacks = 'errbacks'

        class JsonMagicMock(MagicMock):
            def __json__(self):
                return 'whatever'

        mocked_signature = JsonMagicMock(name='s')
        accumulate_mock = JsonMagicMock(name='accumulate', s=mocked_signature)
        self.mytask.app.tasks['celery.accumulate'] = accumulate_mock

        try:
            self.mytask.replace(c)
        except Ignore:
            mocked_signature.return_value.set.assert_called_with(
                chord=None,
                link='callbacks',
                link_error='errbacks',
            )

    def test_replace_group(self):
        c = group([self.mytask.s()], app=self.app)
        c.freeze = Mock(name='freeze')
        c.delay = Mock(name='delay')
        self.mytask.request.id = 'id'
        self.mytask.request.group = 'group'
        self.mytask.request.root_id = 'root_id',
        with pytest.raises(Ignore):
            self.mytask.replace(c)

    def test_add_trail__no_trail(self):
        mytask = self.increment_counter._get_current_object()
        mytask.trail = False
        mytask.add_trail('foo')

    def test_repr_v2_compat(self):
        self.mytask.__v2_compat__ = True
        assert 'v2 compatible' in repr(self.mytask)

    def test_apply_with_self(self):

        @self.app.task(__self__=42, shared=False)
        def tawself(self):
            return self

        assert tawself.apply().get() == 42

        assert tawself() == 42

    def test_context_get(self):
        self.mytask.push_request()
        try:
            request = self.mytask.request
            request.foo = 32
            assert request.get('foo') == 32
            assert request.get('bar', 36) == 36
            request.clear()
        finally:
            self.mytask.pop_request()

    def test_annotate(self):
        with patch('celery.app.task.resolve_all_annotations') as anno:
            anno.return_value = [{'FOO': 'BAR'}]

            @self.app.task(shared=False)
            def task():
                pass

            task.annotate()
            assert task.FOO == 'BAR'

    def test_after_return(self):
        self.mytask.push_request()
        try:
            self.mytask.request.chord = self.mytask.s()
            self.mytask.after_return('SUCCESS', 1.0, 'foobar', (), {}, None)
            self.mytask.request.clear()
        finally:
            self.mytask.pop_request()

    def test_update_state(self):

        @self.app.task(shared=False)
        def yyy():
            pass

        yyy.push_request()
        try:
            tid = uuid()
            yyy.update_state(tid, 'FROBULATING', {'fooz': 'baaz'})
            assert yyy.AsyncResult(tid).status == 'FROBULATING'
            assert yyy.AsyncResult(tid).result == {'fooz': 'baaz'}

            yyy.request.id = tid
            yyy.update_state(state='FROBUZATING', meta={'fooz': 'baaz'})
            assert yyy.AsyncResult(tid).status == 'FROBUZATING'
            assert yyy.AsyncResult(tid).result == {'fooz': 'baaz'}
        finally:
            yyy.pop_request()

    def test_repr(self):

        @self.app.task(shared=False)
        def task_test_repr():
            pass

        assert 'task_test_repr' in repr(task_test_repr)

    def test_has___name__(self):

        @self.app.task(shared=False)
        def yyy2():
            pass

        assert yyy2.__name__


class test_apply_task(TasksCase):

    def test_apply_throw(self):
        with pytest.raises(KeyError):
            self.raising.apply(throw=True)

    def test_apply_with_task_eager_propagates(self):
        self.app.conf.task_eager_propagates = True
        with pytest.raises(KeyError):
            self.raising.apply()

    def test_apply(self):
        self.increment_counter.count = 0

        e = self.increment_counter.apply()
        assert isinstance(e, EagerResult)
        assert e.get() == 1

        e = self.increment_counter.apply(args=[1])
        assert e.get() == 2

        e = self.increment_counter.apply(kwargs={'increment_by': 4})
        assert e.get() == 6

        assert e.successful()
        assert e.ready()
        assert repr(e).startswith('<EagerResult:')

        f = self.raising.apply()
        assert f.ready()
        assert not f.successful()
        assert f.traceback
        with pytest.raises(KeyError):
            f.get()
