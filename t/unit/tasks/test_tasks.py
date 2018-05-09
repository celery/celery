from __future__ import absolute_import, unicode_literals

import socket
import tempfile
from datetime import datetime, timedelta

import pytest
from case import ANY, ContextMock, MagicMock, Mock, patch
from kombu import Queue

from celery import Task, group, uuid
from celery.app.task import _reprtask
from celery.exceptions import Ignore, ImproperlyConfigured, Retry
from celery.five import items, range, string_t
from celery.result import EagerResult
from celery.task.base import Task as OldTask
from celery.utils.time import parse_iso8601

try:
    from urllib.error import HTTPError
except ImportError:  # pragma: no cover
    from urllib2 import HTTPError


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

        @self.app.task(bind=True, autoretry_for=(HTTPError,),
                       retry_backoff=True, shared=False)
        def autoretry_backoff_task(self, url):
            self.iterations += 1
            if "error" in url:
                fp = tempfile.TemporaryFile()
                raise HTTPError(url, '500', 'Error', '', fp)
            return url

        self.autoretry_backoff_task = autoretry_backoff_task

        @self.app.task(bind=True, autoretry_for=(HTTPError,),
                       retry_backoff=True, retry_jitter=True, shared=False)
        def autoretry_backoff_jitter_task(self, url):
            self.iterations += 1
            if "error" in url:
                fp = tempfile.TemporaryFile()
                raise HTTPError(url, '500', 'Error', '', fp)
            return url

        self.autoretry_backoff_jitter_task = autoretry_backoff_jitter_task

        @self.app.task(bind=True)
        def task_check_request_context(self):
            assert self.request.hostname == socket.gethostname()

        self.task_check_request_context = task_check_request_context

        @self.app.task(ignore_result=True)
        def task_with_ignored_result():
            pass

        self.task_with_ignored_result = task_with_ignored_result

        # Remove all messages from memory-transport
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

    @patch('random.randrange', side_effect=lambda i: i - 1)
    def test_autoretry_backoff(self, randrange):
        task = self.autoretry_backoff_task
        task.max_retries = 3
        task.iterations = 0

        with patch.object(task, 'retry', wraps=task.retry) as fake_retry:
            task.apply(("http://httpbin.org/error",))

        assert task.iterations == 4
        retry_call_countdowns = [
            call[1]['countdown'] for call in fake_retry.call_args_list
        ]
        assert retry_call_countdowns == [1, 2, 4, 8]

    @patch('random.randrange', side_effect=lambda i: i - 2)
    def test_autoretry_backoff_jitter(self, randrange):
        task = self.autoretry_backoff_jitter_task
        task.max_retries = 3
        task.iterations = 0

        with patch.object(task, 'retry', wraps=task.retry) as fake_retry:
            task.apply(("http://httpbin.org/error",))

        assert task.iterations == 4
        retry_call_countdowns = [
            call[1]['countdown'] for call in fake_retry.call_args_list
        ]
        assert retry_call_countdowns == [0, 1, 3, 7]

    def test_retry_wrong_eta_when_not_enable_utc(self):
        """Issue #3753"""
        self.app.conf.enable_utc = False
        self.app.conf.timezone = 'US/Eastern'
        self.autoretry_task.iterations = 0
        self.autoretry_task.default_retry_delay = 2

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

    def test_typing(self):
        @self.app.task()
        def add(x, y, kw=1):
            pass

        with pytest.raises(TypeError):
            add.delay(1)

        with pytest.raises(TypeError):
            add.delay(1, kw=2)

        with pytest.raises(TypeError):
            add.delay(1, 2, foobar=3)

        add.delay(2, 2)

    def test_shadow_name(self):
        def shadow_name(task, args, kwargs, options):
            return 'fooxyz'

        @self.app.task(shadow_name=shadow_name)
        def shadowed():
            pass

        old_send_task = self.app.send_task
        self.app.send_task = Mock()

        shadowed.delay()

        self.app.send_task.assert_called_once_with(ANY, ANY, ANY,
                                                   compression=ANY,
                                                   delivery_mode=ANY,
                                                   exchange=ANY,
                                                   expires=ANY,
                                                   immediate=ANY,
                                                   link=ANY,
                                                   link_error=ANY,
                                                   mandatory=ANY,
                                                   priority=ANY,
                                                   producer=ANY,
                                                   queue=ANY,
                                                   result_cls=ANY,
                                                   routing_key=ANY,
                                                   serializer=ANY,
                                                   soft_time_limit=ANY,
                                                   task_id=ANY,
                                                   task_type=ANY,
                                                   time_limit=ANY,
                                                   shadow='fooxyz',
                                                   ignore_result=False)

        self.app.send_task = old_send_task

    def test_shadow_name_old_task_class(self):
        def shadow_name(task, args, kwargs, options):
            return 'fooxyz'

        @self.app.task(base=OldTask, shadow_name=shadow_name)
        def shadowed():
            pass

        old_send_task = self.app.send_task
        self.app.send_task = Mock()

        shadowed.delay()

        self.app.send_task.assert_called_once_with(ANY, ANY, ANY,
                                                   compression=ANY,
                                                   delivery_mode=ANY,
                                                   exchange=ANY,
                                                   expires=ANY,
                                                   immediate=ANY,
                                                   link=ANY,
                                                   link_error=ANY,
                                                   mandatory=ANY,
                                                   priority=ANY,
                                                   producer=ANY,
                                                   queue=ANY,
                                                   result_cls=ANY,
                                                   routing_key=ANY,
                                                   serializer=ANY,
                                                   soft_time_limit=ANY,
                                                   task_id=ANY,
                                                   task_type=ANY,
                                                   time_limit=ANY,
                                                   shadow='fooxyz',
                                                   ignore_result=False)

        self.app.send_task = old_send_task

    def test_typing__disabled(self):
        @self.app.task(typing=False)
        def add(x, y, kw=1):
            pass
        add.delay(1)
        add.delay(1, kw=2)
        add.delay(1, 2, foobar=3)

    def test_typing__disabled_by_app(self):
        with self.Celery(set_as_current=False, strict_typing=False) as app:
            @app.task()
            def add(x, y, kw=1):
                pass
            assert not add.typing
            add.delay(1)
            add.delay(1, kw=2)
            add.delay(1, 2, foobar=3)

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
                                    properties=None, headers=None, **kwargs):
        next_task = consumer.queues[0].get(accept=['pickle', 'json'])
        task_properties = next_task.properties
        task_headers = next_task.headers
        task_body = next_task.decode()
        task_args, task_kwargs, embed = task_body
        assert task_headers['id'] == presult.id
        assert task_headers['task'] == task_name
        if test_eta:
            assert isinstance(task_headers.get('eta'), string_t)
            to_datetime = parse_iso8601(task_headers.get('eta'))
            assert isinstance(to_datetime, datetime)
        if test_expires:
            assert isinstance(task_headers.get('expires'), string_t)
            to_datetime = parse_iso8601(task_headers.get('expires'))
            assert isinstance(to_datetime, datetime)
        properties = properties or {}
        for arg_name, arg_value in items(properties):
            assert task_properties.get(arg_name) == arg_value
        headers = headers or {}
        for arg_name, arg_value in items(headers):
            assert task_headers.get(arg_name) == arg_value
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
        with pytest.raises(TypeError):
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
                kwargs={'name': 'George Costanza'},
            )
            self.assert_next_task_data_equal(
                consumer, presult2, self.mytask.name, name='George Costanza',
            )

            # send_task
            sresult = self.app.send_task(self.mytask.name,
                                         kwargs={'name': 'Elaine M. Benes'})
            self.assert_next_task_data_equal(
                consumer, sresult, self.mytask.name, name='Elaine M. Benes',
            )

            # With ETA.
            presult2 = self.mytask.apply_async(
                kwargs={'name': 'George Costanza'},
                eta=self.now() + timedelta(days=1),
                expires=self.now() + timedelta(days=2),
            )
            self.assert_next_task_data_equal(
                consumer, presult2, self.mytask.name,
                name='George Costanza', test_eta=True, test_expires=True,
            )

            # With countdown.
            presult2 = self.mytask.apply_async(
                kwargs={'name': 'George Costanza'}, countdown=10, expires=12,
            )
            self.assert_next_task_data_equal(
                consumer, presult2, self.mytask.name,
                name='George Costanza', test_eta=True, test_expires=True,
            )

            # Default argsrepr/kwargsrepr behavior
            presult2 = self.mytask.apply_async(
                args=('spam',), kwargs={'name': 'Jerry Seinfeld'}
            )
            self.assert_next_task_data_equal(
                consumer, presult2, self.mytask.name,
                headers={'argsrepr': "('spam',)",
                         'kwargsrepr': "{'name': 'Jerry Seinfeld'}"},
            )

            # With argsrepr/kwargsrepr
            presult2 = self.mytask.apply_async(
                args=('secret',), argsrepr="'***'",
                kwargs={'password': 'foo'}, kwargsrepr="{'password': '***'}",
            )
            self.assert_next_task_data_equal(
                consumer, presult2, self.mytask.name,
                headers={'argsrepr': "'***'",
                         'kwargsrepr': "{'password': '***'}"},
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
            'task-foo', uuid='fb', id=3122,
            retry=True, retry_policy=self.app.conf.task_publish_retry_policy)

    def test_replace(self):
        sig1 = Mock(name='sig1')
        sig1.options = {}
        with pytest.raises(Ignore):
            self.mytask.replace(sig1)

    def test_replace_with_chord(self):
        sig1 = Mock(name='sig1')
        sig1.options = {'chord': None}
        with pytest.raises(ImproperlyConfigured):
            self.mytask.replace(sig1)

    @pytest.mark.usefixtures('depends_on_current_app')
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
            parent = None

            def __json__(self):
                return 'whatever'

            def reprcall(self, *args, **kwargs):
                return 'whatever2'

        mocked_signature = JsonMagicMock(name='s')
        accumulate_mock = JsonMagicMock(name='accumulate', s=mocked_signature)
        self.mytask.app.tasks['celery.accumulate'] = accumulate_mock

        try:
            self.mytask.replace(c)
        except Ignore:
            mocked_signature.return_value.set.assert_called_with(
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

    def test_apply_request_context_is_ok(self):
        self.app.conf.task_eager_propagates = True
        self.task_check_request_context.apply()

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


class test_apply_async(TasksCase):
    def common_send_task_arguments(self):
        return (ANY, ANY, ANY), dict(
            compression=ANY,
            delivery_mode=ANY,
            exchange=ANY,
            expires=ANY,
            immediate=ANY,
            link=ANY,
            link_error=ANY,
            mandatory=ANY,
            priority=ANY,
            producer=ANY,
            queue=ANY,
            result_cls=ANY,
            routing_key=ANY,
            serializer=ANY,
            soft_time_limit=ANY,
            task_id=ANY,
            task_type=ANY,
            time_limit=ANY,
            shadow=None,
            ignore_result=False
        )

    def test_task_with_ignored_result(self):
        with patch.object(self.app, 'send_task') as send_task:
            self.task_with_ignored_result.apply_async()
            expected_args, expected_kwargs = self.common_send_task_arguments()
            expected_kwargs['ignore_result'] = True
            send_task.assert_called_once_with(
                *expected_args,
                **expected_kwargs
            )

    def test_task_with_result(self):
        with patch.object(self.app, 'send_task') as send_task:
            self.mytask.apply_async()
            expected_args, expected_kwargs = self.common_send_task_arguments()
            send_task.assert_called_once_with(
                *expected_args,
                **expected_kwargs
            )

    def test_task_with_result_ignoring_on_call(self):
        with patch.object(self.app, 'send_task') as send_task:
            self.mytask.apply_async(ignore_result=True)
            expected_args, expected_kwargs = self.common_send_task_arguments()
            expected_kwargs['ignore_result'] = True
            send_task.assert_called_once_with(
                *expected_args,
                **expected_kwargs
            )
