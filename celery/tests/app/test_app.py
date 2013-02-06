from __future__ import absolute_import
from __future__ import with_statement

import os

from mock import Mock, patch
from pickle import loads, dumps

from kombu import Exchange

from celery import Celery
from celery import app as _app
from celery import _state
from celery.app import defaults
from celery.loaders.base import BaseLoader
from celery.platforms import pyimplementation
from celery.utils.serialization import pickle

from celery.tests import config
from celery.tests.utils import (Case, mask_modules, platform_pyimp,
                                sys_platform, pypy_version)
from celery.utils import uuid
from celery.utils.mail import ErrorMail

THIS_IS_A_KEY = 'this is a value'


class Object(object):

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def _get_test_config():
    return dict((key, getattr(config, key))
                for key in dir(config)
                if key.isupper() and not key.startswith('_'))

test_config = _get_test_config()


class test_module(Case):

    def test_default_app(self):
        self.assertEqual(_app.default_app, _state.default_app)

    def test_bugreport(self):
        self.assertTrue(_app.bugreport())


class test_App(Case):

    def setUp(self):
        self.app = Celery(set_as_current=False)
        self.app.conf.update(test_config)

    def test_task(self):
        app = Celery('foozibari', set_as_current=False)

        def fun():
            pass

        fun.__module__ = '__main__'
        task = app.task(fun)
        self.assertEqual(task.name, app.main + '.fun')

    def test_with_broker(self):
        prev = os.environ.get('CELERY_BROKER_URL')
        os.environ.pop('CELERY_BROKER_URL', None)
        try:
            app = Celery(set_as_current=False, broker='foo://baribaz')
            self.assertEqual(app.conf.BROKER_HOST, 'foo://baribaz')
        finally:
            os.environ['CELERY_BROKER_URL'] = prev

    def test_repr(self):
        self.assertTrue(repr(self.app))

    def test_custom_task_registry(self):
        app1 = Celery(set_as_current=False)
        app2 = Celery(set_as_current=False, tasks=app1.tasks)
        self.assertIs(app2.tasks, app1.tasks)

    def test_include_argument(self):
        app = Celery(set_as_current=False, include=('foo', 'bar.foo'))
        self.assertEqual(app.conf.CELERY_IMPORTS, ('foo', 'bar.foo'))

    def test_set_as_current(self):
        current = _state._tls.current_app
        try:
            app = Celery(set_as_current=True)
            self.assertIs(_state._tls.current_app, app)
        finally:
            _state._tls.current_app = current

    def test_current_task(self):
        app = Celery(set_as_current=False)

        @app.task
        def foo():
            pass

        _state._task_stack.push(foo)
        try:
            self.assertEqual(app.current_task.name, foo.name)
        finally:
            _state._task_stack.pop()

    def test_task_not_shared(self):
        with patch('celery.app.base.shared_task') as shared_task:
            app = Celery(set_as_current=False)

            @app.task(shared=False)
            def foo():
                pass
            self.assertFalse(shared_task.called)

    def test_task_compat_with_filter(self):
        app = Celery(set_as_current=False, accept_magic_kwargs=True)
        check = Mock()

        def filter(task):
            check(task)
            return task

        @app.task(filter=filter)
        def foo():
            pass
        check.assert_called_with(foo)

    def test_task_with_filter(self):
        app = Celery(set_as_current=False, accept_magic_kwargs=False)
        check = Mock()

        def filter(task):
            check(task)
            return task

        @app.task(filter=filter)
        def foo():
            pass
        check.assert_called_with(foo)

    def test_task_sets_main_name_MP_MAIN_FILE(self):
        from celery import utils as _utils
        _utils.MP_MAIN_FILE = __file__
        try:
            app = Celery('xuzzy', set_as_current=False)

            @app.task
            def foo():
                pass

            self.assertEqual(foo.name, 'xuzzy.foo')
        finally:
            _utils.MP_MAIN_FILE = None

    def test_base_task_inherits_magic_kwargs_from_app(self):
        from celery.task import Task as OldTask

        class timkX(OldTask):
            abstract = True

        app = Celery(set_as_current=False, accept_magic_kwargs=True)
        timkX.bind(app)
        # see #918
        self.assertFalse(timkX.accept_magic_kwargs)

        from celery import Task as NewTask

        class timkY(NewTask):
            abstract = True

        timkY.bind(app)
        self.assertFalse(timkY.accept_magic_kwargs)

    def test_annotate_decorator(self):
        from celery.app.task import Task

        class adX(Task):
            abstract = True

            def run(self, y, z, x):
                return y, z, x

        check = Mock()

        def deco(fun):

            def _inner(*args, **kwargs):
                check(*args, **kwargs)
                return fun(*args, **kwargs)
            return _inner

        app = Celery(set_as_current=False)
        app.conf.CELERY_ANNOTATIONS = {
            adX.name: {'@__call__': deco}
        }
        adX.bind(app)
        self.assertIs(adX.app, app)

        i = adX()
        i(2, 4, x=3)
        check.assert_called_with(i, 2, 4, x=3)

        i.annotate()
        i.annotate()

    def test_apply_async_has__self__(self):
        app = Celery(set_as_current=False)

        @app.task(__self__='hello')
        def aawsX():
            pass

        with patch('celery.app.amqp.TaskProducer.publish_task') as dt:
            aawsX.apply_async((4, 5))
            args = dt.call_args[0][1]
            self.assertEqual(args, ('hello', 4, 5))

    def test_apply_async__connection_arg(self):
        app = Celery(set_as_current=False)

        @app.task()
        def aacaX():
            pass

        connection = app.connection('asd://')
        with self.assertRaises(KeyError):
            aacaX.apply_async(connection=connection)

    def test_apply_async_adds_children(self):
        from celery._state import _task_stack
        app = Celery(set_as_current=False)

        @app.task()
        def a3cX1(self):
            pass

        @app.task()
        def a3cX2(self):
            pass

        _task_stack.push(a3cX1)
        try:
            a3cX1.push_request(called_directly=False)
            try:
                res = a3cX2.apply_async(add_to_parent=True)
                self.assertIn(res, a3cX1.request.children)
            finally:
                a3cX1.pop_request()
        finally:
            _task_stack.pop()

    def test_TaskSet(self):
        ts = self.app.TaskSet()
        self.assertListEqual(ts.tasks, [])
        self.assertIs(ts.app, self.app)

    def test_pickle_app(self):
        changes = dict(THE_FOO_BAR='bars',
                       THE_MII_MAR='jars')
        self.app.conf.update(changes)
        saved = pickle.dumps(self.app)
        self.assertLess(len(saved), 2048)
        restored = pickle.loads(saved)
        self.assertDictContainsSubset(changes, restored.conf)

    def test_worker_main(self):
        from celery.bin import celeryd

        class WorkerCommand(celeryd.WorkerCommand):

            def execute_from_commandline(self, argv):
                return argv

        prev, celeryd.WorkerCommand = celeryd.WorkerCommand, WorkerCommand
        try:
            ret = self.app.worker_main(argv=['--version'])
            self.assertListEqual(ret, ['--version'])
        finally:
            celeryd.WorkerCommand = prev

    def test_config_from_envvar(self):
        os.environ['CELERYTEST_CONFIG_OBJECT'] = 'celery.tests.app.test_app'
        self.app.config_from_envvar('CELERYTEST_CONFIG_OBJECT')
        self.assertEqual(self.app.conf.THIS_IS_A_KEY, 'this is a value')

    def test_config_from_object(self):

        class Object(object):
            LEAVE_FOR_WORK = True
            MOMENT_TO_STOP = True
            CALL_ME_BACK = 123456789
            WANT_ME_TO = False
            UNDERSTAND_ME = True

        self.app.config_from_object(Object())

        self.assertTrue(self.app.conf.LEAVE_FOR_WORK)
        self.assertTrue(self.app.conf.MOMENT_TO_STOP)
        self.assertEqual(self.app.conf.CALL_ME_BACK, 123456789)
        self.assertFalse(self.app.conf.WANT_ME_TO)
        self.assertTrue(self.app.conf.UNDERSTAND_ME)

    def test_config_from_cmdline(self):
        cmdline = ['.always_eager=no',
                   '.result_backend=/dev/null',
                   '.task_error_whitelist=(list)["a", "b", "c"]',
                   'celeryd.prefetch_multiplier=368',
                   '.foobarstring=(string)300',
                   '.foobarint=(int)300',
                   '.result_engine_options=(dict){"foo": "bar"}']
        self.app.config_from_cmdline(cmdline, namespace='celery')
        self.assertFalse(self.app.conf.CELERY_ALWAYS_EAGER)
        self.assertEqual(self.app.conf.CELERY_RESULT_BACKEND, '/dev/null')
        self.assertEqual(self.app.conf.CELERYD_PREFETCH_MULTIPLIER, 368)
        self.assertListEqual(self.app.conf.CELERY_TASK_ERROR_WHITELIST,
                             ['a', 'b', 'c'])
        self.assertEqual(self.app.conf.CELERY_FOOBARSTRING, '300')
        self.assertEqual(self.app.conf.CELERY_FOOBARINT, 300)
        self.assertDictEqual(self.app.conf.CELERY_RESULT_ENGINE_OPTIONS,
                             {'foo': 'bar'})

    def test_compat_setting_CELERY_BACKEND(self):

        self.app.config_from_object(Object(CELERY_BACKEND='set_by_us'))
        self.assertEqual(self.app.conf.CELERY_RESULT_BACKEND, 'set_by_us')

    def test_setting_BROKER_TRANSPORT_OPTIONS(self):

        _args = {'foo': 'bar', 'spam': 'baz'}

        self.app.config_from_object(Object())
        self.assertEqual(self.app.conf.BROKER_TRANSPORT_OPTIONS, {})

        self.app.config_from_object(Object(BROKER_TRANSPORT_OPTIONS=_args))
        self.assertEqual(self.app.conf.BROKER_TRANSPORT_OPTIONS, _args)

    def test_Windows_log_color_disabled(self):
        self.app.IS_WINDOWS = True
        self.assertFalse(self.app.log.supports_color(True))

    def test_compat_setting_CARROT_BACKEND(self):
        self.app.config_from_object(Object(CARROT_BACKEND='set_by_us'))
        self.assertEqual(self.app.conf.BROKER_TRANSPORT, 'set_by_us')

    def test_WorkController(self):
        x = self.app.WorkController
        self.assertIs(x.app, self.app)

    def test_Worker(self):
        x = self.app.Worker
        self.assertIs(x.app, self.app)

    def test_AsyncResult(self):
        x = self.app.AsyncResult('1')
        self.assertIs(x.app, self.app)
        r = loads(dumps(x))
        # not set as current, so ends up as default app after reduce
        self.assertIs(r.app, _state.default_app)

    @patch('celery.bin.celery.CeleryCommand.execute_from_commandline')
    def test_start(self, execute):
        self.app.start()
        self.assertTrue(execute.called)

    def test_mail_admins(self):

        class Loader(BaseLoader):

            def mail_admins(*args, **kwargs):
                return args, kwargs

        self.app.loader = Loader()
        self.app.conf.ADMINS = None
        self.assertFalse(self.app.mail_admins('Subject', 'Body'))
        self.app.conf.ADMINS = [('George Costanza', 'george@vandelay.com')]
        self.assertTrue(self.app.mail_admins('Subject', 'Body'))

    def test_amqp_get_broker_info(self):
        self.assertDictContainsSubset(
            {'hostname': 'localhost',
             'userid': 'guest',
             'password': 'guest',
             'virtual_host': '/'},
            self.app.connection('amqp://').info(),
        )
        self.app.conf.BROKER_PORT = 1978
        self.app.conf.BROKER_VHOST = 'foo'
        self.assertDictContainsSubset(
            {'port': 1978, 'virtual_host': 'foo'},
            self.app.connection('amqp://:1978/foo').info(),
        )
        conn = self.app.connection('amqp:////value')
        self.assertDictContainsSubset({'virtual_host': '/value'},
                                      conn.info())

    def test_BROKER_BACKEND_alias(self):
        self.assertEqual(self.app.conf.BROKER_BACKEND,
                         self.app.conf.BROKER_TRANSPORT)

    def test_with_default_connection(self):

        @self.app.with_default_connection
        def handler(connection=None, foo=None):
            return connection, foo

        connection, foo = handler(foo=42)
        self.assertEqual(foo, 42)
        self.assertTrue(connection)

    def test_after_fork(self):
        p = self.app._pool = Mock()
        self.app._after_fork(self.app)
        p.force_close_all.assert_called_with()
        self.assertIsNone(self.app._pool)
        self.app._after_fork(self.app)

    def test_pool_no_multiprocessing(self):
        with mask_modules('multiprocessing.util'):
            pool = self.app.pool
            self.assertIs(pool, self.app._pool)

    def test_bugreport(self):
        self.assertTrue(self.app.bugreport())

    def test_send_task_sent_event(self):

        class Dispatcher(object):
            sent = []

            def publish(self, type, fields, *args, **kwargs):
                self.sent.append((type, fields))

        conn = self.app.connection()
        chan = conn.channel()
        try:
            for e in ('foo_exchange', 'moo_exchange', 'bar_exchange'):
                chan.exchange_declare(e, 'direct', durable=True)
                chan.queue_declare(e, durable=True)
                chan.queue_bind(e, e, e)
        finally:
            chan.close()
        assert conn.transport_cls == 'memory'

        prod = self.app.amqp.TaskProducer(
            conn, exchange=Exchange('foo_exchange'),
            send_sent_event=True,
        )

        dispatcher = Dispatcher()
        self.assertTrue(prod.publish_task('footask', (), {},
                                          exchange='moo_exchange',
                                          routing_key='moo_exchange',
                                          event_dispatcher=dispatcher))
        self.assertTrue(dispatcher.sent)
        self.assertEqual(dispatcher.sent[0][0], 'task-sent')
        self.assertTrue(prod.publish_task('footask', (), {},
                                          event_dispatcher=dispatcher,
                                          exchange='bar_exchange',
                                          routing_key='bar_exchange'))

    def test_error_mail_sender(self):
        x = ErrorMail.subject % {'name': 'task_name',
                                 'id': uuid(),
                                 'exc': 'FOOBARBAZ',
                                 'hostname': 'lana'}
        self.assertTrue(x)


class test_defaults(Case):

    def test_str_to_bool(self):
        for s in ('false', 'no', '0'):
            self.assertFalse(defaults.strtobool(s))
        for s in ('true', 'yes', '1'):
            self.assertTrue(defaults.strtobool(s))
        with self.assertRaises(TypeError):
            defaults.strtobool('unsure')


class test_debugging_utils(Case):

    def test_enable_disable_trace(self):
        try:
            _app.enable_trace()
            self.assertEqual(_app.app_or_default, _app._app_or_default_trace)
            _app.disable_trace()
            self.assertEqual(_app.app_or_default, _app._app_or_default)
        finally:
            _app.disable_trace()


class test_pyimplementation(Case):

    def test_platform_python_implementation(self):
        with platform_pyimp(lambda: 'Xython'):
            self.assertEqual(pyimplementation(), 'Xython')

    def test_platform_jython(self):
        with platform_pyimp():
            with sys_platform('java 1.6.51'):
                self.assertIn('Jython', pyimplementation())

    def test_platform_pypy(self):
        with platform_pyimp():
            with sys_platform('darwin'):
                with pypy_version((1, 4, 3)):
                    self.assertIn('PyPy', pyimplementation())
                with pypy_version((1, 4, 3, 'a4')):
                    self.assertIn('PyPy', pyimplementation())

    def test_platform_fallback(self):
        with platform_pyimp():
            with sys_platform('darwin'):
                with pypy_version():
                    self.assertEqual('CPython', pyimplementation())
