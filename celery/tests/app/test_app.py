from __future__ import absolute_import, unicode_literals

import gc
import os
import itertools

from copy import deepcopy
from pickle import loads, dumps

from vine import promise

from celery import Celery
from celery import shared_task, current_app
from celery import app as _app
from celery import _state
from celery.app import base as _appbase
from celery.app import defaults
from celery.exceptions import ImproperlyConfigured
from celery.five import keys
from celery.loaders.base import unconfigured
from celery.platforms import pyimplementation
from celery.utils.serialization import pickle
from celery.utils.timeutils import timezone

from celery.tests.case import (
    CELERY_TEST_CONFIG,
    AppCase,
    Mock,
    Case,
    ContextMock,
    depends_on_current_app,
    mock,
    patch,
)
from celery.utils.objects import Bunch

THIS_IS_A_KEY = 'this is a value'


class ObjectConfig(object):
    FOO = 1
    BAR = 2

object_config = ObjectConfig()
dict_config = dict(FOO=10, BAR=20)


class ObjectConfig2(object):
    LEAVE_FOR_WORK = True
    MOMENT_TO_STOP = True
    CALL_ME_BACK = 123456789
    WANT_ME_TO = False
    UNDERSTAND_ME = True


def _get_test_config():
    return deepcopy(CELERY_TEST_CONFIG)
test_config = _get_test_config()


class test_module(AppCase):

    def test_default_app(self):
        self.assertEqual(_app.default_app, _state.default_app)

    def test_bugreport(self):
        self.assertTrue(_app.bugreport(app=self.app))


class test_task_join_will_block(Case):

    def test_task_join_will_block(self):
        prev, _state._task_join_will_block = _state._task_join_will_block, 0
        try:
            self.assertEqual(_state._task_join_will_block, 0)
            _state._set_task_join_will_block(True)
            print(_state.task_join_will_block)
            self.assertTrue(_state.task_join_will_block())
        finally:
            _state._task_join_will_block = prev


class test_App(AppCase):

    def setup(self):
        self.app.add_defaults(test_config)

    def test_task_autofinalize_disabled(self):
        with self.Celery('xyzibari', autofinalize=False) as app:
            @app.task
            def ttafd():
                return 42

            with self.assertRaises(RuntimeError):
                ttafd()

        with self.Celery('xyzibari', autofinalize=False) as app:
            @app.task
            def ttafd2():
                return 42

            app.finalize()
            self.assertEqual(ttafd2(), 42)

    def test_registry_autofinalize_disabled(self):
        with self.Celery('xyzibari', autofinalize=False) as app:
            with self.assertRaises(RuntimeError):
                app.tasks['celery.chain']
            app.finalize()
            self.assertTrue(app.tasks['celery.chain'])

    def test_task(self):
        with self.Celery('foozibari') as app:

            def fun():
                pass

            fun.__module__ = '__main__'
            task = app.task(fun)
            self.assertEqual(task.name, app.main + '.fun')

    def test_task_too_many_args(self):
        with self.assertRaises(TypeError):
            self.app.task(Mock(name='fun'), True)
        with self.assertRaises(TypeError):
            self.app.task(Mock(name='fun'), True, 1, 2)

    def test_with_config_source(self):
        with self.Celery(config_source=ObjectConfig) as app:
            self.assertEqual(app.conf.FOO, 1)
            self.assertEqual(app.conf.BAR, 2)

    @depends_on_current_app
    def test_task_windows_execv(self):
        prev, _appbase.USING_EXECV = _appbase.USING_EXECV, True
        try:
            @self.app.task(shared=False)
            def foo():
                pass

            self.assertTrue(foo._get_current_object())  # is proxy

        finally:
            _appbase.USING_EXECV = prev
        assert not _appbase.USING_EXECV

    def test_task_takes_no_args(self):
        with self.assertRaises(TypeError):
            @self.app.task(1)
            def foo():
                pass

    def test_add_defaults(self):
        self.assertFalse(self.app.configured)
        _conf = {'FOO': 300}

        def conf():
            return _conf

        self.app.add_defaults(conf)
        self.assertIn(conf, self.app._pending_defaults)
        self.assertFalse(self.app.configured)
        self.assertEqual(self.app.conf.FOO, 300)
        self.assertTrue(self.app.configured)
        self.assertFalse(self.app._pending_defaults)

        # defaults not pickled
        appr = loads(dumps(self.app))
        with self.assertRaises(AttributeError):
            appr.conf.FOO

        # add more defaults after configured
        conf2 = {'FOO': 'BAR'}
        self.app.add_defaults(conf2)
        self.assertEqual(self.app.conf.FOO, 'BAR')

        self.assertIn(_conf, self.app.conf.defaults)
        self.assertIn(conf2, self.app.conf.defaults)

    def test_connection_or_acquire(self):
        with self.app.connection_or_acquire(block=True):
            self.assertTrue(self.app.pool._dirty)

        with self.app.connection_or_acquire(pool=False):
            self.assertFalse(self.app.pool._dirty)

    def test_using_v1_reduce(self):
        self.app._using_v1_reduce = True
        self.assertTrue(loads(dumps(self.app)))

    def test_autodiscover_tasks_force(self):
        self.app.loader.autodiscover_tasks = Mock()
        self.app.autodiscover_tasks(['proj.A', 'proj.B'], force=True)
        self.app.loader.autodiscover_tasks.assert_called_with(
            ['proj.A', 'proj.B'], 'tasks',
        )
        self.app.loader.autodiscover_tasks = Mock()

        def lazy_list():
            return ['proj.A', 'proj.B']
        self.app.autodiscover_tasks(
            lazy_list,
            related_name='george',
            force=True,
        )
        self.app.loader.autodiscover_tasks.assert_called_with(
            ['proj.A', 'proj.B'], 'george',
        )

    def test_autodiscover_tasks_lazy(self):
        with patch('celery.signals.import_modules') as import_modules:
            def lazy_list():
                return [1, 2, 3]
            self.app.autodiscover_tasks(lazy_list)
            import_modules.connect.assert_called()
            prom = import_modules.connect.call_args[0][0]
            self.assertIsInstance(prom, promise)
            self.assertEqual(prom.fun, self.app._autodiscover_tasks)
            self.assertEqual(prom.args[0](), [1, 2, 3])

    def test_autodiscover_tasks__no_packages(self):
        fixup1 = Mock(name='fixup')
        fixup2 = Mock(name='fixup')
        self.app._autodiscover_tasks_from_names = Mock(name='auto')
        self.app._fixups = [fixup1, fixup2]
        fixup1.autodiscover_tasks.return_value = ['A', 'B', 'C']
        fixup2.autodiscover_tasks.return_value = ['D', 'E', 'F']
        self.app.autodiscover_tasks(force=True)
        self.app._autodiscover_tasks_from_names.assert_called_with(
            ['A', 'B', 'C', 'D', 'E', 'F'], related_name='tasks',
        )

    @mock.environ('CELERY_BROKER_URL', '')
    def test_with_broker(self):
        with self.Celery(broker='foo://baribaz') as app:
            self.assertEqual(app.conf.broker_url, 'foo://baribaz')

    def test_pending_configuration__setattr(self):
        with self.Celery(broker='foo://bar') as app:
            app.conf.task_default_delivery_mode = 44
            app.conf.worker_agent = 'foo:Bar'
            self.assertFalse(app.configured)
            self.assertEqual(app.conf.worker_agent, 'foo:Bar')
            self.assertEqual(app.conf.broker_url, 'foo://bar')
            self.assertEqual(app._preconf['worker_agent'], 'foo:Bar')

            self.assertTrue(app.configured)
            reapp = pickle.loads(pickle.dumps(app))
            self.assertEqual(reapp._preconf['worker_agent'], 'foo:Bar')
            self.assertFalse(reapp.configured)
            self.assertEqual(reapp.conf.worker_agent, 'foo:Bar')
            self.assertTrue(reapp.configured)
            self.assertEqual(reapp.conf.broker_url, 'foo://bar')
            self.assertEqual(reapp._preconf['worker_agent'], 'foo:Bar')

    def test_pending_configuration__update(self):
        with self.Celery(broker='foo://bar') as app:
            app.conf.update(
                task_default_delivery_mode=44,
                worker_agent='foo:Bar',
            )
            self.assertFalse(app.configured)
            self.assertEqual(app.conf.worker_agent, 'foo:Bar')
            self.assertEqual(app.conf.broker_url, 'foo://bar')
            self.assertEqual(app._preconf['worker_agent'], 'foo:Bar')

    def test_pending_configuration__compat_settings(self):
        with self.Celery(broker='foo://bar', backend='foo') as app:
            app.conf.update(
                CELERY_ALWAYS_EAGER=4,
                CELERY_DEFAULT_DELIVERY_MODE=63,
                CELERYD_AGENT='foo:Barz',
            )
            self.assertEqual(app.conf.task_always_eager, 4)
            self.assertEqual(app.conf.task_default_delivery_mode, 63)
            self.assertEqual(app.conf.worker_agent, 'foo:Barz')
            self.assertEqual(app.conf.broker_url, 'foo://bar')
            self.assertEqual(app.conf.result_backend, 'foo')

    def test_pending_configuration__compat_settings_mixing(self):
        with self.Celery(broker='foo://bar', backend='foo') as app:
            app.conf.update(
                CELERY_ALWAYS_EAGER=4,
                CELERY_DEFAULT_DELIVERY_MODE=63,
                CELERYD_AGENT='foo:Barz',
                worker_consumer='foo:Fooz',
            )
            with self.assertRaises(ImproperlyConfigured):
                self.assertEqual(app.conf.task_always_eager, 4)

    def test_pending_configuration__compat_settings_mixing_new(self):
        with self.Celery(broker='foo://bar', backend='foo') as app:
            app.conf.update(
                task_always_eager=4,
                task_default_delivery_mode=63,
                worker_agent='foo:Barz',
                CELERYD_CONSUMER='foo:Fooz',
                CELERYD_POOL='foo:Xuzzy',
            )
            with self.assertRaises(ImproperlyConfigured):
                self.assertEqual(app.conf.worker_consumer, 'foo:Fooz')

    def test_pending_configuration__compat_settings_mixing_alt(self):
        with self.Celery(broker='foo://bar', backend='foo') as app:
            app.conf.update(
                task_always_eager=4,
                task_default_delivery_mode=63,
                worker_agent='foo:Barz',
                CELERYD_CONSUMER='foo:Fooz',
                worker_consumer='foo:Fooz',
                CELERYD_POOL='foo:Xuzzy',
                worker_pool='foo:Xuzzy'
            )
            self.assertEqual(app.conf.task_always_eager, 4)
            self.assertEqual(app.conf.worker_pool, 'foo:Xuzzy')

    def test_pending_configuration__setdefault(self):
        with self.Celery(broker='foo://bar') as app:
            app.conf.setdefault('worker_agent', 'foo:Bar')
            self.assertFalse(app.configured)

    def test_pending_configuration__iter(self):
        with self.Celery(broker='foo://bar') as app:
            app.conf.worker_agent = 'foo:Bar'
            self.assertFalse(app.configured)
            self.assertTrue(list(keys(app.conf)))
            self.assertFalse(app.configured)
            self.assertIn('worker_agent', app.conf)
            self.assertFalse(app.configured)
            self.assertTrue(dict(app.conf))
            self.assertTrue(app.configured)

    def test_pending_configuration__raises_ImproperlyConfigured(self):
        with self.Celery(set_as_current=False) as app:
            app.conf.worker_agent = 'foo://bar'
            app.conf.task_default_delivery_mode = 44
            app.conf.CELERY_ALWAYS_EAGER = 5
            with self.assertRaises(ImproperlyConfigured):
                app.finalize()

        with self.Celery() as app:
            self.assertFalse(self.app.conf.task_always_eager)

    def test_repr(self):
        self.assertTrue(repr(self.app))

    def test_custom_task_registry(self):
        with self.Celery(tasks=self.app.tasks) as app2:
            self.assertIs(app2.tasks, self.app.tasks)

    def test_include_argument(self):
        with self.Celery(include=('foo', 'bar.foo')) as app:
            self.assertEqual(app.conf.include, ('foo', 'bar.foo'))

    def test_set_as_current(self):
        current = _state._tls.current_app
        try:
            app = self.Celery(set_as_current=True)
            self.assertIs(_state._tls.current_app, app)
        finally:
            _state._tls.current_app = current

    def test_current_task(self):
        @self.app.task
        def foo(shared=False):
            pass

        _state._task_stack.push(foo)
        try:
            self.assertEqual(self.app.current_task.name, foo.name)
        finally:
            _state._task_stack.pop()

    def test_task_not_shared(self):
        with patch('celery.app.base.connect_on_app_finalize') as sh:
            @self.app.task(shared=False)
            def foo():
                pass
            sh.assert_not_called()

    def test_task_compat_with_filter(self):
        with self.Celery() as app:
            check = Mock()

            def filter(task):
                check(task)
                return task

            @app.task(filter=filter, shared=False)
            def foo():
                pass
            check.assert_called_with(foo)

    def test_task_with_filter(self):
        with self.Celery() as app:
            check = Mock()

            def filter(task):
                check(task)
                return task

            assert not _appbase.USING_EXECV

            @app.task(filter=filter, shared=False)
            def foo():
                pass
            check.assert_called_with(foo)

    def test_task_sets_main_name_MP_MAIN_FILE(self):
        from celery.utils import imports as _imports
        _imports.MP_MAIN_FILE = __file__
        try:
            with self.Celery('xuzzy') as app:

                @app.task
                def foo():
                    pass

                self.assertEqual(foo.name, 'xuzzy.foo')
        finally:
            _imports.MP_MAIN_FILE = None

    def test_annotate_decorator(self):
        from celery.app.task import Task

        class adX(Task):

            def run(self, y, z, x):
                return y, z, x

        check = Mock()

        def deco(fun):

            def _inner(*args, **kwargs):
                check(*args, **kwargs)
                return fun(*args, **kwargs)
            return _inner

        self.app.conf.task_annotations = {
            adX.name: {'@__call__': deco}
        }
        adX.bind(self.app)
        self.assertIs(adX.app, self.app)

        i = adX()
        i(2, 4, x=3)
        check.assert_called_with(i, 2, 4, x=3)

        i.annotate()
        i.annotate()

    def test_apply_async_has__self__(self):
        @self.app.task(__self__='hello', shared=False)
        def aawsX(x, y):
            pass

        with self.assertRaises(TypeError):
            aawsX.apply_async(())
        with self.assertRaises(TypeError):
            aawsX.apply_async((2,))

        with patch('celery.app.amqp.AMQP.create_task_message') as create:
            with patch('celery.app.amqp.AMQP.send_task_message') as send:
                create.return_value = Mock(), Mock(), Mock(), Mock()
                aawsX.apply_async((4, 5))
                args = create.call_args[0][2]
                self.assertEqual(args, ('hello', 4, 5))
                send.assert_called()

    def test_apply_async_adds_children(self):
        from celery._state import _task_stack

        @self.app.task(bind=True, shared=False)
        def a3cX1(self):
            pass

        @self.app.task(bind=True, shared=False)
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

    def test_pickle_app(self):
        changes = dict(THE_FOO_BAR='bars',
                       THE_MII_MAR='jars')
        self.app.conf.update(changes)
        saved = pickle.dumps(self.app)
        self.assertLess(len(saved), 2048)
        restored = pickle.loads(saved)
        self.assertDictContainsSubset(changes, restored.conf)

    def test_worker_main(self):
        from celery.bin import worker as worker_bin

        class worker(worker_bin.worker):

            def execute_from_commandline(self, argv):
                return argv

        prev, worker_bin.worker = worker_bin.worker, worker
        try:
            ret = self.app.worker_main(argv=['--version'])
            self.assertListEqual(ret, ['--version'])
        finally:
            worker_bin.worker = prev

    def test_config_from_envvar(self):
        os.environ['CELERYTEST_CONFIG_OBJECT'] = 'celery.tests.app.test_app'
        self.app.config_from_envvar('CELERYTEST_CONFIG_OBJECT')
        self.assertEqual(self.app.conf.THIS_IS_A_KEY, 'this is a value')

    def assert_config2(self):
        self.assertTrue(self.app.conf.LEAVE_FOR_WORK)
        self.assertTrue(self.app.conf.MOMENT_TO_STOP)
        self.assertEqual(self.app.conf.CALL_ME_BACK, 123456789)
        self.assertFalse(self.app.conf.WANT_ME_TO)
        self.assertTrue(self.app.conf.UNDERSTAND_ME)

    def test_config_from_object__lazy(self):
        conf = ObjectConfig2()
        self.app.config_from_object(conf)
        self.assertIs(self.app.loader._conf, unconfigured)
        self.assertIs(self.app._config_source, conf)

        self.assert_config2()

    def test_config_from_object__force(self):
        self.app.config_from_object(ObjectConfig2(), force=True)
        self.assertTrue(self.app.loader._conf)

        self.assert_config2()

    def test_config_from_object__compat(self):

        class Config(object):
            CELERY_ALWAYS_EAGER = 44
            CELERY_DEFAULT_DELIVERY_MODE = 30
            CELERY_TASK_PUBLISH_RETRY = False

        self.app.config_from_object(Config)
        self.assertEqual(self.app.conf.task_always_eager, 44)
        self.assertEqual(self.app.conf.CELERY_ALWAYS_EAGER, 44)
        self.assertFalse(self.app.conf.task_publish_retry)
        self.assertEqual(self.app.conf.task_default_routing_key, 'celery')

    def test_config_from_object__supports_old_names(self):

        class Config(object):
            task_always_eager = 45
            task_default_delivery_mode = 301

        self.app.config_from_object(Config())
        self.assertEqual(self.app.conf.CELERY_ALWAYS_EAGER, 45)
        self.assertEqual(self.app.conf.task_always_eager, 45)
        self.assertEqual(self.app.conf.CELERY_DEFAULT_DELIVERY_MODE, 301)
        self.assertEqual(self.app.conf.task_default_delivery_mode, 301)
        self.assertEqual(self.app.conf.task_default_routing_key, 'testcelery')

    def test_config_from_object__namespace_uppercase(self):

        class Config(object):
            CELERY_TASK_ALWAYS_EAGER = 44
            CELERY_TASK_DEFAULT_DELIVERY_MODE = 301

        self.app.config_from_object(Config(), namespace='CELERY')
        self.assertEqual(self.app.conf.task_always_eager, 44)

    def test_config_from_object__namespace_lowercase(self):

        class Config(object):
            celery_task_always_eager = 44
            celery_task_default_delivery_mode = 301

        self.app.config_from_object(Config(), namespace='celery')
        self.assertEqual(self.app.conf.task_always_eager, 44)

    def test_config_from_object__mixing_new_and_old(self):

        class Config(object):
            task_always_eager = 44
            worker_agent = 'foo:Agent'
            worker_consumer = 'foo:Consumer'
            beat_schedule = '/foo/schedule'
            CELERY_DEFAULT_DELIVERY_MODE = 301

        with self.assertRaises(ImproperlyConfigured) as exc:
            self.app.config_from_object(Config(), force=True)
            self.assertTrue(
                exc.args[0].startswith('CELERY_DEFAULT_DELIVERY_MODE'))
            self.assertIn('task_default_delivery_mode', exc.args[0])

    def test_config_from_object__mixing_old_and_new(self):

        class Config(object):
            CELERY_ALWAYS_EAGER = 46
            CELERYD_AGENT = 'foo:Agent'
            CELERYD_CONSUMER = 'foo:Consumer'
            CELERYBEAT_SCHEDULE = '/foo/schedule'
            task_default_delivery_mode = 301

        with self.assertRaises(ImproperlyConfigured) as exc:
            self.app.config_from_object(Config(), force=True)
            self.assertTrue(
                exc.args[0].startswith('task_default_delivery_mode'))
            self.assertIn('CELERY_DEFAULT_DELIVERY_MODE', exc.args[0])

    def test_config_from_cmdline(self):
        cmdline = ['task_always_eager=no',
                   'result_backend=/dev/null',
                   'worker_prefetch_multiplier=368',
                   '.foobarstring=(string)300',
                   '.foobarint=(int)300',
                   'sqlalchemy_engine_options=(dict){"foo": "bar"}']
        self.app.config_from_cmdline(cmdline, namespace='worker')
        self.assertFalse(self.app.conf.task_always_eager)
        self.assertEqual(self.app.conf.result_backend, '/dev/null')
        self.assertEqual(self.app.conf.worker_prefetch_multiplier, 368)
        self.assertEqual(self.app.conf.worker_foobarstring, '300')
        self.assertEqual(self.app.conf.worker_foobarint, 300)
        self.assertDictEqual(self.app.conf.sqlalchemy_engine_options,
                             {'foo': 'bar'})

    def test_setting__broker_transport_options(self):

        _args = {'foo': 'bar', 'spam': 'baz'}

        self.app.config_from_object(Bunch())
        self.assertEqual(self.app.conf.broker_transport_options, {})

        self.app.config_from_object(Bunch(broker_transport_options=_args))
        self.assertEqual(self.app.conf.broker_transport_options, _args)

    def test_Windows_log_color_disabled(self):
        self.app.IS_WINDOWS = True
        self.assertFalse(self.app.log.supports_color(True))

    def test_WorkController(self):
        x = self.app.WorkController
        self.assertIs(x.app, self.app)

    def test_Worker(self):
        x = self.app.Worker
        self.assertIs(x.app, self.app)

    @depends_on_current_app
    def test_AsyncResult(self):
        x = self.app.AsyncResult('1')
        self.assertIs(x.app, self.app)
        r = loads(dumps(x))
        # not set as current, so ends up as default app after reduce
        self.assertIs(r.app, current_app._get_current_object())

    def test_get_active_apps(self):
        self.assertTrue(list(_state._get_active_apps()))

        app1 = self.Celery()
        appid = id(app1)
        self.assertIn(app1, _state._get_active_apps())
        app1.close()
        del(app1)

        gc.collect()

        # weakref removed from list when app goes out of scope.
        with self.assertRaises(StopIteration):
            next(app for app in _state._get_active_apps() if id(app) == appid)

    def test_config_from_envvar_more(self, key='CELERY_HARNESS_CFG1'):
        self.assertFalse(
            self.app.config_from_envvar(
                'HDSAJIHWIQHEWQU', force=True, silent=True),
        )
        with self.assertRaises(ImproperlyConfigured):
            self.app.config_from_envvar(
                'HDSAJIHWIQHEWQU', force=True, silent=False,
            )
        os.environ[key] = __name__ + '.object_config'
        self.assertTrue(self.app.config_from_envvar(key, force=True))
        self.assertEqual(self.app.conf['FOO'], 1)
        self.assertEqual(self.app.conf['BAR'], 2)

        os.environ[key] = 'unknown_asdwqe.asdwqewqe'
        with self.assertRaises(ImportError):
            self.app.config_from_envvar(key, silent=False)
        self.assertFalse(
            self.app.config_from_envvar(key, force=True, silent=True),
        )

        os.environ[key] = __name__ + '.dict_config'
        self.assertTrue(self.app.config_from_envvar(key, force=True))
        self.assertEqual(self.app.conf['FOO'], 10)
        self.assertEqual(self.app.conf['BAR'], 20)

    @patch('celery.bin.celery.CeleryCommand.execute_from_commandline')
    def test_start(self, execute):
        self.app.start()
        execute.assert_called()

    def test_amqp_get_broker_info(self):
        self.assertDictContainsSubset(
            {'hostname': 'localhost',
             'userid': 'guest',
             'password': 'guest',
             'virtual_host': '/'},
            self.app.connection('pyamqp://').info(),
        )
        self.app.conf.broker_port = 1978
        self.app.conf.broker_vhost = 'foo'
        self.assertDictContainsSubset(
            {'port': 1978, 'virtual_host': 'foo'},
            self.app.connection('pyamqp://:1978/foo').info(),
        )
        conn = self.app.connection('pyamqp:////value')
        self.assertDictContainsSubset({'virtual_host': '/value'},
                                      conn.info())

    def test_amqp_failover_strategy_selection(self):
        # Test passing in a string and make sure the string
        # gets there untouched
        self.app.conf.broker_failover_strategy = 'foo-bar'
        self.assertEqual(
            self.app.connection('amqp:////value').failover_strategy,
            'foo-bar',
        )

        # Try passing in None
        self.app.conf.broker_failover_strategy = None
        self.assertEqual(
            self.app.connection('amqp:////value').failover_strategy,
            itertools.cycle,
        )

        # Test passing in a method
        def my_failover_strategy(it):
            yield True

        self.app.conf.broker_failover_strategy = my_failover_strategy
        self.assertEqual(
            self.app.connection('amqp:////value').failover_strategy,
            my_failover_strategy,
        )

    def test_after_fork(self):
        self.app._pool = Mock()
        self.app.on_after_fork = Mock(name='on_after_fork')
        self.app._after_fork()
        self.assertIsNone(self.app._pool)
        self.app.on_after_fork.send.assert_called_with(sender=self.app)
        self.app._after_fork()

    def test_global_after_fork(self):
        self.app._after_fork = Mock(name='_after_fork')
        _appbase._after_fork_cleanup_app(self.app)
        self.app._after_fork.assert_called_with()

    @patch('celery.app.base.logger')
    def test_after_fork_cleanup_app__raises(self, logger):
        self.app._after_fork = Mock(name='_after_fork')
        exc = self.app._after_fork.side_effect = KeyError()
        _appbase._after_fork_cleanup_app(self.app)
        logger.info.assert_called_with(
            'after forker raised exception: %r', exc, exc_info=1)

    def test_ensure_after_fork__no_multiprocessing(self):
        prev, _appbase.register_after_fork = (
            _appbase.register_after_fork, None)
        try:
            self.app._after_fork_registered = False
            self.app._ensure_after_fork()
            self.assertTrue(self.app._after_fork_registered)
        finally:
            _appbase.register_after_fork = prev

    def test_canvas(self):
        self.assertTrue(self.app.canvas.Signature)

    def test_signature(self):
        sig = self.app.signature('foo', (1, 2))
        self.assertIs(sig.app, self.app)

    def test_timezone__none_set(self):
        self.app.conf.timezone = None
        tz = self.app.timezone
        self.assertEqual(tz, timezone.get_timezone('UTC'))

    def test_compat_on_configure(self):
        _on_configure = Mock(name='on_configure')

        class CompatApp(Celery):

            def on_configure(self, *args, **kwargs):
                # on pypy3 if named on_configure the class function
                # will be called, instead of the mock defined above,
                # so we add the underscore.
                _on_configure(*args, **kwargs)

        with CompatApp(set_as_current=False) as app:
            app.loader = Mock()
            app.loader.conf = {}
            app._load_config()
            _on_configure.assert_called_with()

    def test_add_periodic_task(self):

        @self.app.task
        def add(x, y):
            pass
        assert not self.app.configured
        self.app.add_periodic_task(
            10, self.app.signature('add', (2, 2)),
            name='add1', expires=3,
        )
        self.assertTrue(self.app._pending_periodic_tasks)
        assert not self.app.configured

        sig2 = add.s(4, 4)
        self.assertTrue(self.app.configured)
        self.app.add_periodic_task(20, sig2, name='add2', expires=4)
        self.assertIn('add1', self.app.conf.beat_schedule)
        self.assertIn('add2', self.app.conf.beat_schedule)

    def test_pool_no_multiprocessing(self):
        with mock.mask_modules('multiprocessing.util'):
            pool = self.app.pool
            self.assertIs(pool, self.app._pool)

    def test_bugreport(self):
        self.assertTrue(self.app.bugreport())

    def test_send_task__connection_provided(self):
        connection = Mock(name='connection')
        router = Mock(name='router')
        router.route.return_value = {}
        self.app.amqp = Mock(name='amqp')
        self.app.amqp.Producer.attach_mock(ContextMock(), 'return_value')
        self.app.send_task('foo', (1, 2), connection=connection, router=router)
        self.app.amqp.Producer.assert_called_with(connection)
        self.app.amqp.send_task_message.assert_called_with(
            self.app.amqp.Producer(), 'foo',
            self.app.amqp.create_task_message())

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

        message = self.app.amqp.create_task_message(
            'id', 'footask', (), {}, create_sent_event=True,
        )

        prod = self.app.amqp.Producer(conn)
        dispatcher = Dispatcher()
        self.app.amqp.send_task_message(
            prod, 'footask', message,
            exchange='moo_exchange', routing_key='moo_exchange',
            event_dispatcher=dispatcher,
        )
        self.assertTrue(dispatcher.sent)
        self.assertEqual(dispatcher.sent[0][0], 'task-sent')
        self.app.amqp.send_task_message(
            prod, 'footask', message, event_dispatcher=dispatcher,
            exchange='bar_exchange', routing_key='bar_exchange',
        )

    def test_select_queues(self):
        self.app.amqp = Mock(name='amqp')
        self.app.select_queues({'foo', 'bar'})
        self.app.amqp.queues.select.assert_called_with({'foo', 'bar'})


class test_defaults(AppCase):

    def test_strtobool(self):
        for s in ('false', 'no', '0'):
            self.assertFalse(defaults.strtobool(s))
        for s in ('true', 'yes', '1'):
            self.assertTrue(defaults.strtobool(s))
        with self.assertRaises(TypeError):
            defaults.strtobool('unsure')


class test_debugging_utils(AppCase):

    def test_enable_disable_trace(self):
        try:
            _app.enable_trace()
            self.assertEqual(_app.app_or_default, _app._app_or_default_trace)
            _app.disable_trace()
            self.assertEqual(_app.app_or_default, _app._app_or_default)
        finally:
            _app.disable_trace()


class test_pyimplementation(AppCase):

    def test_platform_python_implementation(self):
        with mock.platform_pyimp(lambda: 'Xython'):
            self.assertEqual(pyimplementation(), 'Xython')

    def test_platform_jython(self):
        with mock.platform_pyimp():
            with mock.sys_platform('java 1.6.51'):
                self.assertIn('Jython', pyimplementation())

    def test_platform_pypy(self):
        with mock.platform_pyimp():
            with mock.sys_platform('darwin'):
                with mock.pypy_version((1, 4, 3)):
                    self.assertIn('PyPy', pyimplementation())
                with mock.pypy_version((1, 4, 3, 'a4')):
                    self.assertIn('PyPy', pyimplementation())

    def test_platform_fallback(self):
        with mock.platform_pyimp():
            with mock.sys_platform('darwin'):
                with mock.pypy_version():
                    self.assertEqual('CPython', pyimplementation())


class test_shared_task(AppCase):

    def test_registers_to_all_apps(self):
        with self.Celery('xproj', set_as_current=True) as xproj:
            xproj.finalize()

            @shared_task
            def foo():
                return 42

            @shared_task()
            def bar():
                return 84

            self.assertIs(foo.app, xproj)
            self.assertIs(bar.app, xproj)
            self.assertTrue(foo._get_current_object())

            with self.Celery('yproj', set_as_current=True) as yproj:
                self.assertIs(foo.app, yproj)
                self.assertIs(bar.app, yproj)

                @shared_task()
                def baz():
                    return 168

                self.assertIs(baz.app, yproj)
