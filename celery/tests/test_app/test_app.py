import os

from celery import Celery
from celery.app import defaults
from celery.app.base import BaseApp
from celery.loaders.base import BaseLoader
from celery.utils.serialization import pickle

from celery.tests import config
from celery.tests.utils import unittest

THIS_IS_A_KEY = "this is a value"


class Object(object):

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def _get_test_config():
    return dict((key, getattr(config, key))
                    for key in dir(config)
                        if key.isupper() and not key.startswith("_"))

test_config = _get_test_config()


class test_App(unittest.TestCase):

    def setUp(self):
        self.app = Celery(set_as_current=False)
        self.app.conf.update(test_config)

    def test_task(self):
        app = Celery("foozibari", set_as_current=False)

        def fun():
            pass

        fun.__module__ = "__main__"
        task = app.task(fun)
        self.assertEqual(task.name, app.main + ".fun")

    def test_TaskSet(self):
        ts = self.app.TaskSet()
        self.assertListEqual(ts.tasks, [])
        self.assertIs(ts.app, self.app)

    def test_pickle_app(self):
        changes = dict(THE_FOO_BAR="bars",
                       THE_MII_MAR="jars")
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
            ret = self.app.worker_main(argv=["--version"])
            self.assertListEqual(ret, ["--version"])
        finally:
            celeryd.WorkerCommand = prev

    def test_config_from_envvar(self):
        os.environ["CELERYTEST_CONFIG_OBJECT"] = \
                "celery.tests.test_app.test_app"
        self.app.config_from_envvar("CELERYTEST_CONFIG_OBJECT")
        self.assertEqual(self.app.conf.THIS_IS_A_KEY, "this is a value")

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
        cmdline = [".always_eager=no",
                   ".result_backend=/dev/null",
                   '.task_error_whitelist=(list)["a", "b", "c"]',
                   "celeryd.prefetch_multiplier=368",
                   ".foobarstring=(string)300",
                   ".foobarint=(int)300",
                   '.result_engine_options=(dict){"foo": "bar"}']
        self.app.config_from_cmdline(cmdline, namespace="celery")
        self.assertFalse(self.app.conf.CELERY_ALWAYS_EAGER)
        self.assertEqual(self.app.conf.CELERY_RESULT_BACKEND, "/dev/null")
        self.assertEqual(self.app.conf.CELERYD_PREFETCH_MULTIPLIER, 368)
        self.assertListEqual(self.app.conf.CELERY_TASK_ERROR_WHITELIST,
                             ["a", "b", "c"])
        self.assertEqual(self.app.conf.CELERY_FOOBARSTRING, "300")
        self.assertEqual(self.app.conf.CELERY_FOOBARINT, 300)
        self.assertDictEqual(self.app.conf.CELERY_RESULT_ENGINE_OPTIONS,
                             {"foo": "bar"})

    def test_compat_setting_CELERY_BACKEND(self):

        self.app.config_from_object(Object(CELERY_BACKEND="set_by_us"))
        self.assertEqual(self.app.conf.CELERY_RESULT_BACKEND, "set_by_us")

    def test_Windows_log_color_disabled(self):
        self.app.IS_WINDOWS = True
        self.assertFalse(self.app.log.supports_color())

    def test_compat_setting_CARROT_BACKEND(self):
        self.app.config_from_object(Object(CARROT_BACKEND="set_by_us"))
        self.assertEqual(self.app.conf.BROKER_BACKEND, "set_by_us")

    def test_mail_admins(self):

        class Loader(BaseLoader):

            def mail_admins(*args, **kwargs):
                return args, kwargs

        self.app.loader = Loader()
        self.app.conf.ADMINS = None
        self.assertFalse(self.app.mail_admins("Subject", "Body"))
        self.app.conf.ADMINS = [("George Costanza", "george@vandelay.com")]
        self.assertTrue(self.app.mail_admins("Subject", "Body"))

    def test_amqp_get_broker_info(self):
        self.assertDictContainsSubset({"hostname": "localhost",
                                       "userid": "guest",
                                       "password": "guest",
                                       "virtual_host": "/"},
                                      self.app.broker_connection().info())
        self.app.conf.BROKER_PORT = 1978
        self.app.conf.BROKER_VHOST = "foo"
        self.assertDictContainsSubset({"port": 1978,
                                       "virtual_host": "foo"},
                                      self.app.broker_connection().info())
        conn = self.app.broker_connection(virtual_host="/value")
        self.assertDictContainsSubset({"virtual_host": "/value"},
                                      conn.info())

    def test_send_task_sent_event(self):
        from celery.app import amqp

        class Dispatcher(object):
            sent = []

            def send(self, type, **fields):
                self.sent.append((type, fields))

        conn = self.app.broker_connection()
        chan = conn.channel()
        try:
            for e in ("foo_exchange", "moo_exchange", "bar_exchange"):
                chan.exchange_declare(e, "direct", durable=True)
                chan.queue_declare(e, durable=True)
                chan.queue_bind(e, e, e)
        finally:
            chan.close()
        assert conn.transport_cls == "memory"

        pub = self.app.amqp.TaskPublisher(conn, exchange="foo_exchange")
        self.assertIn("foo_exchange", amqp._exchanges_declared)

        dispatcher = Dispatcher()
        self.assertTrue(pub.delay_task("footask", (), {},
                                       exchange="moo_exchange",
                                       routing_key="moo_exchange",
                                       event_dispatcher=dispatcher))
        self.assertTrue(dispatcher.sent)
        self.assertEqual(dispatcher.sent[0][0], "task-sent")
        self.assertTrue(pub.delay_task("footask", (), {},
                                       event_dispatcher=dispatcher,
                                       exchange="bar_exchange",
                                       routing_key="bar_exchange"))
        self.assertIn("bar_exchange", amqp._exchanges_declared)


class test_BaseApp(unittest.TestCase):

    def test_on_init(self):
        BaseApp()


class test_defaults(unittest.TestCase):

    def test_str_to_bool(self):
        for s in ("false", "no", "0"):
            self.assertFalse(defaults.str_to_bool(s))
        for s in ("true", "yes", "1"):
            self.assertTrue(defaults.str_to_bool(s))
        self.assertRaises(TypeError, defaults.str_to_bool, "unsure")
