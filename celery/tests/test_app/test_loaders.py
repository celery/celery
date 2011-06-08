import os
import sys

from celery import task
from celery import loaders
from celery.app import app_or_default
from celery.exceptions import ImproperlyConfigured
from celery.loaders import base
from celery.loaders import default
from celery.loaders.app import AppLoader

from celery.tests.compat import catch_warnings
from celery.tests.utils import unittest
from celery.tests.utils import with_environ, execute_context

FOO = 1
BAR = 2


class Object(object):

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class MockMail(object):

    class SendmailWarning(UserWarning):
        pass

    class Message(Object):
        pass

    class Mailer(Object):
        sent = []
        raise_on_send = False

        def send(self, message):
            if self.__class__.raise_on_send:
                raise KeyError("foo")
            self.sent.append(message)


class DummyLoader(base.BaseLoader):

    def read_configuration(self):
        return {"foo": "bar", "CELERY_IMPORTS": ("os", "sys")}

    @property
    def mail(self):
        return MockMail()


class TestLoaders(unittest.TestCase):

    def test_get_loader_cls(self):

        self.assertEqual(loaders.get_loader_cls("default"),
                          default.Loader)

    def test_current_loader(self):
        loader1 = loaders.current_loader()
        loader2 = loaders.current_loader()
        self.assertIs(loader1, loader2)
        self.assertIs(loader2, loaders._loader)

    def test_load_settings(self):
        loader = loaders.current_loader()
        loaders._settings = None
        settings = loaders.load_settings()
        self.assertTrue(loaders._settings)
        settings = loaders.load_settings()
        self.assertIs(settings, loaders._settings)
        self.assertIs(settings, loader.conf)

    @with_environ("CELERY_LOADER", "default")
    def test_detect_loader_CELERY_LOADER(self):
        self.assertIsInstance(loaders.setup_loader(), default.Loader)


class TestLoaderBase(unittest.TestCase):
    message_options = {"subject": "Subject",
                       "body": "Body",
                       "sender": "x@x.com",
                       "to": "y@x.com"}
    server_options = {"host": "smtp.x.com",
                      "port": 1234,
                      "user": "x",
                      "password": "qwerty",
                      "timeout": 3}

    def setUp(self):
        self.loader = DummyLoader()

    def test_handlers_pass(self):
        self.loader.on_task_init("foo.task", "feedface-cafebabe")
        self.loader.on_worker_init()

    def test_import_task_module(self):
        self.assertEqual(sys, self.loader.import_task_module("sys"))

    def test_conf_property(self):
        self.assertEqual(self.loader.conf["foo"], "bar")
        self.assertEqual(self.loader._conf["foo"], "bar")
        self.assertEqual(self.loader.conf["foo"], "bar")

    def test_import_default_modules(self):
        self.assertItemsEqual(self.loader.import_default_modules(),
                              [os, sys, task])

    def test_import_from_cwd_custom_imp(self):

        def imp(module):
            imp.called = True
        imp.called = False

        self.loader.import_from_cwd("foo", imp=imp)
        self.assertTrue(imp.called)

    def test_mail_admins_errors(self):
        MockMail.Mailer.raise_on_send = True
        opts = dict(self.message_options, **self.server_options)

        def with_catch_warnings(log):
            self.loader.mail_admins(fail_silently=True, **opts)
            return log[0].message

        warning = execute_context(catch_warnings(record=True),
                                  with_catch_warnings)
        self.assertIsInstance(warning, MockMail.SendmailWarning)
        self.assertIn("KeyError", warning.args[0])

        self.assertRaises(KeyError, self.loader.mail_admins,
                          fail_silently=False, **opts)

    def test_mail_admins(self):
        MockMail.Mailer.raise_on_send = False
        opts = dict(self.message_options, **self.server_options)

        self.loader.mail_admins(**opts)
        message = MockMail.Mailer.sent.pop()
        self.assertDictContainsSubset(vars(message), self.message_options)

    def test_mail_attribute(self):
        from celery.utils import mail
        loader = base.BaseLoader()
        self.assertIs(loader.mail, mail)

    def test_cmdline_config_ValueError(self):
        self.assertRaises(ValueError, self.loader.cmdline_config_parser,
                         ["broker.port=foobar"])


class TestDefaultLoader(unittest.TestCase):

    def test_wanted_module_item(self):
        self.assertTrue(default.wanted_module_item("FOO"))
        self.assertTrue(default.wanted_module_item("Foo"))
        self.assertFalse(default.wanted_module_item("_FOO"))
        self.assertFalse(default.wanted_module_item("__FOO"))
        self.assertFalse(default.wanted_module_item("foo"))

    def test_read_configuration(self):
        from types import ModuleType

        class ConfigModule(ModuleType):
            pass

        celeryconfig = ConfigModule("celeryconfig")
        celeryconfig.CELERY_IMPORTS = ("os", "sys")
        configname = os.environ.get("CELERY_CONFIG_MODULE") or "celeryconfig"

        prevconfig = sys.modules[configname]
        sys.modules[configname] = celeryconfig
        try:
            l = default.Loader()
            settings = l.read_configuration()
            self.assertTupleEqual(settings.CELERY_IMPORTS, ("os", "sys"))
            settings = l.read_configuration()
            self.assertTupleEqual(settings.CELERY_IMPORTS, ("os", "sys"))
            l.on_worker_init()
        finally:
            sys.modules[configname] = prevconfig

    def test_import_from_cwd(self):
        l = default.Loader()
        old_path = list(sys.path)
        try:
            sys.path.remove(os.getcwd())
        except ValueError:
            pass
        celery = sys.modules.pop("celery", None)
        try:
            self.assertTrue(l.import_from_cwd("celery"))
            sys.modules.pop("celery", None)
            sys.path.insert(0, os.getcwd())
            self.assertTrue(l.import_from_cwd("celery"))
        finally:
            sys.path = old_path
            sys.modules["celery"] = celery

    def test_unconfigured_settings(self):
        context_executed = [False]

        class _Loader(default.Loader):

            def import_from_cwd(self, name):
                raise ImportError(name)

        def with_catch_warnings(log):
            l = _Loader()
            self.assertEqual(l.conf.CELERY_RESULT_BACKEND, "amqp")
            context_executed[0] = True

        context = catch_warnings(record=True)
        execute_context(context, with_catch_warnings)
        self.assertTrue(context_executed[0])


class test_AppLoader(unittest.TestCase):

    def setUp(self):
        self.app = app_or_default()
        self.loader = AppLoader(app=self.app)

    def test_config_from_envvar(self, key="CELERY_HARNESS_CFG1"):
        self.assertFalse(self.loader.config_from_envvar("HDSAJIHWIQHEWQU",
                                                        silent=True))
        self.assertRaises(ImproperlyConfigured,
                          self.loader.config_from_envvar, "HDSAJIHWIQHEWQU",
                          silent=False)
        os.environ[key] = __name__
        print(os.environ[key])
        self.assertTrue(self.loader.config_from_envvar(key))
        self.assertEqual(self.loader.conf["FOO"], 1)
        self.assertEqual(self.loader.conf["BAR"], 2)

        os.environ[key] = "unknown_asdwqe.asdwqewqe"
        self.assertRaises(ImportError,
                          self.loader.config_from_envvar, key, silent=False)
        self.assertFalse(self.loader.config_from_envvar(key, silent=True))

    def test_on_worker_init(self):
        self.loader.conf["CELERY_IMPORTS"] = ("subprocess", )
        sys.modules.pop("subprocess", None)
        self.loader.on_worker_init()
        self.assertIn("subprocess", sys.modules)
