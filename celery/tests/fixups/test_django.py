from __future__ import absolute_import, unicode_literals

import os

from contextlib import contextmanager

from celery.fixups.django import (
    _maybe_close_fd,
    fixup,
    DjangoFixup,
    DjangoWorkerFixup,
)
from celery.utils.objects import Bunch

from celery.tests.case import AppCase, Mock, mock, patch


class FixupCase(AppCase):
    Fixup = None

    @contextmanager
    def fixup_context(self, app):
        with patch('celery.fixups.django.DjangoWorkerFixup.validate_models'):
            with patch('celery.fixups.django.symbol_by_name') as symbyname:
                with patch('celery.fixups.django.import_module') as impmod:
                    f = self.Fixup(app)
                    yield f, impmod, symbyname


class test_DjangoFixup(FixupCase):
    Fixup = DjangoFixup

    def test_setting_default_app(self):
        from celery.fixups import django
        prev, django.default_app = django.default_app, None
        try:
            app = Mock(name='app')
            DjangoFixup(app)
            app.set_default.assert_called_with()
        finally:
            django.default_app = prev

    @patch('celery.fixups.django.DjangoWorkerFixup')
    def test_worker_fixup_property(self, DjangoWorkerFixup):
        f = DjangoFixup(self.app)
        f._worker_fixup = None
        self.assertIs(f.worker_fixup, DjangoWorkerFixup())
        self.assertIs(f.worker_fixup, DjangoWorkerFixup())

    def test_on_import_modules(self):
        f = DjangoFixup(self.app)
        f.worker_fixup = Mock(name='worker_fixup')
        f.on_import_modules()
        f.worker_fixup.validate_models.assert_called_with()

    def test_autodiscover_tasks_pre17(self):
        self.mask_modules('django.apps')
        f = DjangoFixup(self.app)
        f._settings = Mock(name='_settings')
        self.assertIs(f.autodiscover_tasks(), f._settings.INSTALLED_APPS)

    def test_autodiscover_tasks(self):
        self.mock_modules('django.apps')
        from django.apps import apps
        f = DjangoFixup(self.app)
        configs = [Mock(name='c1'), Mock(name='c2')]
        apps.get_app_configs.return_value = configs
        self.assertEqual(
            f.autodiscover_tasks(),
            [c.name for c in configs],
        )

    def test_fixup(self):
        with patch('celery.fixups.django.DjangoFixup') as Fixup:
            with patch.dict(os.environ, DJANGO_SETTINGS_MODULE=''):
                fixup(self.app)
                Fixup.assert_not_called()
            with patch.dict(os.environ, DJANGO_SETTINGS_MODULE='settings'):
                with mock.mask_modules('django'):
                    with self.assertWarnsRegex(UserWarning, 'but Django is'):
                        fixup(self.app)
                    Fixup.assert_not_called()
                with mock.module_exists('django'):
                    fixup(self.app)
                    Fixup.assert_called()

    def test_maybe_close_fd(self):
        with patch('os.close'):
            _maybe_close_fd(Mock())
            _maybe_close_fd(object())

    def test_init(self):
        with self.fixup_context(self.app) as (f, importmod, sym):
            self.assertTrue(f)

            def se(name):
                if name == 'django.utils.timezone:now':
                    raise ImportError()
                return Mock()
            sym.side_effect = se
            self.assertTrue(self.Fixup(self.app)._now)

    def test_install(self):
        self.app.loader = Mock()
        self.cw = self.patch('os.getcwd')
        self.p = self.patch('sys.path')
        self.sigs = self.patch('celery.fixups.django.signals')
        with self.fixup_context(self.app) as (f, _, _):
            self.cw.return_value = '/opt/vandelay'
            f.install()
            self.sigs.worker_init.connect.assert_called_with(f.on_worker_init)
            self.assertEqual(self.app.loader.now, f.now)
            self.p.append.assert_called_with('/opt/vandelay')

    def test_now(self):
        with self.fixup_context(self.app) as (f, _, _):
            self.assertTrue(f.now(utc=True))
            f._now.assert_not_called()
            self.assertTrue(f.now(utc=False))
            f._now.assert_called()

    def test_on_worker_init(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch('celery.fixups.django.DjangoWorkerFixup') as DWF:
                f.on_worker_init()
                DWF.assert_called_with(f.app)
                DWF.return_value.install.assert_called_with()
                self.assertIs(f._worker_fixup, DWF.return_value)


class test_DjangoWorkerFixup(FixupCase):
    Fixup = DjangoWorkerFixup

    def test_init(self):
        with self.fixup_context(self.app) as (f, importmod, sym):
            self.assertTrue(f)

            def se(name):
                if name == 'django.db:close_old_connections':
                    raise ImportError()
                return Mock()
            sym.side_effect = se
            self.assertIsNone(self.Fixup(self.app)._close_old_connections)

    def test_install(self):
        self.app.conf = {'CELERY_DB_REUSE_MAX': None}
        self.app.loader = Mock()
        with self.fixup_context(self.app) as (f, _, _):
            with patch('celery.fixups.django.signals') as sigs:
                f.install()
                sigs.beat_embedded_init.connect.assert_called_with(
                    f.close_database,
                )
                sigs.worker_ready.connect.assert_called_with(f.on_worker_ready)
                sigs.task_prerun.connect.assert_called_with(f.on_task_prerun)
                sigs.task_postrun.connect.assert_called_with(f.on_task_postrun)
                sigs.worker_process_init.connect.assert_called_with(
                    f.on_worker_process_init,
                )

    def test_on_worker_process_init(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch('celery.fixups.django._maybe_close_fd') as mcf:
                    _all = f._db.connections.all = Mock()
                    conns = _all.return_value = [
                        Mock(), Mock(),
                    ]
                    conns[0].connection = None
                    with patch.object(f, 'close_cache'):
                        with patch.object(f, '_close_database'):
                            f.on_worker_process_init()
                            mcf.assert_called_with(conns[1].connection)
                            f.close_cache.assert_called_with()
                            f._close_database.assert_called_with()

                            mcf.reset_mock()
                            _all.side_effect = AttributeError()
                            f.on_worker_process_init()
                            mcf.assert_called_with(f._db.connection.connection)
                            f._db.connection = None
                            f.on_worker_process_init()

                            f.validate_models = Mock(name='validate_models')
                            self.mock_environ('FORKED_BY_MULTIPROCESSING', '1')
                            f.on_worker_process_init()
                            f.validate_models.assert_called_with()

    def test_on_task_prerun(self):
        task = Mock()
        with self.fixup_context(self.app) as (f, _, _):
            task.request.is_eager = False
            with patch.object(f, 'close_database'):
                f.on_task_prerun(task)
                f.close_database.assert_called_with()

            task.request.is_eager = True
            with patch.object(f, 'close_database'):
                f.on_task_prerun(task)
                f.close_database.assert_not_called()

    def test_on_task_postrun(self):
        task = Mock()
        with self.fixup_context(self.app) as (f, _, _):
            with patch.object(f, 'close_cache'):
                task.request.is_eager = False
                with patch.object(f, 'close_database'):
                    f.on_task_postrun(task)
                    f.close_database.assert_called()
                    f.close_cache.assert_called()

            # when a task is eager, do not close connections
            with patch.object(f, 'close_cache'):
                task.request.is_eager = True
                with patch.object(f, 'close_database'):
                    f.on_task_postrun(task)
                    f.close_database.assert_not_called()
                    f.close_cache.assert_not_called()

    def test_close_database(self):
        with self.fixup_context(self.app) as (f, _, _):
            f._close_old_connections = Mock()
            f.close_database()
            f._close_old_connections.assert_called_with()
            f._close_old_connections = None
            with patch.object(f, '_close_database') as _close:
                f.db_reuse_max = None
                f.close_database()
                _close.assert_called_with()
                _close.reset_mock()

                f.db_reuse_max = 10
                f._db_recycles = 3
                f.close_database()
                _close.assert_not_called()
                self.assertEqual(f._db_recycles, 4)
                _close.reset_mock()

                f._db_recycles = 20
                f.close_database()
                _close.assert_called_with()
                self.assertEqual(f._db_recycles, 1)

    def test_close_database__django16(self):
        with self.fixup_context(self.app) as (f, _, _):
            f._db.connections = Mock(name='db.connections')
            f._db.connections.all.side_effect = AttributeError()
            f._close_database()
            f._db.close_old_connections.assert_called_with()

    def test__close_database(self):
        with self.fixup_context(self.app) as (f, _, _):
            conns = [Mock(), Mock(), Mock()]
            conns[1].close.side_effect = KeyError('already closed')
            f.database_errors = (KeyError,)
            f.interface_errors = ()

            f._db.connections = Mock()  # ConnectionHandler
            f._db.connections.all.side_effect = lambda: conns

            f._close_database()
            conns[0].close.assert_called_with()
            conns[1].close.assert_called_with()
            conns[2].close.assert_called_with()

            conns[1].close.side_effect = KeyError('omg')
            with self.assertRaises(KeyError):
                f._close_database()

            o = Bunch(close_connection=Mock())
            f._db = o
            f._close_database()
            o.close_connection.assert_called_with()

    def test_close_cache(self):
        with self.fixup_context(self.app) as (f, _, _):
            f.close_cache()
            f._cache.cache.close.assert_called_with()
            f._cache.cache.close.side_effect = TypeError()
            f.close_cache()

    def test_on_worker_ready(self):
        with self.fixup_context(self.app) as (f, _, _):
            f._settings.DEBUG = False
            f.on_worker_ready()
            with self.assertWarnsRegex(UserWarning, r'leads to a memory leak'):
                f._settings.DEBUG = True
                f.on_worker_ready()

    def test_validate_models(self):
        self.patch('celery.fixups.django.symbol_by_name')
        self.patch('celery.fixups.django.import_module')
        f = self.Fixup(self.app)
        self.mock_modules('django.core.management.validation')
        f.django_setup = Mock(name='django.setup')
        from django.core.management.validation import get_validation_errors
        get_validation_errors.return_value = 0
        f.validate_models()
        f.django_setup.assert_called_with()
        get_validation_errors.return_value = 3
        with self.assertRaises(RuntimeError):
            f.validate_models()

        self.mask_modules('django.core.management.validation')
        f._validate_models_django17 = Mock('validate17')
        f.validate_models()
        f._validate_models_django17.assert_called_with()

    def test_validate_models_django17(self):
        self.patch('celery.fixups.django.symbol_by_name')
        self.patch('celery.fixups.django.import_module')
        self.mock_modules('django.core.management.base')
        from django.core.management import base
        f = self.Fixup(self.app)
        f._validate_models_django17()
        base.BaseCommand.assert_called_with()
        base.BaseCommand().check.assert_called_with()

    def test_django_setup(self):
        self.patch('celery.fixups.django.symbol_by_name')
        self.patch('celery.fixups.django.import_module')
        django, = self.mock_modules('django')
        f = self.Fixup(self.app)
        f.django_setup()
        django.setup.assert_called_with()

    def test_mysql_errors(self):
        with mock.module_exists('MySQLdb'):
            import MySQLdb as mod
            mod.DatabaseError = Mock()
            mod.InterfaceError = Mock()
            mod.OperationalError = Mock()
            with self.fixup_context(self.app) as (f, _, _):
                self.assertIn(mod.DatabaseError, f.database_errors)
                self.assertIn(mod.InterfaceError, f.database_errors)
                self.assertIn(mod.OperationalError, f.database_errors)
        with mock.mask_modules('MySQLdb'):
            with self.fixup_context(self.app):
                pass

    def test_pg_errors(self):
        with mock.module_exists('psycopg2'):
            import psycopg2 as mod
            mod.DatabaseError = Mock()
            mod.InterfaceError = Mock()
            mod.OperationalError = Mock()
            with self.fixup_context(self.app) as (f, _, _):
                self.assertIn(mod.DatabaseError, f.database_errors)
                self.assertIn(mod.InterfaceError, f.database_errors)
                self.assertIn(mod.OperationalError, f.database_errors)
        with mock.mask_modules('psycopg2'):
            with self.fixup_context(self.app):
                pass

    def test_sqlite_errors(self):
        with mock.module_exists('sqlite3'):
            import sqlite3 as mod
            mod.DatabaseError = Mock()
            mod.InterfaceError = Mock()
            mod.OperationalError = Mock()
            with self.fixup_context(self.app) as (f, _, _):
                self.assertIn(mod.DatabaseError, f.database_errors)
                self.assertIn(mod.InterfaceError, f.database_errors)
                self.assertIn(mod.OperationalError, f.database_errors)
        with mock.mask_modules('sqlite3'):
            with self.fixup_context(self.app):
                pass

    def test_oracle_errors(self):
        with mock.module_exists('cx_Oracle'):
            import cx_Oracle as mod
            mod.DatabaseError = Mock()
            mod.InterfaceError = Mock()
            mod.OperationalError = Mock()
            with self.fixup_context(self.app) as (f, _, _):
                self.assertIn(mod.DatabaseError, f.database_errors)
                self.assertIn(mod.InterfaceError, f.database_errors)
                self.assertIn(mod.OperationalError, f.database_errors)
        with mock.mask_modules('cx_Oracle'):
            with self.fixup_context(self.app):
                pass
