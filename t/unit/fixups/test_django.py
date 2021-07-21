from contextlib import contextmanager
from unittest.mock import Mock, patch

import pytest
from case import mock

from celery.fixups.django import (DjangoFixup, DjangoWorkerFixup,
                                  FixupWarning, _maybe_close_fd, fixup)


class FixupCase:
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
        from celery import _state
        prev, _state.default_app = _state.default_app, None
        try:
            app = Mock(name='app')
            DjangoFixup(app)
            app.set_default.assert_called_with()
        finally:
            _state.default_app = prev

    @patch('celery.fixups.django.DjangoWorkerFixup')
    def test_worker_fixup_property(self, DjangoWorkerFixup):
        f = DjangoFixup(self.app)
        f._worker_fixup = None
        assert f.worker_fixup is DjangoWorkerFixup()
        assert f.worker_fixup is DjangoWorkerFixup()

    def test_on_import_modules(self):
        f = DjangoFixup(self.app)
        f.worker_fixup = Mock(name='worker_fixup')
        f.on_import_modules()
        f.worker_fixup.validate_models.assert_called_with()

    def test_autodiscover_tasks(self, patching):
        patching.modules('django.apps')
        from django.apps import apps
        f = DjangoFixup(self.app)
        configs = [Mock(name='c1'), Mock(name='c2')]
        apps.get_app_configs.return_value = configs
        assert f.autodiscover_tasks() == [c.name for c in configs]

    def test_fixup(self, patching):
        with patch('celery.fixups.django.DjangoFixup') as Fixup:
            patching.setenv('DJANGO_SETTINGS_MODULE', '')
            fixup(self.app)
            Fixup.assert_not_called()

            patching.setenv('DJANGO_SETTINGS_MODULE', 'settings')
            with mock.mask_modules('django'):
                with pytest.warns(FixupWarning):
                    fixup(self.app)
                Fixup.assert_not_called()
            with mock.module_exists('django'):
                import django
                django.VERSION = (1, 11, 1)
                fixup(self.app)
                Fixup.assert_called()

    def test_maybe_close_fd(self):
        with patch('os.close'):
            _maybe_close_fd(Mock())
            _maybe_close_fd(object())

    def test_init(self):
        with self.fixup_context(self.app) as (f, importmod, sym):
            assert f

    def test_install(self, patching):
        self.app.loader = Mock()
        self.cw = patching('os.getcwd')
        self.p = patching('sys.path')
        self.sigs = patching('celery.fixups.django.signals')
        with self.fixup_context(self.app) as (f, _, _):
            self.cw.return_value = '/opt/vandelay'
            f.install()
            self.sigs.worker_init.connect.assert_called_with(f.on_worker_init)
            assert self.app.loader.now == f.now
            self.p.insert.assert_called_with(0, '/opt/vandelay')

    def test_now(self):
        with self.fixup_context(self.app) as (f, _, _):
            assert f.now(utc=True)
            f._now.assert_not_called()
            assert f.now(utc=False)
            f._now.assert_called()

    def test_on_worker_init(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch('celery.fixups.django.DjangoWorkerFixup') as DWF:
                f.on_worker_init()
                DWF.assert_called_with(f.app)
                DWF.return_value.install.assert_called_with()
                assert f._worker_fixup is DWF.return_value


class test_DjangoWorkerFixup(FixupCase):
    Fixup = DjangoWorkerFixup

    def test_init(self):
        with self.fixup_context(self.app) as (f, importmod, sym):
            assert f

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

    def test_on_worker_process_init(self, patching):
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
                        f._close_database.assert_called_with(force=True)

                        f.validate_models = Mock(name='validate_models')
                        patching.setenv('FORKED_BY_MULTIPROCESSING', '1')
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

            # when a task is eager, don't close connections
            with patch.object(f, 'close_cache'):
                task.request.is_eager = True
                with patch.object(f, 'close_database'):
                    f.on_task_postrun(task)
                    f.close_database.assert_not_called()
                    f.close_cache.assert_not_called()

    def test_close_database(self):
        with self.fixup_context(self.app) as (f, _, _):
            with patch.object(f, '_close_database') as _close:
                f.db_reuse_max = None
                f.close_database()
                _close.assert_called_with()
                _close.reset_mock()

                f.db_reuse_max = 10
                f._db_recycles = 3
                f.close_database()
                _close.assert_not_called()
                assert f._db_recycles == 4
                _close.reset_mock()

                f._db_recycles = 20
                f.close_database()
                _close.assert_called_with()
                assert f._db_recycles == 1

    def test__close_database(self):
        with self.fixup_context(self.app) as (f, _, _):
            conns = [Mock(), Mock(), Mock()]
            conns[1].close.side_effect = KeyError('already closed')
            f.DatabaseError = KeyError
            f.interface_errors = ()

            f._db.connections = Mock()  # ConnectionHandler
            f._db.connections.all.side_effect = lambda: conns

            f._close_database(force=True)
            conns[0].close.assert_called_with()
            conns[0].close_if_unusable_or_obsolete.assert_not_called()
            conns[1].close.assert_called_with()
            conns[1].close_if_unusable_or_obsolete.assert_not_called()
            conns[2].close.assert_called_with()
            conns[2].close_if_unusable_or_obsolete.assert_not_called()

            for conn in conns:
                conn.reset_mock()

            f._close_database()
            conns[0].close.assert_not_called()
            conns[0].close_if_unusable_or_obsolete.assert_called_with()
            conns[1].close.assert_not_called()
            conns[1].close_if_unusable_or_obsolete.assert_called_with()
            conns[2].close.assert_not_called()
            conns[2].close_if_unusable_or_obsolete.assert_called_with()

            conns[1].close.side_effect = KeyError(
                'omg')
            f._close_database()
            with pytest.raises(KeyError):
                f._close_database(force=True)

            conns[1].close.side_effect = None
            conns[1].close_if_unusable_or_obsolete.side_effect = KeyError(
                'omg')
            f._close_database(force=True)
            with pytest.raises(KeyError):
                f._close_database()

    def test_close_cache(self):
        with self.fixup_context(self.app) as (f, _, _):
            f.close_cache()
            f._cache.close_caches.assert_called_with()

    def test_on_worker_ready(self):
        with self.fixup_context(self.app) as (f, _, _):
            f._settings.DEBUG = False
            f.on_worker_ready()
            with pytest.warns(UserWarning):
                f._settings.DEBUG = True
                f.on_worker_ready()

    def test_validate_models(self, patching):
        with mock.module('django', 'django.db', 'django.core',
                         'django.core.cache', 'django.conf',
                         'django.db.utils'):
            f = self.Fixup(self.app)
            f.django_setup = Mock(name='django.setup')
            patching.modules('django.core.checks')
            from django.core.checks import run_checks
            f.validate_models()
            f.django_setup.assert_called_with()
            run_checks.assert_called_with()

    def test_django_setup(self, patching):
        patching('celery.fixups.django.symbol_by_name')
        patching('celery.fixups.django.import_module')
        django, = patching.modules('django')
        f = self.Fixup(self.app)
        f.django_setup()
        django.setup.assert_called_with()
