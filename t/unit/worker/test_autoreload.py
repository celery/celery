import os
import sys
import time
from unittest.mock import Mock

import pytest

from celery.worker.autoreload import Autoreloader


@pytest.fixture
def project_module(tmp_path, monkeypatch):
    """A module outside of stdlib/site-packages, imported by the 'worker'."""
    src = tmp_path / 'autoreload_dep.py'
    src.write_text('VALUE = 1\n')
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, 'autoreload_dep', raising=False)
    import autoreload_dep  # noqa: F401
    yield src
    sys.modules.pop('autoreload_dep', None)


def touch(path):
    st = os.stat(path)
    os.utime(path, (st.st_atime, st.st_mtime + 10))


class test_Autoreloader:

    def test_watches_imported_project_modules(self, project_module):
        files = Autoreloader(Mock()).module_files()
        assert os.path.realpath(project_module) in files

    def test_ignores_stdlib_and_site_packages(self):
        files = Autoreloader(Mock()).module_files()
        assert os.path.realpath(os.__file__) not in files

    def test_detects_change_in_dependency(self, project_module):
        reloader = Autoreloader(Mock())
        assert reloader.find_changes() is None  # baseline
        assert reloader.find_changes() is None
        touch(project_module)
        assert reloader.find_changes() == os.path.realpath(project_module)
        assert reloader.find_changes() is None

    def test_deleted_file_is_ignored(self, project_module):
        reloader = Autoreloader(Mock())
        reloader.find_changes()
        project_module.unlink()
        assert reloader.find_changes() is None

    def test_run_calls_on_change_once(self, project_module):
        on_change = Mock()
        reloader = Autoreloader(on_change, interval=0.01)
        reloader.find_changes()
        touch(project_module)
        reloader.start()
        reloader.join(timeout=5)
        assert not reloader.is_alive()
        on_change.assert_called_once_with(os.path.realpath(project_module))

    @pytest.mark.parametrize('first_scan', [None, OSError('boom')])
    def test_run_keeps_scanning_until_a_change(self, first_scan):
        on_change = Mock()
        reloader = Autoreloader(on_change, interval=0.01)
        reloader.find_changes = Mock(side_effect=[first_scan, '/mod.py'])
        reloader._check_source = Mock(return_value=None)
        reloader._mtimes['/seen.py'] = 1  # skip the baseline scan
        reloader.start()
        reloader.join(timeout=5)
        assert not reloader.is_alive()
        on_change.assert_called_once_with('/mod.py')

    def test_watches_unimported_files_next_to_modules(self, project_module):
        pkg = project_module.parent
        lazy = pkg / 'lazy_only_in_task.py'
        lazy.write_text('X = 1\n')
        sub = pkg / 'sub'
        sub.mkdir()
        (sub / 'deep.py').write_text('X = 1\n')
        reloader = Autoreloader(Mock())
        files = reloader.project_files()
        assert os.path.realpath(lazy) in files
        assert os.path.join(os.path.realpath(sub), 'deep.py') in files

    def test_detects_change_in_unimported_file(self, project_module):
        lazy = project_module.parent / 'lazy_only_in_task.py'
        lazy.write_text('X = 1\n')
        reloader = Autoreloader(Mock())
        reloader.find_changes()
        touch(lazy)
        assert reloader.find_changes() == os.path.join(
            os.path.realpath(lazy.parent), lazy.name)

    def test_skips_hidden_cache_and_virtualenv_dirs(self, project_module):
        pkg = project_module.parent
        for name in ('.git', '__pycache__', 'node_modules', 'venv', 'ok'):
            (pkg / name).mkdir(exist_ok=True)
            (pkg / name / 'mod.py').write_text('X = 1\n')
        (pkg / 'venv' / 'pyvenv.cfg').write_text('home = /usr/bin\n')
        files = Autoreloader(Mock()).project_files()
        real = os.path.realpath(pkg)
        assert os.path.join(real, 'ok', 'mod.py') in files
        for name in ('.git', '__pycache__', 'node_modules', 'venv'):
            assert os.path.join(real, name, 'mod.py') not in files

    def test_skips_dirs_under_ignored_prefixes(self, project_module):
        pkg = os.path.realpath(project_module.parent)
        site = project_module.parent / 'site'
        site.mkdir()
        (site / 'mod.py').write_text('X = 1\n')
        reloader = Autoreloader(Mock())
        reloader._ignored = (os.path.join(pkg, 'site') + os.sep,)
        assert os.path.join(pkg, 'site', 'mod.py') not in reloader.project_files()

    def test_roots_are_not_nested(self):
        reloader = Autoreloader(Mock())
        roots = reloader._roots(['/a/x.py', '/a/b/y.py', '/a-b/z.py'])
        assert roots == ['/a', '/a-b']

    def test_does_not_walk_home_or_filesystem_root(self):
        reloader = Autoreloader(Mock())
        home = os.path.realpath(os.path.expanduser('~'))
        assert reloader._roots(
            [os.path.join(home, 'tasks.py'), '/tasks.py']) == []

    def test_check_source(self, tmp_path):
        good = tmp_path / 'good.py'
        good.write_text('X = 1\n')
        bad = tmp_path / 'bad.py'
        bad.write_text('def broken(:\n')
        assert Autoreloader._check_source(str(good)) is None
        assert isinstance(Autoreloader._check_source(str(bad)), SyntaxError)
        assert isinstance(
            Autoreloader._check_source(str(tmp_path / 'gone.py')), OSError)

    def test_does_not_restart_on_syntax_error(self, project_module):
        on_change = Mock()
        reloader = Autoreloader(on_change, interval=0.01)
        reloader.find_changes()
        project_module.write_text('def broken(:\n')
        touch(project_module)
        reloader.start()
        time.sleep(0.2)  # several scans while the file is broken
        on_change.assert_not_called()
        assert reloader.is_alive()
        project_module.write_text('VALUE = 2\n')
        touch(project_module)
        touch(project_module)
        reloader.join(timeout=5)
        assert not reloader.is_alive()
        on_change.assert_called_once_with(os.path.realpath(project_module))

    def test_stop(self):
        reloader = Autoreloader(Mock(), interval=0.01)
        reloader.start()
        reloader.stop()
        reloader.join(timeout=5)
        assert not reloader.is_alive()
