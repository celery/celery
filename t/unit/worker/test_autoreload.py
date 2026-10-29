import os
import sys
import time
from unittest.mock import Mock, patch

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
        assert reloader.find_changes() == []  # baseline
        assert reloader.find_changes() == []
        touch(project_module)
        assert reloader.find_changes() == [os.path.realpath(project_module)]
        assert reloader.find_changes() == []

    def test_deleted_file_is_ignored(self, project_module):
        reloader = Autoreloader(Mock())
        reloader.find_changes()
        project_module.unlink()
        assert reloader.find_changes() == []

    def test_run_calls_on_change_once(self, project_module):
        on_change = Mock()
        reloader = Autoreloader(on_change, interval=0.01)
        reloader.find_changes()
        touch(project_module)
        reloader.start()
        reloader.join(timeout=5)
        assert not reloader.is_alive()
        on_change.assert_called_once_with(os.path.realpath(project_module))

    @pytest.mark.parametrize('first_scan', [[], OSError('boom')])
    def test_run_keeps_scanning_until_a_change(self, first_scan):
        on_change = Mock()
        reloader = Autoreloader(on_change, interval=0.01)
        reloader.find_changes = Mock(side_effect=[first_scan, ['/mod.py']])
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
        assert reloader.find_changes() == [os.path.join(
            os.path.realpath(lazy.parent), lazy.name)]

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
        assert Autoreloader._check_source(str(tmp_path / 'gone.py')) is None

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

    def test_reports_every_changed_file(self, project_module):
        other = project_module.parent / 'other.py'
        other.write_text('X = 1\n')
        reloader = Autoreloader(Mock())
        reloader.find_changes()
        touch(project_module)
        touch(other)
        assert sorted(reloader.find_changes()) == sorted([
            os.path.realpath(project_module),
            os.path.join(os.path.realpath(other.parent), other.name),
        ])

    def test_does_not_restart_if_any_changed_file_is_broken(self, tmp_path):
        good, bad = str(tmp_path / 'good.py'), str(tmp_path / 'bad.py')
        open(good, 'w').write('X = 1\n')
        open(bad, 'w').write('def broken(:\n')
        on_change = Mock()
        reloader = Autoreloader(on_change, interval=0.01)
        reloader._mtimes['/seen.py'] = 1  # skip the baseline scan
        # both change together, then only the good one changes again: the
        # bad one must still block the restart until it's fixed.
        scans = [[good, bad], [good], [bad]]
        reloader.find_changes = Mock(
            side_effect=lambda: scans.pop(0) if scans else [])
        reloader.start()
        time.sleep(0.2)
        on_change.assert_not_called()
        assert reloader._broken == {bad}
        open(bad, 'w').write('X = 2\n')
        scans.append([bad])
        reloader.join(timeout=5)
        assert not reloader.is_alive()
        on_change.assert_called_once_with(bad)

    def test_baseline_scan_error_does_not_kill_the_thread(self):
        on_change = Mock()
        reloader = Autoreloader(on_change, interval=0.01)
        reloader.find_changes = Mock(
            side_effect=[TypeError('bad __file__'), [], ['/mod.py']])
        reloader._check_source = Mock(return_value=None)
        reloader.start()
        reloader.join(timeout=5)
        on_change.assert_called_once_with('/mod.py')

    def test_repeated_scan_error_is_logged_once(self):
        reloader = Autoreloader(Mock())
        reloader.find_changes = Mock(side_effect=OSError('boom'))
        with patch('celery.worker.autoreload.logger') as logger:
            for _ in range(3):
                assert reloader._scan() == []
            assert logger.exception.call_count == 1
            reloader.find_changes = Mock(return_value=[])
            reloader._scan()
            reloader.find_changes = Mock(side_effect=OSError('boom'))
            reloader._scan()
            assert logger.exception.call_count == 2

    def test_no_restart_once_stopped(self):
        on_change = Mock()
        reloader = Autoreloader(on_change, interval=0.01)
        reloader._mtimes['/seen.py'] = 1  # skip the baseline scan

        scans = iter([[], ['/mod.py']])

        def scan():
            changed = next(scans)
            if changed:
                reloader.stop()  # shutdown starts while scanning
            return changed
        reloader.find_changes = Mock(side_effect=scan)
        reloader._check_source = Mock(return_value=None)
        # the stop event is only checked between scans, so call run() here.
        reloader._stopped.wait = Mock(return_value=False)
        reloader.run()
        on_change.assert_not_called()

    def test_stop(self):
        reloader = Autoreloader(Mock(), interval=0.01)
        reloader.start()
        reloader.stop()
        reloader.join(timeout=5)
        assert not reloader.is_alive()
