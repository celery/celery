import os
import sys
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

    def test_stop(self):
        reloader = Autoreloader(Mock(), interval=0.01)
        reloader.start()
        reloader.stop()
        reloader.join(timeout=5)
        assert not reloader.is_alive()
