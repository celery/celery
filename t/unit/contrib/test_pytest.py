import importlib
import sys
import pytest

try:
    from pytest import PytestUnknownMarkWarning  # noqa: F401

    pytest_marker_warnings = True
except ImportError:
    pytest_marker_warnings = False


pytest_plugins = ["pytester"]


@pytest.mark.skipif(
    not pytest_marker_warnings,
    reason="Older pytest version without marker warnings",
)
def test_pytest_celery_marker_registration(testdir):
    """Verify that using the 'celery' marker does not result in a warning"""
    testdir.plugins.append("celery")
    testdir.makepyfile(
        """
        import pytest
        @pytest.mark.celery(foo="bar")
        def test_noop():
            pass
        """
    )

    result = testdir.runpytest('-q')
    with pytest.raises(ValueError):
        result.stdout.fnmatch_lines_random(
            "*PytestUnknownMarkWarning: Unknown pytest.mark.celery*"
        )


@pytest.fixture
def fixed_import(mocker):
    def import_module(module, package=None):
        want_reload_module = sys.modules.get(module, None)
        if want_reload_module:
            importlib.reload(want_reload_module)
        return importlib.import_module(module, package=package)

    mocker.patch('celery.loaders.base.BaseLoader.import_module', side_effect=import_module)


@pytest.fixture
def celery_includes():
    return [
        't.unit.contrib.proj.foo'
    ]


def test_pytest_import_passes(celery_app, celery_worker):
    task = celery_app.send_task("t.unit.contrib.proj.foo.bar")
    task.get()


def test_pytest_second_import_fails_without_patched_module(celery_app, celery_worker):
    task = celery_app.send_task("t.unit.contrib.proj.foo.bar")
    with pytest.raises(KeyError):
        task.get()


def test_pytest_second_import_passes(fixed_import, celery_app, celery_worker):
    task = celery_app.send_task("t.unit.contrib.proj.foo.bar")
    task.get()
