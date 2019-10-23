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
