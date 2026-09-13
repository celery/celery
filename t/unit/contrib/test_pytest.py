import pytest

pytest_plugins = ["pytester"]

try:
    pytest.fail()
except BaseException as e:
    Failed = type(e)


@pytest.mark.skipif(
    not hasattr(pytest, "PytestUnknownMarkWarning"),
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
    with pytest.raises((ValueError, Failed)):
        result.stdout.fnmatch_lines_random(
            "*PytestUnknownMarkWarning: Unknown pytest.mark.celery*"
        )
