from django.conf import settings
from django.test.simple import run_tests as run_tests_orig

USAGE = """\
Custom test runner to allow testing of celery delayed tasks.
"""

def run_tests(test_labels, *args, **kwargs):
    """Django test runner allowing testing of celery delayed tasks.

    All tasks are run locally, not in a worker.

    To use this runner set ``settings.TEST_RUNNER``::

        TEST_RUNNER = "celery.contrib.test_runner.run_tests"

    """
    settings.CELERY_ALWAYS_EAGER = True
    return run_tests_orig(test_labels, *args, **kwargs)
