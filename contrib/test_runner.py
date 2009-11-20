from django.conf import settings
from django.test.simple import run_tests as run_tests_orig

USAGE = """\
Custom test runner to allow testing of celery .delay() tasks.
"""

def run_tests(test_labels, *args, **kwargs):
    settings.CELERY_ALWAYS_EAGER = True
    return run_tests_orig(test_labels, *args, **kwargs)
