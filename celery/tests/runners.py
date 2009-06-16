from django.conf import settings
from django.test.simple import run_tests as django_test_runner


def run_tests(test_labels, verbosity=1, interactive=True, extra_tests=None,
        **kwargs):
    """ Test runner that only runs tests for the apps
    listed in ``settings.TEST_APPS``.
    """
    extra_tests = extra_tests or []
    app_labels = getattr(settings, "TEST_APPS", test_labels)
    return django_test_runner(app_labels,
                              verbosity=verbosity, interactive=interactive,
                              extra_tests=extra_tests, **kwargs)
