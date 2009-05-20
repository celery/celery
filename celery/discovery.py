"""celery.discovery"""
from django.conf import settings


def autodiscover():
    """Include tasks for all applications in :setting:`INSTALLED_APPS`."""
    return filter(None, [find_related_module(app, "tasks")
                            for app in settings.INSTALLED_APPS])


def find_related_module(app, related_name):
    """Given an application name and a module name, tries to find that
    module in the application, and running handler' if it finds it.
    """

    try:
        module = __import__(app, {}, {}, [related_name])
    except ImportError:
        return None

    try:
        related_module = getattr(module, related_name)
    except AttributeError:
        return None

    return related_module
