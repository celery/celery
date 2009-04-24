import imp
from django.conf import settings
from django.core import exceptions


def autodiscover():
    """Include tasks for all applications in settings.INSTALLED_APPS."""
    return filter(None, [tasks_for_app(app)
                            for app in settings.INSTALLED_APPS])


def tasks_for_app(app):
    """Given an application name, imports any tasks.py file for that app."""

    def found_tasks_module_handler(app_path, app_basename):
        return __import__("%s.tasks" % app)

    return find_related_module(app, "tasks", found_tasks_module_handler)


def find_related_module(app, related_name, handler):
    """Given an application name and a module name, tries to find that
    module in the application, and running handler' if it finds it.
    """

    # See django.contrib.admin.autodiscover for an explanation of this code.
    try:
        app_basename = app.split('.')[-1]
        app_path = __import__(app, {}, {}, app_basename).__path__
    except AttributeError:
        return None

    try:
        imp.find_module(related_name, app_path)
    except ImportError:
        return None

    return handler(app_path, app_basename)
