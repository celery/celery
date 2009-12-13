"""celery.discovery"""
import imp
import importlib
_RACE_PROTECTION = False


def autodiscover():
    """Include tasks for all applications in :setting:`INSTALLED_APPS`."""
    from django.conf import settings
    global _RACE_PROTECTION

    if _RACE_PROTECTION:
        return
    _RACE_PROTECTION = True
    try:
        return filter(None, [find_related_module(app, "tasks")
                                for app in settings.INSTALLED_APPS])
    finally:
        _RACE_PROTECTION = False


def find_related_module(app, related_name):
    """Given an application name and a module name, tries to find that
    module in the application."""

    try:
        app_path = importlib.import_module(app).__path__
    except AttributeError:
        return

    try:
        imp.find_module(related_name, app_path)
    except ImportError:
        return

    module = importlib.import_module("%s.%s" % (app, related_name))

    try:
        return getattr(module, related_name)
    except AttributeError:
        return
