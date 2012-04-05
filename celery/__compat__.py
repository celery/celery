from __future__ import absolute_import

import sys

from types import ModuleType

from .local import Proxy

MODULE_DEPRECATED = """
The module %s is deprecated and will be removed in a future version.
"""


def _compat_task_decorator(*args, **kwargs):
    from celery import current_app
    kwargs.setdefault("accept_magic_kwargs", True)
    return current_app.task(*args, **kwargs)


def _compat_periodic_task_decorator(*args, **kwargs):
    from celery.task import periodic_task
    kwargs.setdefault("accept_magic_kwargs", True)
    return periodic_task(*args, **kwargs)


modules = {
    "celery": {
        "decorators": {
            "task": _compat_task_decorator,
            "periodic_task": _compat_periodic_task_decorator,
        },
        "log": {
            "get_default_logger": "log.get_default_logger",
            "setup_logger": "log.setup_logger",
            "setup_task_logger": "log.setup_task_logger",
            "get_task_logger": "log.get_task_logger",
            "setup_loggig_subsystem": "log.setup_logging_subsystem",
            "redirect_stdouts_to_logger": "log.redirect_stdouts_to_logger",
        },
        "messaging": {
            "TaskPublisher": "amqp.TaskPublisher",
            "ConsumerSet": "amqp.ConsumerSet",
            "TaskConsumer": "amqp.TaskConsumer",
            "establish_connection": "broker_connection",
            "with_connection": "with_default_connection",
            "get_consumer_set": "amqp.get_task_consumer"
        },
        "registry": {
            "tasks": "tasks",
        },
    },
}


def rgetattr(obj, path):
    return reduce(lambda a, b: getattr(a, b), [obj] + path)


def get_compat(app, pkg, name, bases=(ModuleType, )):
    from warnings import warn
    from .exceptions import CDeprecationWarning

    fqdn = '.'.join([pkg.__name__, name])
    warn(CDeprecationWarning(MODULE_DEPRECATED % fqdn))

    def build_attr(attr):
        if isinstance(attr, basestring):
            return Proxy(rgetattr, (app, attr.split('.')))
        return attr
    attrs = dict((name, build_attr(attr))
                    for name, attr in modules[pkg.__name__][name].iteritems())
    sys.modules[fqdn] = module = type(name, bases, attrs)(fqdn)
    return module


class class_property(object):

    def __init__(self, fget=None, fset=None):
        assert fget and isinstance(fget, classmethod)
        assert fset and isinstance(fset, classmethod)
        self.__get = fget
        self.__set = fset

        info = fget.__get__(object)  # just need the info attrs.
        self.__doc__ = info.__doc__
        self.__name__ = info.__name__
        self.__module__ = info.__module__

    def __get__(self, obj, type=None):
        if obj and type is None:
            type = obj.__class__
        return self.__get.__get__(obj, type)()

    def __set__(self, obj, value):
        if obj is None:
            return self
        return self.__set.__get__(obj)(value)
