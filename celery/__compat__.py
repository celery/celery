# -*- coding: utf-8 -*-
"""
    celery.__compat__
    ~~~~~~~~~~~~~~~~~

    This module contains utilities to dynamically
    recreate modules, either for lazy loading or
    to create old modules at runtime instead of
    having them litter the source tree.

"""
from __future__ import absolute_import

import operator
import sys

from functools import reduce
from importlib import import_module
from types import ModuleType

from .local import Proxy

MODULE_DEPRECATED = """
The module %s is deprecated and will be removed in a future version.
"""

DEFAULT_ATTRS = set(['__file__', '__path__', '__doc__', '__all__'])

# im_func is no longer available in Py3.
# instead the unbound method itself can be used.
if sys.version_info[0] == 3:  # pragma: no cover
    def fun_of_method(method):
        return method
else:
    def fun_of_method(method):  # noqa
        return method.im_func


def getappattr(path):
    """Gets attribute from the current_app recursively,
    e.g. getappattr('amqp.get_task_consumer')``."""
    from celery import current_app
    return current_app._rgetattr(path)


def _compat_task_decorator(*args, **kwargs):
    from celery import current_app
    kwargs.setdefault('accept_magic_kwargs', True)
    return current_app.task(*args, **kwargs)


def _compat_periodic_task_decorator(*args, **kwargs):
    from celery.task import periodic_task
    kwargs.setdefault('accept_magic_kwargs', True)
    return periodic_task(*args, **kwargs)


COMPAT_MODULES = {
    'celery': {
        'execute': {
            'send_task': 'send_task',
        },
        'decorators': {
            'task': _compat_task_decorator,
            'periodic_task': _compat_periodic_task_decorator,
        },
        'log': {
            'get_default_logger': 'log.get_default_logger',
            'setup_logger': 'log.setup_logger',
            'setup_loggig_subsystem': 'log.setup_logging_subsystem',
            'redirect_stdouts_to_logger': 'log.redirect_stdouts_to_logger',
        },
        'messaging': {
            'TaskPublisher': 'amqp.TaskPublisher',
            'TaskConsumer': 'amqp.TaskConsumer',
            'establish_connection': 'connection',
            'with_connection': 'with_default_connection',
            'get_consumer_set': 'amqp.TaskConsumer',
        },
        'registry': {
            'tasks': 'tasks',
        },
    },
    'celery.task': {
        'control': {
            'broadcast': 'control.broadcast',
            'rate_limit': 'control.rate_limit',
            'time_limit': 'control.time_limit',
            'ping': 'control.ping',
            'revoke': 'control.revoke',
            'discard_all': 'control.purge',
            'inspect': 'control.inspect',
        },
        'schedules': 'celery.schedules',
        'chords': 'celery.canvas',
    }
}


class class_property(object):

    def __init__(self, fget=None, fset=None):
        assert fget and isinstance(fget, classmethod)
        assert isinstance(fset, classmethod) if fset else True
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


def reclassmethod(method):
    return classmethod(fun_of_method(method))


class MagicModule(ModuleType):
    _compat_modules = ()
    _all_by_module = {}
    _direct = {}
    _object_origins = {}

    def __getattr__(self, name):
        if name in self._object_origins:
            module = __import__(self._object_origins[name], None, None, [name])
            for item in self._all_by_module[module.__name__]:
                setattr(self, item, getattr(module, item))
            return getattr(module, name)
        elif name in self._direct:
            module = __import__(self._direct[name], None, None, [name])
            setattr(self, name, module)
            return module
        return ModuleType.__getattribute__(self, name)

    def __dir__(self):
        return list(set(self.__all__) | DEFAULT_ATTRS)


def create_module(name, attrs, cls_attrs=None, pkg=None,
                  base=MagicModule, prepare_attr=None):
    fqdn = '.'.join([pkg.__name__, name]) if pkg else name
    cls_attrs = {} if cls_attrs is None else cls_attrs

    attrs = dict((attr_name, prepare_attr(attr) if prepare_attr else attr)
                 for attr_name, attr in attrs.iteritems())
    module = sys.modules[fqdn] = type(name, (base, ), cls_attrs)(fqdn)
    module.__dict__.update(attrs)
    return module


def recreate_module(name, compat_modules=(), by_module={}, direct={},
                    base=MagicModule, **attrs):
    old_module = sys.modules[name]
    origins = get_origins(by_module)
    compat_modules = COMPAT_MODULES.get(name, ())

    cattrs = dict(
        _compat_modules=compat_modules,
        _all_by_module=by_module, _direct=direct,
        _object_origins=origins,
        __all__=tuple(set(reduce(
            operator.add,
            [tuple(v) for v in [compat_modules, origins, direct, attrs]],
        ))),
    )
    new_module = create_module(name, attrs, cls_attrs=cattrs, base=base)
    new_module.__dict__.update(dict((mod, get_compat_module(new_module, mod))
                               for mod in compat_modules))
    return old_module, new_module


def get_compat_module(pkg, name):

    def prepare(attr):
        if isinstance(attr, basestring):
            return Proxy(getappattr, (attr, ))
        return attr

    attrs = COMPAT_MODULES[pkg.__name__][name]
    if isinstance(attrs, basestring):
        fqdn = '.'.join([pkg.__name__, name])
        module = sys.modules[fqdn] = import_module(attrs)
        return module
    attrs['__all__'] = list(attrs)
    return create_module(name, dict(attrs), pkg=pkg, prepare_attr=prepare)


def get_origins(defs):
    origins = {}
    for module, items in defs.iteritems():
        origins.update(dict((item, module) for item in items))
    return origins
