# -*- coding: utf-8 -*-
"""
    celery.five
    ~~~~~~~~~~~

    Compatibility implementations of features
    only available in newer Python versions.


"""
from __future__ import absolute_import, unicode_literals

import operator
import sys

from importlib import import_module
from types import ModuleType

# extends vine.five
from vine import five
from vine.five import *  # noqa
from vine.five import __all__ as _all_five

# bloody flake8
items = five.items
bytes_if_py2 = five.bytes_if_py2
string_t = five.string_t

try:
    from functools import reduce
except ImportError:
    pass

__all__ = [
    'class_property', 'reclassmethod', 'create_module', 'recreate_module',
]
__all__ += _all_five

#  ############# Module Generation ##########################

# Utilities to dynamically
# recreate modules, either for lazy loading or
# to create old modules at runtime instead of
# having them litter the source tree.

# import fails in python 2.5. fallback to reduce in stdlib

MODULE_DEPRECATED = """
The module %s is deprecated and will be removed in a future version.
"""

DEFAULT_ATTRS = {'__file__', '__path__', '__doc__', '__all__'}

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


def _compat_periodic_task_decorator(*args, **kwargs):
    from celery.task import periodic_task
    return periodic_task(*args, **kwargs)

COMPAT_MODULES = {
    'celery': {
        'execute': {
            'send_task': 'send_task',
        },
        'decorators': {
            'task': 'task',
            'periodic_task': _compat_periodic_task_decorator,
        },
        'log': {
            'get_default_logger': 'log.get_default_logger',
            'setup_logger': 'log.setup_logger',
            'setup_logging_subsystem': 'log.setup_logging_subsystem',
            'redirect_stdouts_to_logger': 'log.redirect_stdouts_to_logger',
        },
        'messaging': {
            'TaskConsumer': 'amqp.TaskConsumer',
            'establish_connection': 'connection',
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

    def __init__(self, getter=None, setter=None):
        if getter is not None and not isinstance(getter, classmethod):
            getter = classmethod(getter)
        if setter is not None and not isinstance(setter, classmethod):
            setter = classmethod(setter)
        self.__get = getter
        self.__set = setter

        info = getter.__get__(object)  # just need the info attrs.
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

    def setter(self, setter):
        return self.__class__(self.__get, setter)


def reclassmethod(method):
    return classmethod(fun_of_method(method))


class LazyModule(ModuleType):
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
        elif name in self._direct:  # pragma: no cover
            module = __import__(self._direct[name], None, None, [name])
            setattr(self, name, module)
            return module
        return ModuleType.__getattribute__(self, name)

    def __dir__(self):
        return list(set(self.__all__) | DEFAULT_ATTRS)

    def __reduce__(self):
        return import_module, (self.__name__,)


def create_module(name, attrs, cls_attrs=None, pkg=None,
                  base=LazyModule, prepare_attr=None):
    fqdn = '.'.join([pkg.__name__, name]) if pkg else name
    cls_attrs = {} if cls_attrs is None else cls_attrs
    pkg, _, modname = name.rpartition('.')
    cls_attrs['__module__'] = pkg

    attrs = {
        attr_name: (prepare_attr(attr) if prepare_attr else attr)
        for attr_name, attr in items(attrs)
    }
    module = sys.modules[fqdn] = type(
        bytes_if_py2(modname), (base,), cls_attrs)(bytes_if_py2(name))
    module.__dict__.update(attrs)
    return module


def recreate_module(name, compat_modules=(), by_module={}, direct={},
                    base=LazyModule, **attrs):
    old_module = sys.modules[name]
    origins = get_origins(by_module)
    compat_modules = COMPAT_MODULES.get(name, ())

    _all = tuple(set(reduce(
        operator.add,
        [tuple(v) for v in [compat_modules, origins, direct, attrs]],
    )))
    if sys.version_info[0] < 3:
        _all = [s.encode() for s in _all]
    cattrs = dict(
        _compat_modules=compat_modules,
        _all_by_module=by_module, _direct=direct,
        _object_origins=origins,
        __all__=_all,
    )
    new_module = create_module(name, attrs, cls_attrs=cattrs, base=base)
    new_module.__dict__.update({
        mod: get_compat_module(new_module, mod) for mod in compat_modules
    })
    return old_module, new_module


def get_compat_module(pkg, name):
    from .local import Proxy

    def prepare(attr):
        if isinstance(attr, string_t):
            return Proxy(getappattr, (attr,))
        return attr

    attrs = COMPAT_MODULES[pkg.__name__][name]
    if isinstance(attrs, string_t):
        fqdn = '.'.join([pkg.__name__, name])
        module = sys.modules[fqdn] = import_module(attrs)
        return module
    attrs['__all__'] = list(attrs)
    return create_module(name, dict(attrs), pkg=pkg, prepare_attr=prepare)


def get_origins(defs):
    origins = {}
    for module, attrs in items(defs):
        origins.update({attr: module for attr in attrs})
    return origins
