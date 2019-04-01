# -*- coding: utf-8 -*-
"""Abstract classes."""
from __future__ import absolute_import, unicode_literals

from abc import ABCMeta, abstractmethod, abstractproperty

from celery.five import with_metaclass

try:
    from collections.abc import Callable
except ImportError:
    # TODO: Remove this when we drop Python 2.7 support
    from collections import Callable


__all__ = ('CallableTask', 'CallableSignature')


def _hasattr(C, attr):
    return any(attr in B.__dict__ for B in C.__mro__)


@with_metaclass(ABCMeta)
class _AbstractClass(object):
    __required_attributes__ = frozenset()

    @classmethod
    def _subclasshook_using(cls, parent, C):
        return (
            cls is parent and
            all(_hasattr(C, attr) for attr in cls.__required_attributes__)
        ) or NotImplemented

    @classmethod
    def register(cls, other):
        # we override `register` to return other for use as a decorator.
        type(cls).register(cls, other)
        return other


class CallableTask(_AbstractClass, Callable):  # pragma: no cover
    """Task interface."""

    __required_attributes__ = frozenset({
        'delay', 'apply_async', 'apply',
    })

    @abstractmethod
    def delay(self, *args, **kwargs):
        pass

    @abstractmethod
    def apply_async(self, *args, **kwargs):
        pass

    @abstractmethod
    def apply(self, *args, **kwargs):
        pass

    @classmethod
    def __subclasshook__(cls, C):
        return cls._subclasshook_using(CallableTask, C)


class CallableSignature(CallableTask):  # pragma: no cover
    """Celery Signature interface."""

    __required_attributes__ = frozenset({
        'clone', 'freeze', 'set', 'link', 'link_error', '__or__',
    })

    @abstractproperty
    def name(self):
        pass

    @abstractproperty
    def type(self):
        pass

    @abstractproperty
    def app(self):
        pass

    @abstractproperty
    def id(self):
        pass

    @abstractproperty
    def task(self):
        pass

    @abstractproperty
    def args(self):
        pass

    @abstractproperty
    def kwargs(self):
        pass

    @abstractproperty
    def options(self):
        pass

    @abstractproperty
    def subtask_type(self):
        pass

    @abstractproperty
    def chord_size(self):
        pass

    @abstractproperty
    def immutable(self):
        pass

    @abstractmethod
    def clone(self, args=None, kwargs=None):
        pass

    @abstractmethod
    def freeze(self, id=None, group_id=None, chord=None, root_id=None):
        pass

    @abstractmethod
    def set(self, immutable=None, **options):
        pass

    @abstractmethod
    def link(self, callback):
        pass

    @abstractmethod
    def link_error(self, errback):
        pass

    @abstractmethod
    def __or__(self, other):
        pass

    @abstractmethod
    def __invert__(self):
        pass

    @classmethod
    def __subclasshook__(cls, C):
        return cls._subclasshook_using(CallableSignature, C)
